# -*- coding: utf-8 -*-
"""Bản đồ "chấm mực" học đường bay từ các fix radar thật.

Module này không gọi FR24 và không phụ thuộc Flask/Firestore. Máy fetcher gửi toàn bộ
``details.trail`` lên server online; server dùng các điểm thật để tạo một đồ thị có
hướng, giữ riêng các nhánh có hướng khác nhau. Điểm do web dự đoán tuyệt đối không đi
vào đây.

Mục tiêu thiết kế:
  * cùng một đường được bay nhiều lần -> support tăng, tâm đường và tốc độ hội tụ;
  * hai nhánh giao/cắt nhau -> không bị lấy trung bình thành một đường giả;
  * trước khi được quyền lái marker, atlas phải tự chứng minh tốt hơn bay thẳng bằng
    các fix radar đến sau (đánh giá out-of-sample theo từng vết mới);
  * dữ liệu xuất được chia shard để không chạm giới hạn 1 MiB/document Firestore.
"""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
import math
import sqlite3
import threading
import time
from typing import Any, Iterable, Optional


EARTH_R_M = 6_371_000.0
DAD_LAT = 16.0439
DAD_LNG = 108.1995
SCHEMA = "nkdt-radar-ink-v1"
VERSION = 2

# Bốn tim băng tách riêng theo WGS-84 trong AIP. Heading ở đây là true track
# (ADS-B/FR24), không dùng số hiệu từ 170/350 đã làm tròn. Atlas cần biết L/R để
# một vệt final 35L không kéo tàu ngang sang 35R ở những giây cuối.
RUNWAYS = {
    "17L": {"lat": 16.060372222, "lng": 108.198000000, "bearing": 172.01},
    "35R": {"lat": 16.029047222, "lng": 108.202547222, "bearing": 352.01},
    "17R": {"lat": 16.057416667, "lng": 108.196411111, "bearing": 172.01},
    "35L": {"lat": 16.030133333, "lng": 108.200372222, "bearing": 352.01},
}

PHASE_SHORT = {
    "ARRIVAL_ENROUTE": "E",
    "DESCENT": "D",
    "TRANSITION": "T",
    "INTERCEPT": "I",
    "FINAL": "F",
    "ROLLOUT": "R",
    "GROUND": "G",
    "DEPARTURE": "P",
    "OVERFLIGHT": "O",
    "GO_AROUND": "A",
}
SHORT_PHASE = {value: key for key, value in PHASE_SHORT.items()}

# Mỗi cấp là một lớp mạng riêng. Heading-bin tiếp tục tách hai luồng ngược chiều
# hoặc hai nhánh cắt nhau trong cùng một ô.
LEVELS = {
    0: {"name": "GROUND", "cell_m": 40.0, "heading_bin_deg": 20.0},
    1: {"name": "TERMINAL", "cell_m": 180.0, "heading_bin_deg": 15.0},
    2: {"name": "REGIONAL", "cell_m": 700.0, "heading_bin_deg": 15.0},
    3: {"name": "ENROUTE", "cell_m": 2500.0, "heading_bin_deg": 15.0},
}

MIN_EDGE_SUPPORT = 3
MIN_EVAL_SAMPLES = 20
MIN_MAP_IMPROVEMENT = 0.10
MAX_NODES = 120_000
MAX_OUT_EDGES = 8
MAX_ORIGINS_PER_EDGE = 8
MAX_TRACK_HASHES_PER_EDGE = 32
MAX_TRACK_STATES = 1500
MAX_CONTEXTS_PER_EDGE = 12


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _norm360(value: float) -> float:
    return float(value) % 360.0


def _angle_diff(a: float, b: float) -> float:
    return (float(b) - float(a) + 540.0) % 360.0 - 180.0


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
    return 2.0 * EARTH_R_M * math.asin(math.sqrt(max(0.0, min(1.0, a))))


def _bearing(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dl = math.radians(lng2 - lng1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return _norm360(math.degrees(math.atan2(y, x)))


def _project(lat: float, lng: float) -> tuple[float, float]:
    """Web-Mercator mét; đủ ổn định trong vùng khai thác của các chuyến về DAD."""
    safe_lat = _clamp(float(lat), -85.0, 85.0)
    x = EARTH_R_M * math.radians(float(lng))
    y = EARTH_R_M * math.log(math.tan(math.pi / 4.0 + math.radians(safe_lat) / 2.0))
    return x, y


def _destination_point(lat: float, lng: float, bearing_deg: float, distance_m: float) -> tuple[float, float]:
    br = math.radians(bearing_deg)
    p1 = math.radians(lat)
    l1 = math.radians(lng)
    d = distance_m / EARTH_R_M
    p2 = math.asin(math.sin(p1) * math.cos(d) + math.cos(p1) * math.sin(d) * math.cos(br))
    l2 = l1 + math.atan2(math.sin(br) * math.sin(d) * math.cos(p1), math.cos(d) - math.sin(p1) * math.sin(p2))
    return math.degrees(p2), (math.degrees(l2) + 540.0) % 360.0 - 180.0


def _along_cross(lat: float, lng: float, runway_id: str) -> tuple[float, float]:
    """Chiếu lên tim băng: along tăng theo chiều hạ, cross dương về bên phải."""
    runway = RUNWAYS.get(str(runway_id or "").upper())
    if not runway:
        return 0.0, 0.0
    d_n = (float(lat) - runway["lat"]) * 111_320.0
    d_e = (float(lng) - runway["lng"]) * 111_320.0 * math.cos(
        math.radians((float(lat) + runway["lat"]) / 2.0)
    )
    rad = math.radians(runway["bearing"])
    a_n, a_e = math.cos(rad), math.sin(rad)
    return d_n * a_n + d_e * a_e, -d_n * a_e + d_e * a_n


def _phase_key(movement: str, runway_id: str, phase: str) -> str:
    """Khóa ngắn để hàng triệu edge vẫn nằm dưới giới hạn Firestore."""
    movement_short = {"ARRIVAL": "A", "DEPARTURE": "P", "OVERFLIGHT": "O", "GO_AROUND": "G"}.get(
        str(movement or "").upper(), "U"
    )
    runway_short = str(runway_id or "*").upper() if str(runway_id or "").upper() in RUNWAYS else "*"
    return f"{movement_short}|{runway_short}|{PHASE_SHORT.get(str(phase or '').upper(), 'U')}"


def _parse_phase_key(key: str) -> tuple[str, str, str]:
    parts = str(key or "").split("|")
    if len(parts) != 3:
        return "", "", ""
    movement = {"A": "ARRIVAL", "P": "DEPARTURE", "O": "OVERFLIGHT", "G": "GO_AROUND"}.get(parts[0], "")
    return movement, parts[1], SHORT_PHASE.get(parts[2], "")


def _is_grid_coarse(lat: float, lng: float) -> bool:
    """Bounds-feed FR24 bị làm tròn 0,01°; không cho loại điểm này làm mực."""
    return (
        abs(float(lat) * 100.0 - round(float(lat) * 100.0)) < 1e-7
        and abs(float(lng) * 100.0 - round(float(lng) * 100.0)) < 1e-7
    )


def _level_for(lat: float, lng: float, altitude_ft: Optional[float], speed_kt: Optional[float]) -> int:
    dist_km = _haversine_m(lat, lng, DAD_LAT, DAD_LNG) / 1000.0
    spd = float(speed_kt) if _finite(speed_kt) else None
    alt = float(altitude_ft) if _finite(altitude_ft) else None
    if dist_km <= 8.0 and spd is not None and spd <= 80.0 and (alt is None or alt <= 450.0):
        return 0
    if dist_km <= 80.0 or (alt is not None and alt <= 12_000.0):
        return 1
    if dist_km <= 450.0:
        return 2
    return 3


def _track_hash(track_id: str) -> str:
    return hashlib.sha1(str(track_id).encode("utf-8", "ignore")).hexdigest()[:12]


def _shard_of(node_id: str, shard_count: int) -> int:
    raw = hashlib.sha1(node_id.encode("utf-8", "ignore")).digest()
    return int.from_bytes(raw[:2], "big") % max(1, int(shard_count))


@dataclass(frozen=True)
class InkPoint:
    t: int
    lat: float
    lng: float
    altitude_ft: Optional[float]
    speed_kt: Optional[float]
    heading: Optional[float]
    level: int


class RadarInkMap:
    """Đồ thị đường bay nhiều nhánh học từ các vết thật đã hoàn chỉnh."""

    def __init__(self, shard_count: int = 32):
        self.shard_count = max(4, int(shard_count))
        self.nodes: dict[str, dict[str, Any]] = {}
        self.edges: dict[str, dict[str, dict[str, Any]]] = {}
        self.metrics: dict[str, dict[str, Any]] = {}
        # watermark + điểm đuôi giúp khử trùng cùng chuyến từ nhiều máy và nối đúng
        # cạnh khi một trail phải upload qua nhiều batch.
        self.track_states: dict[str, dict[str, Any]] = {}
        self.rev = 0
        self.updated_ms = 0
        self._index: dict[tuple[int, int, int], set[str]] = {}
        self._dirty_shards: set[int] = set()
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Chuẩn hóa / tạo node
    # ------------------------------------------------------------------
    def _normalize_points(self, raw_points: Iterable[Any]) -> list[InkPoint]:
        now_ms = int(time.time() * 1000)
        unique: dict[int, InkPoint] = {}
        for raw in raw_points or []:
            if isinstance(raw, (list, tuple)):
                # [ts,lat,lng,alt,spd,hd,quality?]
                if len(raw) < 3:
                    continue
                ts, lat, lng = raw[0], raw[1], raw[2]
                alt = raw[3] if len(raw) > 3 else None
                spd = raw[4] if len(raw) > 4 else None
                hd = raw[5] if len(raw) > 5 else None
                quality = str(raw[6] if len(raw) > 6 else "FINE").upper()
            elif isinstance(raw, dict):
                ts = raw.get("ts") or raw.get("t")
                lat = raw.get("lat")
                lng = raw.get("lng") if raw.get("lng") is not None else raw.get("lon")
                alt = raw.get("altitude_ft") if raw.get("altitude_ft") is not None else raw.get("alt")
                spd = raw.get("speed_kt") if raw.get("speed_kt") is not None else raw.get("spd")
                hd = raw.get("heading") if raw.get("heading") is not None else raw.get("hd")
                quality = str(raw.get("quality") or "FINE").upper()
            else:
                continue
            if not (_finite(ts) and _finite(lat) and _finite(lng)):
                continue
            t = int(float(ts))
            if t < 1_000_000_000_000:
                t *= 1000
            lat_f, lng_f = float(lat), float(lng)
            if not (-85.0 <= lat_f <= 85.0 and -180.0 <= lng_f <= 180.0):
                continue
            # Máy cơ quan có thể mất mạng/Render ngủ lâu. Giữ cửa sổ 30 ngày để outbox
            # thực sự bền; timestamp + track_id vẫn khử trùng nên không làm dày giả.
            if t < now_ms - 30 * 24 * 3600 * 1000 or t > now_ms + 10 * 60 * 1000:
                continue
            if _haversine_m(lat_f, lng_f, DAD_LAT, DAD_LNG) > 2_200_000:
                continue
            if quality != "FINE" or _is_grid_coarse(lat_f, lng_f):
                continue
            alt_f = float(alt) if _finite(alt) and -1000 <= float(alt) <= 65_000 else None
            spd_f = float(spd) if _finite(spd) and 0 <= float(spd) <= 750 else None
            hd_f = _norm360(float(hd)) if _finite(hd) else None
            unique[t] = InkPoint(t, lat_f, lng_f, alt_f, spd_f, hd_f, _level_for(lat_f, lng_f, alt_f, spd_f))
        return [unique[t] for t in sorted(unique)]

    @staticmethod
    def _infer_runway(points: Iterable[InkPoint]) -> tuple[str, float]:
        """Nhận tim băng từ nhiều fix final; không dùng nhãn LANDED dễ đến sớm."""
        scored = []
        for runway_id, runway in RUNWAYS.items():
            samples = []
            along_values = []
            for point in points or []:
                if point.speed_kt is not None and not (70.0 <= point.speed_kt <= 280.0):
                    continue
                if point.altitude_ft is not None and point.altitude_ft > 8_000.0:
                    continue
                along, cross = _along_cross(point.lat, point.lng, runway_id)
                if not (-42_000.0 <= along <= 4_500.0 and abs(cross) <= 5_000.0):
                    continue
                hd_error = abs(_angle_diff(point.heading, runway["bearing"])) if point.heading is not None else 35.0
                if hd_error > 58.0:
                    continue
                closeness = math.exp(-abs(cross) / 850.0)
                alignment = math.exp(-hd_error / 24.0)
                threshold_bonus = 1.45 if -18_000.0 <= along <= 1_500.0 else 1.0
                samples.append(closeness * alignment * threshold_bonus)
                along_values.append(along)
            if not samples:
                continue
            progress = max(along_values) - min(along_values) if len(along_values) >= 2 else 0.0
            score = sum(samples) + min(3.0, progress / 8_000.0)
            scored.append((score, len(samples), runway_id))
        if not scored:
            return "", 0.0
        scored.sort(reverse=True)
        best_score, best_n, best_id = scored[0]
        if best_n < 2 and best_score < 1.15:
            return "", 0.0
        same_direction = [row for row in scored[1:] if abs(_angle_diff(RUNWAYS[best_id]["bearing"], RUNWAYS[row[2]]["bearing"])) < 5]
        rival = same_direction[0][0] if same_direction else 0.0
        confidence = _clamp(best_score / max(0.001, best_score + rival), 0.0, 1.0)
        return best_id, confidence

    @staticmethod
    def _looks_go_around(points: list[InkPoint], runway_id: str) -> bool:
        """Tách missed approach để nhánh leo lại không được chọn như nhánh hạ."""
        if not runway_id or len(points) < 5:
            return False
        runway = RUNWAYS[runway_id]
        final_rows = []
        for index, point in enumerate(points):
            along, cross = _along_cross(point.lat, point.lng, runway_id)
            hd_error = abs(_angle_diff(point.heading, runway["bearing"])) if point.heading is not None else 0.0
            if -20_000 <= along <= 2_000 and abs(cross) <= 2_500 and hd_error <= 32:
                final_rows.append((index, point, along))
        if len(final_rows) < 2:
            return False
        closest = max(final_rows, key=lambda row: row[2])
        index, low_point, along = closest
        # Đã tới sát ngưỡng thì nhiều khả năng là hạ bình thường; chỉ gọi go-around khi
        # vệt quay ra trước ngưỡng rồi leo rõ ràng.
        if along > -600:
            return False
        low_alt = low_point.altitude_ft
        for point in points[index + 1:]:
            dist_growth = _haversine_m(point.lat, point.lng, DAD_LAT, DAD_LNG) - _haversine_m(
                low_point.lat, low_point.lng, DAD_LAT, DAD_LNG
            )
            climbed = low_alt is not None and point.altitude_ft is not None and point.altitude_ft >= low_alt + 1_200
            turned_away = point.heading is not None and abs(_angle_diff(runway["bearing"], point.heading)) >= 55
            if dist_growth >= 5_000 and (climbed or turned_away):
                return True
        return False

    @staticmethod
    def _movement_for(points: list[InkPoint], origin: str, meta: Optional[dict[str, Any]], runway_id: str) -> str:
        meta = meta or {}
        destination = str(meta.get("destination") or meta.get("dest") or "").strip().upper()
        origin_key = str(origin or meta.get("origin") or "").strip().upper()
        if runway_id and RadarInkMap._looks_go_around(points, runway_id):
            return "GO_AROUND"
        if destination in {"DAD", "VVDN"}:
            return "ARRIVAL"
        if origin_key in {"DAD", "VVDN"}:
            return "DEPARTURE"
        if len(points) >= 2:
            first_d = _haversine_m(points[0].lat, points[0].lng, DAD_LAT, DAD_LNG)
            last_d = _haversine_m(points[-1].lat, points[-1].lng, DAD_LAT, DAD_LNG)
            if last_d + 8_000 < first_d:
                return "ARRIVAL"
            if first_d + 8_000 < last_d:
                return "DEPARTURE"
        return "OVERFLIGHT"

    @staticmethod
    def _phase_for(point: InkPoint, movement: str, runway_id: str = "") -> str:
        movement = str(movement or "").upper()
        dist_km = _haversine_m(point.lat, point.lng, DAD_LAT, DAD_LNG) / 1000.0
        alt = point.altitude_ft
        speed = point.speed_kt
        if movement == "GO_AROUND":
            return "GO_AROUND"
        if movement == "DEPARTURE":
            return "GROUND" if dist_km <= 7 and (speed is None or speed <= 80) else "DEPARTURE"
        if movement != "ARRIVAL":
            return "OVERFLIGHT"

        # Đoạn lăn/rollout phải tách khỏi final dù heading còn cùng tim băng.
        if dist_km <= 7.0 and (speed is not None and speed <= 80.0) and (alt is None or alt <= 450.0):
            return "GROUND"
        if runway_id in RUNWAYS:
            runway = RUNWAYS[runway_id]
            along, cross = _along_cross(point.lat, point.lng, runway_id)
            hd_error = abs(_angle_diff(point.heading, runway["bearing"])) if point.heading is not None else 35.0
            low_enough = alt is None or alt <= 8_000.0
            if low_enough and -32_000 <= along <= 3_500 and abs(cross) <= 3_200 and hd_error <= 42:
                if along >= -22_000 and abs(cross) <= 2_200 and hd_error <= 30:
                    return "FINAL"
                return "INTERCEPT"
        if dist_km <= 100.0 and (alt is None or alt <= 18_000.0):
            return "TRANSITION"
        if dist_km <= 230.0 or (alt is not None and alt <= 28_000.0):
            return "DESCENT"
        return "ARRIVAL_ENROUTE"

    def context_hint(
        self,
        lat: float,
        lng: float,
        heading: Optional[float],
        speed_kt: Optional[float],
        altitude_ft: Optional[float],
        movement: str = "ARRIVAL",
        runway_hint: str = "",
    ) -> dict[str, Any]:
        """Ngữ cảnh gọn cho app.py chọn đường băng đang khai thác mà không đọc DB."""
        if not (_finite(lat) and _finite(lng)):
            return {"movement": movement, "runway_id": "", "phase": ""}
        point = InkPoint(
            int(time.time() * 1000), float(lat), float(lng),
            float(altitude_ft) if _finite(altitude_ft) else None,
            float(speed_kt) if _finite(speed_kt) else None,
            _norm360(float(heading)) if _finite(heading) else None,
            _level_for(float(lat), float(lng), altitude_ft, speed_kt),
        )
        runway_id = str(runway_hint or "").upper() if str(runway_hint or "").upper() in RUNWAYS else ""
        confidence = 0.0
        inferred_id, inferred_conf = self._infer_runway([point])
        if inferred_id:
            runway_id, confidence = inferred_id, inferred_conf
        phase = self._phase_for(point, movement, runway_id)
        return {
            "movement": str(movement or "").upper(),
            "runway_id": runway_id,
            "runway_confidence": round(confidence, 3),
            "phase": phase,
            "distance_dad_km": round(_haversine_m(point.lat, point.lng, DAD_LAT, DAD_LNG) / 1000.0, 2),
        }

    def recent_runway_hint(self, max_age_ms: int = 3 * 3600 * 1000) -> str:
        """Runway đã được các vệt hạ gần đây xác nhận; chỉ là tie-break, không ép final."""
        now_ms = int(time.time() * 1000)
        counts: dict[str, float] = {}
        with self._lock:
            for row in self.track_states.values():
                runway_id = str(row.get("runway_id") or "")
                updated = int(row.get("updated") or 0)
                if runway_id not in RUNWAYS or now_ms - updated > max(60_000, int(max_age_ms)):
                    continue
                age_weight = max(0.1, 1.0 - (now_ms - updated) / max(1.0, float(max_age_ms)))
                confidence = max(0.35, float(row.get("runway_confidence") or 0.0))
                counts[runway_id] = counts.get(runway_id, 0.0) + age_weight * confidence
        return max(counts, key=counts.get) if counts else ""

    def _node_key(self, point: InkPoint, heading: float) -> tuple[str, int, int]:
        cfg = LEVELS[point.level]
        x, y = _project(point.lat, point.lng)
        ix = int(round(x / cfg["cell_m"]))
        iy = int(round(y / cfg["cell_m"]))
        hb = int(round(_norm360(heading) / cfg["heading_bin_deg"])) % int(round(360.0 / cfg["heading_bin_deg"]))
        return f"{point.level}_{ix}_{iy}_{hb}", ix, iy

    def _register_index(self, node_id: str, level: int, ix: int, iy: int) -> None:
        self._index.setdefault((level, ix, iy), set()).add(node_id)

    def _upsert_node(self, point: InkPoint, heading: float) -> str:
        node_id, ix, iy = self._node_key(point, heading)
        row = self.nodes.get(node_id)
        if row is None:
            if len(self.nodes) >= MAX_NODES:
                return ""
            row = {
                "lat": point.lat, "lng": point.lng, "n": 1,
                "heading": _norm360(heading),
                "speed": point.speed_kt,
                "alt": point.altitude_ft,
                "last": point.t,
                "level": point.level,
                "ix": ix, "iy": iy,
            }
            self.nodes[node_id] = row
            self._register_index(node_id, point.level, ix, iy)
        else:
            n_old = max(1, int(row.get("n") or 1))
            # Sau 500 quan sát chuyển sang EMA để bản đồ vẫn thích nghi theo thời gian.
            alpha = 1.0 / min(500, n_old + 1)
            row["lat"] += (point.lat - row["lat"]) * alpha
            row["lng"] += (point.lng - row["lng"]) * alpha
            row["heading"] = _norm360(row["heading"] + _angle_diff(row["heading"], heading) * alpha)
            if point.speed_kt is not None:
                if row.get("speed") is None:
                    row["speed"] = point.speed_kt
                else:
                    row["speed"] += (point.speed_kt - row["speed"]) * alpha
            if point.altitude_ft is not None:
                if row.get("alt") is None:
                    row["alt"] = point.altitude_ft
                else:
                    row["alt"] += (point.altitude_ft - row["alt"]) * alpha
            row["n"] = min(500, n_old + 1)
            row["last"] = max(int(row.get("last") or 0), point.t)
        self._dirty_shards.add(_shard_of(node_id, self.shard_count))
        return node_id

    def _segment_plausible(self, a: InkPoint, b: InkPoint) -> tuple[bool, float, float, float]:
        dt_s = (b.t - a.t) / 1000.0
        if dt_s < 1.0 or dt_s > 120.0:
            return False, dt_s, 0.0, 0.0
        distance_m = _haversine_m(a.lat, a.lng, b.lat, b.lng)
        if distance_m < 8.0:
            return False, dt_s, distance_m, 0.0
        implied_kt = distance_m / dt_s / 0.514444
        ground = a.level == 0 or b.level == 0
        max_kt = 100.0 if ground else 800.0
        if implied_kt > max_kt:
            return False, dt_s, distance_m, implied_kt
        # Khe dài chỉ được nối khi cả hai hướng xác nhận đây là đoạn gần thẳng. Nếu không,
        # chỉ giữ hai chấm rời để không vẽ dây tắt qua một khúc cua chưa quan sát.
        seg_brg = _bearing(a.lat, a.lng, b.lat, b.lng)
        if dt_s > (30.0 if ground else 45.0):
            if a.heading is None or b.heading is None:
                return False, dt_s, distance_m, implied_kt
            if abs(_angle_diff(a.heading, seg_brg)) > 22.0 or abs(_angle_diff(b.heading, seg_brg)) > 30.0:
                return False, dt_s, distance_m, implied_kt
        return True, dt_s, distance_m, implied_kt

    def _upsert_edge(
        self,
        from_id: str,
        to_id: str,
        track_hash: str,
        origin: str,
        movement: str,
        runway_id: str,
        phase: str,
        dt_s: float,
        speed_kt: float,
        now_ms: int,
    ) -> bool:
        if not from_id or not to_id or from_id == to_id:
            return False
        outgoing = self.edges.setdefault(from_id, {})
        row = outgoing.get(to_id)
        if row is None:
            row = {
                "to": to_id, "support": 0, "dt": float(dt_s), "speed": float(speed_kt),
                "last": int(now_ms), "origins": {}, "tracks": [], "contexts": {},
            }
            outgoing[to_id] = row
        tracks = list(row.get("tracks") or [])
        if track_hash in tracks:
            return False
        support_old = max(0, int(row.get("support") or 0))
        alpha = 1.0 / min(255, support_old + 1)
        row["dt"] = float(row.get("dt") or dt_s) + (float(dt_s) - float(row.get("dt") or dt_s)) * alpha
        row["speed"] = float(row.get("speed") or speed_kt) + (float(speed_kt) - float(row.get("speed") or speed_kt)) * alpha
        row["support"] = min(255, support_old + 1)
        row["last"] = max(int(row.get("last") or 0), int(now_ms))
        tracks.append(track_hash)
        row["tracks"] = tracks[-MAX_TRACK_HASHES_PER_EDGE:]
        if origin:
            origins = dict(row.get("origins") or {})
            origins[origin] = min(255, int(origins.get(origin) or 0) + 1)
            if len(origins) > MAX_ORIGINS_PER_EDGE:
                origins = dict(sorted(origins.items(), key=lambda item: item[1], reverse=True)[:MAX_ORIGINS_PER_EDGE])
            row["origins"] = origins
        context_key = _phase_key(movement, runway_id, phase)
        contexts = dict(row.get("contexts") or {})
        contexts[context_key] = min(255, int(contexts.get(context_key) or 0) + 1)
        if len(contexts) > MAX_CONTEXTS_PER_EDGE:
            contexts = dict(sorted(contexts.items(), key=lambda item: item[1], reverse=True)[:MAX_CONTEXTS_PER_EDGE])
        row["contexts"] = contexts
        if len(outgoing) > MAX_OUT_EDGES:
            weakest = min(outgoing.items(), key=lambda item: (int(item[1].get("support") or 0), int(item[1].get("last") or 0)))[0]
            if weakest != to_id:
                outgoing.pop(weakest, None)
        self._dirty_shards.add(_shard_of(from_id, self.shard_count))
        return True

    def _enrich_track_context(self, track_hash: str, movement: str, runway_id: str) -> int:
        """Khi cuối trail mới lộ runway, tô ngược context cho phần vòng vào đã gửi trước."""
        if not track_hash or not runway_id:
            return 0
        changed = 0
        for from_id, outgoing in self.edges.items():
            node = self.nodes.get(from_id)
            if not node:
                continue
            point = InkPoint(
                int(node.get("last") or 0), float(node["lat"]), float(node["lng"]),
                float(node["alt"]) if _finite(node.get("alt")) else None,
                float(node["speed"]) if _finite(node.get("speed")) else None,
                float(node["heading"]) if _finite(node.get("heading")) else None,
                int(node.get("level") or 0),
            )
            phase = self._phase_for(point, movement, runway_id)
            context_key = _phase_key(movement, runway_id, phase)
            for edge in outgoing.values():
                if track_hash not in (edge.get("tracks") or []):
                    continue
                contexts = dict(edge.get("contexts") or {})
                if context_key in contexts:
                    continue
                contexts[context_key] = 1
                if len(contexts) > MAX_CONTEXTS_PER_EDGE:
                    contexts = dict(sorted(contexts.items(), key=lambda item: item[1], reverse=True)[:MAX_CONTEXTS_PER_EDGE])
                edge["contexts"] = contexts
                self._dirty_shards.add(_shard_of(from_id, self.shard_count))
                changed += 1
        return changed

    # ------------------------------------------------------------------
    # Hướng dẫn từ đồ thị
    # ------------------------------------------------------------------
    def _candidate_nodes(
        self,
        lat: float,
        lng: float,
        heading: Optional[float],
        altitude_ft: Optional[float],
        speed_kt: Optional[float],
    ) -> list[tuple[float, str, float, float]]:
        preferred = _level_for(lat, lng, altitude_ft, speed_kt)
        levels = [preferred] + [level for level in LEVELS if level != preferred]
        candidates: list[tuple[float, str, float, float]] = []
        for level in levels:
            cfg = LEVELS[level]
            x, y = _project(lat, lng)
            ix = int(round(x / cfg["cell_m"]))
            iy = int(round(y / cfg["cell_m"]))
            radius_cells = 2 if level == preferred else 1
            max_dist = cfg["cell_m"] * (2.8 if level == preferred else 1.8)
            for dx in range(-radius_cells, radius_cells + 1):
                for dy in range(-radius_cells, radius_cells + 1):
                    for node_id in self._index.get((level, ix + dx, iy + dy), ()):
                        node = self.nodes.get(node_id)
                        if not node or not self.edges.get(node_id):
                            continue
                        dist = _haversine_m(lat, lng, node["lat"], node["lng"])
                        if dist > max_dist:
                            continue
                        hd_err = abs(_angle_diff(heading, node["heading"])) if heading is not None else 0.0
                        if heading is not None and hd_err > 70.0:
                            continue
                        support = sum(int(edge.get("support") or 0) for edge in self.edges.get(node_id, {}).values())
                        alt_penalty = 0.0
                        if altitude_ft is not None and _finite(node.get("alt")):
                            # Ở terminal, hai tàu cùng tọa độ/hướng nhưng một chiếc 4.000ft,
                            # một chiếc 14.000ft có thể thuộc hai vòng hoàn toàn khác nhau.
                            alt_scale = 4_500.0 if level == 1 else 12_000.0
                            alt_penalty = min(1.4, abs(float(altitude_ft) - float(node["alt"])) / alt_scale)
                        speed_penalty = 0.0
                        if speed_kt is not None and _finite(node.get("speed")):
                            speed_scale = 130.0 if level == 1 else 260.0
                            speed_penalty = min(0.8, abs(float(speed_kt) - float(node["speed"])) / speed_scale)
                        score = (
                            dist / max(1.0, max_dist) + hd_err / 90.0
                            + alt_penalty * 0.55 + speed_penalty * 0.25
                            - min(0.35, math.log1p(support) / 15.0)
                        )
                        candidates.append((score, node_id, dist, hd_err))
            if candidates and level == preferred:
                break
        return sorted(candidates, key=lambda item: item[0])[:8]

    @staticmethod
    def _context_support(
        edge: dict[str, Any], movement: str, runway_id: str, phase: str
    ) -> tuple[float, int, int]:
        """Điểm context, tổng cùng nghiệp vụ và tổng runway đối nghịch của một cạnh."""
        contexts = edge.get("contexts") or {}
        if not isinstance(contexts, dict) or not contexts:
            return 0.0, 0, 0
        movement = str(movement or "").upper()
        runway_id = str(runway_id or "").upper()
        phase = str(phase or "").upper()
        compatible = {
            "ARRIVAL_ENROUTE": {"ARRIVAL_ENROUTE", "DESCENT"},
            "DESCENT": {"ARRIVAL_ENROUTE", "DESCENT", "TRANSITION"},
            "TRANSITION": {"DESCENT", "TRANSITION", "INTERCEPT"},
            "INTERCEPT": {"TRANSITION", "INTERCEPT", "FINAL"},
            "FINAL": {"INTERCEPT", "FINAL"},
            "GROUND": {"ROLLOUT", "GROUND"},
        }.get(phase, {phase})
        score = 0.0
        same_movement = 0
        other_runway = 0
        for key, raw_count in contexts.items():
            count = max(0, int(raw_count or 0))
            edge_movement, edge_runway, edge_phase = _parse_phase_key(key)
            if edge_movement != movement:
                continue
            same_movement += count
            if runway_id and edge_runway not in {"*", runway_id}:
                other_runway += count
                continue
            if edge_phase == phase:
                score += count * (1.45 if runway_id and edge_runway == runway_id else 1.15)
            elif edge_phase in compatible:
                score += count * 0.55
            else:
                score += count * 0.12
        return score, same_movement, other_runway

    def _metric_summary(self, level: int, origin: Optional[str] = None) -> dict[str, Any]:
        # None dùng cho trang thống kê tổng; chuỗi rỗng là nhóm "không rõ origin" riêng.
        origin_key = None if origin is None else (str(origin or "").strip().upper()[:8] or "*")
        rows = []
        for key, row in self.metrics.items():
            if not key.startswith(f"{level}:"):
                continue
            if origin_key is not None and not key.endswith(f":{origin_key}"):
                continue
            # ALL là phép chấm dùng chung terminal. Trang tổng không cộng nó lần nữa
            # vì các hàng origin riêng đã chứa cùng mẫu.
            if origin_key is None and key.endswith(":ALL"):
                continue
            rows.append(row)
        n = sum(int(row.get("n") or 0) for row in rows)
        map_sum = sum(float(row.get("map_sum") or 0.0) for row in rows)
        straight_sum = sum(float(row.get("straight_sum") or 0.0) for row in rows)
        map_wins = sum(int(row.get("map_wins") or 0) for row in rows)
        map_mean = map_sum / n if n else None
        straight_mean = straight_sum / n if n else None
        win_rate = map_wins / n if n else None
        improvement = (
            1.0 - map_mean / straight_mean
            if n and straight_mean and straight_mean > 0 and map_mean is not None else None
        )
        approved = bool(
            n >= MIN_EVAL_SAMPLES and improvement is not None
            and improvement >= MIN_MAP_IMPROVEMENT and win_rate is not None and win_rate >= 0.55
        )
        return {
            "n": n,
            "map_mean_m": round(map_mean, 1) if map_mean is not None else None,
            "straight_mean_m": round(straight_mean, 1) if straight_mean is not None else None,
            "improvement": round(improvement, 4) if improvement is not None else None,
            "win_rate": round(win_rate, 4) if win_rate is not None else None,
            "approved": approved,
        }

    def guidance(
        self,
        lat: float,
        lng: float,
        heading: Optional[float],
        speed_kt: Optional[float],
        altitude_ft: Optional[float],
        origin: str = "",
        horizon_s: int = 180,
        movement: str = "ARRIVAL",
        runway_hint: str = "",
        phase_hint: str = "",
    ) -> dict[str, Any]:
        with self._lock:
            if not (_finite(lat) and _finite(lng)):
                return {"available": False, "use": False, "reason": "BAD_POSITION", "rev": self.rev}
            lat_f, lng_f = float(lat), float(lng)
            hd = _norm360(float(heading)) if _finite(heading) else None
            spd = float(speed_kt) if _finite(speed_kt) and float(speed_kt) >= 0 else None
            alt = float(altitude_ft) if _finite(altitude_ft) else None
            movement_key = str(movement or "ARRIVAL").strip().upper()
            if movement_key not in {"ARRIVAL", "DEPARTURE", "OVERFLIGHT", "GO_AROUND"}:
                movement_key = "ARRIVAL"
            runway_id = str(runway_hint or "").strip().upper()
            if runway_id not in RUNWAYS:
                runway_id = ""
            current_point = InkPoint(
                int(time.time() * 1000), lat_f, lng_f, alt, spd, hd,
                _level_for(lat_f, lng_f, alt, spd),
            )
            if not runway_id:
                runway_id, _ = self._infer_runway([current_point])
            current_phase = str(phase_hint or "").strip().upper()
            if current_phase not in PHASE_SHORT:
                current_phase = self._phase_for(current_point, movement_key, runway_id)
            candidates = self._candidate_nodes(lat_f, lng_f, hd, alt, spd)
            if not candidates:
                return {"available": False, "use": False, "reason": "NO_NEAR_INK", "rev": self.rev}
            _, current_id, start_dist, start_hd_err = candidates[0]
            current_node = self.nodes[current_id]
            level = int(current_node.get("level") or 0)
            origin_key = str(origin or "").strip().upper()[:8]
            path = [{
                "seconds_ahead": 0.0,
                "lat": round(lat_f, 6), "lng": round(lng_f, 6),
                "speed_kt": round(spd, 1) if spd is not None else None,
                "altitude_ft": round(alt) if alt is not None else None,
                "heading": round(hd, 1) if hd is not None else None,
                "phase": current_phase,
                "support": 0,
            }]
            elapsed = 0.0
            min_support = 10**9
            min_branch = 1.0
            seen: dict[str, int] = {current_id: 1}
            current_heading = hd if hd is not None else float(current_node.get("heading") or 0.0)
            max_steps = 42
            for _ in range(max_steps):
                outgoing = self.edges.get(current_id) or {}
                choices = []
                for to_id, edge in outgoing.items():
                    to_node = self.nodes.get(to_id)
                    if not to_node or seen.get(to_id, 0) >= 2:
                        continue
                    edge_brg = _bearing(current_node["lat"], current_node["lng"], to_node["lat"], to_node["lng"])
                    hd_err = abs(_angle_diff(current_heading, edge_brg))
                    if hd_err > 105.0:
                        continue
                    support = int(edge.get("support") or 0)
                    context_support = int((edge.get("origins") or {}).get(origin_key) or 0) if origin_key else 0
                    phase_support, same_movement, other_runway = self._context_support(
                        edge, movement_key, runway_id, current_phase
                    )
                    score = support + context_support * 1.35 + phase_support * 2.1
                    if edge.get("contexts") and same_movement <= 0:
                        score *= 0.18
                    if runway_id and other_runway > 0 and phase_support <= 0:
                        score *= 0.35
                    if alt is not None and _finite(to_node.get("alt")):
                        alt_scale = 5_000.0 if level == 1 else 13_000.0
                        score *= math.exp(-abs(alt - float(to_node["alt"])) / alt_scale * 0.35)
                    score *= max(0.15, 1.0 - hd_err / 125.0)
                    choices.append((score, support, hd_err, to_id, edge, to_node, edge_brg))
                if not choices:
                    break
                choices.sort(key=lambda item: item[0], reverse=True)
                total = sum(max(0.001, item[0]) for item in choices)
                best = choices[0]
                branch_probability = best[0] / total if total > 0 else 0.0
                # Khi hai nhánh còn ngang nhau, chỉ đi tới điểm phân nhánh rồi chờ fix mới;
                # không tự bịa một cú rẽ có thể kéo tàu sai nhiều km.
                if len(path) >= 2 and branch_probability < 0.56:
                    break
                _, support, _, to_id, edge, to_node, edge_brg = best
                distance_m = _haversine_m(current_node["lat"], current_node["lng"], to_node["lat"], to_node["lng"])
                observed_dt = _clamp(float(edge.get("dt") or 1.0), 1.0, 60.0)
                local_speed_kt = float(edge.get("speed") or spd or 180.0)
                geom_dt = distance_m / max(2.0, local_speed_kt * 0.514444)
                edge_dt = _clamp(observed_dt * 0.7 + geom_dt * 0.3, 1.0, 60.0)
                elapsed += edge_dt
                min_support = min(min_support, support)
                min_branch = min(min_branch, branch_probability)
                path.append({
                    "seconds_ahead": round(elapsed, 1),
                    "lat": round(float(to_node["lat"]), 6),
                    "lng": round(float(to_node["lng"]), 6),
                    "speed_kt": round(local_speed_kt, 1),
                    "altitude_ft": round(float(to_node["alt"])) if _finite(to_node.get("alt")) else None,
                    "heading": round(float(to_node.get("heading") or edge_brg), 1),
                    "phase": self._phase_for(
                        InkPoint(
                            int(to_node.get("last") or 0), float(to_node["lat"]), float(to_node["lng"]),
                            float(to_node["alt"]) if _finite(to_node.get("alt")) else None,
                            float(to_node["speed"]) if _finite(to_node.get("speed")) else None,
                            float(to_node["heading"]) if _finite(to_node.get("heading")) else edge_brg,
                            int(to_node.get("level") or level),
                        ),
                        movement_key, runway_id,
                    ),
                    "support": support,
                })
                current_id = to_id
                current_node = to_node
                current_heading = edge_brg
                current_phase = str(path[-1].get("phase") or current_phase)
                if _finite(to_node.get("alt")):
                    alt = float(to_node["alt"])
                seen[to_id] = seen.get(to_id, 0) + 1
                if elapsed >= max(10, min(300, int(horizon_s))):
                    break
            if len(path) < 3:
                return {"available": False, "use": False, "reason": "INK_TOO_SHORT", "rev": self.rev}
            min_support = 0 if min_support == 10**9 else min_support
            cfg = LEVELS[level]
            match_conf = math.exp(-start_dist / max(1.0, cfg["cell_m"] * 2.2))
            if hd is not None:
                match_conf *= math.exp(-start_hd_err / 90.0)
            support_conf = min(1.0, math.log1p(max(0, min_support)) / math.log(9.0))
            confidence = _clamp(match_conf * (0.55 + 0.45 * min_branch) * support_conf, 0.0, 1.0)
            # Chấm riêng theo sân bay đi: một luồng HAN tốt không được tự cấp quyền
            # cho nhánh SGN chưa chứng minh, dù hai đường dùng chung vài node.
            quality = self._metric_summary(level, origin_key)
            quality_scope = origin_key or "*"
            if level == 1 and (int(quality.get("n") or 0) < MIN_EVAL_SAMPLES or not quality.get("approved")):
                shared_quality = self._metric_summary(level, "ALL")
                if shared_quality.get("approved") or int(shared_quality.get("n") or 0) > int(quality.get("n") or 0):
                    quality = shared_quality
                    quality_scope = "TERMINAL_SHARED"
            use = bool(min_support >= MIN_EDGE_SUPPORT and confidence >= 0.62 and quality["approved"])
            return {
                "schema": SCHEMA,
                "rev": self.rev,
                "available": True,
                "use": use,
                "shadow": not use,
                "reason": "APPROVED" if use else "LEARNING_SHADOW",
                "level": LEVELS[level]["name"],
                "level_id": level,
                "movement": movement_key,
                "phase": str(path[0].get("phase") or ""),
                "runway_id": runway_id or None,
                "approach": bool(level == 1 and movement_key in {"ARRIVAL", "GO_AROUND"}),
                "confidence": round(confidence, 3),
                "support": int(min_support),
                "branch_probability": round(min_branch, 3),
                "match_distance_m": round(start_dist),
                "quality": quality,
                "quality_scope": quality_scope,
                "path": path,
            }

    # ------------------------------------------------------------------
    # Tự chấm bằng fix kế tiếp rồi mới đóng góp mực
    # ------------------------------------------------------------------
    @staticmethod
    def _path_point_at(path: list[dict[str, Any]], seconds_ahead: float) -> Optional[tuple[float, float]]:
        if not path:
            return None
        if seconds_ahead <= float(path[0].get("seconds_ahead") or 0):
            return float(path[0]["lat"]), float(path[0]["lng"])
        for i in range(1, len(path)):
            a, b = path[i - 1], path[i]
            ta = float(a.get("seconds_ahead") or 0)
            tb = float(b.get("seconds_ahead") or 0)
            if seconds_ahead <= tb:
                u = (seconds_ahead - ta) / max(0.001, tb - ta)
                return (
                    float(a["lat"]) + (float(b["lat"]) - float(a["lat"])) * u,
                    float(a["lng"]) + (float(b["lng"]) - float(a["lng"])) * u,
                )
        return None

    def _evaluate_track(self, points: list[InkPoint], origin: str, track_context: Optional[dict[str, Any]] = None) -> int:
        if len(points) < 2 or not self.nodes:
            return 0
        pairs = [(points[i], points[i + 1]) for i in range(len(points) - 1)]
        # Tối đa 80 phép chấm cho một lần upload để endpoint vẫn nhẹ khi trail dài.
        if len(pairs) > 80:
            stride = max(1, len(pairs) // 80)
            pairs = pairs[::stride][:80]
        evaluated = 0
        track_context = track_context or {}
        movement = str(track_context.get("movement") or "ARRIVAL")
        runway_id = str(track_context.get("runway_id") or "")
        for a, b in pairs:
            dt_s = (b.t - a.t) / 1000.0
            if not (5.0 <= dt_s <= 120.0):
                continue
            phase = self._phase_for(a, movement, runway_id)
            guide = self.guidance(
                a.lat, a.lng, a.heading, a.speed_kt, a.altitude_ft,
                origin, int(dt_s + 10), movement, runway_id, phase,
            )
            if not guide.get("available"):
                continue
            predicted = self._path_point_at(guide.get("path") or [], dt_s)
            if not predicted:
                continue
            map_error = _haversine_m(predicted[0], predicted[1], b.lat, b.lng)
            if a.heading is not None and a.speed_kt is not None:
                straight = _destination_point(a.lat, a.lng, a.heading, a.speed_kt * 0.514444 * dt_s)
                straight_error = _haversine_m(straight[0], straight[1], b.lat, b.lng)
            else:
                straight_error = _haversine_m(a.lat, a.lng, b.lat, b.lng)
            horizon = 30 if dt_s <= 30 else (60 if dt_s <= 60 else 120)
            metric_keys = {f"{a.level}:{horizon}:{origin or '*'}"}
            if a.level == 1:
                metric_keys.add(f"{a.level}:{horizon}:ALL")
            for key in metric_keys:
                row = self.metrics.setdefault(key, {"n": 0, "map_sum": 0.0, "straight_sum": 0.0, "map_wins": 0, "last": 0})
                row["n"] = min(100_000, int(row.get("n") or 0) + 1)
                row["map_sum"] = float(row.get("map_sum") or 0.0) + map_error
                row["straight_sum"] = float(row.get("straight_sum") or 0.0) + straight_error
                if map_error < straight_error:
                    row["map_wins"] = int(row.get("map_wins") or 0) + 1
                row["last"] = b.t
            evaluated += 1
        return evaluated

    def ingest_track(
        self,
        track_id: str,
        points: Iterable[Any],
        origin: str = "",
        meta: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        track_id = str(track_id or "").strip()
        if not track_id:
            return {"accepted": False, "reason": "NO_TRACK_ID", "rev": self.rev}
        normalized = self._normalize_points(points)
        if not normalized:
            return {"accepted": False, "reason": "NOT_ENOUGH_FINE_POINTS", "points": 0, "rev": self.rev}
        origin_key = str(origin or "").strip().upper()[:8]
        th = _track_hash(track_id)
        inferred_runway, runway_confidence = self._infer_runway(normalized)
        # API cũ của module chỉ nhận origin và vốn dành riêng các chuyến về DAD; giữ
        # mặc định ARRIVAL để atlas v1/test cũ không đổi nghĩa. Importer đa sân bay phải
        # truyền meta destination/origin rõ ràng mới bật phân loại departure/overflight.
        inferred_movement = "ARRIVAL" if meta is None else self._movement_for(
            normalized, origin_key, meta, inferred_runway
        )
        with self._lock:
            previous = self.track_states.get(th) or {}
            previous_runway = str(previous.get("runway_id") or "")
            runway_id = inferred_runway or previous_runway
            movement = inferred_movement
            if movement == "OVERFLIGHT" and previous.get("movement"):
                movement = str(previous.get("movement"))
            track_context = {
                "movement": movement,
                "runway_id": runway_id,
                "runway_confidence": runway_confidence or float(previous.get("runway_confidence") or 0.0),
            }
            enriched_edges = 0
            if runway_id and not previous_runway:
                # Trail tới từng phần: khi batch cuối mới lộ 35L/35R, gắn runway ngược
                # cho các edge downwind/base đã nhận ở batch trước của cùng chuyến.
                enriched_edges = self._enrich_track_context(th, movement, runway_id)
            max_t = int(previous.get("max_t") or 0)
            fresh = [point for point in normalized if point.t > max_t]
            if not fresh:
                return {
                    "accepted": False, "reason": "DUPLICATE_TRACK_POINTS", "points": len(normalized),
                    "fine_points": 0, "segments": 0, "edges_added": 0, "evaluated": 0,
                    "nodes": len(self.nodes), "edges": sum(len(v) for v in self.edges.values()), "rev": self.rev,
                }
            tail = previous.get("last")
            if isinstance(tail, (list, tuple)) and len(tail) >= 7:
                try:
                    tail_point = InkPoint(
                        int(tail[0]), float(tail[1]), float(tail[2]),
                        float(tail[3]) if tail[3] is not None else None,
                        float(tail[4]) if tail[4] is not None else None,
                        float(tail[5]) if tail[5] is not None else None,
                        int(tail[6]),
                    )
                    if tail_point.t < fresh[0].t:
                        fresh.insert(0, tail_point)
                except (TypeError, ValueError):
                    pass
            normalized = fresh
            if len(normalized) < 2:
                point = normalized[-1]
                self.track_states[th] = {
                    "max_t": point.t,
                    "last": [point.t, point.lat, point.lng, point.altitude_ft, point.speed_kt, point.heading, point.level],
                    "updated": int(time.time() * 1000),
                    **track_context,
                }
                return {"accepted": False, "reason": "WAIT_NEXT_POINT", "fine_points": 1, "rev": self.rev}
            evaluated = self._evaluate_track(normalized, origin_key, track_context)
            node_count_before = len(self.nodes)
            edges_added = 0
            accepted_segments = 0
            for a, b in zip(normalized, normalized[1:]):
                plausible, dt_s, _, implied_kt = self._segment_plausible(a, b)
                seg_brg = _bearing(a.lat, a.lng, b.lat, b.lng)
                # FR24 đôi khi dùng 0 thay cho "không có heading". Chỉ coi 0 là Bắc
                # thật khi chuyển động cũng gần Bắc; nếu lệch lớn thì track hai fix
                # chính xác hơn và tránh tạo cả một luồng heading giả.
                def usable_heading(raw_heading: Optional[float]) -> float:
                    if raw_heading is None:
                        return seg_brg
                    if abs(_angle_diff(0.0, raw_heading)) < 0.25 and abs(_angle_diff(raw_heading, seg_brg)) > 55.0:
                        return seg_brg
                    return raw_heading

                heading_a = usable_heading(a.heading)
                heading_b = usable_heading(b.heading)
                from_id = self._upsert_node(a, heading_a)
                to_id = self._upsert_node(b, heading_b)
                if not plausible:
                    continue
                speed_sample = (
                    (a.speed_kt + b.speed_kt) / 2.0
                    if a.speed_kt is not None and b.speed_kt is not None else implied_kt
                )
                phase = self._phase_for(a, movement, runway_id)
                accepted_segments += 1
                if self._upsert_edge(
                    from_id, to_id, th, origin_key, movement, runway_id, phase,
                    dt_s, speed_sample, b.t,
                ):
                    edges_added += 1
            changed = len(self.nodes) > node_count_before or edges_added > 0 or enriched_edges > 0
            last_point = normalized[-1]
            self.track_states[th] = {
                "max_t": last_point.t,
                "last": [
                    last_point.t, round(last_point.lat, 7), round(last_point.lng, 7),
                    last_point.altitude_ft, last_point.speed_kt, last_point.heading, last_point.level,
                ],
                "updated": int(time.time() * 1000),
                **track_context,
            }
            if len(self.track_states) > MAX_TRACK_STATES:
                oldest = sorted(self.track_states.items(), key=lambda item: int(item[1].get("updated") or 0))
                for old_key, _ in oldest[: len(self.track_states) - MAX_TRACK_STATES]:
                    self.track_states.pop(old_key, None)
            if changed:
                self.rev += 1
                self.updated_ms = int(time.time() * 1000)
            return {
                "accepted": changed,
                "track_id": track_id,
                "fine_points": len(normalized),
                "segments": accepted_segments,
                "edges_added": edges_added,
                "evaluated": evaluated,
                "movement": movement,
                "runway_id": runway_id or None,
                "runway_confidence": round(float(track_context.get("runway_confidence") or 0.0), 3),
                "enriched_edges": enriched_edges,
                "nodes": len(self.nodes),
                "edges": sum(len(v) for v in self.edges.values()),
                "rev": self.rev,
            }

    # ------------------------------------------------------------------
    # Lưu/nạp Firestore theo shard
    # ------------------------------------------------------------------
    def dirty_shards(self) -> set[int]:
        with self._lock:
            return set(self._dirty_shards)

    def mark_persisted(self, shards: Iterable[int]) -> None:
        with self._lock:
            self._dirty_shards.difference_update(int(s) for s in shards)

    def export_meta(self) -> dict[str, Any]:
        with self._lock:
            return {
                "schema": SCHEMA,
                "version": VERSION,
                "rev": self.rev,
                "shard_count": self.shard_count,
                "updated_ms": self.updated_ms,
                "node_count": len(self.nodes),
                "edge_count": sum(len(v) for v in self.edges.values()),
                "metrics": json.loads(json.dumps(self.metrics)),
                "tracks": json.loads(json.dumps(self.track_states)),
            }

    def export_shard(self, shard: int) -> dict[str, Any]:
        shard = int(shard)
        with self._lock:
            node_ids = [node_id for node_id in self.nodes if _shard_of(node_id, self.shard_count) == shard]
            nodes = {}
            edges = {}
            for node_id in node_ids:
                row = self.nodes[node_id]
                nodes[node_id] = [
                    round(float(row["lat"]), 7), round(float(row["lng"]), 7), int(row.get("n") or 0),
                    round(float(row.get("heading") or 0), 1),
                    round(float(row["speed"]), 1) if row.get("speed") is not None else None,
                    round(float(row["alt"]), 0) if row.get("alt") is not None else None,
                    int(row.get("last") or 0), int(row.get("level") or 0),
                    int(row.get("ix") or 0), int(row.get("iy") or 0),
                ]
                outgoing = self.edges.get(node_id) or {}
                if outgoing:
                    edges[node_id] = [
                        [
                            to_id, int(edge.get("support") or 0), round(float(edge.get("dt") or 0), 2),
                            round(float(edge.get("speed") or 0), 1), int(edge.get("last") or 0),
                            dict(edge.get("origins") or {}), list(edge.get("tracks") or []),
                            dict(edge.get("contexts") or {}),
                        ]
                        for to_id, edge in outgoing.items()
                    ]
            # Firestore cấm array lồng trong array ("invalid nested entity") nên edges
            # phải đóng gói thành chuỗi JSON. nodes giữ map->array số (hợp lệ).
            return {
                "schema": SCHEMA, "version": VERSION, "rev": self.rev, "shard": shard,
                "nodes": nodes,
                "edges_json": json.dumps(edges, separators=(",", ":")),
            }

    def load_documents(self, meta: dict[str, Any], shard_docs: Iterable[dict[str, Any]]) -> bool:
        if not isinstance(meta, dict) or meta.get("schema") != SCHEMA:
            return False
        shard_count = int(meta.get("shard_count") or self.shard_count)
        nodes: dict[str, dict[str, Any]] = {}
        edges: dict[str, dict[str, dict[str, Any]]] = {}
        index: dict[tuple[int, int, int], set[str]] = {}
        for doc in shard_docs or []:
            if not isinstance(doc, dict) or doc.get("schema") != SCHEMA:
                continue
            for node_id, packed in (doc.get("nodes") or {}).items():
                if not isinstance(packed, (list, tuple)) or len(packed) < 10:
                    continue
                row = {
                    "lat": float(packed[0]), "lng": float(packed[1]), "n": int(packed[2]),
                    "heading": float(packed[3]), "speed": packed[4], "alt": packed[5],
                    "last": int(packed[6]), "level": int(packed[7]), "ix": int(packed[8]), "iy": int(packed[9]),
                }
                nodes[node_id] = row
                index.setdefault((row["level"], row["ix"], row["iy"]), set()).add(node_id)
            packed_edge_map = doc.get("edges") or {}
            raw_edges_json = doc.get("edges_json")
            if isinstance(raw_edges_json, str) and raw_edges_json:
                # Định dạng mới: edges nằm trong chuỗi JSON (né giới hạn Firestore).
                try:
                    packed_edge_map = json.loads(raw_edges_json)
                except ValueError:
                    packed_edge_map = {}
            for from_id, packed_edges in (packed_edge_map or {}).items():
                outgoing = {}
                for packed in packed_edges or []:
                    if not isinstance(packed, (list, tuple)) or len(packed) < 7:
                        continue
                    outgoing[str(packed[0])] = {
                        "to": str(packed[0]), "support": int(packed[1]), "dt": float(packed[2]),
                        "speed": float(packed[3]), "last": int(packed[4]),
                        "origins": dict(packed[5] or {}), "tracks": list(packed[6] or []),
                        "contexts": dict(packed[7] or {}) if len(packed) >= 8 else {},
                    }
                if outgoing:
                    edges[from_id] = outgoing
        with self._lock:
            self.shard_count = max(4, shard_count)
            self.nodes = nodes
            self.edges = edges
            self._index = index
            self.metrics = dict(meta.get("metrics") or {})
            self.track_states = dict(meta.get("tracks") or {})
            self.rev = int(meta.get("rev") or 0)
            self.updated_ms = int(meta.get("updated_ms") or 0)
            self._dirty_shards.clear()
        return True

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                **self.export_meta(),
                "dirty_shards": len(self._dirty_shards),
                "track_count": len(self.track_states),
                "quality": {LEVELS[level]["name"]: self._metric_summary(level) for level in LEVELS},
            }


class RadarInkOutbox:
    """Hàng đợi SQLite trên từng máy radar; mất mạng/Render ngủ không mất vết."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path, timeout=10.0)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        return con

    def _ensure_schema(self) -> None:
        with self._lock, closing(self._connect()) as con, con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS tracks (
                  track_id TEXT PRIMARY KEY,
                  meta_json TEXT NOT NULL,
                  updated_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS points (
                  track_id TEXT NOT NULL,
                  ts INTEGER NOT NULL,
                  payload_json TEXT NOT NULL,
                  uploaded INTEGER NOT NULL DEFAULT 0,
                  PRIMARY KEY(track_id, ts)
                );
                CREATE INDEX IF NOT EXISTS idx_points_pending ON points(uploaded, track_id, ts);
                """
            )

    def add(self, track_id: str, meta: dict[str, Any], points: Iterable[Any]) -> int:
        rows = []
        for raw in points or []:
            if not isinstance(raw, (list, tuple)) or len(raw) < 3 or not _finite(raw[0]):
                continue
            ts = int(float(raw[0]))
            if ts < 1_000_000_000_000:
                ts *= 1000
            rows.append((track_id, ts, json.dumps(list(raw), ensure_ascii=False, separators=(",", ":"))))
        if not track_id or not rows:
            return 0
        with self._lock, closing(self._connect()) as con, con:
            con.execute(
                "INSERT INTO tracks(track_id,meta_json,updated_ms) VALUES(?,?,?) "
                "ON CONFLICT(track_id) DO UPDATE SET meta_json=excluded.meta_json,updated_ms=excluded.updated_ms",
                (track_id, json.dumps(meta or {}, ensure_ascii=False, separators=(",", ":")), int(time.time() * 1000)),
            )
            before = con.total_changes
            con.executemany("INSERT OR IGNORE INTO points(track_id,ts,payload_json,uploaded) VALUES(?,?,?,0)", rows)
            return max(0, con.total_changes - before)

    def batch(self, max_points: int = 2500, max_tracks: int = 12) -> tuple[list[dict[str, Any]], list[tuple[str, int]]]:
        with self._lock, closing(self._connect()) as con, con:
            raw = con.execute(
                "SELECT p.track_id,p.ts,p.payload_json,t.meta_json FROM points p "
                "JOIN tracks t ON t.track_id=p.track_id WHERE p.uploaded=0 ORDER BY p.track_id,p.ts LIMIT ?",
                (max(1, int(max_points)),),
            ).fetchall()
        grouped: dict[str, dict[str, Any]] = {}
        ids: list[tuple[str, int]] = []
        for track_id, ts, payload_json, meta_json in raw:
            if track_id not in grouped and len(grouped) >= max_tracks:
                break
            item = grouped.setdefault(track_id, {"track_id": track_id, **json.loads(meta_json or "{}"), "points": []})
            item["points"].append(json.loads(payload_json))
            ids.append((track_id, int(ts)))
        return list(grouped.values()), ids

    def mark_uploaded(self, ids: Iterable[tuple[str, int]]) -> int:
        rows = list(ids or [])
        if not rows:
            return 0
        with self._lock, closing(self._connect()) as con, con:
            con.executemany("UPDATE points SET uploaded=1 WHERE track_id=? AND ts=?", rows)
            # Chỉ dọn dữ liệu đã gửi quá 14 ngày; vết chưa gửi được giữ nguyên để retry.
            cutoff = int(time.time() * 1000) - 14 * 24 * 3600 * 1000
            con.execute("DELETE FROM points WHERE uploaded=1 AND ts<?", (cutoff,))
            return len(rows)

    def pending_count(self) -> int:
        with self._lock, closing(self._connect()) as con, con:
            row = con.execute("SELECT COUNT(*) FROM points WHERE uploaded=0").fetchone()
            return int(row[0] if row else 0)
