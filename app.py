"""
ETA tracker cho tàu bay về Đà Nẵng (DAD).

Pipeline:
  - Poller chạy nền mỗi POLL_INTERVAL giây, quét vùng bounds bao DAD.
  - Với mỗi mã trong QUEUE: gọi get_flight_details song song.
  - Phân loại trạng thái theo altitude/ground_speed/distance:
      PENDING → EN_ROUTE → APPROACH → FINAL → LANDED
      (LOST nếu mất tín hiệu lâu mà không ở giai đoạn approach/final)
  - ETA = blend(FR24 estimated.arrival, physics distance/ground_speed)
    theo state. Smooth bằng EMA + median outlier filter.
  - Touchdown phát hiện qua on_ground flag, time.real.arrival,
    hoặc altitude ≤ field_elev + 200ft && ground_speed < 80kt.
  - Khi flight mất khỏi scan: bump miss_count. FINAL + miss≥2 → LANDED
    (FR24 thường ngừng track sau touchdown). EN_ROUTE + miss≥5 → LOST.
  - v2.1: thêm latitude, longitude, heading để app vẽ bản đồ.

API giữ tương thích bản cũ: /api/etas, /api/track/<code>, /api/get_eta/<code>.
Response v2.1 thêm: state, confidence, altitude_ft, ground_speed_kt,
distance_km, on_ground, latitude, longitude, heading, stale_seconds, landed.
"""
from __future__ import annotations

from flask import Flask, jsonify, request
from FlightRadar24 import FlightRadar24API
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass, field
from typing import Optional
import hmac
import logging
import math
import os
import threading
import time

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ===========================================================
# CẤU HÌNH
# ===========================================================

ETA_API_KEY = os.environ.get(
    "ETA_API_KEY",
    "0982e7c09397ca3ed579775a9a29ff208a1715d0526fbb65b67a61c1cb126923",
)

# Bounds bao quanh các route đến DAD (north,south,west,east).
# Mở rộng đông tới 123° để bắt route từ Manila/Đài Loan, nam tới 1°
# để bắt route từ Singapore/Jakarta khi đã qua eo Malay.
BOUNDS_DAD = os.environ.get("ETA_BOUNDS", "27,1,98,123")

TARGET_AIRPORT = "DAD"
DAD_LAT = 16.0439
DAD_LNG = 108.1989
DAD_FIELD_ELEV_FT = 33  # Đà Nẵng gần mực nước biển

# Nhịp poll
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
IMMEDIATE_POLL_COOLDOWN_MS = 10_000
# Nếu app hỏi lại mà dữ liệu cũ hơn ngưỡng này thì server poll FR24 ngay,
# không chỉ trả cache. Dùng để bảo đảm marker bản đồ chạy theo vị trí thật.
POSITION_REFRESH_STALE_MS = int(os.environ.get("POSITION_REFRESH_STALE_MS", "45000"))

# Auto-discovery: quét mọi chuyến đang bay về DAD trong N phút tới.
AUTO_SCAN_DEFAULT_MINUTES = int(os.environ.get("AUTO_SCAN_DEFAULT_MINUTES", "60"))
AUTO_SCAN_MAX_MINUTES = int(os.environ.get("AUTO_SCAN_MAX_MINUTES", "180"))
AUTO_SCAN_MAX_CANDIDATES = int(os.environ.get("AUTO_SCAN_MAX_CANDIDATES", "120"))
AUTO_SCAN_MAX_RESULTS = int(os.environ.get("AUTO_SCAN_MAX_RESULTS", "60"))

# Parallel get_flight_details
DETAIL_TIMEOUT_S = 8
DETAIL_POOL_SIZE = 5

# Ngưỡng phân loại trạng thái
FINAL_ALT_FT = 3000           # altitude < 3000 ft + dist < 20 km → FINAL
FINAL_DIST_KM = 20
APPROACH_ALT_FT = 10_000      # altitude < 10000 ft + dist < 100 km → APPROACH
APPROACH_DIST_KM = 100
TOUCHDOWN_ALT_AGL_FT = 200    # alt - field_elev ≤ 200 ft
TOUCHDOWN_GS_KT = 80          # ground_speed < 80 kt

# Validate ETA: bỏ qua nếu quá khứ hoặc tương lai quá xa
ETA_MIN_FUTURE_MS = -60_000           # cho phép lệch 60s do clock skew
ETA_MAX_FUTURE_MS = 5 * 3600 * 1000   # tối đa 5 tiếng

# Smoothing
ETA_HISTORY_LEN = 3
ETA_JUMP_THRESHOLD_MS = 5 * 60 * 1000   # nhảy > 5 phút → median outlier
EMA_ALPHA = 0.3                         # 30% giá trị mới + 70% cũ
APPROACH_FR24_WEIGHT = 0.6              # blend FR24 vs physics ở APPROACH
APPROACH_PHYS_WEIGHT = 0.4

# Miss handling
MISS_LANDED_FROM_FINAL = 2   # state cũ = FINAL & miss ≥ 2 → LANDED
MISS_DROP_THRESHOLD = 5      # miss ≥ 5 → LOST (hoặc LANDED nếu trước đó APPROACH)

# Theo dõi sau hạ cánh: tiếp tục refresh khi tàu đã touchdown/taxi,
# chỉ dừng khi speed rất thấp gần sân bay hoặc mất track quá lâu sau touchdown.
PARKED_GS_KT = int(os.environ.get("PARKED_GS_KT", "5"))
PARKED_DIST_KM = float(os.environ.get("PARKED_DIST_KM", "5.0"))
LANDED_MISS_TO_PARKED = int(os.environ.get("LANDED_MISS_TO_PARKED", "12"))
GROUND_ACTIVE_STATES = {"LANDED", "TAXIING"}
TERMINAL_STATES = {"PARKED", "LOST"}

# ===========================================================
# STATE TOÀN CỤC
# ===========================================================

_lock = threading.RLock()
_immediate_poll_lock = threading.Lock()
_poller_started = False
_poller_started_lock = threading.Lock()

QUEUE: set = set()
TRACKED: dict = {}
LAST_POLL_MS: Optional[int] = None
LAST_IMMEDIATE_POLL_MS = 0
LAST_ERROR: Optional[str] = None
LAST_POLL_DURATION_MS: Optional[int] = None


@dataclass
class FlightEntry:
    """Trạng thái 1 mã đang theo dõi."""
    state: str = "PENDING"               # PENDING/EN_ROUTE/APPROACH/FINAL/LANDED/LOST
    eta_millis: Optional[int] = None
    confidence: str = "MEDIUM"           # HIGH/MEDIUM/LOW
    altitude_ft: Optional[int] = None
    ground_speed_kt: Optional[int] = None
    vertical_speed: Optional[int] = None
    distance_km: Optional[float] = None
    on_ground: bool = False
    # Vị trí + hướng để app vẽ marker bản đồ
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    heading: Optional[int] = None
    updated_at: int = 0                  # ms — lần cuối có data thật
    history: list = field(default_factory=list)
    miss_count: int = 0
    landed: bool = False

    def to_public(self, now_ms: int) -> dict:
        stale_seconds = (now_ms - self.updated_at) // 1000 if self.updated_at else None
        return {
            "state": self.state,
            "eta_millis": self.eta_millis,
            "confidence": self.confidence,
            "altitude_ft": self.altitude_ft,
            "ground_speed_kt": self.ground_speed_kt,
            "vertical_speed": self.vertical_speed,
            "distance_km": self.distance_km,
            "on_ground": self.on_ground,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "heading": self.heading,
            "updated_at": self.updated_at,
            "stale_seconds": stale_seconds,
            "stale": stale_seconds is not None and stale_seconds > 600,
            "landed": self.state in ("LANDED", "TAXIING", "PARKED") or self.landed,
            "taxiing": self.state == "TAXIING",
            "parked": self.state == "PARKED",
            "miss_count": self.miss_count,
        }


# ===========================================================
# HÌNH HỌC & VẬT LÝ
# ===========================================================

def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Khoảng cách great-circle giữa 2 điểm (km)."""
    R = 6371.0
    rlat1, rlng1, rlat2, rlng2 = map(math.radians, [lat1, lng1, lat2, lng2])
    dlat = rlat2 - rlat1
    dlng = rlng2 - rlng1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlng / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _physics_eta_ms(distance_km: Optional[float], gs_kt: Optional[int], now_ms: int) -> Optional[int]:
    """ETA = distance / ground_speed + buffer descent 1 phút.

    Không dùng được nếu thiếu data hoặc tốc độ quá thấp (taxi/đứng yên).
    """
    if distance_km is None or not gs_kt or gs_kt < 50:
        return None
    gs_kmh = gs_kt * 1.852
    hours = distance_km / gs_kmh
    eta_ms = now_ms + int(hours * 3600 * 1000)
    if distance_km > 5:
        eta_ms += 60_000  # buffer 1 phút cho descent/alignment cuối
    return eta_ms


# ===========================================================
# PHÂN LOẠI TRẠNG THÁI
# ===========================================================

def _is_parked_on_ground(gs_kt: Optional[int], dist_km: Optional[float]) -> bool:
    """Tàu được xem là đã vào bến/dừng khi ground speed rất thấp gần DAD."""
    if gs_kt is None or dist_km is None:
        return False
    return gs_kt <= PARKED_GS_KT and dist_km <= PARKED_DIST_KM


def _classify_state(
    on_ground: bool,
    alt_ft: Optional[int],
    gs_kt: Optional[int],
    dist_km: Optional[float],
    real_arrival_ms: Optional[int],
    fr24_live: bool,
    now_ms: int,
) -> str:
    # Sau touchdown vẫn tiếp tục theo dõi. Không dùng LANDED làm state kết thúc nữa.
    if on_ground:
        return "PARKED" if _is_parked_on_ground(gs_kt, dist_km) else "TAXIING"

    if real_arrival_ms is not None and real_arrival_ms <= now_ms:
        return "PARKED" if _is_parked_on_ground(gs_kt, dist_km) else "LANDED"

    if not fr24_live:
        # FR24 có thể tắt live ngay sau touchdown. Chưa xem là PARKED nếu không có speed thấp.
        return "PARKED" if _is_parked_on_ground(gs_kt, dist_km) else "LANDED"

    # Physical detection phòng khi on_ground không ổn định
    if alt_ft is not None and gs_kt is not None:
        if alt_ft <= DAD_FIELD_ELEV_FT + TOUCHDOWN_ALT_AGL_FT and gs_kt < TOUCHDOWN_GS_KT:
            return "PARKED" if _is_parked_on_ground(gs_kt, dist_km) else "TAXIING"

    # Thiếu data → mặc định EN_ROUTE để app vẫn nhận ETA từ FR24
    if alt_ft is None or dist_km is None:
        return "EN_ROUTE"

    if alt_ft < FINAL_ALT_FT and dist_km < FINAL_DIST_KM:
        return "FINAL"
    if alt_ft < APPROACH_ALT_FT and dist_km < APPROACH_DIST_KM:
        return "APPROACH"
    return "EN_ROUTE"


# ===========================================================
# ETA BLEND + SMOOTH
# ===========================================================

def _validate_eta(eta_ms: Optional[int], now_ms: int) -> bool:
    if eta_ms is None:
        return False
    if eta_ms < now_ms + ETA_MIN_FUTURE_MS:
        return False
    if eta_ms > now_ms + ETA_MAX_FUTURE_MS:
        return False
    return True


def _blend_eta(state: str, fr24_eta: Optional[int], physics_eta: Optional[int]) -> Optional[int]:
    """FINAL tin physics; APPROACH blend; EN_ROUTE tin FR24."""
    if state == "FINAL":
        return physics_eta if physics_eta is not None else fr24_eta
    if state == "APPROACH":
        if fr24_eta is not None and physics_eta is not None:
            return int(APPROACH_FR24_WEIGHT * fr24_eta + APPROACH_PHYS_WEIGHT * physics_eta)
        return fr24_eta if fr24_eta is not None else physics_eta
    return fr24_eta if fr24_eta is not None else physics_eta


def _smooth_eta(raw_new: int, history: list, state: str) -> int:
    """EMA cho EN_ROUTE/APPROACH, bypass cho FINAL. Median khi gặp outlier > 5 phút."""
    if state == "FINAL" or not history:
        return raw_new
    last = history[-1]
    if abs(raw_new - last) > ETA_JUMP_THRESHOLD_MS:
        sorted_vals = sorted(history + [raw_new])
        return sorted_vals[len(sorted_vals) // 2]
    return int(EMA_ALPHA * raw_new + (1 - EMA_ALPHA) * last)


def _compute_confidence(history: list) -> str:
    if len(history) < 2:
        return "MEDIUM"
    spread = max(history) - min(history)
    if spread < 2 * 60 * 1000:
        return "HIGH"
    if spread < 5 * 60 * 1000:
        return "MEDIUM"
    return "LOW"


# ===========================================================
# CHUẨN HÓA MÃ BAY & REFRESH CACHE
# ===========================================================

def _normalize_code(value) -> str:
    """Chuẩn hóa mã bay để so khớp ổn định: 'VJ 962' == 'VJ962'."""
    if value is None:
        return ""
    return "".join(ch for ch in str(value).upper().strip() if ch.isalnum())


def _clean_code(value) -> str:
    """Chuẩn hóa mã từ app trước khi đưa vào QUEUE."""
    return _normalize_code(value)


def _code_aliases(value) -> set[str]:
    """
    Tạo alias IATA/ICAO thường gặp để tránh mất match giữa lịch bay và FR24.
    Ví dụ: VN134 ↔ HVN134, VJ962 ↔ VJC962.
    """
    base = _normalize_code(value)
    if not base:
        return set()
    aliases = {base}
    airline_aliases = {
        "VN": "HVN",
        "HVN": "VN",
        "VJ": "VJC",
        "VJC": "VJ",
        "QH": "BAV",
        "BAV": "QH",
        "VU": "VAG",
        "VAG": "VU",
    }
    for prefix, alt_prefix in airline_aliases.items():
        if base.startswith(prefix) and len(base) > len(prefix):
            suffix = base[len(prefix):]
            if suffix.isdigit():
                aliases.add(alt_prefix + suffix)
    return aliases


def _entry_needs_refresh(entry: Optional[FlightEntry], now_ms: int) -> bool:
    """Có cần poll lại FR24 để cập nhật live position không."""
    if entry is None:
        return True
    if entry.state in TERMINAL_STATES:
        return False
    if not entry.updated_at:
        return True
    return now_ms - entry.updated_at >= POSITION_REFRESH_STALE_MS


def _server_needs_poll_for_codes(codes: list[str], now_ms: int) -> bool:
    """Bảo vệ trường hợp background poller không chạy / HTTP chỉ đang trả cache cũ."""
    if not codes:
        return False
    if LAST_POLL_MS is None:
        return True
    if now_ms - LAST_POLL_MS >= POSITION_REFRESH_STALE_MS:
        return True
    with _lock:
        return any(_entry_needs_refresh(TRACKED.get(code), now_ms) for code in codes)


# ===========================================================
# TRÍCH XUẤT DỮ LIỆU TỪ FR24
# ===========================================================

def _extract_sensors(flight) -> dict:
    """Đọc altitude/speed/lat/lng/on_ground/heading từ Flight object, sanitize."""
    lat = getattr(flight, "latitude", None)
    lng = getattr(flight, "longitude", None)
    alt = getattr(flight, "altitude", None)
    gs = getattr(flight, "ground_speed", None)
    vs = getattr(flight, "vertical_speed", None)
    heading = getattr(flight, "heading", None)
    on_ground = (getattr(flight, "on_ground", 0) or 0) == 1

    # Giá trị ≤0 thường là sentinel "không có data"
    if isinstance(alt, (int, float)) and alt <= 0:
        alt = None
    if isinstance(gs, (int, float)) and gs < 0:
        gs = None
    # heading hợp lệ 0-359
    if isinstance(heading, (int, float)):
        heading = int(heading) % 360
    else:
        heading = None

    distance_km = None
    lat_clean = None
    lng_clean = None
    if lat is not None and lng is not None:
        try:
            lat_clean = float(lat)
            lng_clean = float(lng)
            distance_km = _haversine_km(lat_clean, lng_clean, DAD_LAT, DAD_LNG)
        except (TypeError, ValueError):
            pass

    return {
        "alt_ft": int(alt) if isinstance(alt, (int, float)) else None,
        "gs_kt": int(gs) if isinstance(gs, (int, float)) else None,
        "vs": int(vs) if isinstance(vs, (int, float)) else None,
        "on_ground": on_ground,
        "distance_km": distance_km,
        "lat": lat_clean,
        "lng": lng_clean,
        "heading": heading,
    }


def _extract_eta_from_details(details: dict):
    """Trả về (fr24_eta_ms, real_arrival_ms, fr24_live, dest_iata)."""
    airport = details.get("airport") or {}
    dest = airport.get("destination") or {}
    dest_iata = ((dest.get("code") or {}).get("iata") or "").upper()

    time_info = details.get("time") or {}
    est = (time_info.get("estimated") or {}).get("arrival")
    real = (time_info.get("real") or {}).get("arrival")

    fr24_eta_ms = None
    real_arrival_ms = None
    if isinstance(est, (int, float)) and est > 0:
        fr24_eta_ms = int(est * 1000)
    if isinstance(real, (int, float)) and real > 0:
        real_arrival_ms = int(real * 1000)

    status = details.get("status") or {}
    fr24_live = bool(status.get("live", True))

    return fr24_eta_ms, real_arrival_ms, fr24_live, dest_iata


# ===========================================================
# XỬ LÝ TỪNG MÃ
# ===========================================================

def _process_match(code: str, flight, details: Optional[dict],
                   old: Optional[FlightEntry], now_ms: int) -> Optional[FlightEntry]:
    """Mã được tìm thấy trong scan. Trả về entry mới, hoặc None nếu drop."""
    sensors = _extract_sensors(flight)

    fr24_eta_ms = None
    real_arrival_ms = None
    fr24_live = True
    dest_iata = ""

    if details:
        fr24_eta_ms, real_arrival_ms, fr24_live, dest_iata = _extract_eta_from_details(details)
        # Destination rõ ràng KHÔNG phải DAD → drop khỏi TRACKED
        if dest_iata and dest_iata != TARGET_AIRPORT:
            log.info("Drop %s: destination=%s ≠ %s", code, dest_iata, TARGET_AIRPORT)
            return None

    if not _validate_eta(fr24_eta_ms, now_ms):
        fr24_eta_ms = None

    physics_eta_ms = _physics_eta_ms(sensors["distance_km"], sensors["gs_kt"], now_ms)
    if not _validate_eta(physics_eta_ms, now_ms):
        physics_eta_ms = None

    state = _classify_state(
        on_ground=sensors["on_ground"],
        alt_ft=sensors["alt_ft"],
        gs_kt=sensors["gs_kt"],
        dist_km=sensors["distance_km"],
        real_arrival_ms=real_arrival_ms,
        fr24_live=fr24_live,
        now_ms=now_ms,
    )

    # Sticky PARKED: đã dừng/vào bến rồi thì không revert về taxi/airborne.
    if old and old.state == "PARKED":
        state = "PARKED"

    distance_rounded = round(sensors["distance_km"], 1) if sensors["distance_km"] is not None else None

    # ETA cuối cùng theo state
    if state in ("LANDED", "TAXIING", "PARKED"):
        if real_arrival_ms is not None:
            eta_ms = real_arrival_ms
        elif old and old.eta_millis is not None:
            eta_ms = old.eta_millis
        else:
            eta_ms = now_ms
        history = (old.history[:] if old else [])
        confidence = "HIGH"
    else:
        raw_eta = _blend_eta(state, fr24_eta_ms, physics_eta_ms)
        if raw_eta is None or not _validate_eta(raw_eta, now_ms):
            # Cycle này không có ETA hợp lệ. Giữ ETA cũ nếu có, đánh dấu LOW.
            return FlightEntry(
                state=state,
                eta_millis=old.eta_millis if old else None,
                confidence="LOW",
                altitude_ft=sensors["alt_ft"],
                ground_speed_kt=sensors["gs_kt"],
                vertical_speed=sensors["vs"],
                distance_km=distance_rounded,
                on_ground=sensors["on_ground"],
                latitude=sensors["lat"] if sensors["lat"] is not None else (old.latitude if old else None),
                longitude=sensors["lng"] if sensors["lng"] is not None else (old.longitude if old else None),
                heading=sensors["heading"] if sensors["heading"] is not None else (old.heading if old else None),
                updated_at=now_ms,
                history=old.history[:] if old else [],
                miss_count=0,
                landed=False,
            )

        # Reset history khi sang FINAL (dynamics khác hẳn)
        history = (old.history[:] if old else [])
        if old and old.state != "FINAL" and state == "FINAL":
            history = []

        eta_ms = _smooth_eta(raw_eta, history, state)
        history = (history + [raw_eta])[-ETA_HISTORY_LEN:]
        confidence = _compute_confidence(history)

    # Log transition để debug trên Render dashboard
    if old and old.state != state:
        dist_str = f"{sensors['distance_km']:.1f}" if sensors["distance_km"] is not None else "?"
        log.info(
            "%s: %s → %s (alt=%s ft, gs=%s kt, dist=%s km)",
            code, old.state, state, sensors["alt_ft"], sensors["gs_kt"], dist_str,
        )

    # Log từng cycle để kiểm tra map có nhận live lat/lng hay server đang trả cache cũ.
    old_lat = old.latitude if old else None
    old_lng = old.longitude if old else None
    changed = (
        old is None
        or old_lat != sensors["lat"]
        or old_lng != sensors["lng"]
        or (old.heading if old else None) != sensors["heading"]
    )
    log.info(
        "Live %s state=%s eta=%s lat=%s lng=%s heading=%s changed=%s",
        code, state, eta_ms, sensors["lat"], sensors["lng"], sensors["heading"], changed,
    )

    return FlightEntry(
        state=state,
        eta_millis=eta_ms,
        confidence=confidence,
        altitude_ft=sensors["alt_ft"],
        ground_speed_kt=sensors["gs_kt"],
        vertical_speed=sensors["vs"],
        distance_km=distance_rounded,
        on_ground=sensors["on_ground"],
        latitude=sensors["lat"] if sensors["lat"] is not None else (old.latitude if old else None),
        longitude=sensors["lng"] if sensors["lng"] is not None else (old.longitude if old else None),
        heading=sensors["heading"] if sensors["heading"] is not None else (old.heading if old else None),
        updated_at=now_ms,
        history=history,
        miss_count=0,
        landed=(state in ("LANDED", "TAXIING", "PARKED")),
    )


def _process_miss(code: str, old: Optional[FlightEntry], now_ms: int) -> Optional[FlightEntry]:
    """Mã KHÔNG xuất hiện trong scan cycle này. Giữ vị trí cuối để map vẫn vẽ được."""
    if old is None:
        return None

    # Đã PARKED: trạng thái kết thúc, giữ nguyên.
    if old.state == "PARKED":
        return old

    miss_count = old.miss_count + 1

    # Sau touchdown/taxi nếu FR24 mất track quá lâu thì coi như đã vào bến/dừng.
    if old.state in GROUND_ACTIVE_STATES and miss_count >= LANDED_MISS_TO_PARKED:
        log.info("%s: %s + missed %d → PARKED", code, old.state, miss_count)
        return FlightEntry(
            state="PARKED",
            eta_millis=old.eta_millis or now_ms,
            confidence="LOW",
            altitude_ft=old.altitude_ft,
            ground_speed_kt=0 if old.ground_speed_kt is None else old.ground_speed_kt,
            vertical_speed=old.vertical_speed,
            distance_km=old.distance_km,
            on_ground=True,
            latitude=old.latitude,
            longitude=old.longitude,
            heading=old.heading,
            updated_at=old.updated_at,
            history=old.history,
            miss_count=miss_count,
            landed=True,
        )

    # Trước đó FINAL + miss ≥ 2 → tàu đã touchdown, FR24 ngừng track
    if old.state == "FINAL" and miss_count >= MISS_LANDED_FROM_FINAL:
        log.info("%s: FINAL → LANDED (missed %d cycles)", code, miss_count)
        return FlightEntry(
            state="LANDED",
            eta_millis=old.eta_millis or now_ms,
            confidence="MEDIUM",
            altitude_ft=old.altitude_ft,
            ground_speed_kt=old.ground_speed_kt,
            vertical_speed=old.vertical_speed,
            distance_km=old.distance_km,
            on_ground=old.on_ground,
            latitude=old.latitude,
            longitude=old.longitude,
            heading=old.heading,
            updated_at=old.updated_at,
            history=old.history,
            miss_count=miss_count,
            landed=True,
        )

    # Vượt ngưỡng drop
    if miss_count >= MISS_DROP_THRESHOLD:
        if old.state == "APPROACH":
            log.info("%s: APPROACH + missed %d → LANDED", code, miss_count)
            return FlightEntry(
                state="LANDED",
                eta_millis=old.eta_millis or now_ms,
                confidence="LOW",
                altitude_ft=old.altitude_ft,
                ground_speed_kt=old.ground_speed_kt,
                vertical_speed=old.vertical_speed,
                distance_km=old.distance_km,
                on_ground=old.on_ground,
                latitude=old.latitude,
                longitude=old.longitude,
                heading=old.heading,
                updated_at=old.updated_at,
                history=old.history,
                miss_count=miss_count,
                landed=True,
            )
        log.info("%s: %s + missed %d → LOST", code, old.state, miss_count)
        return FlightEntry(
            state="LOST",
            eta_millis=old.eta_millis,
            confidence="LOW",
            altitude_ft=old.altitude_ft,
            ground_speed_kt=old.ground_speed_kt,
            vertical_speed=old.vertical_speed,
            distance_km=old.distance_km,
            on_ground=old.on_ground,
            latitude=old.latitude,
            longitude=old.longitude,
            heading=old.heading,
            updated_at=old.updated_at,
            history=old.history,
            miss_count=miss_count,
            landed=False,
        )

    # Chưa tới ngưỡng — chỉ bump counter, giữ data cũ
    return FlightEntry(
        state=old.state,
        eta_millis=old.eta_millis,
        confidence=old.confidence,
        altitude_ft=old.altitude_ft,
        ground_speed_kt=old.ground_speed_kt,
        vertical_speed=old.vertical_speed,
        distance_km=old.distance_km,
        on_ground=old.on_ground,
        latitude=old.latitude,
        longitude=old.longitude,
        heading=old.heading,
        updated_at=old.updated_at,
        history=old.history,
        miss_count=miss_count,
        landed=old.landed,
    )



# ===========================================================
# AUTO-DISCOVERY ARRIVALS VỀ DAD
# ===========================================================

def _safe_nested_get(data: Optional[dict], *path, default=None):
    """Lấy field lồng nhau từ dict FR24 một cách an toàn."""
    cur = data or {}
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return cur if cur is not None else default


def _best_flight_code(flight, details: Optional[dict] = None) -> str:
    """Ưu tiên số hiệu thương mại nếu FR24 có; fallback sang callsign."""
    candidates = [
        _safe_nested_get(details, "identification", "number", "default"),
        _safe_nested_get(details, "identification", "number", "iata"),
        getattr(flight, "number", None),
        _safe_nested_get(details, "identification", "callsign"),
        getattr(flight, "callsign", None),
        _safe_nested_get(details, "identification", "number", "icao"),
    ]
    for value in candidates:
        code = _clean_code(value)
        if code:
            return code
    return ""


def _extract_arrival_metadata(flight, details: Optional[dict]) -> dict:
    """Metadata nhẹ để app tự tạo thẻ hàng chờ nếu lịch bay chưa có."""
    origin_iata = _safe_nested_get(details, "airport", "origin", "code", "iata", default="") or ""
    dest_iata = _safe_nested_get(details, "airport", "destination", "code", "iata", default="") or ""
    aircraft_type = (
        _safe_nested_get(details, "aircraft", "model", "code", default="")
        or getattr(flight, "aircraft_code", None)
        or ""
    )
    return {
        "origin_iata": str(origin_iata).upper(),
        "destination_iata": str(dest_iata).upper(),
        "aircraft_type": str(aircraft_type).upper(),
        "callsign": _normalize_code(getattr(flight, "callsign", None)),
        "number": _normalize_code(getattr(flight, "number", None)),
    }


def _eta_within_scan_window(entry: FlightEntry, now_ms: int, max_minutes: int) -> bool:
    if entry.state in TERMINAL_STATES:
        return False
    if entry.eta_millis is None:
        return False
    # Cho phép lệch quá khứ rất nhỏ do clock skew, nhưng không lấy tàu đã quá giờ lâu.
    return now_ms - 60_000 <= entry.eta_millis <= now_ms + max_minutes * 60_000


def _discover_dad_arrivals(max_minutes: int = AUTO_SCAN_DEFAULT_MINUTES, add_to_queue: bool = True) -> list[dict]:
    """
    Quét toàn vùng bounds, lấy mọi chuyến đang bay về DAD có ETA trong max_minutes.

    Khác với _do_poll(): hàm này không cần QUEUE có sẵn. Nó tự tìm ứng viên DAD,
    lấy details, tính ETA/vị trí, rồi tùy add_to_queue để đưa vào QUEUE/TRACKED.
    """
    max_minutes = max(5, min(int(max_minutes or AUTO_SCAN_DEFAULT_MINUTES), AUTO_SCAN_MAX_MINUTES))
    now_ms = int(time.time() * 1000)
    fr_api = FlightRadar24API()
    flights = fr_api.get_flights(bounds=BOUNDS_DAD)

    # Candidate nhanh: giữ lại flight có destination=DAD hoặc chưa có destination field.
    # Không bỏ flight thiếu dest vì FR24 đôi khi chỉ có dest trong get_flight_details().
    candidates = []
    for f in flights:
        dest_hint = (getattr(f, "destination_airport_iata", None) or "").upper()
        if dest_hint and dest_hint != TARGET_AIRPORT:
            continue
        candidates.append(f)
        if len(candidates) >= AUTO_SCAN_MAX_CANDIDATES:
            break

    details_by_idx = {}
    if candidates:
        with ThreadPoolExecutor(max_workers=DETAIL_POOL_SIZE) as ex:
            futures = {
                idx: ex.submit(_safe_get_details, fr_api, flight)
                for idx, flight in enumerate(candidates)
            }
            for idx, fut in futures.items():
                try:
                    details_by_idx[idx] = fut.result(timeout=DETAIL_TIMEOUT_S)
                except FuturesTimeout:
                    log.warning("Auto-scan detail timeout idx=%s", idx)
                    details_by_idx[idx] = None
                except Exception as e:
                    log.warning("Auto-scan detail fail idx=%s: %s", idx, e)
                    details_by_idx[idx] = None

    discovered: list[dict] = []
    staged_updates: dict[str, FlightEntry] = {}
    staged_queue: set[str] = set()

    for idx, flight in enumerate(candidates):
        details = details_by_idx.get(idx)
        dest_iata = ""
        fr24_eta_ms = None
        if details:
            fr24_eta_ms, _, _, dest_iata = _extract_eta_from_details(details)
        dest_hint = (getattr(flight, "destination_airport_iata", None) or "").upper()
        if dest_iata and dest_iata != TARGET_AIRPORT:
            continue
        if not dest_iata and dest_hint and dest_hint != TARGET_AIRPORT:
            continue
        if not dest_iata and not dest_hint:
            # Không xác định được có về DAD hay không thì bỏ để tránh thêm nhầm.
            continue

        code = _best_flight_code(flight, details)
        if not code:
            continue

        with _lock:
            old = TRACKED.get(code)

        entry = _process_match(code, flight, details, old, now_ms)
        if entry is None:
            continue
        if not _eta_within_scan_window(entry, now_ms, max_minutes):
            continue

        public = entry.to_public(now_ms)
        meta = _extract_arrival_metadata(flight, details)
        public.update(meta)
        public.update({
            "flight_code": code,
            "code": code,
            "destination": TARGET_AIRPORT,
        })
        discovered.append(public)
        staged_updates[code] = entry
        staged_queue.add(code)

    discovered.sort(key=lambda item: item.get("eta_millis") or 9_999_999_999_999)
    if len(discovered) > AUTO_SCAN_MAX_RESULTS:
        discovered = discovered[:AUTO_SCAN_MAX_RESULTS]
        keep_codes = {item["flight_code"] for item in discovered}
        staged_updates = {code: entry for code, entry in staged_updates.items() if code in keep_codes}
        staged_queue = {code for code in staged_queue if code in keep_codes}

    if add_to_queue and staged_queue:
        with _lock:
            QUEUE.update(staged_queue)
            TRACKED.update(staged_updates)

    log.info(
        "Auto-scan arrivals: bounds=%d, candidates=%d, found=%d, add_to_queue=%s, max_minutes=%d",
        len(flights), len(candidates), len(discovered), add_to_queue, max_minutes,
    )
    return discovered

# ===========================================================
# POLLER
# ===========================================================

def _safe_get_details(fr_api, flight):
    try:
        return fr_api.get_flight_details(flight)
    except Exception as e:
        log.warning("get_flight_details exception: %s", e)
        return None


def _do_poll() -> None:
    """1 chu kỳ poll: scan bounds + parallel get_flight_details + update TRACKED."""
    global LAST_POLL_MS, LAST_ERROR, LAST_POLL_DURATION_MS

    started = time.time()

    with _lock:
        queue_snapshot = set(QUEUE)

    if not queue_snapshot:
        LAST_POLL_MS = int(started * 1000)
        LAST_POLL_DURATION_MS = 0
        return

    fr_api = FlightRadar24API()
    flights = fr_api.get_flights(bounds=BOUNDS_DAD)

    # Match codes ổn định. Không dùng trực tiếp number/callsign làm key,
    # vì FR24 có lúc trả "VJ 962", có lúc "VJ962", có lúc callsign khác.
    queue_by_norm = {}
    for code in queue_snapshot:
        for alias in _code_aliases(code):
            queue_by_norm[alias] = code

    targets = {}
    for f in flights:
        number_raw = getattr(f, "number", None)
        callsign_raw = getattr(f, "callsign", None)
        dest = (getattr(f, "destination_airport_iata", None) or "").upper()
        if dest and dest != TARGET_AIRPORT:
            continue

        candidate_keys = set()
        candidate_keys.update(_code_aliases(number_raw))
        candidate_keys.update(_code_aliases(callsign_raw))

        for key in candidate_keys:
            if key and key in queue_by_norm:
                requested_code = queue_by_norm[key]
                targets[requested_code] = f
                break

    # Parallel fetch details (5 worker, timeout 8s/call)
    details_map = {}
    if targets:
        with ThreadPoolExecutor(max_workers=DETAIL_POOL_SIZE) as ex:
            futures = {
                code: ex.submit(_safe_get_details, fr_api, flight)
                for code, flight in targets.items()
            }
            for code, fut in futures.items():
                try:
                    details_map[code] = fut.result(timeout=DETAIL_TIMEOUT_S)
                except FuturesTimeout:
                    log.warning("Detail timeout: %s", code)
                    details_map[code] = None
                except Exception as e:
                    log.warning("Detail fail %s: %s", code, e)
                    details_map[code] = None

    now_ms = int(time.time() * 1000)
    updates = {}
    drops = []

    with _lock:
        for code in queue_snapshot:
            old = TRACKED.get(code)
            if code in targets:
                new_entry = _process_match(code, targets[code], details_map.get(code), old, now_ms)
                if new_entry is None:
                    drops.append(code)
                else:
                    updates[code] = new_entry
            else:
                new_entry = _process_miss(code, old, now_ms)
                if new_entry is not None:
                    updates[code] = new_entry

        for code, entry in updates.items():
            TRACKED[code] = entry
        for code in drops:
            TRACKED.pop(code, None)

        LAST_POLL_MS = now_ms
        LAST_ERROR = None

    duration_ms = int((time.time() - started) * 1000)
    LAST_POLL_DURATION_MS = duration_ms

    matched = sum(1 for c in queue_snapshot
                  if c in updates and updates[c].state not in ("LOST",))
    log.info(
        "Poll: bounds=%d, queue=%d, matched=%d, drops=%d, took=%dms",
        len(flights), len(queue_snapshot), matched, len(drops), duration_ms,
    )


def _try_immediate_poll(reason: str = "", bypass_cooldown: bool = False) -> bool:
    global LAST_IMMEDIATE_POLL_MS, LAST_ERROR
    with _immediate_poll_lock:
        now = int(time.time() * 1000)
        if not bypass_cooldown and now - LAST_IMMEDIATE_POLL_MS < IMMEDIATE_POLL_COOLDOWN_MS:
            log.info("Immediate poll skipped by cooldown (%s)", reason)
            return False
        LAST_IMMEDIATE_POLL_MS = now
    try:
        _do_poll()
        log.info("Immediate poll OK (%s)", reason)
        return True
    except Exception as e:
        with _lock:
            LAST_ERROR = str(e)
        log.warning("Immediate poll fail (%s): %s", reason, e)
        return False


def poll_loop() -> None:
    global LAST_ERROR
    time.sleep(2)
    while True:
        try:
            _do_poll()
        except Exception as e:
            with _lock:
                LAST_ERROR = str(e)
            log.exception("Scheduled poll failed")
        time.sleep(POLL_INTERVAL)


# ===========================================================
# HTTP HANDLERS
# ===========================================================

def require_api_key():
    client_key = request.headers.get("X-API-Key", "")
    if not ETA_API_KEY:
        return jsonify({"status": "error", "message": "Server chưa cấu hình ETA_API_KEY"}), 500
    if not hmac.compare_digest(client_key, ETA_API_KEY):
        return jsonify({"status": "error", "message": "Sai mật khẩu truy cập server"}), 401
    return None


def build_etas_payload() -> dict:
    now_ms = int(time.time() * 1000)
    with _lock:
        result = {}
        for code in QUEUE:
            entry = TRACKED.get(code)
            if entry is None:
                result[code] = {"state": "PENDING", "status": "pending"}
            else:
                result[code] = entry.to_public(now_ms)
        return {
            "status": "success",
            "server_time_millis": now_ms,
            "last_poll_millis": LAST_POLL_MS,
            "last_poll_duration_ms": LAST_POLL_DURATION_MS,
            "poll_interval_seconds": POLL_INTERVAL,
            "last_error": LAST_ERROR,
            "pid": os.getpid(),
            "flights": result,
        }


@app.route("/", methods=["GET"])
def health():
    """Health check public (không yêu cầu API key) để cron-job.org ping keep-alive."""
    with _lock:
        return jsonify({
            "status": "ok",
            "server_time_millis": int(time.time() * 1000),
            "last_poll_millis": LAST_POLL_MS,
            "last_poll_duration_ms": LAST_POLL_DURATION_MS,
            "poll_interval_seconds": POLL_INTERVAL,
            "tracked_count": len(TRACKED),
            "queue_count": len(QUEUE),
            "pid": os.getpid(),
        })



@app.route("/api/scan_arrivals", methods=["GET", "POST"])
def scan_arrivals():
    """Quét mọi tàu đang bay về DAD trong N phút tới và tùy chọn đưa vào QUEUE server."""
    if (err := require_api_key()):
        return err

    body = request.get_json(silent=True) or {}
    raw_minutes = body.get("max_minutes") or request.args.get("max_minutes") or AUTO_SCAN_DEFAULT_MINUTES
    try:
        max_minutes = int(raw_minutes)
    except (TypeError, ValueError):
        max_minutes = AUTO_SCAN_DEFAULT_MINUTES
    max_minutes = max(5, min(max_minutes, AUTO_SCAN_MAX_MINUTES))

    add_to_queue_raw = body.get("add_to_queue", request.args.get("add_to_queue", "true"))
    if isinstance(add_to_queue_raw, str):
        add_to_queue = add_to_queue_raw.strip().lower() not in ("0", "false", "no", "off")
    else:
        add_to_queue = bool(add_to_queue_raw)

    try:
        arrivals = _discover_dad_arrivals(max_minutes=max_minutes, add_to_queue=add_to_queue)
        now_ms = int(time.time() * 1000)
        with _lock:
            tracked = sorted(QUEUE)
        return jsonify({
            "status": "success",
            "server_time_millis": now_ms,
            "max_minutes": max_minutes,
            "add_to_queue": add_to_queue,
            "count": len(arrivals),
            "flights": arrivals,
            "tracked": tracked,
            "pid": os.getpid(),
        })
    except Exception as e:
        log.exception("Auto-scan arrivals failed")
        with _lock:
            global LAST_ERROR
            LAST_ERROR = str(e)
        return jsonify({
            "status": "error",
            "message": str(e),
            "server_time_millis": int(time.time() * 1000),
        }), 500


@app.route("/api/etas", methods=["GET", "POST"])
def get_all_etas():
    if (err := require_api_key()):
        return err
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        codes = body.get("codes", []) or []
        cleaned = [_clean_code(c) for c in codes if isinstance(c, str) and _clean_code(c)]
        force_refresh = bool(body.get("force_refresh") or body.get("client_time_millis"))
        if cleaned:
            now_ms = int(time.time() * 1000)
            with _lock:
                new_codes = [c for c in cleaned if c not in TRACKED]
                QUEUE.update(cleaned)
            # Bản cũ chỉ poll khi có mã mới. Vì vậy mã đã theo dõi có thể chỉ trả cache cũ,
            # làm bản đồ đứng im. Bản này poll lại khi app hỏi refresh / dữ liệu stale.
            if new_codes or force_refresh or _server_needs_poll_for_codes(cleaned, now_ms):
                _try_immediate_poll(
                    f"etas refresh new={new_codes} force={force_refresh} codes={cleaned}",
                    bypass_cooldown=force_refresh,
                )
    return jsonify(build_etas_payload())


@app.route("/api/track/<flight_code>", methods=["POST"])
def add_track(flight_code):
    if (err := require_api_key()):
        return err
    code = _clean_code(flight_code)
    with _lock:
        is_new = code not in TRACKED
        QUEUE.add(code)
    if is_new:
        _try_immediate_poll(f"track {code}")
    return jsonify({"status": "success", "tracked": sorted(QUEUE)})


@app.route("/api/track/<flight_code>", methods=["DELETE"])
def remove_track(flight_code):
    if (err := require_api_key()):
        return err
    code = _clean_code(flight_code)
    with _lock:
        QUEUE.discard(code)
        TRACKED.pop(code, None)
    return jsonify({"status": "success", "tracked": sorted(QUEUE)})


@app.route("/api/get_eta/<flight_code>", methods=["GET"])
def get_eta(flight_code):
    if (err := require_api_key()):
        return err
    code = _clean_code(flight_code)
    now_ms = int(time.time() * 1000)
    with _lock:
        QUEUE.add(code)
        entry = TRACKED.get(code)

    # GET từng chuyến cũng phải có quyền kéo live data mới, không chỉ trả cache.
    if entry is None or _entry_needs_refresh(entry, now_ms):
        _try_immediate_poll(f"get_eta refresh {code}")
        with _lock:
            entry = TRACKED.get(code)

    now_ms = int(time.time() * 1000)
    if entry is None or entry.eta_millis is None:
        return jsonify({
            "status": "pending",
            "flight_code": code,
            "message": f"Chưa có ETA cho {code}",
            "server_time_millis": now_ms,
            "pid": os.getpid(),
        })

    public = entry.to_public(now_ms)
    public.update({
        "status": "success",
        "flight_code": code,
        "destination": "Da Nang (DAD)",
        "server_time_millis": now_ms,
        "pid": os.getpid(),
    })
    return jsonify(public)


# ===========================================================
# STARTUP
# ===========================================================

def _start_poller():
    global _poller_started
    with _poller_started_lock:
        if _poller_started:
            return
        _poller_started = True
    t = threading.Thread(target=poll_loop, daemon=True, name="poll_loop")
    t.start()


_start_poller()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    # Production trên Render nên dùng gunicorn:
    #   gunicorn -w 1 -k gthread --threads 4 --timeout 90 app:app
    # (-w 1: 1 worker để chia sẻ TRACKED giữa các thread HTTP và poller.
    #  --threads 4: 4 thread HTTP, đủ cho concurrency. --timeout 90: chịu được
    #  poll dài khi nhiều flight trong queue.)
    app.run(host="0.0.0.0", port=port)
