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
            "landed": self.landed,
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

def _classify_state(
    on_ground: bool,
    alt_ft: Optional[int],
    gs_kt: Optional[int],
    dist_km: Optional[float],
    real_arrival_ms: Optional[int],
    fr24_live: bool,
    now_ms: int,
) -> str:
    # Touchdown đã chắc chắn
    if on_ground:
        return "LANDED"
    if real_arrival_ms is not None and real_arrival_ms <= now_ms:
        return "LANDED"
    if not fr24_live:
        return "LANDED"

    # Physical detection phòng khi on_ground không ổn định
    if alt_ft is not None and gs_kt is not None:
        if alt_ft <= DAD_FIELD_ELEV_FT + TOUCHDOWN_ALT_AGL_FT and gs_kt < TOUCHDOWN_GS_KT:
            return "LANDED"

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

    # Sticky LANDED: đã hạ rồi thì không revert
    if old and old.state == "LANDED":
        state = "LANDED"

    distance_rounded = round(sensors["distance_km"], 1) if sensors["distance_km"] is not None else None

    # ETA cuối cùng theo state
    if state == "LANDED":
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
                latitude=sensors["lat"],
                longitude=sensors["lng"],
                heading=sensors["heading"],
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

    return FlightEntry(
        state=state,
        eta_millis=eta_ms,
        confidence=confidence,
        altitude_ft=sensors["alt_ft"],
        ground_speed_kt=sensors["gs_kt"],
        vertical_speed=sensors["vs"],
        distance_km=distance_rounded,
        on_ground=sensors["on_ground"],
        latitude=sensors["lat"],
        longitude=sensors["lng"],
        heading=sensors["heading"],
        updated_at=now_ms,
        history=history,
        miss_count=0,
        landed=(state == "LANDED"),
    )


def _process_miss(code: str, old: Optional[FlightEntry], now_ms: int) -> Optional[FlightEntry]:
    """Mã KHÔNG xuất hiện trong scan cycle này. Giữ vị trí cuối để map vẫn vẽ được."""
    if old is None:
        return None

    # Đã LANDED: giữ nguyên, không bump
    if old.state == "LANDED":
        return old

    miss_count = old.miss_count + 1

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

    # Match codes. Pre-filter theo destination_iata để bỏ qua tàu rõ ràng không về DAD.
    targets = {}
    for f in flights:
        number = (getattr(f, "number", None) or "").upper()
        callsign = (getattr(f, "callsign", None) or "").upper()
        dest = (getattr(f, "destination_airport_iata", None) or "").upper()
        if dest and dest != TARGET_AIRPORT:
            continue
        if number and number in queue_snapshot:
            targets[number] = f
        elif callsign and callsign in queue_snapshot:
            targets[callsign] = f

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


def _try_immediate_poll(reason: str = "") -> bool:
    global LAST_IMMEDIATE_POLL_MS, LAST_ERROR
    with _immediate_poll_lock:
        now = int(time.time() * 1000)
        if now - LAST_IMMEDIATE_POLL_MS < IMMEDIATE_POLL_COOLDOWN_MS:
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
        })


@app.route("/api/etas", methods=["GET", "POST"])
def get_all_etas():
    if (err := require_api_key()):
        return err
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        codes = body.get("codes", []) or []
        cleaned = [c.strip().upper() for c in codes if isinstance(c, str) and c.strip()]
        if cleaned:
            with _lock:
                new_codes = [c for c in cleaned if c not in TRACKED]
                QUEUE.update(cleaned)
            if new_codes:
                _try_immediate_poll(f"new {new_codes}")
    return jsonify(build_etas_payload())


@app.route("/api/track/<flight_code>", methods=["POST"])
def add_track(flight_code):
    if (err := require_api_key()):
        return err
    code = flight_code.strip().upper()
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
    code = flight_code.strip().upper()
    with _lock:
        QUEUE.discard(code)
        TRACKED.pop(code, None)
    return jsonify({"status": "success", "tracked": sorted(QUEUE)})


@app.route("/api/get_eta/<flight_code>", methods=["GET"])
def get_eta(flight_code):
    if (err := require_api_key()):
        return err
    code = flight_code.strip().upper()
    with _lock:
        QUEUE.add(code)
        entry = TRACKED.get(code)

    if entry is None:
        _try_immediate_poll(f"miss {code}")
        with _lock:
            entry = TRACKED.get(code)

    now_ms = int(time.time() * 1000)
    if entry is None or entry.eta_millis is None:
        return jsonify({
            "status": "pending",
            "flight_code": code,
            "message": f"Chưa có ETA cho {code}",
            "server_time_millis": now_ms,
        })

    public = entry.to_public(now_ms)
    public.update({
        "status": "success",
        "flight_code": code,
        "destination": "Da Nang (DAD)",
        "server_time_millis": now_ms,
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
