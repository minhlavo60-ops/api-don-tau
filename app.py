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
  - v2.2: lọc theo lịch bay.
      * QUEUE chứa toàn bộ mã trong lịch bay → server bám đến cùng,
        kể cả khi FR24 không trả destination (N/A) hoặc trả nhầm.
      * SCHEDULE_HINTS giữ SIBT cho mỗi mã. Khi FR24 chưa thấy chuyến,
        server vẫn trả state SCHEDULED + ETA = SIBT để app vẽ card.
      * `_process_match` không drop chuyến nếu code đã được lịch bay
        bảo lãnh (trusted=True).

API giữ tương thích bản cũ: /api/etas, /api/track/<code>, /api/get_eta/<code>.
Response v2.3 thêm: scheduled, sibt_millis (cho state SCHEDULED) và actual_parked_* / in_block_* cho giờ tàu dừng bến.
"""
from __future__ import annotations

from flask import Flask, jsonify, request
from flask_cors import CORS
from FlightRadar24 import FlightRadar24API
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional
import base64
import hmac
import json
import logging
import math
import os
import threading
import time

try:
    import firebase_admin
    from firebase_admin import credentials, firestore as firebase_firestore
except Exception:  # firebase-admin là dependency tùy chọn; server vẫn chạy radar nếu chưa cấu hình.
    firebase_admin = None
    credentials = None
    firebase_firestore = None

app = Flask(__name__)
CORS(
    app,
    resources={r"/api/*": {
        "origins": [
            "https://nhat-ky-don.web.app",
            "https://nhat-ky-don.firebaseapp.com",
        ],
        "allow_headers": ["X-API-Key", "X-Import-Secret", "Content-Type", "Cache-Control", "Pragma"],
        "methods": ["GET", "POST", "OPTIONS"],
        "max_age": 3600,
    }},
)
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

# Nhịp poll nền tối đa. Adaptive polling bên dưới sẽ tự giảm xuống 20s/30s khi có tàu gần hạ.
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
IMMEDIATE_POLL_COOLDOWN_MS = 10_000

# Adaptive polling: giảm tải server miễn phí nhưng vẫn cập nhật dày khi tàu gần DAD/taxi.
ADAPTIVE_POLL_FAR_MS = int(os.environ.get("ADAPTIVE_POLL_FAR_MS", "60000"))       # tàu còn xa
ADAPTIVE_POLL_MID_MS = int(os.environ.get("ADAPTIVE_POLL_MID_MS", "30000"))       # approach xa / còn 10-20 phút
ADAPTIVE_POLL_NEAR_MS = int(os.environ.get("ADAPTIVE_POLL_NEAR_MS", "20000"))     # final / còn dưới 10 phút
ADAPTIVE_POLL_TAXI_MS = int(os.environ.get("ADAPTIVE_POLL_TAXI_MS", "20000"))     # đã hạ, đang taxi
ADAPTIVE_MID_REMAINING_MS = int(os.environ.get("ADAPTIVE_MID_REMAINING_MS", str(20 * 60_000)))
ADAPTIVE_NEAR_REMAINING_MS = int(os.environ.get("ADAPTIVE_NEAR_REMAINING_MS", str(10 * 60_000)))
ADAPTIVE_MID_DISTANCE_KM = float(os.environ.get("ADAPTIVE_MID_DISTANCE_KM", "120"))
ADAPTIVE_NEAR_DISTANCE_KM = float(os.environ.get("ADAPTIVE_NEAR_DISTANCE_KM", "45"))

# Nếu app hỏi lại mà dữ liệu cũ hơn ngưỡng này thì server poll FR24 ngay,
# không chỉ trả cache. Dùng làm fallback khi app không gửi min_refresh_ms.
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

# Theo dõi sau hạ cánh: tiếp tục refresh khi tàu đã touchdown/taxi.
# PARKED là mốc tàu gần như đứng yên trong khu vực sân bay, ổn định đủ lâu.
# Không nên để bán kính quá rộng vì tàu có thể dừng tạm trên taxiway/runway.
PARKED_GS_KT = int(os.environ.get("PARKED_GS_KT", "3"))
PARKED_DIST_KM = float(os.environ.get("PARKED_DIST_KM", "2.5"))
# Tàu vẫn còn radar nhưng đứng yên đủ lâu cũng được coi như đã vào bến.
# 4 phút = 240 s đủ để loại trừ dừng tạm trên taxiway và bắt kịp các trường hợp FR24 không tự tắt.
PARKED_STABLE_MS = int(os.environ.get("PARKED_STABLE_MS", "240000"))
# Nếu tàu đã LANDED/TAXIING rồi mất radar, không đợi quá lâu mới chốt PARKED.
# 4 chu kỳ poll giúp tránh tàu nằm mãi trên bản đồ khi ADS-B/FR24 tắt sau hạ cánh.
LANDED_MISS_TO_PARKED = int(os.environ.get("LANDED_MISS_TO_PARKED", "4"))
# Quy tắc nghiệp vụ mới: khi radar xác nhận tàu đã hạ cánh (marker xanh),
# lưu mốc hạ cánh. Nếu không có mốc vào bến tốt hơn, lấy hạ cánh + 3 phút
# làm giờ dừng bến/chốt hồ sơ để không sót chuyến đã có người đón.
LANDING_TO_PARK_FALLBACK_MS = int(os.environ.get("LANDING_TO_PARK_FALLBACK_MS", "180000"))
# Khi FR24 mất tín hiệu trong lúc đang taxi mà chưa có landed_at, dùng buffer cũ làm dự phòng thấp.
TAXI_BUFFER_MS = int(os.environ.get("TAXI_BUFFER_MS", "120000"))
LANDED_BUFFER_MS = int(os.environ.get("LANDED_BUFFER_MS", "180000"))
GROUND_ACTIVE_STATES = {"LANDED", "TAXIING"}
TERMINAL_STATES = {"PARKED", "LOST"}

# Lịch bay: giữ mã trong SCHEDULE_HINTS bao lâu sau SIBT trước khi prune.
# Sau khi tàu thực sự hạ cánh, FR24 sẽ chiếm ưu tiên; sau đó vẫn giữ thêm
# vài giờ để app còn xem được state PARKED kèm thông tin lịch bay.
SCHEDULE_RETAIN_PAST_MS = int(os.environ.get("SCHEDULE_RETAIN_PAST_MS", str(6 * 3600 * 1000)))
SCHEDULE_RETAIN_FUTURE_MS = int(os.environ.get("SCHEDULE_RETAIN_FUTURE_MS", str(36 * 3600 * 1000)))

# Firestore online mode: server tự đọc lịch bay đã nạp và tự ghi mốc giờ dừng bến.
# Bật mặc định. Nếu chưa cấu hình Firebase Admin, server vẫn chạy radar nhưng không tự ghi Firestore.
FIRESTORE_SYNC_ENABLED = os.environ.get("FIRESTORE_SYNC_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")
FIRESTORE_SCHEDULE_SYNC_MS = int(os.environ.get("FIRESTORE_SCHEDULE_SYNC_MS", "60000"))
# Fallback nghiệp vụ: nếu một chuyến đã có người đón nhưng radar không sinh PARKED,
# sau SIBT một khoảng an toàn server sẽ chốt bằng giờ lịch để hồ sơ không treo mãi.
SCHEDULE_FALLBACK_FINALIZE_GRACE_MS = int(os.environ.get("SCHEDULE_FALLBACK_FINALIZE_GRACE_MS", str(30 * 60 * 1000)))
FIRESTORE_SCHEDULE_FALLBACK_SYNC_MS = int(os.environ.get("FIRESTORE_SCHEDULE_FALLBACK_SYNC_MS", "60000"))
FIRESTORE_SCHEDULE_DAYS_BACK = int(os.environ.get("FIRESTORE_SCHEDULE_DAYS_BACK", "1"))
FIRESTORE_SCHEDULE_DAYS_AHEAD = int(os.environ.get("FIRESTORE_SCHEDULE_DAYS_AHEAD", "1"))
FIRESTORE_PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID", "").strip()
FIREBASE_SERVICE_ACCOUNT_JSON = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip()
FIREBASE_SERVICE_ACCOUNT_B64 = os.environ.get("FIREBASE_SERVICE_ACCOUNT_B64", "").strip()

# Gmail auto-import: mã bí mật Apps Script phải gửi trong header X-Import-Secret.
# Khuyến nghị vẫn đặt biến môi trường GMAIL_IMPORT_SECRET trên Render bằng đúng mã dưới đây.
GMAIL_IMPORT_SECRET = os.environ.get("GMAIL_IMPORT_SECRET", "dad_gmail_import_2026_pcQyLzatiQyeYTr2jZTnvcWgYOtOgXgY").strip()
GMAIL_IMPORT_MIN_FLIGHTS = int(os.environ.get("GMAIL_IMPORT_MIN_FLIGHTS", "5"))

# ===========================================================
# STATE TOÀN CỤC
# ===========================================================

_lock = threading.RLock()
_immediate_poll_lock = threading.Lock()
_poller_started = False
_poller_started_lock = threading.Lock()

QUEUE: set = set()
TRACKED: dict = {}
# code (normalized) → {sibt_millis, origin, aircraft, route, date_key, registered_at_ms}
SCHEDULE_HINTS: dict = {}
LAST_POLL_MS: Optional[int] = None
LAST_IMMEDIATE_POLL_MS = 0
LAST_ERROR: Optional[str] = None
LAST_POLL_DURATION_MS: Optional[int] = None
LAST_FIRESTORE_SCHEDULE_SYNC_MS: Optional[int] = None
LAST_FIRESTORE_SYNC_ERROR: Optional[str] = None
LAST_FIRESTORE_PARKED_WRITE_MS: Optional[int] = None
LAST_FIRESTORE_FALLBACK_WRITE_MS: Optional[int] = None
FIRESTORE_STATUS = "not_configured"
FIRESTORE_SCHEDULE_DATES: list[str] = []
PARKED_FIRESTORE_SYNCED: set[str] = set()
SCHEDULE_FALLBACK_FIRESTORE_SYNCED: set[str] = set()
_FIRESTORE_CLIENT = None

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def _format_hhmm(ms: Optional[int]) -> Optional[str]:
    """Format epoch milliseconds sang giờ Việt Nam HH:MM để app hiển thị nhanh."""
    if not isinstance(ms, int) or ms <= 0:
        return None
    return datetime.fromtimestamp(ms / 1000, VN_TZ).strftime("%H:%M")


def _format_hhmmss(ms: Optional[int]) -> Optional[str]:
    """Format epoch milliseconds sang giờ Việt Nam HH:MM:SS."""
    if not isinstance(ms, int) or ms <= 0:
        return None
    return datetime.fromtimestamp(ms / 1000, VN_TZ).strftime("%H:%M:%S")


def _to_int_ms(value) -> Optional[int]:
    """Ép timestamp từ Firestore/API về epoch milliseconds.

    Hỗ trợ:
      - int/float dạng milliseconds
      - int/float dạng seconds
      - chuỗi số
      - Firestore Timestamp / datetime có method timestamp()

    Hàm này dùng cho các luồng auto-finalize khi đọc lại mốc
    actualParkedAtMillis / actualLandedAtMillis từ Firestore.
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        n = int(value)
        if n <= 0:
            return None
        # Epoch giây thường nhỏ hơn 1e12; app/server dùng milliseconds.
        return n * 1000 if n < 1_000_000_000_000 else n

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            n = float(raw)
            if n <= 0:
                return None
            n = int(n)
            return n * 1000 if n < 1_000_000_000_000 else n
        except Exception:
            return None

    try:
        if hasattr(value, "timestamp"):
            return int(value.timestamp() * 1000)
    except Exception:
        return None

    return None


def _date_key_from_millis(ms: Optional[int] = None) -> str:
    """Date key yyyy-mm-dd theo múi giờ Việt Nam."""
    dt = datetime.fromtimestamp((ms or int(time.time() * 1000)) / 1000, VN_TZ)
    return dt.strftime("%Y-%m-%d")


def _date_key_with_offset(days: int = 0, base_ms: Optional[int] = None) -> str:
    """Date key VN với offset ngày, không cần import timedelta bằng cách cộng millis."""
    base = base_ms or int(time.time() * 1000)
    return _date_key_from_millis(base + days * 24 * 3600 * 1000)


def _parse_sibt_to_millis(value, date_key: str) -> Optional[int]:
    """Parse giờ lịch bay/SIBT từ Firestore flightPlans sang epoch ms VN."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and value > 0:
        # Hỗ trợ cả epoch giây và milliseconds nếu app/server từng lưu dạng số.
        raw = int(value)
        return raw * 1000 if raw < 1_000_000_000_000 else raw
    raw = str(value or "").strip()
    if not raw or raw in ("--", "-", "N/A", "NA"):
        return None
    # Nhận 07:05, 7:05, 0705, 705.
    digits = "".join(ch for ch in raw if ch.isdigit())
    hour = minute = None
    if ":" in raw or "h" in raw.lower() or "." in raw:
        import re
        m = re.search(r"(\d{1,2})\s*[:h.]\s*(\d{1,2})", raw.lower())
        if m:
            hour, minute = int(m.group(1)), int(m.group(2))
    elif len(digits) == 3:
        hour, minute = int(digits[:1]), int(digits[1:])
    elif len(digits) == 4:
        hour, minute = int(digits[:2]), int(digits[2:])
    elif len(digits) in (1, 2):
        hour, minute = int(digits), 0
    if hour is None or minute is None or hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    try:
        y, m, d = [int(part) for part in str(date_key).split("-")]
        dt = datetime(y, m, d, hour, minute, 0, tzinfo=VN_TZ)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _extract_origin_from_route(route: str) -> str:
    """Lấy origin từ route kiểu HAN-DAD-DAD-HAN hoặc SGN-DAD."""
    raw = str(route or "").strip().upper()
    if not raw:
        return ""
    first = raw.split("-")[0].strip()
    return first if 2 <= len(first) <= 4 else ""


def _init_firestore_client():
    """Khởi tạo Firebase Admin SDK nếu có credentials. Trả về client hoặc None."""
    global _FIRESTORE_CLIENT, FIRESTORE_STATUS, LAST_FIRESTORE_SYNC_ERROR
    if _FIRESTORE_CLIENT is not None:
        return _FIRESTORE_CLIENT
    if not FIRESTORE_SYNC_ENABLED:
        FIRESTORE_STATUS = "disabled"
        return None
    if firebase_admin is None or firebase_firestore is None:
        FIRESTORE_STATUS = "missing_firebase_admin"
        LAST_FIRESTORE_SYNC_ERROR = "Chưa cài firebase-admin trong requirements.txt"
        return None
    try:
        if not firebase_admin._apps:
            cred_obj = None
            if FIREBASE_SERVICE_ACCOUNT_JSON:
                cred_obj = credentials.Certificate(json.loads(FIREBASE_SERVICE_ACCOUNT_JSON))
            elif FIREBASE_SERVICE_ACCOUNT_B64:
                decoded = base64.b64decode(FIREBASE_SERVICE_ACCOUNT_B64).decode("utf-8")
                cred_obj = credentials.Certificate(json.loads(decoded))
            elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
                cred_obj = credentials.ApplicationDefault()
            else:
                # Trên Google Cloud có thể dùng ADC; trên Render thường cần JSON/B64.
                cred_obj = credentials.ApplicationDefault()

            options = {"projectId": FIRESTORE_PROJECT_ID} if FIRESTORE_PROJECT_ID else None
            firebase_admin.initialize_app(cred_obj, options=options)
        _FIRESTORE_CLIENT = firebase_firestore.client()
        FIRESTORE_STATUS = "ready"
        LAST_FIRESTORE_SYNC_ERROR = None
        return _FIRESTORE_CLIENT
    except Exception as e:
        FIRESTORE_STATUS = "error"
        LAST_FIRESTORE_SYNC_ERROR = str(e)
        log.warning("Firestore init failed: %s", e)
        return None


def _flight_plan_doc_to_schedule_payload(date_key: str, data: dict) -> dict:
    """Chuyển flightPlans/{date_key}.flights[] thành payload cho SCHEDULE_HINTS."""
    result: dict = {}
    flights = data.get("flights") if isinstance(data, dict) else None
    if not isinstance(flights, list):
        return result
    for item in flights:
        if not isinstance(item, dict):
            continue
        code = _clean_code(item.get("arrivalFlight") or item.get("code") or item.get("flight") or item.get("arrival_flight") or "")
        if not code:
            continue
        sibt = (
            item.get("sibt_millis") or item.get("sibtMillis") or
            _parse_sibt_to_millis(item.get("sibt") or item.get("plannedTime") or item.get("time"), date_key)
        )
        if not sibt:
            continue
        route = str(item.get("arrivalRoute") or item.get("route") or item.get("arrival_route") or "").upper()
        result[code] = {
            "sibt_millis": sibt,
            "origin": str(item.get("origin") or item.get("origin_iata") or _extract_origin_from_route(route) or "").upper(),
            "aircraft": str(item.get("aircraftType") or item.get("aircraft") or item.get("aircraft_type") or "").upper(),
            "route": route,
            "stand": str(item.get("plannedStand") or item.get("stand") or item.get("planned_stand") or "").strip(),
            "date_key": date_key,
        }
    return result


def _refresh_schedule_from_firestore(force: bool = False) -> int:
    """Server tự đọc flightPlans trong Firestore để không phụ thuộc người dùng mở web."""
    global LAST_FIRESTORE_SCHEDULE_SYNC_MS, LAST_FIRESTORE_SYNC_ERROR, FIRESTORE_SCHEDULE_DATES
    now_ms = int(time.time() * 1000)
    if not force and LAST_FIRESTORE_SCHEDULE_SYNC_MS and now_ms - LAST_FIRESTORE_SCHEDULE_SYNC_MS < FIRESTORE_SCHEDULE_SYNC_MS:
        return 0
    db = _init_firestore_client()
    if db is None:
        LAST_FIRESTORE_SCHEDULE_SYNC_MS = now_ms
        return 0

    date_keys = [
        _date_key_with_offset(offset, now_ms)
        for offset in range(-max(0, FIRESTORE_SCHEDULE_DAYS_BACK), max(0, FIRESTORE_SCHEDULE_DAYS_AHEAD) + 1)
    ]
    total_written = 0
    loaded_dates: list[str] = []
    try:
        for date_key in date_keys:
            snap = db.collection("flightPlans").document(date_key).get()
            if not snap.exists:
                continue
            payload = _flight_plan_doc_to_schedule_payload(date_key, snap.to_dict() or {})
            if not payload:
                continue
            written = _register_schedule(payload, now_ms)
            codes = [_clean_code(c) for c in payload.keys() if _clean_code(c)]
            with _lock:
                QUEUE.update(codes)
            total_written += written
            loaded_dates.append(date_key)
        LAST_FIRESTORE_SCHEDULE_SYNC_MS = now_ms
        LAST_FIRESTORE_SYNC_ERROR = None
        FIRESTORE_SCHEDULE_DATES = loaded_dates
        if loaded_dates:
            log.info("Firestore schedule sync: dates=%s registered=%d", ",".join(loaded_dates), total_written)
        return total_written
    except Exception as e:
        LAST_FIRESTORE_SCHEDULE_SYNC_MS = now_ms
        LAST_FIRESTORE_SYNC_ERROR = str(e)
        log.warning("Firestore schedule sync failed: %s", e)
        return 0


def _sync_single_parked_entry_to_firestore(code: str, entry: FlightEntry) -> bool:
    """Ghi giờ dừng bến vào pickups/{date}/flights/{code} bằng server, không cần web đang mở."""
    global LAST_FIRESTORE_PARKED_WRITE_MS, LAST_FIRESTORE_SYNC_ERROR
    if not entry or entry.state != "PARKED" or not entry.parked_at_millis:
        return False
    db = _init_firestore_client()
    if db is None:
        return False

    with _lock:
        hint = SCHEDULE_HINTS.get(code) or {}
    date_key = str(hint.get("date_key") or "").strip() or _date_key_from_millis(entry.parked_at_millis)
    doc_id = _clean_code(code)
    if not date_key or not doc_id:
        return False

    sync_key = f"{date_key}:{doc_id}:{entry.parked_at_millis}"

    # Phân biệt nguồn để dễ dọn rác và để web hiển thị mức tin cậy.
    if entry.parked_source == "signal_lost":
        new_source = "server_radar_signal_lost"
        new_confidence = "LOW"
    elif entry.parked_source == "landing_plus_3min":
        new_source = "server_landing_plus_3min"
        new_confidence = "MEDIUM"
    else:
        new_source = "server_radar_ground_stop"
        new_confidence = "HIGH"

    try:
        ref = db.collection("pickups").document(date_key).collection("flights").document(doc_id)
        snap = ref.get()
        old = snap.to_dict() or {} if snap.exists else {}
        # User đã chốt tay (quick-complete hoặc full flow) hoặc auto-finalize web đã ghi:
        # server không được phép ghi đè vì user là source-of-truth khi đã commit.
        # Chỉ chấp nhận ghi nếu pickup chưa finalized hoặc finalize do chính server radar.
        if old.get("finalized") and not (old.get("autoFinalizedByRadar") or old.get("autoFinalizedByLandingFallback")):
            PARKED_FIRESTORE_SYNCED.add(sync_key)
            return False
        # Không ghi đè mốc đáng tin cậy đã có. Riêng dữ liệu cũ do web tự suy từ state PARKED
        # ('radar_state_parked'/'web_state_parked'/'web_state_landed_stale') hoặc mốc server
        # dự phòng ('server_radar_signal_lost') thì cho phép ghi đè khi sau đó server có mốc
        # ổn định thật ('server_radar_ground_stop').
        old_picker_name = str(old.get("pickerName") or "").strip()
        old_picker_uid = str(old.get("pickerUid") or "").strip()
        old_stand = str(old.get("stand") or hint.get("stand") or "").strip()
        should_auto_finalize = bool(old_picker_name and old_stand and not old.get("finalized") and not old.get("locked"))
        old_source = str(old.get("actualParkedSource") or "")
        has_old_parked_time = bool(old.get("actualParkedAtMillis") or old.get("actualParkedTime"))
        overwritable_sources = {
            "radar_state_parked",
            "web_state_parked",
            "web_state_landed_stale",
            "server_radar_signal_lost",
            "landing_plus_3min",
            "web_landing_plus_3min",
            "server_landing_plus_3min",
        }
        if has_old_parked_time and not should_auto_finalize:
            if old_source not in overwritable_sources:
                PARKED_FIRESTORE_SYNCED.add(sync_key)
                return False
            # Mốc cũ là dự phòng. Chỉ ghi đè khi mốc mới có chất lượng cao hơn.
            if new_source != "server_radar_ground_stop":
                PARKED_FIRESTORE_SYNCED.add(sync_key)
                return False

        parked_hhmm = _format_hhmm(entry.parked_at_millis)
        parked_hhmmss = _format_hhmmss(entry.parked_at_millis)
        payload = {
            "flightCode": old.get("flightCode") or doc_id,
            "displayFlightCode": old.get("displayFlightCode") or doc_id,
            "actualLandedAtMillis": int(entry.landed_at_millis) if entry.landed_at_millis else None,
            "landedAtMillis": int(entry.landed_at_millis) if entry.landed_at_millis else None,
            "touchdownMillis": int(entry.landed_at_millis) if entry.landed_at_millis else None,
            "actualLandedTime": _format_hhmm(entry.landed_at_millis),
            "actualLandedTimeFull": _format_hhmmss(entry.landed_at_millis),
            "actualLandedSource": entry.landed_source,
            "actualParkedByRadar": new_source != "server_landing_plus_3min",
            "actualParkedByLandingFallback": new_source == "server_landing_plus_3min",
            "actualParkedAtMillis": int(entry.parked_at_millis),
            "actualParkedTime": parked_hhmm,
            "actualParkedTimeFull": parked_hhmmss,
            "actualParkedSource": new_source,
            "actualParkedConfidence": new_confidence,
            "actualParkedUpdatedAt": firebase_firestore.SERVER_TIMESTAMP,
            "radarState": entry.state,
            "radarGroundSpeedKt": entry.ground_speed_kt,
            "radarDistanceKm": entry.distance_km,
            "radarAltitudeFt": entry.altitude_ft,
            "radarUpdatedAtMillis": entry.updated_at,
            "lastUpdatedAt": firebase_firestore.SERVER_TIMESTAMP,
            "lastUpdatedByName": "server-radar",
        }

        # Nếu chuyến đã có người đón + bến, server có thể chốt hồ sơ tự động bằng giờ vào bến radar.
        # Nếu thiếu người đón/bến thì chỉ lưu actualParked*, để người dùng vào lịch sử sửa/chốt sau.
        # Nếu bản trước đã được auto-finalize bằng mốc LOW do mất tín hiệu, khi có mốc HIGH hơn thì cập nhật lại giờ chốt.
        should_refresh_auto_finalized_time = bool(
            (old.get("autoFinalizedByRadar") or old.get("autoFinalizedByLandingFallback"))
            and new_source == "server_radar_ground_stop"
        )
        if should_auto_finalize or should_refresh_auto_finalized_time:
            payload.update({
                "stand": old_stand,
                "completedTime": parked_hhmm,
                "completedTimeFull": parked_hhmmss,
                "completedAtMillis": int(entry.parked_at_millis),
                "completedDateIso": date_key,
                "completedAt": firebase_firestore.SERVER_TIMESTAMP,
                "completedSource": new_source,
                "finalized": True,
                "locked": True,
                "autoFinalizedByRadar": new_source != "server_landing_plus_3min",
                "autoFinalizedByLandingFallback": new_source == "server_landing_plus_3min",
                "autoFinalizedAt": firebase_firestore.SERVER_TIMESTAMP,
                "lastUpdatedByUid": old_picker_uid or "server-radar",
                "lastUpdatedByName": "server-radar",
            })
            if should_auto_finalize:
                payload["history"] = firebase_firestore.ArrayUnion([{
                    "action": "auto_finalize_by_landing_plus_3min" if new_source == "server_landing_plus_3min" else "auto_finalize_by_radar",
                    "pickerUid": old_picker_uid or None,
                    "pickerName": old_picker_name,
                    "stand": old_stand,
                    "completedTime": parked_hhmm,
                    "completedAtMillis": int(entry.parked_at_millis),
                    "source": new_source,
                    "confidence": new_confidence,
                    "changedByUid": "server-radar",
                    "changedByName": "server-radar",
                    "changedAtMillis": int(time.time() * 1000),
                }])
        elif old_picker_name and not (old.get("finalized") or old.get("locked")):
            payload["autoFinalizePending"] = True
            payload["autoFinalizePendingReason"] = "missing_stand" if not old_stand else "missing_required_data"

        if hint.get("origin"):
            payload.setdefault("originIata", hint.get("origin"))
        if hint.get("aircraft"):
            payload.setdefault("aircraftType", hint.get("aircraft"))
        ref.set(payload, merge=True)
        PARKED_FIRESTORE_SYNCED.add(sync_key)
        LAST_FIRESTORE_PARKED_WRITE_MS = int(time.time() * 1000)
        LAST_FIRESTORE_SYNC_ERROR = None
        log.info("Firestore parked write OK: %s/%s at %s", date_key, doc_id, payload["actualParkedTimeFull"])
        return True
    except Exception as e:
        LAST_FIRESTORE_SYNC_ERROR = str(e)
        log.warning("Firestore parked write failed %s/%s: %s", date_key, doc_id, e)
        return False


def _sync_parked_entries_to_firestore(entries: dict[str, FlightEntry]) -> int:
    """Ghi mọi entry PARKED mới vào Firestore."""
    if not entries:
        return 0
    written = 0
    for code, entry in list(entries.items()):
        try:
            if _sync_single_parked_entry_to_firestore(code, entry):
                written += 1
        except Exception as e:
            log.warning("Unexpected parked sync error for %s: %s", code, e)
    return written


def _entry_still_live_for_schedule_fallback(entry) -> bool:
    """True nếu vẫn nên chờ radar thay vì dùng SIBT fallback."""
    if not entry:
        return False
    if getattr(entry, "parked_at_millis", None) or getattr(entry, "state", "") == "PARKED":
        return False
    return getattr(entry, "state", "") in {"EN_ROUTE", "APPROACH", "FINAL", "LANDED", "TAXIING"}


def _sync_overdue_schedule_claims_to_firestore(now_ms: Optional[int] = None) -> int:
    """
    Chốt dự phòng các chuyến đã có người đón nhưng radar không sinh mốc PARKED.

    Đây là lớp an toàn cho nghiệp vụ lưu hồ sơ: radar/FR24 có thể mất tín hiệu,
    sai code-share hoặc không trả ground stop. Nếu pickup đã có pickerName và SIBT
    đã quá grace, server chốt bằng giờ lịch với confidence LOW.
    """
    global LAST_FIRESTORE_FALLBACK_WRITE_MS, LAST_FIRESTORE_SYNC_ERROR
    now_ms = int(now_ms or time.time() * 1000)
    db = _init_firestore_client()
    if db is None:
        return 0

    with _lock:
        hints_snapshot = {code: dict(hint or {}) for code, hint in SCHEDULE_HINTS.items()}
        tracked_snapshot = dict(TRACKED)

    written = 0
    for code, hint in hints_snapshot.items():
        try:
            sibt = hint.get("sibt_millis")
            if not isinstance(sibt, int):
                continue
            if now_ms < sibt + SCHEDULE_FALLBACK_FINALIZE_GRACE_MS:
                continue
            entry = tracked_snapshot.get(code)
            if _entry_still_live_for_schedule_fallback(entry):
                continue

            date_key = str(hint.get("date_key") or "").strip() or _date_key_from_millis(sibt)
            doc_id = _clean_code(code)
            if not date_key or not doc_id:
                continue
            sync_key = f"{date_key}:{doc_id}:{sibt}:schedule_fallback"
            if sync_key in SCHEDULE_FALLBACK_FIRESTORE_SYNCED:
                continue

            ref = db.collection("pickups").document(date_key).collection("flights").document(doc_id)
            snap = ref.get()
            if not snap.exists:
                continue
            old = snap.to_dict() or {}
            if old.get("finalized") or old.get("locked"):
                SCHEDULE_FALLBACK_FIRESTORE_SYNCED.add(sync_key)
                continue

            picker_name = str(old.get("pickerName") or "").strip()
            if not picker_name:
                continue
            picker_uid = str(old.get("pickerUid") or "").strip()
            stand = str(old.get("stand") or hint.get("stand") or "").strip()

            # Nếu đã có mốc radar trong doc nhưng chưa finalized, dùng mốc đó để chốt.
            # Nếu đã có mốc hạ cánh nhưng chưa có PARKED, chốt dự phòng = hạ cánh + 3 phút.
            parked_ms = _to_int_ms(old.get("actualParkedAtMillis") or old.get("parkedAtMillis"))
            source = str(old.get("actualParkedSource") or "").strip()
            confidence = str(old.get("actualParkedConfidence") or "LOW").strip()
            if not parked_ms:
                landed_ms = _to_int_ms(old.get("actualLandedAtMillis") or old.get("landedAtMillis") or old.get("touchdownMillis"))
                if landed_ms:
                    parked_ms = int(landed_ms) + LANDING_TO_PARK_FALLBACK_MS
                    source = "server_landing_plus_3min"
                    confidence = "MEDIUM"
                else:
                    parked_ms = int(sibt)
                    source = "schedule_overdue_fallback"
                    confidence = "LOW"
            if source == "schedule_overdue_fallback" or not old.get("actualParkedAtMillis"):
                confidence = "MEDIUM" if source == "server_landing_plus_3min" else "LOW"

            hhmm = _format_hhmm(parked_ms)
            hhmmss = _format_hhmmss(parked_ms)
            payload = {
                "flightCode": old.get("flightCode") or doc_id,
                "displayFlightCode": old.get("displayFlightCode") or doc_id,
                "pickerUid": picker_uid,
                "pickerName": picker_name,
                "stand": stand,
                "actualParkedByRadar": bool(source and source not in ("schedule_overdue_fallback", "server_landing_plus_3min")),
                "actualParkedByLandingFallback": source == "server_landing_plus_3min",
                "actualParkedByScheduleFallback": source == "schedule_overdue_fallback",
                "actualParkedAtMillis": int(parked_ms),
                "actualParkedTime": hhmm,
                "actualParkedTimeFull": hhmmss,
                "actualParkedSource": source,
                "actualParkedConfidence": confidence,
                "actualParkedUpdatedAt": firebase_firestore.SERVER_TIMESTAMP,
                "completedTime": hhmm,
                "completedTimeFull": hhmmss,
                "completedAtMillis": int(parked_ms),
                "completedDateIso": date_key,
                "completedAt": firebase_firestore.SERVER_TIMESTAMP,
                "completedSource": source,
                "finalized": True,
                "locked": True,
                "autoFinalizedByScheduleFallback": source == "schedule_overdue_fallback",
                "autoFinalizedAt": firebase_firestore.SERVER_TIMESTAMP,
                "lastUpdatedAt": firebase_firestore.SERVER_TIMESTAMP,
                "lastUpdatedByUid": picker_uid or "server-schedule-fallback",
                "lastUpdatedByName": "server-schedule-fallback",
                "history": firebase_firestore.ArrayUnion([{
                    "action": "auto_finalize_by_schedule_fallback",
                    "pickerUid": picker_uid or None,
                    "pickerName": picker_name,
                    "stand": stand or None,
                    "completedTime": hhmm,
                    "completedAtMillis": int(parked_ms),
                    "source": source,
                    "confidence": confidence,
                    "changedByUid": "server-schedule-fallback",
                    "changedByName": "server-schedule-fallback",
                    "changedAtMillis": now_ms,
                }])
            }
            if hint.get("origin"):
                payload.setdefault("originIata", hint.get("origin"))
            if hint.get("aircraft"):
                payload.setdefault("aircraftType", hint.get("aircraft"))

            ref.set(payload, merge=True)
            SCHEDULE_FALLBACK_FIRESTORE_SYNCED.add(sync_key)
            LAST_FIRESTORE_FALLBACK_WRITE_MS = now_ms
            LAST_FIRESTORE_SYNC_ERROR = None
            written += 1
            log.info("Firestore schedule fallback finalize OK: %s/%s at %s", date_key, doc_id, hhmmss)
        except Exception as e:
            LAST_FIRESTORE_SYNC_ERROR = str(e)
            log.warning("Firestore schedule fallback finalize failed %s: %s", code, e)
    return written


@dataclass
class FlightEntry:
    """Trạng thái 1 mã đang theo dõi."""
    state: str = "PENDING"               # PENDING/SCHEDULED/EN_ROUTE/APPROACH/FINAL/LANDED/TAXIING/PARKED/LOST
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
    # Mốc tàu vừa hạ cánh/touchdown. Đây là lớp dự phòng quan trọng:
    # nếu không lấy được giờ vào bến từ radar, chốt bằng landed_at + 3 phút.
    landed_at_millis: Optional[int] = None
    landed_source: Optional[str] = None  # "radar_ground" | "real_arrival" | "final_missed" | "approach_missed"
    # Mốc giờ tàu dừng hẳn/vào bến suy luận từ radar.
    # parked_candidate_since_ms là thời điểm bắt đầu thấy tín hiệu đứng yên;
    # parked_at_millis chỉ được chốt sau khi tín hiệu ổn định PARKED_STABLE_MS
    # hoặc khi FR24 mất tín hiệu hẳn lúc tàu đang trên mặt đất.
    # parked_source giúp Firestore/web phân biệt mốc nào do tàu đứng yên ổn định
    # (ground_stop) vs mốc dự phòng do mất tín hiệu (signal_lost).
    parked_at_millis: Optional[int] = None
    parked_candidate_since_ms: Optional[int] = None
    parked_source: Optional[str] = None  # "ground_stop" | "signal_lost"

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
            # Alias rõ ràng cho frontend: tránh frontend fallback sang serverNow khiến marker cũ tưởng như mới.
            "updated_at_millis": self.updated_at,
            "server_seen_millis": self.updated_at,
            "last_seen_millis": self.updated_at,
            "stale_seconds": stale_seconds,
            "stale": stale_seconds is not None and stale_seconds > 600,
            "landed": self.state in ("LANDED", "TAXIING", "PARKED") or self.landed,
            "taxiing": self.state == "TAXIING",
            "parked": self.state == "PARKED",
            "miss_count": self.miss_count,
            # Mốc hạ cánh/touchdown. Frontend dùng mốc này để chốt dự phòng +3 phút
            # nếu radar không sinh được mốc vào bến thực tế.
            "actual_landed_at_millis": self.landed_at_millis,
            "landed_at_millis": self.landed_at_millis,
            "touchdown_millis": self.landed_at_millis,
            "actual_landed_time": _format_hhmm(self.landed_at_millis),
            "actual_landed_time_full": _format_hhmmss(self.landed_at_millis),
            "actual_landed_source": self.landed_source,
            # Các alias cùng chỉ về một mốc: giờ tàu dừng bến/in-block suy luận từ radar.
            # Giữ nhiều tên field để app cũ/mới đều đọc được.
            "actual_parked_at_millis": self.parked_at_millis,
            "parked_at_millis": self.parked_at_millis,
            "in_block_millis": self.parked_at_millis,
            "on_block_millis": self.parked_at_millis,
            "stand_arrival_millis": self.parked_at_millis,
            "stopped_at_millis": self.parked_at_millis,
            "actual_parked_time": _format_hhmm(self.parked_at_millis),
            "actual_parked_time_full": _format_hhmmss(self.parked_at_millis),
            "actual_parked_source": (
                ("radar_signal_lost" if self.parked_source == "signal_lost" else "radar_ground_stop")
                if self.parked_at_millis else None
            ),
            "actual_parked_confidence": (
                ("LOW" if self.parked_source == "signal_lost" else "HIGH")
                if self.parked_at_millis else None
            ),
            "parked_candidate_since_ms": self.parked_candidate_since_ms,
        }


def _resolve_landed_at_millis(
    state: str,
    old: Optional[FlightEntry],
    now_ms: int,
    real_arrival_ms: Optional[int] = None,
    source: str = "radar_ground",
) -> tuple[Optional[int], Optional[str]]:
    """Giữ mốc hạ cánh đầu tiên.

    Khi state chuyển sang LANDED/TAXIING/PARKED, đây là mốc "tàu vừa hạ cánh"
    để frontend/server dùng làm dự phòng: giờ vào bến = landed_at + 3 phút.
    """
    if old and old.landed_at_millis:
        return old.landed_at_millis, old.landed_source or source
    if state not in ("LANDED", "TAXIING", "PARKED"):
        return (old.landed_at_millis if old else None), (old.landed_source if old else None)
    if real_arrival_ms and 0 < int(real_arrival_ms) <= now_ms + 10 * 60 * 1000:
        return int(real_arrival_ms), "real_arrival"
    return int(now_ms), source

def _fallback_parked_from_landing(landed_at_millis: Optional[int], now_ms: int) -> Optional[int]:
    """Trả về mốc vào bến dự phòng = hạ cánh + 3 phút nếu đã tới hạn."""
    if not landed_at_millis:
        return None
    fallback = int(landed_at_millis) + LANDING_TO_PARK_FALLBACK_MS
    return fallback if now_ms >= fallback else None


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
    """Tàu được xem là đã vào bến/dừng khi ground speed rất thấp gần sân bay."""
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
    Ví dụ: VN134 ↔ HVN134, VJ962 ↔ VJC962. Cũng xử lý leading-zero như VN0123 ↔ VN123.
    """
    base = _normalize_code(value)
    if not base:
        return set()
    aliases = {base}

    # Xử lý leading-zero: tách prefix chữ + suffix số, strip zero ở đầu suffix
    prefix_chars = []
    i = 0
    while i < len(base) and base[i].isalpha():
        prefix_chars.append(base[i])
        i += 1
    if prefix_chars and i < len(base):
        prefix = "".join(prefix_chars)
        suffix = base[i:]
        if suffix.isdigit():
            stripped = suffix.lstrip("0") or "0"
            aliases.add(prefix + stripped)
            # Cũng thêm padded variants cho trường hợp FR24 trả "VN0123"
            for pad in (3, 4):
                if len(stripped) < pad:
                    aliases.add(prefix + stripped.zfill(pad))

    # Alias IATA ↔ ICAO cho các hãng VN phổ biến
       airline_aliases = {
        # Việt Nam (6)
        "VN": "HVN", "HVN": "VN",
        "VJ": "VJC", "VJC": "VJ",
        "QH": "BAV", "BAV": "QH",
        "VU": "VAG", "VAG": "VU",
        "BL": "PIC", "PIC": "BL",
        "9G": "PQA", "PQA": "9G",      # Phú Quốc Airlines (Sun Group)
        # Hàn Quốc (7)
        "KE": "KAL", "KAL": "KE",
        "OZ": "AAR", "AAR": "OZ",
        "7C": "JJA", "JJA": "7C",
        "LJ": "JNA", "JNA": "LJ",
        "BX": "ABL", "ABL": "BX",
        "RS": "ASV", "ASV": "RS",
        "TW": "TWB", "TWB": "TW",
        # Hong Kong / Macau (3)
        "HX": "CRK", "CRK": "HX",
        "UO": "HKE", "HKE": "UO",
        "NX": "AMU", "AMU": "NX",
        # Đài Loan (4)
        "JX": "SJX", "SJX": "JX",
        "CI": "CAL", "CAL": "CI",
        "BR": "EVA", "EVA": "BR",
        "IT": "TTW", "TTW": "IT",
        # Thái Lan (3)
        "FD": "AIQ", "AIQ": "FD",
        "VZ": "TVJ", "TVJ": "VZ",
        "WE": "THD", "THD": "WE",
        # Malaysia / Singapore / Phil / Indo (8)
        "AK": "AXM", "AXM": "AK",
        "MH": "MAS", "MAS": "MH",
        "OD": "BTK", "BTK": "OD",
        "SQ": "SIA", "SIA": "SQ",
        "TR": "TGW", "TGW": "TR",
        "PR": "PAL", "PAL": "PR",
        "5J": "CEB", "CEB": "5J",
        "Z2": "APG", "APG": "Z2",
        # Trung Đông (1)
        "EK": "UAE", "UAE": "EK",
        # Đông Dương lân cận (2)
        "8M": "MMA", "MMA": "8M",
        "K6": "KHV", "KHV": "K6",
        # Trung Á — charter Kazakhstan (1)
        "DV": "VSV", "VSV": "DV",
    }
    # Tạo alias cho mọi mã đã có trong set (kể cả leading-zero variants)
    for alias in list(aliases):
        # Tách prefix chữ
        j = 0
        while j < len(alias) and alias[j].isalpha():
            j += 1
        if j == 0 or j == len(alias):
            continue
        prefix = alias[:j]
        suffix = alias[j:]
        if not suffix.isdigit():
            continue
        alt = airline_aliases.get(prefix)
        if alt:
            aliases.add(alt + suffix)
    return aliases


def _desired_refresh_interval_ms(entry: Optional[FlightEntry], now_ms: int) -> int:
    """Chu kỳ refresh thông minh cho từng tàu.

    Server/app biết tàu xa hay gần nhờ state, ETA còn lại và distance_km đã lấy từ FR24.
    - Xa: 60s
    - Approach/còn 10-20 phút: 30s
    - Final/còn dưới 10 phút/gần sân bay/taxi: 20s
    """
    if entry is None:
        return ADAPTIVE_POLL_NEAR_MS
    if entry.state in TERMINAL_STATES:
        return ADAPTIVE_POLL_FAR_MS
    if entry.state in GROUND_ACTIVE_STATES or entry.state == "FINAL":
        return ADAPTIVE_POLL_NEAR_MS
    if entry.state == "APPROACH":
        return ADAPTIVE_POLL_MID_MS

    if entry.eta_millis is not None:
        remaining = entry.eta_millis - now_ms
        if remaining <= ADAPTIVE_NEAR_REMAINING_MS:
            return ADAPTIVE_POLL_NEAR_MS
        if remaining <= ADAPTIVE_MID_REMAINING_MS:
            return ADAPTIVE_POLL_MID_MS

    if entry.distance_km is not None:
        if entry.distance_km <= ADAPTIVE_NEAR_DISTANCE_KM:
            return ADAPTIVE_POLL_NEAR_MS
        if entry.distance_km <= ADAPTIVE_MID_DISTANCE_KM:
            return ADAPTIVE_POLL_MID_MS

    return ADAPTIVE_POLL_FAR_MS


def _entry_needs_refresh(entry: Optional[FlightEntry], now_ms: int, min_refresh_ms: Optional[int] = None) -> bool:
    """Có cần poll lại FR24 để cập nhật live position không."""
    if entry is None:
        return True
    if entry.state in TERMINAL_STATES:
        return False
    if not entry.updated_at:
        return True
    desired = min_refresh_ms if min_refresh_ms is not None else _desired_refresh_interval_ms(entry, now_ms)
    desired = max(5_000, int(desired))
    return now_ms - entry.updated_at >= desired


def _server_needs_poll_for_codes(codes: list[str], now_ms: int, min_refresh_ms: Optional[int] = None) -> bool:
    """Bảo vệ trường hợp background poller không chạy / HTTP chỉ đang trả cache cũ."""
    if not codes:
        return False
    if LAST_POLL_MS is None:
        return True
    desired = min_refresh_ms if min_refresh_ms is not None else POSITION_REFRESH_STALE_MS
    desired = max(5_000, int(desired))
    if now_ms - LAST_POLL_MS >= desired:
        return True
    with _lock:
        return any(_entry_needs_refresh(TRACKED.get(code), now_ms, desired) for code in codes)


def _adaptive_poll_interval_ms(now_ms: Optional[int] = None) -> int:
    """Chu kỳ poll nền toàn server. Nếu có ít nhất 1 tàu gần hạ/taxi thì poll nhanh hơn."""
    now_ms = now_ms or int(time.time() * 1000)
    with _lock:
        active_entries = [entry for entry in TRACKED.values() if entry.state not in TERMINAL_STATES]
    if not active_entries:
        return min(POLL_INTERVAL * 1000, ADAPTIVE_POLL_FAR_MS)
    desired = min(_desired_refresh_interval_ms(entry, now_ms) for entry in active_entries)
    return min(POLL_INTERVAL * 1000, desired)


# ===========================================================
# SCHEDULE HINTS (lịch bay)
# ===========================================================

def _prune_old_schedule_hints(now_ms: Optional[int] = None) -> int:
    """Xóa hint quá cũ hoặc quá xa tương lai. Trả về số entry đã prune."""
    now_ms = now_ms or int(time.time() * 1000)
    removed = 0
    with _lock:
        stale_codes = []
        for code, hint in SCHEDULE_HINTS.items():
            sibt = hint.get("sibt_millis")
            if not isinstance(sibt, int):
                stale_codes.append(code)
                continue
            if sibt < now_ms - SCHEDULE_RETAIN_PAST_MS:
                stale_codes.append(code)
            elif sibt > now_ms + SCHEDULE_RETAIN_FUTURE_MS:
                stale_codes.append(code)
        for code in stale_codes:
            SCHEDULE_HINTS.pop(code, None)
            removed += 1
    return removed


def _register_schedule(schedule_payload: dict, now_ms: Optional[int] = None) -> int:
    """Lưu SIBT/origin/aircraft cho từng mã. Trả về số entry đã ghi."""
    if not isinstance(schedule_payload, dict):
        return 0
    now_ms = now_ms or int(time.time() * 1000)
    written = 0
    with _lock:
        for raw_code, hint in schedule_payload.items():
            code = _clean_code(raw_code)
            if not code or not isinstance(hint, dict):
                continue
            sibt_millis = hint.get("sibt_millis")
            if sibt_millis is None:
                continue
            try:
                sibt_millis = int(sibt_millis)
            except (TypeError, ValueError):
                continue
            # Bỏ qua hint quá cũ ngay khi nhận
            if sibt_millis < now_ms - SCHEDULE_RETAIN_PAST_MS:
                continue
            if sibt_millis > now_ms + SCHEDULE_RETAIN_FUTURE_MS:
                continue
            SCHEDULE_HINTS[code] = {
                "sibt_millis": sibt_millis,
                "origin": str(hint.get("origin", "") or "").upper(),
                "aircraft": str(hint.get("aircraft", "") or "").upper(),
                "route": str(hint.get("route", "") or "").upper(),
                "stand": str(hint.get("stand", "") or "").strip(),
                "date_key": str(hint.get("date_key", "") or ""),
                "registered_at_ms": now_ms,
            }
            written += 1
    _prune_old_schedule_hints(now_ms)
    return written


def _schedule_public_entry(code: str, hint: dict, now_ms: int) -> dict:
    """Tạo entry public state=SCHEDULED cho app, khi server chưa có live data."""
    sibt = hint.get("sibt_millis")
    return {
        "state": "SCHEDULED",
        "eta_millis": sibt,
        "confidence": "LOW",
        "altitude_ft": None,
        "ground_speed_kt": None,
        "vertical_speed": None,
        "distance_km": None,
        "on_ground": False,
        "latitude": None,
        "longitude": None,
        "heading": None,
        "updated_at": 0,
        "stale_seconds": None,
        "stale": False,
        "landed": False,
        "taxiing": False,
        "parked": False,
        "miss_count": 0,
        "scheduled": True,
        "sibt_millis": sibt,
        "origin_iata": hint.get("origin", ""),
        "aircraft_type": hint.get("aircraft", ""),
        "route": hint.get("route", ""),
        "date_key": hint.get("date_key", ""),
        "actual_parked_at_millis": None,
        "parked_at_millis": None,
        "in_block_millis": None,
        "on_block_millis": None,
        "stand_arrival_millis": None,
        "stopped_at_millis": None,
        "actual_parked_time": None,
        "actual_parked_time_full": None,
        "actual_parked_source": None,
    }


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

def _process_match(
    code: str,
    flight,
    details: Optional[dict],
    old: Optional[FlightEntry],
    now_ms: int,
    trusted: bool = False,
) -> Optional[FlightEntry]:
    """Mã được tìm thấy trong scan. Trả về entry mới, hoặc None nếu drop.

    `trusted=True` khi code đã được lịch bay/người dùng bảo lãnh (lấy từ QUEUE).
    Khi đó không drop dù FR24 trả dest khác DAD — vì FR24 đôi khi cache stale
    hoặc chưa cập nhật destination cho chuyến mới khởi hành.
    """
    sensors = _extract_sensors(flight)

    fr24_eta_ms = None
    real_arrival_ms = None
    fr24_live = True
    dest_iata = ""

    if details:
        fr24_eta_ms, real_arrival_ms, fr24_live, dest_iata = _extract_eta_from_details(details)
        if dest_iata and dest_iata != TARGET_AIRPORT:
            if trusted:
                log.info("Keep %s despite FR24 dest=%s (trusted by schedule)", code, dest_iata)
            else:
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

    # Chốt giờ dừng bến/in-block từ radar.
    # Không chốt ngay khi thấy gs thấp, vì tàu có thể dừng tạm trên taxiway.
    # Chỉ chốt PARKED sau khi tín hiệu đứng yên ổn định PARKED_STABLE_MS.
    parking_signal = _is_parked_on_ground(sensors["gs_kt"], sensors["distance_km"])
    parked_at_millis = old.parked_at_millis if old else None
    parked_candidate_since_ms = old.parked_candidate_since_ms if old else None
    parked_source = old.parked_source if old else None
    landed_at_millis, landed_source = _resolve_landed_at_millis(
        state, old, now_ms, real_arrival_ms=real_arrival_ms, source="radar_ground"
    )

    if old and old.state == "PARKED":
        # Sticky PARKED: đã dừng/vào bến rồi thì không revert về taxi/airborne.
        # Không tự gán now_ms/updated_at làm giờ dừng bến; chỉ giữ timestamp đã được xác nhận ổn định.
        state = "PARKED"
        parked_at_millis = old.parked_at_millis
        parked_source = old.parked_source
    elif state in ("LANDED", "TAXIING", "PARKED") and parking_signal:
        if parked_candidate_since_ms is None:
            parked_candidate_since_ms = now_ms
        if now_ms - parked_candidate_since_ms >= PARKED_STABLE_MS:
            state = "PARKED"
            if parked_at_millis is None:
                parked_at_millis = parked_candidate_since_ms
                parked_source = "ground_stop"
        elif state == "PARKED":
            # _classify_state có thể trả PARKED ngay. Hạ xuống TAXIING cho tới khi ổn định.
            state = "TAXIING"
    else:
        parked_candidate_since_ms = None

    # Lớp dự phòng đơn giản theo yêu cầu nghiệp vụ:
    # tàu chuyển xanh/hạ cánh -> lưu landed_at; nếu sau 3 phút chưa có PARKED tốt hơn
    # thì tự coi giờ vào bến = landed_at + 3 phút.
    landing_fallback_parked = _fallback_parked_from_landing(landed_at_millis, now_ms)
    if parked_at_millis is None and landing_fallback_parked:
        parked_at_millis = landing_fallback_parked
        parked_source = "landing_plus_3min"
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
            # Nếu trước đó cũng chưa có ETA mà lịch bay có SIBT, dùng SIBT làm fallback.
            fallback_eta = old.eta_millis if old else None
            if fallback_eta is None:
                hint = SCHEDULE_HINTS.get(code)
                if hint and isinstance(hint.get("sibt_millis"), int):
                    fallback_eta = hint["sibt_millis"]
            return FlightEntry(
                state=state,
                eta_millis=fallback_eta,
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
                landed=(state in ("LANDED", "TAXIING", "PARKED")),
                landed_at_millis=landed_at_millis,
                landed_source=landed_source,
                parked_at_millis=parked_at_millis,
                parked_candidate_since_ms=parked_candidate_since_ms,
                parked_source=parked_source,
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
            "%s: %s → %s (alt=%s ft, gs=%s kt, dist=%s km, parked_at=%s)",
            code, old.state, state, sensors["alt_ft"], sensors["gs_kt"], dist_str, parked_at_millis,
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
        "Live %s state=%s eta=%s parked_at=%s lat=%s lng=%s heading=%s changed=%s",
        code, state, eta_ms, parked_at_millis, sensors["lat"], sensors["lng"], sensors["heading"], changed,
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
        landed_at_millis=landed_at_millis,
        landed_source=landed_source,
        parked_at_millis=parked_at_millis,
        parked_candidate_since_ms=parked_candidate_since_ms,
        parked_source=parked_source,
    )


def _process_miss(code: str, old: Optional[FlightEntry], now_ms: int) -> Optional[FlightEntry]:
    """Mã KHÔNG xuất hiện trong scan cycle này. Giữ vị trí cuối để map vẫn vẽ được.

    Nếu chưa có entry cũ và lịch bay có SIBT cho mã này → tạo entry SCHEDULED.
    """
    if old is None:
        # Chưa từng thấy live. Nếu lịch bay có SIBT thì tạo placeholder.
        hint = SCHEDULE_HINTS.get(code)
        if hint and isinstance(hint.get("sibt_millis"), int):
            return FlightEntry(
                state="SCHEDULED",
                eta_millis=hint["sibt_millis"],
                confidence="LOW",
                miss_count=0,
                landed=False,
            )
        return None

    # Đã PARKED: trạng thái kết thúc, giữ nguyên.
    if old.state == "PARKED":
        return old

    # SCHEDULED state có sẵn: giữ nguyên, không tăng miss vì chưa từng có live.
    if old.state == "SCHEDULED":
        return old

    miss_count = old.miss_count + 1

    # Sau touchdown/taxi: ưu tiên chốt bằng landed_at + 3 phút.
    # Không cần đợi 12/30 phút; mọi chuyến đã có người đón sẽ có mốc hoàn tất ổn định.
    if old.state in GROUND_ACTIVE_STATES:
        landed_at = old.landed_at_millis or old.updated_at or now_ms
        fallback_parked = _fallback_parked_from_landing(landed_at, now_ms)
        if old.parked_at_millis:
            parked_at = old.parked_at_millis
            new_source = old.parked_source or "ground_stop"
        elif fallback_parked:
            parked_at = fallback_parked
            new_source = "landing_plus_3min"
        elif miss_count >= LANDED_MISS_TO_PARKED:
            # Nếu polling thưa hoặc clock lệch, vẫn dùng landed_at +3 nhưng không quá now.
            parked_at = min(int(landed_at) + LANDING_TO_PARK_FALLBACK_MS, now_ms)
            new_source = "landing_plus_3min"
        else:
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
                landed=True,
                landed_at_millis=landed_at,
                landed_source=old.landed_source or "radar_ground",
                parked_at_millis=old.parked_at_millis,
                parked_candidate_since_ms=old.parked_candidate_since_ms,
                parked_source=old.parked_source,
            )

        log.info(
            "%s: %s + missed %d → PARKED (parked_at=%s, source=%s)",
            code, old.state, miss_count, parked_at, new_source,
        )
        return FlightEntry(
            state="PARKED",
            eta_millis=old.eta_millis or now_ms,
            confidence="LOW" if new_source != "ground_stop" else "HIGH",
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
            landed_at_millis=landed_at,
            landed_source=old.landed_source or "radar_ground",
            parked_at_millis=parked_at,
            parked_candidate_since_ms=old.parked_candidate_since_ms,
            parked_source=new_source,
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
            landed_at_millis=old.landed_at_millis or now_ms,
            landed_source=old.landed_source or "final_missed",
            parked_at_millis=old.parked_at_millis,
            parked_candidate_since_ms=old.parked_candidate_since_ms,
            parked_source=old.parked_source,
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
                landed_at_millis=old.landed_at_millis or now_ms,
                landed_source=old.landed_source or "approach_missed",
                parked_at_millis=old.parked_at_millis,
                parked_candidate_since_ms=old.parked_candidate_since_ms,
                parked_source=old.parked_source,
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
            landed_at_millis=old.landed_at_millis,
            landed_source=old.landed_source,
            parked_at_millis=old.parked_at_millis,
            parked_candidate_since_ms=old.parked_candidate_since_ms,
            parked_source=old.parked_source,
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
        landed_at_millis=old.landed_at_millis,
        landed_source=old.landed_source,
        parked_at_millis=old.parked_at_millis,
        parked_candidate_since_ms=old.parked_candidate_since_ms,
        parked_source=old.parked_source,
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

    v2.2: nới lọc — nếu candidate trong vùng bao DAD mà chưa có destination
    rõ ràng, vẫn giữ lại để xác minh qua details. Sau đó:
      - Có dest=DAD → giữ.
      - Có dest khác → loại.
      - Vẫn không có dest sau khi lấy details: chỉ giữ nếu code khớp với
        SCHEDULE_HINTS (lịch bay đã bảo lãnh).
    """
    max_minutes = max(5, min(int(max_minutes or AUTO_SCAN_DEFAULT_MINUTES), AUTO_SCAN_MAX_MINUTES))
    now_ms = int(time.time() * 1000)
    fr_api = FlightRadar24API()
    flights = fr_api.get_flights(bounds=BOUNDS_DAD)

    # Snapshot schedule hints để dùng làm whitelist khi FR24 thiếu dest
    with _lock:
        schedule_codes = set(SCHEDULE_HINTS.keys())

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

        code = _best_flight_code(flight, details)
        if not code:
            continue

        # Lọc dest
        if dest_iata and dest_iata != TARGET_AIRPORT:
            continue
        if not dest_iata and dest_hint and dest_hint != TARGET_AIRPORT:
            continue
        if not dest_iata and not dest_hint:
            # Không xác định được dest. Chỉ giữ nếu lịch bay bảo lãnh.
            code_aliases = _code_aliases(code)
            if not (schedule_codes and code_aliases & schedule_codes):
                continue
            log.info("Keep %s in scan (no FR24 dest, schedule-vouched)", code)

        with _lock:
            old = TRACKED.get(code)

        # trusted=True nếu code khớp với schedule (đã được bảo lãnh)
        trusted = bool(schedule_codes and _code_aliases(code) & schedule_codes)
        entry = _process_match(code, flight, details, old, now_ms, trusted=trusted)
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
        # KHÔNG skip khi dest khác DAD ở đây — _process_match sẽ check với trusted=True.
        # FR24 đôi khi cache stale destination khi tàu vừa khởi hành.

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
                # trusted=True: code đã trong QUEUE (do user/lịch bay đẩy lên).
                # Không drop dù FR24 trả dest != DAD vì FR24 hay cache stale.
                new_entry = _process_match(code, targets[code], details_map.get(code), old, now_ms, trusted=True)
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

    # Online mode: server tự ghi giờ tàu dừng bến vào Firestore, không cần web đang mở.
    _sync_parked_entries_to_firestore(updates)
    # Lớp dự phòng: chuyến đã có người đón nhưng radar không sinh PARKED thì không để treo mãi.
    _sync_overdue_schedule_claims_to_firestore(now_ms)


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
            _refresh_schedule_from_firestore(force=False)
            _do_poll()
            _prune_old_schedule_hints()
        except Exception as e:
            with _lock:
                LAST_ERROR = str(e)
            log.exception("Scheduled poll failed")
        sleep_ms = _adaptive_poll_interval_ms()
        log.info("Next scheduled poll in %.1fs", sleep_ms / 1000.0)
        time.sleep(max(5.0, sleep_ms / 1000.0))


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
    """Build response cho /api/etas: live data + SCHEDULED placeholder cho mã chưa có live."""
    now_ms = int(time.time() * 1000)
    with _lock:
        result = {}
        for code in QUEUE:
            entry = TRACKED.get(code)
            if entry is None:
                # Chưa có live data. Nếu lịch bay có SIBT thì trả SCHEDULED.
                hint = SCHEDULE_HINTS.get(code)
                if hint and isinstance(hint.get("sibt_millis"), int):
                    result[code] = _schedule_public_entry(code, hint, now_ms)
                else:
                    result[code] = {"state": "PENDING", "status": "pending"}
            else:
                public = entry.to_public(now_ms)
                # Đính kèm hint từ lịch bay (origin/aircraft/sibt) để app có context.
                hint = SCHEDULE_HINTS.get(code)
                if hint:
                    sibt = hint.get("sibt_millis")
                    if isinstance(sibt, int):
                        public.setdefault("sibt_millis", sibt)
                    if hint.get("origin"):
                        public.setdefault("origin_iata", hint["origin"])
                    if hint.get("aircraft"):
                        public.setdefault("aircraft_type", hint["aircraft"])
                    if hint.get("route"):
                        public.setdefault("route", hint["route"])
                    if hint.get("date_key"):
                        public.setdefault("date_key", hint["date_key"])
                result[code] = public
        return {
            "status": "success",
            "server_time_millis": now_ms,
            "last_poll_millis": LAST_POLL_MS,
            "last_poll_duration_ms": LAST_POLL_DURATION_MS,
            "poll_interval_seconds": POLL_INTERVAL,
            "adaptive_poll_interval_ms": _adaptive_poll_interval_ms(now_ms),
            "last_error": LAST_ERROR,
            "schedule_count": len(SCHEDULE_HINTS),
            "queue_count": len(QUEUE),
            "tracked_count": len(TRACKED),
            "firestore_status": FIRESTORE_STATUS,
            "last_firestore_schedule_sync_ms": LAST_FIRESTORE_SCHEDULE_SYNC_MS,
            "last_firestore_parked_write_ms": LAST_FIRESTORE_PARKED_WRITE_MS,
            "last_firestore_fallback_write_ms": LAST_FIRESTORE_FALLBACK_WRITE_MS,
            "last_firestore_sync_error": LAST_FIRESTORE_SYNC_ERROR,
            "firestore_schedule_dates": FIRESTORE_SCHEDULE_DATES,
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
            "adaptive_poll_interval_ms": _adaptive_poll_interval_ms(),
            "tracked_count": len(TRACKED),
            "queue_count": len(QUEUE),
            "schedule_count": len(SCHEDULE_HINTS),
            "pid": os.getpid(),
            "firestore_status": FIRESTORE_STATUS,
            "last_firestore_schedule_sync_ms": LAST_FIRESTORE_SCHEDULE_SYNC_MS,
            "last_firestore_parked_write_ms": LAST_FIRESTORE_PARKED_WRITE_MS,
            "last_firestore_fallback_write_ms": LAST_FIRESTORE_FALLBACK_WRITE_MS,
            "last_firestore_sync_error": LAST_FIRESTORE_SYNC_ERROR,
            "firestore_schedule_dates": FIRESTORE_SCHEDULE_DATES,
        })



@app.route("/api/firestore_sync", methods=["POST"])
def firestore_sync_now():
    """Ép server đọc lại flightPlans từ Firestore ngay. Dùng để debug/admin."""
    if (err := require_api_key()):
        return err
    written = _refresh_schedule_from_firestore(force=True)
    now_ms = int(time.time() * 1000)
    with _lock:
        return jsonify({
            "status": "success" if FIRESTORE_STATUS in ("ready", "disabled", "missing_firebase_admin") else "error",
            "server_time_millis": now_ms,
            "registered": written,
            "firestore_status": FIRESTORE_STATUS,
            "last_firestore_schedule_sync_ms": LAST_FIRESTORE_SCHEDULE_SYNC_MS,
            "last_firestore_parked_write_ms": LAST_FIRESTORE_PARKED_WRITE_MS,
            "last_firestore_fallback_write_ms": LAST_FIRESTORE_FALLBACK_WRITE_MS,
            "last_firestore_sync_error": LAST_FIRESTORE_SYNC_ERROR,
            "firestore_schedule_dates": FIRESTORE_SCHEDULE_DATES,
            "queue_count": len(QUEUE),
            "schedule_count": len(SCHEDULE_HINTS),
            "tracked_count": len(TRACKED),
            "pid": os.getpid(),
        })

@app.route("/api/cleanup_bad_parked_times", methods=["POST"])
def cleanup_bad_parked_times():
    """Xóa các mốc Dừng bến bị web cũ tự ghi nhầm.

    Body tối thiểu: {"date_key":"2026-05-17"}
    Mặc định chỉ xóa document có actualParkedSource == "radar_state_parked".
    Có thể truyền thêm actual_parked_time để lọc hẹp, ví dụ "17:53".
    """
    if (err := require_api_key()):
        return err
    db = _init_firestore_client()
    if db is None:
        return jsonify({
            "status": "error",
            "message": "Firestore chưa sẵn sàng. Kiểm tra FIREBASE_SERVICE_ACCOUNT_JSON/B64.",
            "firestore_status": FIRESTORE_STATUS,
            "last_firestore_sync_error": LAST_FIRESTORE_SYNC_ERROR,
        }), 500

    body = request.get_json(silent=True) or {}
    date_key = str(body.get("date_key") or body.get("date") or "").strip()
    source = str(body.get("source") or "radar_state_parked").strip()
    exact_time = str(body.get("actual_parked_time") or body.get("time") or "").strip()
    dry_run = bool(body.get("dry_run", False))
    if not date_key:
        return jsonify({"status": "error", "message": "Thiếu date_key, ví dụ 2026-05-17"}), 400

    col = db.collection("pickups").document(date_key).collection("flights")
    docs = list(col.stream())
    matched = []
    for snap in docs:
        data = snap.to_dict() or {}
        if source and str(data.get("actualParkedSource") or "") != source:
            continue
        if exact_time and str(data.get("actualParkedTime") or "") != exact_time:
            continue
        if not (data.get("actualParkedAtMillis") or data.get("actualParkedTime") or data.get("actualParkedSource")):
            continue
        matched.append(snap)

    if not dry_run:
        for snap in matched:
            snap.reference.update({
                "actualParkedByRadar": firebase_firestore.DELETE_FIELD,
                "actualParkedAtMillis": firebase_firestore.DELETE_FIELD,
                "actualParkedTime": firebase_firestore.DELETE_FIELD,
                "actualParkedTimeFull": firebase_firestore.DELETE_FIELD,
                "actualParkedSource": firebase_firestore.DELETE_FIELD,
                "actualParkedUpdatedAt": firebase_firestore.DELETE_FIELD,
                "radarState": firebase_firestore.DELETE_FIELD,
                "radarGroundSpeedKt": firebase_firestore.DELETE_FIELD,
                "radarDistanceKm": firebase_firestore.DELETE_FIELD,
                "radarAltitudeFt": firebase_firestore.DELETE_FIELD,
                "radarUpdatedAtMillis": firebase_firestore.DELETE_FIELD,
                "lastUpdatedAt": firebase_firestore.SERVER_TIMESTAMP,
                "lastUpdatedByName": "server-cleanup",
            })

    return jsonify({
        "status": "success",
        "date_key": date_key,
        "source": source,
        "actual_parked_time": exact_time or None,
        "dry_run": dry_run,
        "matched": len(matched),
        "updated": 0 if dry_run else len(matched),
        "codes": [snap.id for snap in matched[:200]],
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
    _refresh_schedule_from_firestore(force=False)
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        codes = body.get("codes", []) or []
        cleaned = [_clean_code(c) for c in codes if isinstance(c, str) and _clean_code(c)]
        force_refresh = bool(body.get("force_refresh"))
        raw_min_refresh = body.get("min_refresh_ms") or body.get("requested_min_refresh_ms")
        try:
            min_refresh_ms = int(raw_min_refresh) if raw_min_refresh is not None else None
        except (TypeError, ValueError):
            min_refresh_ms = None
        if min_refresh_ms is not None:
            min_refresh_ms = max(5_000, min(min_refresh_ms, ADAPTIVE_POLL_FAR_MS))

        # Lịch bay: app gửi SIBT/origin/aircraft cho từng mã trong body.schedule.
        # Server lưu vào SCHEDULE_HINTS để: (1) trả SCHEDULED placeholder cho mã
        # FR24 chưa thấy, (2) trust dest=N/A khi auto-scan, (3) đính meta cho live.
        schedule_payload = body.get("schedule")
        scheduled_written = _register_schedule(schedule_payload) if schedule_payload else 0

        if cleaned:
            now_ms = int(time.time() * 1000)
            with _lock:
                new_codes = [c for c in cleaned if c not in TRACKED]
                QUEUE.update(cleaned)
            # Không coi client_time_millis là force_refresh nữa. App gửi min_refresh_ms theo mức gần/xa;
            # server chỉ poll khi dữ liệu đã stale theo ngưỡng đó để tránh quá tải server miễn phí.
            if new_codes or force_refresh or _server_needs_poll_for_codes(cleaned, now_ms, min_refresh_ms):
                _try_immediate_poll(
                    f"etas refresh new={len(new_codes)} force={force_refresh} min={min_refresh_ms} sched={scheduled_written}",
                    bypass_cooldown=force_refresh,
                )
    return jsonify(build_etas_payload())


@app.route("/api/schedule", methods=["POST"])
def post_schedule():
    """Nhận lịch bay từ app: list mã + SIBT cho mỗi mã.

    Body: {
      "flights": [
        {"code": "VN132", "sibt_millis": 1747469700000, "origin": "SGN",
         "aircraft": "A321", "route": "SGN-DAD", "date_key": "2026-05-17"},
        ...
      ]
    }
    Hoặc: {"schedule": {code: hint, ...}} cũng được chấp nhận.

    Server: lưu vào SCHEDULE_HINTS, đẩy mọi code vào QUEUE, trigger poll ngay.
    """
    if (err := require_api_key()):
        return err

    body = request.get_json(silent=True) or {}
    flights_payload = body.get("flights")
    schedule_payload = body.get("schedule")

    # Hỗ trợ cả 2 dạng input
    hints_dict: dict = {}
    if isinstance(schedule_payload, dict):
        hints_dict.update(schedule_payload)
    if isinstance(flights_payload, list):
        for item in flights_payload:
            if not isinstance(item, dict):
                continue
            code = _clean_code(item.get("code") or item.get("arrival_flight") or "")
            if not code:
                continue
            hints_dict[code] = {
                "sibt_millis": item.get("sibt_millis"),
                "origin": item.get("origin") or item.get("origin_iata") or "",
                "aircraft": item.get("aircraft") or item.get("aircraft_type") or "",
                "route": item.get("route") or item.get("arrival_route") or "",
                "stand": item.get("stand") or item.get("plannedStand") or item.get("planned_stand") or "",
                "date_key": item.get("date_key") or "",
            }

    written = _register_schedule(hints_dict)
    codes = [c for c in hints_dict.keys() if _clean_code(c)]

    if codes:
        with _lock:
            new_codes = [c for c in codes if c not in TRACKED]
            QUEUE.update(codes)
        if new_codes:
            _try_immediate_poll(f"schedule new={len(new_codes)}")

    now_ms = int(time.time() * 1000)
    with _lock:
        snapshot = {
            "queue_count": len(QUEUE),
            "schedule_count": len(SCHEDULE_HINTS),
            "tracked_count": len(TRACKED),
        }
    return jsonify({
        "status": "success",
        "registered": written,
        "received": len(hints_dict),
        "server_time_millis": now_ms,
        **snapshot,
    })


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
        SCHEDULE_HINTS.pop(code, None)
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
    if entry is None:
        # Không có live data. Nếu lịch bay có SIBT thì trả SCHEDULED.
        with _lock:
            hint = SCHEDULE_HINTS.get(code)
        if hint and isinstance(hint.get("sibt_millis"), int):
            public = _schedule_public_entry(code, hint, now_ms)
            public.update({
                "status": "success",
                "flight_code": code,
                "destination": "Da Nang (DAD)",
                "server_time_millis": now_ms,
                "pid": os.getpid(),
            })
            return jsonify(public)
        return jsonify({
            "status": "pending",
            "flight_code": code,
            "message": f"Chưa có ETA cho {code}",
            "server_time_millis": now_ms,
            "pid": os.getpid(),
        })

    if entry.eta_millis is None:
        return jsonify({
            "status": "pending",
            "flight_code": code,
            "message": f"Chưa có ETA cho {code}",
            "server_time_millis": now_ms,
            "pid": os.getpid(),
        })

    public = entry.to_public(now_ms)
    with _lock:
        hint = SCHEDULE_HINTS.get(code)
    if hint:
        sibt = hint.get("sibt_millis")
        if isinstance(sibt, int):
            public.setdefault("sibt_millis", sibt)
        if hint.get("origin"):
            public.setdefault("origin_iata", hint["origin"])
        if hint.get("aircraft"):
            public.setdefault("aircraft_type", hint["aircraft"])
    public.update({
        "status": "success",
        "flight_code": code,
        "destination": "Da Nang (DAD)",
        "server_time_millis": now_ms,
        "pid": os.getpid(),
    })
    return jsonify(public)



# ===========================================================
# GMAIL AUTO IMPORT FLIGHT PLAN
# ===========================================================

def _compact_text(value) -> str:
    """Gộp khoảng trắng/xuống dòng trong text lấy từ Word."""
    import re
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def _compact_flight_code(value) -> str:
    """Giữ mã bay dạng gọn: 'VN 07187' -> 'VN07187'."""
    return "".join(ch for ch in str(value or "").upper().strip() if ch.isalnum())


def _canonical_flight_code(value) -> str:
    """
    Chuẩn hóa mã bay để khớp radar:
      TW025  -> TW25
      VN07187 -> VN7187
      VJ0521 -> VJ521
    """
    import re
    raw = _compact_flight_code(value)
    m = re.match(r"^([A-Z]+)0+(\d+)$", raw)
    if not m:
        return raw
    return f"{m.group(1)}{int(m.group(2))}"


def _extract_date_key_from_texts(*texts) -> Optional[str]:
    """Lấy ngày từ tên file/tiêu đề: 18.05.2026 hoặc 18.5.2026 -> 2026-05-18."""
    import re
    for text in texts:
        raw = str(text or "")
        m = re.search(r"(\d{1,2})[./-](\d{1,2})[./-](\d{4})", raw)
        if not m:
            continue
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            dt = datetime(year, month, day, tzinfo=VN_TZ)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def _looks_like_flight_code(value: str) -> bool:
    """Kiểm tra chuỗi có giống mã chuyến bay không."""
    import re
    return bool(re.match(r"^[A-Z]{1,4}\d{1,5}[A-Z]?$", str(value or "").strip().upper()))


def _parse_docx_flight_plan_bytes(file_bytes: bytes, date_key: str) -> list[dict]:
    """
    Parse file lịch bay .docx dạng bảng 12 cột:
      STT · Loại tàu · Số hiệu đến · Đi · Tuyến đến · Tuyến đi · SIBT · SOBT · EIBT · ? · APRK · RMK

    Có thêm lớp fallback nhẹ cho trường hợp Word bị merge cột hoặc có cột trống.
    """
    from io import BytesIO
    from docx import Document

    doc = Document(BytesIO(file_bytes))
    flights: list[dict] = []
    seen_codes: set[str] = set()

    def add_row(cells: list[str]) -> None:
        while cells and not cells[-1]:
            cells.pop()
        if len(cells) < 7:
            return

        joined_lower = " ".join(cells).lower()
        if "số hiệu" in joined_lower or "sibt" in joined_lower or "aprk" in joined_lower:
            return
        if cells[0].strip().lower() in ("stt", "no", "n/o", "tt"):
            return

        # Format chuẩn của lịch AMO: 12 cột.
        aircraft_type = cells[1] if len(cells) > 1 else ""
        arrival_raw = cells[2] if len(cells) > 2 else ""
        departure_raw = cells[3] if len(cells) > 3 else ""
        arrival_route = cells[4] if len(cells) > 4 else ""
        departure_route = cells[5] if len(cells) > 5 else ""
        sibt = cells[6] if len(cells) > 6 else ""
        sobt = cells[7] if len(cells) > 7 else ""
        eibt = cells[8] if len(cells) > 8 else ""
        planned_stand = cells[10] if len(cells) > 10 else (cells[9] if len(cells) > 9 else "")
        rmk = cells[11] if len(cells) > 11 else ""

        arrival_display = _compact_flight_code(arrival_raw)
        arrival_code = _canonical_flight_code(arrival_raw)
        departure_code = _canonical_flight_code(departure_raw)

        # Fallback nếu cột bị lệch: tìm mã bay + giờ trong dòng.
        if not _looks_like_flight_code(arrival_code):
            possible_codes = [_canonical_flight_code(c) for c in cells if _looks_like_flight_code(_canonical_flight_code(c))]
            possible_times = [c for c in cells if _parse_sibt_to_millis(c, date_key)]
            if possible_codes and possible_times:
                arrival_code = possible_codes[0]
                arrival_display = _compact_flight_code(possible_codes[0])
                sibt = possible_times[0]
            else:
                return

        if not arrival_code or not _looks_like_flight_code(arrival_code):
            return

        sibt_ms = _parse_sibt_to_millis(sibt, date_key)
        if not sibt_ms:
            return

        # Chống trùng mã trong cùng file.
        if arrival_code in seen_codes:
            return
        seen_codes.add(arrival_code)

        flights.append({
            "stt": _compact_text(cells[0]) if cells else "",
            "aircraftType": _compact_text(aircraft_type).upper(),
            "arrivalFlight": arrival_code,
            "displayArrivalFlight": arrival_display or arrival_code,
            "rawArrivalFlight": arrival_display or arrival_code,
            "departureFlight": departure_code,
            "arrivalRoute": _compact_text(arrival_route).upper(),
            "departureRoute": _compact_text(departure_route).upper(),
            "sibt": _compact_text(sibt),
            "sibtMillis": int(sibt_ms),
            "plannedTime": _compact_text(sibt),
            "sobt": _compact_text(sobt),
            "eibt": _compact_text(eibt),
            "plannedStand": _compact_text(planned_stand),
            "stand": _compact_text(planned_stand),
            "rmk": _compact_text(rmk),
        })

    for table in doc.tables:
        for row in table.rows:
            add_row([_compact_text(cell.text) for cell in row.cells])

    flights.sort(key=lambda item: item.get("sibtMillis") or _parse_sibt_to_millis(item.get("sibt"), date_key) or 0)
    return flights


def _gmail_import_id(message_id: str, filename: str) -> str:
    import hashlib
    raw = f"{message_id}|{filename}".encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()


@app.route("/api/import-flight-plan-from-gmail", methods=["POST", "OPTIONS"])
def import_flight_plan_from_gmail():
    """
    Nhận file .docx từ Google Apps Script:
    {
      messageId, threadId, subject, from, receivedAt,
      filename, mimeType, fileBase64
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    if not GMAIL_IMPORT_SECRET:
        return jsonify({
            "status": "error",
            "message": "Server chưa cấu hình GMAIL_IMPORT_SECRET trên Render."
        }), 500

    incoming_secret = request.headers.get("X-Import-Secret", "")
    if not hmac.compare_digest(str(incoming_secret), str(GMAIL_IMPORT_SECRET)):
        return jsonify({
            "status": "error",
            "message": "Sai X-Import-Secret."
        }), 401

    body = request.get_json(silent=True) or {}

    message_id = str(body.get("messageId") or "").strip()
    thread_id = str(body.get("threadId") or "").strip()
    subject = str(body.get("subject") or "").strip()
    sender = str(body.get("from") or "").strip()
    received_at = str(body.get("receivedAt") or "").strip()
    filename = str(body.get("filename") or "").strip()
    file_b64 = str(body.get("fileBase64") or "").strip()
    force = bool(body.get("force"))

    if not filename.lower().endswith(".docx"):
        return jsonify({
            "status": "error",
            "message": "Chỉ hỗ trợ file .docx ở bước này.",
            "filename": filename,
        }), 400

    date_key = str(body.get("dateKey") or "").strip() or _extract_date_key_from_texts(filename, subject)
    if not date_key:
        return jsonify({
            "status": "error",
            "message": "Không xác định được ngày lịch bay từ tên file hoặc tiêu đề.",
            "filename": filename,
            "subject": subject,
        }), 400

    if not file_b64:
        return jsonify({
            "status": "error",
            "message": "Thiếu fileBase64."
        }), 400

    db = _init_firestore_client()
    if db is None:
        return jsonify({
            "status": "error",
            "message": "Firestore chưa sẵn sàng trên server.",
            "firestore_status": FIRESTORE_STATUS,
            "last_firestore_sync_error": LAST_FIRESTORE_SYNC_ERROR,
        }), 500

    import_id = _gmail_import_id(message_id or subject, filename)
    log_ref = db.collection("gmailImportLogs").document(import_id)

    old_log = log_ref.get()
    if old_log.exists and not force:
        old_data = old_log.to_dict() or {}
        if old_data.get("status") == "success":
            return jsonify({
                "status": "skipped",
                "message": "Email/file này đã import thành công trước đó.",
                "date_key": old_data.get("dateKey") or date_key,
                "count": old_data.get("count"),
                "filename": filename,
                "import_id": import_id,
            })

    try:
        file_bytes = base64.b64decode(file_b64)
        flights = _parse_docx_flight_plan_bytes(file_bytes, date_key)
    except Exception as e:
        log.exception("Parse Gmail flight plan failed")
        log_ref.set({
            "status": "error",
            "stage": "parse_docx",
            "message": str(e),
            "dateKey": date_key,
            "filename": filename,
            "subject": subject,
            "from": sender,
            "updatedAt": firebase_firestore.SERVER_TIMESTAMP,
        }, merge=True)
        return jsonify({
            "status": "error",
            "message": f"Parse file .docx lỗi: {e}",
            "date_key": date_key,
            "filename": filename,
        }), 422

    if len(flights) < GMAIL_IMPORT_MIN_FLIGHTS and not force:
        log_ref.set({
            "status": "error",
            "stage": "validate_count",
            "message": f"Parse được quá ít chuyến: {len(flights)}",
            "dateKey": date_key,
            "count": len(flights),
            "filename": filename,
            "subject": subject,
            "from": sender,
            "updatedAt": firebase_firestore.SERVER_TIMESTAMP,
        }, merge=True)
        return jsonify({
            "status": "error",
            "message": f"Parse được quá ít chuyến: {len(flights)}. Không ghi đè lịch.",
            "date_key": date_key,
            "count": len(flights),
        }), 422

    plan_ref = db.collection("flightPlans").document(date_key)
    existing_snap = plan_ref.get()
    existing_count = 0
    if existing_snap.exists:
        old_plan = existing_snap.to_dict() or {}
        old_flights = old_plan.get("flights") if isinstance(old_plan.get("flights"), list) else []
        existing_count = len(old_flights)

    # Chống trường hợp file lỗi format làm ghi đè lịch đầy đủ bằng lịch rỗng/quá ít.
    if existing_count >= 10 and len(flights) < existing_count * 0.6 and not force:
        log_ref.set({
            "status": "error",
            "stage": "validate_existing_count",
            "message": f"Lịch mới chỉ có {len(flights)} chuyến, thấp hơn nhiều so với lịch cũ {existing_count} chuyến.",
            "dateKey": date_key,
            "count": len(flights),
            "existingCount": existing_count,
            "filename": filename,
            "subject": subject,
            "from": sender,
            "updatedAt": firebase_firestore.SERVER_TIMESTAMP,
        }, merge=True)
        return jsonify({
            "status": "error",
            "message": f"Lịch mới chỉ có {len(flights)} chuyến, thấp hơn nhiều so với lịch cũ {existing_count} chuyến. Không ghi đè.",
            "date_key": date_key,
            "count": len(flights),
            "existing_count": existing_count,
        }), 422

    payload = {
        "flights": flights,
        "count": len(flights),
        "uploadedBy": "gmail-auto-import",
        "uploadedByName": "Gmail tự động",
        "uploadedAt": firebase_firestore.SERVER_TIMESTAMP,
        "source": "gmail",
        "sourceEmailSubject": subject,
        "sourceEmailFrom": sender,
        "sourceEmailReceivedAt": received_at,
        "sourceMessageId": message_id,
        "sourceThreadId": thread_id,
        "sourceFilename": filename,
        "importedAt": firebase_firestore.SERVER_TIMESTAMP,
        "importId": import_id,
    }

    try:
        plan_ref.set(payload, merge=True)

        log_ref.set({
            "status": "success",
            "dateKey": date_key,
            "count": len(flights),
            "existingCount": existing_count,
            "filename": filename,
            "subject": subject,
            "from": sender,
            "messageId": message_id,
            "threadId": thread_id,
            "receivedAt": received_at,
            "updatedAt": firebase_firestore.SERVER_TIMESTAMP,
        }, merge=True)

        # Đăng ký ngay vào radar server, không cần chờ chu kỳ sync Firestore.
        schedule_payload = _flight_plan_doc_to_schedule_payload(date_key, {"flights": flights})
        registered = _register_schedule(schedule_payload)

        codes = [_clean_code(code) for code in schedule_payload.keys() if _clean_code(code)]
        if codes:
            with _lock:
                new_codes = [code for code in codes if code not in TRACKED]
                QUEUE.update(codes)
            if new_codes:
                _try_immediate_poll(f"gmail import {date_key} new={len(new_codes)}")

        return jsonify({
            "status": "success",
            "message": f"Đã import {len(flights)} chuyến từ Gmail.",
            "date_key": date_key,
            "count": len(flights),
            "registered": registered,
            "filename": filename,
            "import_id": import_id,
            "firestore_status": FIRESTORE_STATUS,
        })

    except Exception as e:
        log.exception("Write Gmail flight plan to Firestore failed")
        log_ref.set({
            "status": "error",
            "stage": "write_firestore",
            "message": str(e),
            "dateKey": date_key,
            "count": len(flights),
            "filename": filename,
            "subject": subject,
            "from": sender,
            "updatedAt": firebase_firestore.SERVER_TIMESTAMP,
        }, merge=True)
        return jsonify({
            "status": "error",
            "message": f"Ghi Firestore lỗi: {e}",
            "date_key": date_key,
            "count": len(flights),
        }), 500

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


# Khởi tạo Firestore sớm để log trạng thái ngay khi deploy. Poller vẫn sẽ retry nếu credentials chưa sẵn sàng.
_init_firestore_client()
_start_poller()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    # Production trên Render nên dùng gunicorn:
    #   gunicorn -w 1 -k gthread --threads 4 --timeout 90 app:app
    # (-w 1: 1 worker để chia sẻ TRACKED giữa các thread HTTP và poller.
    #  --threads 4: 4 thread HTTP, đủ cho concurrency. --timeout 90: chịu được
    #  poll dài khi nhiều flight trong queue.)
    app.run(host="0.0.0.0", port=port)
