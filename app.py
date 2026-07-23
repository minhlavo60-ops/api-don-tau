# -*- coding: utf-8 -*-
r"""
RADAR DAD — server radar MỚI cho app "Nhật Ký Đón Tàu"
Nguồn: Flightradar24 (ADS-B cộng đồng) qua FlightRadarAPI. KHÔNG học, KHÔNG nội suy —
chỉ dữ liệu thật 10s/lần + nạp lịch bay + quét cả tàu ngoài lịch về DAD + chốt giờ hạ cánh.

MỘT FILE, HAI VAI (env RADAR_MODE, tự nhận):
  ghi  (máy nhà/cơ quan): quét FR24 → phục vụ local → (nếu có RADAR_PUSH_URL) đẩy lên Render
  phat (trên Render):     KHÔNG gọi FR24 (FR24 chặn IP Render) — nhận gói /api/ingest rồi phục vụ

API (giữ tương thích app cũ): POST /api/etas, /api/scan_arrivals, /api/schedule
Mini radar: GET /radar.html + /api/flights

Chạy local:  .venv\Scripts\python.exe server.py   → http://localhost:8600
Lên Render:  đổi tên file thành app.py (hoặc start command: gunicorn server:app);
             Render tự nhận vai "phat" qua biến môi trường RENDER.
"""

import io
import json
import math
import os
import re
import sys
import threading
import time
import urllib.request
from collections import deque
from datetime import datetime

from flask import Flask, jsonify, request, send_from_directory

# console Windows mặc định cp1252 — ép UTF-8; line_buffering để print ra ngay khi chạy dưới pipe
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

# ---------------------------------------------------------------- vai trò
# [thử nghiệm theo yêu cầu] Mặc định vai GHI ở MỌI NƠI, kể cả Render — tự quét FR24
# trực tiếp, không cần máy tính đẩy dữ liệu, không cần đặt env gì cả.
# Nếu FR24 chặn IP Render (log 'Feed rỗng' lặp mãi, live luôn 0) thì đặt env
# RADAR_MODE=phat rồi bật máy ghi ở nhà đẩy lên (xem HUONG-DAN-LEN-RENDER.md).
MODE = ((os.environ.get("RADAR_MODE") or "ghi").strip().lower() or "ghi")

# Vai ghi: đẩy dữ liệu lên đâu (ví dụ https://api-don-tau.onrender.com). Rỗng = không đẩy.
PUSH_URL = (os.environ.get("RADAR_PUSH_URL") or "").rstrip("/")
# Mật khẩu chung giữa máy ghi và Render. NHỚ ĐỔI (đặt env 2 bên giống nhau).
INGEST_SECRET = os.environ.get("RADAR_INGEST_SECRET") or "doi-mat-khau-nay-di"

# ---------------------------------------------------------------- cấu hình
DAD_LAT, DAD_LON = 16.0439, 108.1994
POLL_INTERVAL = 10
BACKOFF_START = 60
STALE_DROP = 180                # mất tín hiệu 180s -> rớt khỏi live
TERMINAL_KEEP_S = 30 * 60      # tàu đã hạ: giữ trả /api/etas thêm 30' để app chốt giờ
TRAIL_LEN = 120
INGEST_STALE_S = 120           # phat: gói đẩy cũ hơn 120s coi như radar nguồn đang mất

FINAL_DIST_KM = 8.0
FINAL_ALT_FT = 3000
APPROACH_DIST_KM = 70.0
NEAR_AIRPORT_KM = 8.0
ROLLOUT_MIN_KT = 45
PARKED_MAX_KT = 4

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)
SCHEDULE_FILE = os.path.join(DATA_DIR, "schedule.json")
LANDINGS_FILE = os.path.join(DATA_DIR, "landings.json")

REGIONS = [
    "40.0,20.0,90.0,120.0",   # Trung Quốc + Bắc ĐNÁ
    "40.0,20.0,120.0,145.0",  # Hàn + Nhật + Đài Loan
    "20.0,0.0,90.0,145.0",    # Việt Nam + Nam ĐNÁ
]

AIRLINE_MAP = {
    "VN": "HVN", "VJ": "VJC", "QH": "BAV", "BL": "PIC", "VU": "VAG",
    "KE": "KAL", "OZ": "AAR", "TW": "TWB", "LJ": "JNA", "7C": "JJA",
    "BX": "ABL", "RS": "ASV", "ZE": "ESR", "CI": "CAL", "BR": "EVA",
    "IT": "TTW", "JX": "SJX", "FD": "AIQ", "VZ": "TVJ", "SL": "TLM",
    "TG": "THA", "WE": "THD", "AK": "AXM", "TR": "TGW", "SQ": "SIA",
    "MU": "CES", "CZ": "CSN", "CA": "CCA", "9C": "CQH", "HO": "DKH",
    "MF": "CXA", "UO": "HKE", "CX": "CPA", "HX": "CRK", "5J": "CEB",
    "PR": "PAL", "OD": "MXD", "D7": "XAX", "NH": "ANA", "JL": "JAL",
    "MM": "APJ", "GS": "GCR", "K6": "KHV", "QV": "LAO", "MI": "SLK",
}
ICAO_TO_IATA = {v: k for k, v in AIRLINE_MAP.items()}

app = Flask(__name__, static_folder="static", static_url_path="")
HAS_STATIC = os.path.isdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "static"))

lock = threading.Lock()

# live: fr24_id -> dict thuần (dùng chung cho cả 2 vai — vai phat nhận qua /api/ingest)
live: dict[str, dict] = {}
trails: dict[str, deque] = {}          # chỉ vai ghi
terminal_cache: dict[str, dict] = {}   # alias -> {"entry":..., "at": epoch}
schedule_by_code: dict[str, dict] = {}
schedule_rev = 0

state = {
    "last_update": None,      # lần có dữ liệu tươi gần nhất (poll ok / ingest ok)
    "last_error": None,
    "error_count": 0,
    "poll_count": 0,          # ghi: số chu kỳ quét; phat: số gói ingest đã nhận
    "backoff_until": 0,
    "last_push_ok": None,     # ghi: lần đẩy Render thành công gần nhất
    "last_push_error": None,
}


# ---------------------------------------------------------------- tiện ích
def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


CODE_RE = re.compile(r"^([A-Z]{1,3}|[A-Z]\d|\d[A-Z])0*(\d{1,4})([A-Z]?)$")

def canon(code):
    s = re.sub(r"[^A-Z0-9]", "", str(code or "").upper())
    m = CODE_RE.match(s)
    return f"{m.group(1)}{m.group(2)}{m.group(3)}" if m else s

def code_variants(code):
    c = canon(code)
    out = {c} if c else set()
    m = CODE_RE.match(c)
    if m:
        prefix, num, suf = m.group(1), m.group(2), m.group(3)
        if prefix in AIRLINE_MAP:
            out.add(f"{AIRLINE_MAP[prefix]}{num}{suf}")
        if prefix in ICAO_TO_IATA:
            out.add(f"{ICAO_TO_IATA[prefix]}{num}{suf}")
    return out


def load_json(path, default):
    try:
        return json.load(io.open(path, encoding="utf-8"))
    except Exception:
        return default

def save_json(path, obj):
    try:
        tmp = path + ".tmp"
        io.open(tmp, "w", encoding="utf-8").write(json.dumps(obj, ensure_ascii=False))
        os.replace(tmp, path)
    except Exception as e:
        print("[!] Không ghi được", path, e)


def date_key_now():
    # "ngày bay" đổi lúc 3h sáng (ca đêm thuộc hôm trước) — khớp app
    dt = datetime.now()
    if dt.hour < 3:
        dt = datetime.fromtimestamp(time.time() - 3 * 3600)
    return dt.strftime("%Y-%m-%d")


landings_store = load_json(LANDINGS_FILE, {})   # {date_key: {alias: {landed_ms, parked_ms}}}

def remember_landing(aliases, landed_ms=None, parked_ms=None):
    day = landings_store.setdefault(date_key_now(), {})
    changed = False
    for a in aliases:
        rec = day.setdefault(a, {})
        if landed_ms and not rec.get("landed_ms"):
            rec["landed_ms"] = int(landed_ms); changed = True
        if parked_ms and not rec.get("parked_ms"):
            rec["parked_ms"] = int(parked_ms); changed = True
    if changed:
        save_json(LANDINGS_FILE, landings_store)

def recall_landing(aliases):
    day = landings_store.get(date_key_now(), {})
    for a in aliases:
        if a in day:
            return day[a]
    return {}


_saved = load_json(SCHEDULE_FILE, {})
if isinstance(_saved, dict) and MODE == "ghi":
    schedule_by_code.update(_saved)
    if schedule_by_code:
        print(f"[i] Nạp lại lịch bay đã lưu: {len(schedule_by_code)} chuyến")


# ---------------------------------------------------------------- phân loại + entry
def classify(d):
    spd = d.get("speed_kt") or 0
    dist = d.get("dist_km") or 9999
    if d.get("on_ground"):
        if dist > NEAR_AIRPORT_KM * 3:
            return "EN_ROUTE"          # on_ground xa DAD = dữ liệu lạ / còn ở sân đi
        if spd >= ROLLOUT_MIN_KT:
            return "LANDED"
        if spd <= PARKED_MAX_KT and d.get("stop_streak", 0) >= 2:
            return "PARKED"
        return "TAXIING"
    if dist <= FINAL_DIST_KM and (d.get("alt_ft") or 0) <= FINAL_ALT_FT:
        return "FINAL"
    if dist <= APPROACH_DIST_KM:
        return "APPROACH"
    return "EN_ROUTE"


def build_entry(d, now_ms, sched=None):
    """Entry đúng các trường app đọc trong normalizeFlightFromApi()."""
    dist = d.get("dist_km")
    spd_kt = d.get("speed_kt") or 0
    state_str = classify(d)
    eta_ms = None
    if not d.get("on_ground") and spd_kt > 80 and dist is not None:
        eta_ms = now_ms + int(dist / (spd_kt * 1.852) * 3600 * 1000)
    remembered = recall_landing(d.get("aliases") or [])
    landed_ms = d.get("landed_ms") or remembered.get("landed_ms")
    parked_ms = d.get("parked_ms") or remembered.get("parked_ms")
    entry = {
        "state": state_str,
        "eta_millis": eta_ms,
        "sibt_millis": (sched or {}).get("sibt_millis"),
        "latitude": d.get("lat"),
        "longitude": d.get("lon"),
        "heading": d.get("heading"),
        "altitude_ft": d.get("alt_ft"),
        "ground_speed_kt": spd_kt,
        "distance_km": round(dist, 1) if dist is not None else None,
        "aircraft_type": d.get("aircraft") or (sched or {}).get("aircraft") or "",
        "origin_iata": d.get("origin") or (sched or {}).get("origin") or "",
        "destination_iata": "DAD",
        "confidence": "HIGH",
        "source": "rada-dad",
        "position_time_millis": d.get("time_ms") or now_ms,
        "seen_at_millis": int((d.get("seen") or time.time()) * 1000),
        "merge_time_quality": "telemetry",
        "merge_position_quality": "FINE",
        "merge_quality_score": 95,
        "registration": d.get("registration") or "",
        "callsign": d.get("callsign") or "",
    }
    if landed_ms:
        entry["actual_landed_at_millis"] = int(landed_ms)
        entry["actual_landed_source"] = "radar_ground"
        # [yêu cầu user] Tàu HẠ CÁNH là ẨN khỏi bản đồ ngay (app đọc map_hidden).
        # List + chốt giờ vẫn đầy đủ — chỉ marker trên map biến mất.
        entry["map_hidden"] = True
    if parked_ms:
        entry["actual_parked_at_millis"] = int(parked_ms)
        entry["actual_parked_source"] = "radar_stop"
        entry["actual_parked_provisional"] = state_str != "PARKED"
    return entry


def alias_index():
    idx = {}
    for fid, d in live.items():
        for a in d.get("aliases") or []:
            idx[a] = fid
    return idx


def freeze_terminal(d, now):
    """Tàu có mốc hạ cánh: đóng băng entry để còn trả app 30' nữa dù rớt feed."""
    ent = build_entry(d, int(now * 1000))
    for a in d.get("aliases") or []:
        terminal_cache[a] = {"entry": ent, "at": now}


# ---------------------------------------------------------------- vai GHI: quét FR24
if MODE == "ghi":
    from FlightRadarAPI import FlightRadar24API
    fr_api = FlightRadar24API()

def flight_to_plain(f, prev, now, now_ms):
    """Flight FR24 -> dict thuần (kèm theo dõi chạm đất/dừng để chốt giờ)."""
    dist = haversine_km(f.latitude, f.longitude, DAD_LAT, DAD_LON)
    spd = f.ground_speed or 0
    aliases = set()
    for c in (f.number, f.callsign):
        aliases |= code_variants(c)
    aliases.discard("")
    d = {
        "id": f.id,
        "number": f.number or "", "callsign": f.callsign or "",
        "airline": f.airline_iata or f.airline_icao or "",
        "aircraft": f.aircraft_code or "", "registration": f.registration or "",
        "lat": f.latitude, "lon": f.longitude,
        "alt_ft": f.altitude, "speed_kt": spd,
        "heading": f.heading, "vspeed": f.vertical_speed,
        "on_ground": bool(f.on_ground),
        "origin": f.origin_airport_iata or "",
        "time_ms": int(f.time * 1000) if getattr(f, "time", None) else now_ms,
        "aliases": sorted(aliases),
        "dist_km": dist,
        "seen": now,
        "landed_ms": (prev or {}).get("landed_ms"),
        "parked_ms": (prev or {}).get("parked_ms"),
        "stop_streak": (prev or {}).get("stop_streak", 0),
    }
    near = dist <= NEAR_AIRPORT_KM
    if d["on_ground"] and dist <= NEAR_AIRPORT_KM * 3:
        if d["landed_ms"] is None and near:
            d["landed_ms"] = now_ms                       # mốc chạm đất, lệch tối đa 1 nhịp 10s
        d["stop_streak"] = d["stop_streak"] + 1 if spd <= PARKED_MAX_KT else 0
        if d["parked_ms"] is None and d["stop_streak"] >= 2 and near:
            d["parked_ms"] = now_ms - POLL_INTERVAL * 1000  # mốc bắt đầu dừng hẳn
    else:
        d["stop_streak"] = 0
    return d


def poll_once():
    now = time.time()
    now_ms = int(now * 1000)
    fresh = {}
    total = 0
    for bounds in REGIONS:
        batch = fr_api.get_flights(bounds=bounds)
        total += len(batch)
        hits = 0
        for f in batch:
            if f.destination_airport_iata == "DAD" and f.latitude and f.longitude:
                with lock:
                    prev = live.get(f.id)
                fresh[f.id] = flight_to_plain(f, prev, now, now_ms)
                hits += 1
        print(f"[poll #{state['poll_count'] + 1}] box {bounds}: {len(batch)} tàu, về DAD: {hits}")
        time.sleep(0.4)
    if total == 0:
        return 0    # FR24 chặn mềm (feed rỗng) — poll_loop xử lý

    with lock:
        for fid, d in fresh.items():
            live[fid] = d
            trail = trails.setdefault(fid, deque(maxlen=TRAIL_LEN))
            if not trail or (trail[-1][0] != d["lat"] or trail[-1][1] != d["lon"]):
                trail.append([d["lat"], d["lon"]])
            if d.get("landed_ms") or d.get("parked_ms"):
                remember_landing(d["aliases"], d.get("landed_ms"), d.get("parked_ms"))
            if d.get("landed_ms"):
                freeze_terminal(d, now)
        # dọn tàu mất tín hiệu / đã đỗ lâu
        for fid in list(live):
            d = live[fid]
            parked_long = d.get("parked_ms") and time.time() * 1000 - d["parked_ms"] > 300_000
            if parked_long or now - d["seen"] > STALE_DROP:
                live.pop(fid, None); trails.pop(fid, None)
        for a in list(terminal_cache):
            if now - terminal_cache[a]["at"] > TERMINAL_KEEP_S:
                terminal_cache.pop(a, None)
        state["last_update"] = now
        state["poll_count"] += 1
        state["last_error"] = None
    return total


def push_snapshot():
    """Vai ghi: đẩy toàn bộ trạng thái lên Render (vai phat) sau mỗi chu kỳ."""
    if not PUSH_URL:
        return
    with lock:
        payload = {
            "sent_at_millis": int(time.time() * 1000),
            "date_key": date_key_now(),
            "live": list(live.values()),
            "terminal": {a: t["entry"] for a, t in terminal_cache.items()},
            "landings_today": landings_store.get(date_key_now(), {}),
        }
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        PUSH_URL + "/api/ingest", data=data, method="POST",
        headers={"Content-Type": "application/json", "X-Radar-Secret": INGEST_SECRET})
    try:
        with urllib.request.urlopen(req, timeout=8) as res:
            res.read()
        state["last_push_ok"] = time.time()
        state["last_push_error"] = None
    except Exception as e:
        state["last_push_error"] = f"{type(e).__name__}: {e}"
        print("[!] Đẩy Render lỗi:", state["last_push_error"])


def poll_loop():
    global fr_api
    backoff = BACKOFF_START
    empty_backoff = 30
    empty_streak = 0
    while True:
        if time.time() >= state["backoff_until"]:
            try:
                total = poll_once()
                if total == 0:
                    empty_streak += 1
                    with lock:
                        state["last_error"] = (f"FR24 trả feed rỗng lần {empty_streak} "
                                               "(chặn mềm) — đang chờ FR24 nguôi")
                    # 2 lần đầu: thử ngay phiên mới (nhiều khi thoát liền).
                    # Từ lần 3: giữ nguyên phiên + chờ lâu dần — tạo phiên mới liên tục
                    # từ IP đang bị nghi chỉ khiến FR24 chặn dai hơn.
                    if empty_streak <= 2 or empty_streak % 3 == 0:
                        fr_api = FlightRadar24API()
                        note = "tạo phiên FR24 mới"
                    else:
                        note = "giữ phiên, kiên nhẫn chờ"
                    print(f"[!] Feed rỗng lần {empty_streak} — {note}, nghỉ {empty_backoff}s")
                    state["backoff_until"] = time.time() + empty_backoff
                    empty_backoff = min(int(empty_backoff * 1.7), 600)
                else:
                    if empty_streak:
                        print(f"[i] FR24 đã tha — có dữ liệu lại sau {empty_streak} lần rỗng")
                    backoff = BACKOFF_START
                    empty_backoff = 30
                    empty_streak = 0
                    push_snapshot()
            except Exception as e:
                with lock:
                    state["last_error"] = f"{type(e).__name__}: {e}"
                    state["error_count"] += 1
                msg = str(e).lower()
                if "429" in msg or "402" in msg or "403" in msg or "too many" in msg:
                    state["backoff_until"] = time.time() + backoff
                    print(f"[!] Bị giới hạn tốc độ, nghỉ {backoff}s...")
                    backoff = min(backoff * 2, 900)
                else:
                    print(f"[!] Lỗi poll: {e}")
        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------- vai PHÁT: nhận gói
@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    if request.headers.get("X-Radar-Secret") != INGEST_SECRET:
        return jsonify({"status": "forbidden"}), 403
    body = request.get_json(silent=True) or {}
    now = time.time()
    with lock:
        live.clear()
        for d in body.get("live") or []:
            if d.get("id"):
                live[d["id"]] = d
        terminal_cache.clear()
        for a, ent in (body.get("terminal") or {}).items():
            terminal_cache[a] = {"entry": ent, "at": now}
        day = landings_store.setdefault(body.get("date_key") or date_key_now(), {})
        for a, rec in (body.get("landings_today") or {}).items():
            cur = day.setdefault(a, {})
            for k, v in rec.items():
                cur.setdefault(k, v)
        state["last_update"] = now
        state["poll_count"] += 1
        state["last_error"] = None
    return jsonify({"status": "success"})


# ---------------------------------------------------------------- API chung cho app
def current_revision():
    return f"rada-dad-{schedule_rev}-{state['poll_count']}"

def source_stale():
    return not state["last_update"] or time.time() - state["last_update"] > INGEST_STALE_S


def prune_schedule():
    """Cơ chế ĐỔI NGÀY BAY 3h sáng: lịch của ngày bay CŨ tự rụng (giữ hôm nay + tương lai,
    vd admin nạp trước lịch ngày mai). Gọi ở mọi đường vào, không đợi ai nạp lịch mới."""
    global schedule_rev
    fd = date_key_now()
    removed = 0
    for c in list(schedule_by_code):
        dk = schedule_by_code[c].get("date_key")
        if dk and dk < fd:
            schedule_by_code.pop(c)
            removed += 1
    if removed:
        schedule_rev += 1
        if MODE == "ghi":
            save_json(SCHEDULE_FILE, schedule_by_code)
        print(f"[i] Đổi ngày bay {fd}: gỡ {removed} chuyến lịch cũ")
    return removed


@app.route("/api/schedule", methods=["POST"])
def api_schedule():
    global schedule_rev
    body = request.get_json(silent=True) or {}
    items = body.get("flights") or []
    n = 0
    with lock:
        for it in items:
            code = canon(it.get("code"))
            if not code:
                continue
            aliases = set(code_variants(code))
            for a in it.get("code_aliases") or []:
                aliases |= code_variants(a)
            schedule_by_code[code] = {
                "code": code,
                "sibt_millis": it.get("sibt_millis"),
                "origin": (it.get("origin") or "").upper(),
                "aircraft": (it.get("aircraft") or "").upper(),
                "route": it.get("route") or "",
                "stand": it.get("stand") or "",
                "date_key": it.get("date_key") or date_key_now(),
                "aliases": sorted(aliases),
            }
            n += 1
        prune_schedule()
        schedule_rev += 1
        if MODE == "ghi":
            save_json(SCHEDULE_FILE, schedule_by_code)   # Render disk là tạm, chỉ lưu ở máy ghi
    print(f"[i] Nhận lịch bay: {n} chuyến (tổng {len(schedule_by_code)})")
    return jsonify({"status": "success", "count": n})


@app.route("/api/etas", methods=["POST"])
def api_etas():
    body = request.get_json(silent=True) or {}
    codes = [canon(c) for c in (body.get("codes") or []) if canon(c)]
    known_rev = body.get("known_revision")
    now_ms = int(time.time() * 1000)
    rev = current_revision()
    if known_rev and known_rev == rev and not body.get("force_refresh"):
        return jsonify({"status": "success", "server_time_millis": now_ms,
                        "feed_revision": rev, "not_modified": True,
                        "radar_live_stale": source_stale(), "flights": {}})
    out = {}
    with lock:
        prune_schedule()   # đổi ngày bay 3h sáng tự động
        idx = alias_index()
        for code in codes:
            sched = schedule_by_code.get(code)
            aliases = set(code_variants(code))
            if sched:
                aliases |= set(sched.get("aliases") or [])
            # 1) đang live trên radar
            fid = next((idx[a] for a in aliases if a in idx), None)
            if fid:
                out[code] = build_entry(live[fid], now_ms, sched)
                continue
            # 2) vừa hạ xong, đã rớt feed -> entry đóng băng (để app chốt giờ)
            frozen = next((terminal_cache[a] for a in aliases if a in terminal_cache), None)
            if frozen:
                ent = dict(frozen["entry"])
                if sched and sched.get("sibt_millis") and not ent.get("sibt_millis"):
                    ent["sibt_millis"] = sched["sibt_millis"]
                out[code] = ent
                continue
            # 3) chưa live nhưng có lịch -> SCHEDULED để app vẽ card, không sót tàu
            if sched and sched.get("sibt_millis"):
                remembered = recall_landing(aliases)
                ent = {
                    "state": "SCHEDULED", "scheduled": True,
                    "sibt_millis": sched["sibt_millis"], "eta_millis": None,
                    "origin_iata": sched.get("origin") or "",
                    "aircraft_type": sched.get("aircraft") or "",
                    "destination_iata": "DAD", "confidence": "LOW",
                    "source": "rada-dad", "seen_at_millis": now_ms,
                }
                if remembered.get("landed_ms"):
                    ent["actual_landed_at_millis"] = remembered["landed_ms"]
                    ent["actual_landed_source"] = "radar_ground"
                    ent["state"] = "PARKED"
                    ent["map_hidden"] = True   # đã hạ từ trước -> không vẽ lại lên map
                out[code] = ent
    return jsonify({"status": "success", "server_time_millis": now_ms,
                    "feed_revision": rev, "not_modified": False,
                    "radar_live_stale": source_stale(), "flights": out})


@app.route("/api/scan_arrivals", methods=["POST"])
def api_scan_arrivals():
    """Tàu NGOÀI lịch vẫn bay về DAD — quét 3 ô đã bắt sẵn, chỉ việc trả."""
    body = request.get_json(silent=True) or {}
    max_minutes = float(body.get("max_minutes") or 60)
    now_ms = int(time.time() * 1000)
    out = []
    with lock:
        prune_schedule()   # đổi ngày bay 3h sáng tự động
        scheduled_aliases = set()
        for sched in schedule_by_code.values():
            scheduled_aliases |= set(sched.get("aliases") or [])
        for d in live.values():
            aliases = set(d.get("aliases") or [])
            ent = build_entry(d, now_ms)
            if ent["eta_millis"] and (ent["eta_millis"] - now_ms) > max_minutes * 60000:
                continue
            ent["flight_code"] = canon(d.get("number") or d.get("callsign"))
            ent["outside_schedule"] = not (aliases & scheduled_aliases)
            ent["discovery_source"] = "scan_arrivals"
            out.append(ent)
    return jsonify({"status": "success", "server_time_millis": now_ms,
                    "feed_revision": current_revision(),
                    "radar_live_stale": source_stale(), "flights": out})


# ---------------------------------------------------------------- mini radar + tĩnh
@app.route("/api/flights")
def api_flights():
    now = time.time()
    with lock:
        outs = []
        for d in live.values():
            spd_kmh = (d.get("speed_kt") or 0) * 1.852
            dist = d.get("dist_km") or 0
            outs.append({
                "id": d["id"], "callsign": d.get("callsign") or "?",
                "number": d.get("number") or "", "airline": d.get("airline") or "",
                "aircraft": d.get("aircraft") or "", "registration": d.get("registration") or "",
                "lat": d.get("lat"), "lon": d.get("lon"), "alt_ft": d.get("alt_ft"),
                "speed_kt": d.get("speed_kt"), "speed_kmh": round(spd_kmh),
                "heading": d.get("heading"), "vspeed": d.get("vspeed"),
                "origin": d.get("origin") or "?", "dist_km": round(dist),
                "eta_min": round(dist / spd_kmh * 60) if spd_kmh > 50 else None,
                "landed": bool(d.get("landed_ms")), "landed_at": (d.get("landed_ms") or 0) / 1000 or None,
                "seen": d.get("seen"),
            })
        outs.sort(key=lambda x: x["dist_km"])
        payload = {
            "airport": {"iata": "DAD", "name": "Sân bay quốc tế Đà Nẵng",
                        "lat": DAD_LAT, "lon": DAD_LON},
            "poll_interval": POLL_INTERVAL,
            "last_update": state["last_update"],
            "last_error": state["last_error"],
            "error_count": state["error_count"],
            "poll_count": state["poll_count"],
            "backoff_until": state["backoff_until"] if state["backoff_until"] > now else None,
            "flights": outs,
            "trails": {fid: list(t) for fid, t in trails.items()},
        }
    return jsonify(payload)


@app.route("/")
def index():
    if HAS_STATIC:
        return send_from_directory("static", "index.html")
    # Trên Render không kèm static -> "/" làm health check + warmup cho app
    return jsonify({"status": "ok", "role": MODE, "revision": current_revision(),
                    "live": len(live), "stale": source_stale()})


# CORS: web app (nhat-ky-don.web.app) gọi chéo origin vào Render + gửi header Authorization
@app.after_request
def add_cors(res):
    res.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin") or "*"
    res.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    res.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, Cache-Control, X-Radar-Secret"
    res.headers["Access-Control-Max-Age"] = "3600"
    return res

@app.route("/api/<path:_any>", methods=["OPTIONS"])
def cors_preflight(_any):
    return ("", 204)


# ---------------------------------------------------------------- khởi động
print("=" * 60)
print(f"  RADAR DAD — vai: {MODE.upper()}"
      + ("  (quét FR24 + phục vụ local" + (" + đẩy Render)" if PUSH_URL else ")") if MODE == "ghi"
         else "  (KHÔNG gọi FR24 — nhận /api/ingest rồi phục vụ)"))
if MODE == "ghi" and PUSH_URL:
    print(f"  Đẩy dữ liệu lên: {PUSH_URL}")
if INGEST_SECRET == "doi-mat-khau-nay-di":
    print("  [!] Đang dùng mật khẩu ingest MẶC ĐỊNH — nhớ đặt RADAR_INGEST_SECRET cả 2 bên!")
print("=" * 60)

# Vòng quét PHẢI chạy trong CHÍNH tiến trình đang trả lời web (gunicorn có thể nạp
# code ở tiến trình mẹ rồi đẻ con — thread bật lúc nạp sẽ kẹt ở mẹ, con rỗng bộ nhớ).
# Cách chắc ăn: bật lười ở request đầu tiên — tiến trình nào phục vụ web thì tự quét.
_poll_started = False
_poll_start_lock = threading.Lock()

def ensure_poll_thread():
    global _poll_started
    if MODE != "ghi" or _poll_started:
        return
    with _poll_start_lock:
        if _poll_started:
            return
        _poll_started = True
        threading.Thread(target=poll_loop, daemon=True).start()
        print(f"[i] (pid {os.getpid()}) Bật vòng quét FR24 trong tiến trình phục vụ web")

@app.before_request
def _boot_poller():
    ensure_poll_thread()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8600"))
    print(f"  Mở: http://localhost:{port}" + ("  •  mini radar: /radar.html" if HAS_STATIC else ""))
    ensure_poll_thread()   # chạy tay (python server.py): bật quét ngay, khỏi chờ request
    app.run(host="127.0.0.1" if MODE == "ghi" and not os.environ.get("RENDER") else "0.0.0.0",
            port=port, debug=False)
