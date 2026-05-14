from flask import Flask, jsonify, request
from FlightRadar24 import FlightRadar24API
import hmac
import os
import threading
import time
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ETA_API_KEY = os.environ.get(
    "ETA_API_KEY",
    "0982e7c09397ca3ed579775a9a29ff208a1715d0526fbb65b67a61c1cb126923",
)

POLL_INTERVAL = 60                       # poll nền: 1 phút/lần
IMMEDIATE_POLL_COOLDOWN_MS = 10_000      # ép poll ngay nhưng tối thiểu cách nhau 10s
TARGET_AIRPORT = "DAD"
STALE_AFTER_MS = 10 * 60 * 1000

_lock = threading.Lock()
_immediate_poll_lock = threading.Lock()
QUEUE = set()
TRACKED = {}
LAST_POLL_MS = None
LAST_IMMEDIATE_POLL_MS = 0
LAST_ERROR = None

fr_api = FlightRadar24API()


def _do_poll():
    """1 chu kỳ poll FR24 → cập nhật TRACKED. Có thể raise lên trên."""
    global LAST_POLL_MS, LAST_ERROR
    airport = fr_api.get_airport_details(TARGET_AIRPORT)
    arrivals = (
        airport.get('airport', {})
               .get('pluginData', {})
               .get('schedule', {})
               .get('arrivals', {})
               .get('data', []) or []
    )

    now_ms = int(time.time() * 1000)
    seen = {}
    for item in arrivals:
        flight = item.get('flight') or {}
        ident = flight.get('identification') or {}
        number = ((ident.get('number') or {}).get('default') or "").upper()
        callsign = (ident.get('callsign') or "").upper()
        eta_sec = ((flight.get('time') or {}).get('estimated') or {}).get('arrival')
        status_text = ((flight.get('status') or {}).get('text') or "")
        entry = {
            "eta_millis": eta_sec * 1000 if eta_sec else None,
            "status": status_text,
            "updated_at": now_ms,
        }
        if number:
            seen[number] = entry
        if callsign:
            seen[callsign] = entry

    with _lock:
        matched = 0
        for code in QUEUE:
            if code in seen:
                TRACKED[code] = seen[code]
                matched += 1
        LAST_POLL_MS = now_ms
        LAST_ERROR = None
    log.info(f"Poll DAD: {len(arrivals)} arrivals, matched {matched}/{len(QUEUE)}")


def _try_immediate_poll(reason: str = "") -> bool:
    """Poll ngay khi có sự kiện 'mã mới', nhưng không sát nhau quá COOLDOWN."""
    global LAST_IMMEDIATE_POLL_MS, LAST_ERROR
    with _immediate_poll_lock:
        now = int(time.time() * 1000)
        if now - LAST_IMMEDIATE_POLL_MS < IMMEDIATE_POLL_COOLDOWN_MS:
            return False
        LAST_IMMEDIATE_POLL_MS = now

    try:
        _do_poll()
        log.info(f"Immediate poll OK ({reason})")
        return True
    except Exception as e:
        with _lock:
            LAST_ERROR = str(e)
        log.warning(f"Immediate poll fail ({reason}): {e}")
        return False


def poll_arrivals_loop():
    global LAST_ERROR
    while True:
        try:
            _do_poll()
        except Exception as e:
            with _lock:
                LAST_ERROR = str(e)
            log.exception("Scheduled poll failed")
        time.sleep(POLL_INTERVAL)


def require_api_key():
    client_key = request.headers.get("X-API-Key", "")
    if not ETA_API_KEY:
        return jsonify({"status": "error", "message": "Server chưa cấu hình ETA_API_KEY"}), 500
    if not hmac.compare_digest(client_key, ETA_API_KEY):
        return jsonify({"status": "error", "message": "Sai mật khẩu truy cập server"}), 401
    return None


def build_etas_payload():
    now_ms = int(time.time() * 1000)
    with _lock:
        result = {}
        for code in QUEUE:
            data = TRACKED.get(code)
            if data is None:
                result[code] = {"status": "pending"}
            else:
                stale = (now_ms - data["updated_at"]) > STALE_AFTER_MS
                result[code] = {**data, "stale": stale}
        return {
            "status": "success",
            "server_time_millis": now_ms,
            "last_poll_millis": LAST_POLL_MS,
            "last_error": LAST_ERROR,
            "flights": result,
        }


@app.route('/api/track/<flight_code>', methods=['POST'])
def add_track(flight_code):
    if (err := require_api_key()): return err
    code = flight_code.strip().upper()
    with _lock:
        is_new = code not in TRACKED
        QUEUE.add(code)
    if is_new:
        _try_immediate_poll(f"track new {code}")
    return jsonify({"status": "success", "tracked": sorted(QUEUE)})


@app.route('/api/track/<flight_code>', methods=['DELETE'])
def remove_track(flight_code):
    if (err := require_api_key()): return err
    code = flight_code.strip().upper()
    with _lock:
        QUEUE.discard(code)
        TRACKED.pop(code, None)
    return jsonify({"status": "success", "tracked": sorted(QUEUE)})


@app.route('/api/etas', methods=['GET', 'POST'])
def get_all_etas():
    if (err := require_api_key()): return err

    if request.method == 'POST':
        body = request.get_json(silent=True) or {}
        codes = body.get('codes', []) or []
        cleaned = [c.strip().upper() for c in codes if isinstance(c, str) and c.strip()]
        if cleaned:
            with _lock:
                new_codes = [c for c in cleaned if c not in TRACKED]
                QUEUE.update(cleaned)
            if new_codes:
                _try_immediate_poll(f"new codes {new_codes}")

    return jsonify(build_etas_payload())


@app.route('/api/get_eta/<flight_code>', methods=['GET'])
def get_eta(flight_code):
    if (err := require_api_key()): return err
    code = flight_code.strip().upper()
    with _lock:
        QUEUE.add(code)
        data = TRACKED.get(code)

    # Cache trống → ép poll ngay (cooldown sẽ kiểm soát tần suất)
    if data is None:
        _try_immediate_poll(f"miss {code}")
        with _lock:
            data = TRACKED.get(code)

    if data is None:
        return jsonify({
            "status": "pending",
            "message": f"Đã thêm {code} vào queue, FR24 chưa thấy chuyến này"
        })
    if data.get("eta_millis") is None:
        return jsonify({
            "status": "error",
            "message": f"Chuyến {code} chưa có ETA / đã hạ cánh",
            "status_text": data.get("status"),
        }), 400

    return jsonify({
        "status": "success",
        "flight_code": code,
        "destination": "Da Nang (DAD)",
        "eta_millis": data["eta_millis"],
        "status_text": data.get("status"),
        "updated_at": data.get("updated_at"),
    })


def _start_poller():
    t = threading.Thread(target=poll_arrivals_loop, daemon=True)
    t.start()


_start_poller()


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
