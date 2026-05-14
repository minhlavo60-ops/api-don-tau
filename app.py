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

POLL_INTERVAL = 60           # 1 phút
TARGET_AIRPORT = "DAD"
STALE_AFTER_MS = 10 * 60 * 1000   # data > 10 phút coi như stale

_lock = threading.Lock()
QUEUE = set()           # flight codes app đăng ký theo dõi
TRACKED = {}            # code -> {eta_millis, status, updated_at}
LAST_POLL_MS = None
LAST_ERROR = None

fr_api = FlightRadar24API()


# =====================================================
# Background poller — 1 call FR24/phút, cập nhật cả nhóm
# =====================================================
def poll_arrivals_loop():
    global LAST_POLL_MS, LAST_ERROR
    while True:
        try:
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

            log.info(f"Polled DAD: {len(arrivals)} arrivals, matched {matched}/{len(QUEUE)} tracked")

        except Exception as e:
            with _lock:
                LAST_ERROR = str(e)
            log.exception("Poll failed")

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


# =====================================================
# Endpoints
# =====================================================
@app.route('/api/track/<flight_code>', methods=['POST'])
def add_track(flight_code):
    if (err := require_api_key()): return err
    code = flight_code.strip().upper()
    with _lock:
        QUEUE.add(code)
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
    """
    GET  → trả về toàn bộ tàu đang theo dõi (cache).
    POST → body {"codes":["VN123","VJ456",...]} sẽ đăng ký thêm các mã rồi trả về.
    App nên gọi POST với danh sách flight code hiện đang có để vừa đảm bảo
    server biết theo dõi vừa nhận ETA trong 1 call.
    """
    if (err := require_api_key()): return err

    if request.method == 'POST':
        body = request.get_json(silent=True) or {}
        codes = body.get('codes', []) or []
        cleaned = [c.strip().upper() for c in codes if isinstance(c, str) and c.strip()]
        if cleaned:
            with _lock:
                QUEUE.update(cleaned)

    return jsonify(build_etas_payload())


@app.route('/api/get_eta/<flight_code>', methods=['GET'])
def get_eta(flight_code):
    """Endpoint cũ — giữ tương thích, đọc từ cache, tự thêm vào queue."""
    if (err := require_api_key()): return err
    code = flight_code.strip().upper()
    with _lock:
        QUEUE.add(code)
        data = TRACKED.get(code)

    if data is None:
        return jsonify({
            "status": "pending",
            "message": f"Đã thêm {code} vào queue, chờ poll kế tiếp (<{POLL_INTERVAL}s)"
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


# =====================================================
# Khởi động poller
# =====================================================
def _start_poller():
    t = threading.Thread(target=poll_arrivals_loop, daemon=True)
    t.start()


_start_poller()


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
