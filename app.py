from flask import Flask, jsonify, request
from FlightRadar24 import FlightRadar24API
import hmac
import os

app = Flask(__name__)

# Mật khẩu/token truy cập server.
# Trên Render nên tạo Environment Variable:
#   ETA_API_KEY=0982e7c09397ca3ed579775a9a29ff208a1715d0526fbb65b67a61c1cb126923
# Nếu chưa cấu hình ENV, server sẽ dùng token mặc định bên dưới.
ETA_API_KEY = os.environ.get(
    "ETA_API_KEY",
    "0982e7c09397ca3ed579775a9a29ff208a1715d0526fbb65b67a61c1cb126923",
)


def require_api_key():
    """Kiểm tra mật khẩu từ app gửi lên qua header X-API-Key."""
    client_key = request.headers.get("X-API-Key", "")
    if not ETA_API_KEY:
        return jsonify({"status": "error", "message": "Server chưa cấu hình ETA_API_KEY"}), 500
    if not hmac.compare_digest(client_key, ETA_API_KEY):
        return jsonify({"status": "error", "message": "Sai mật khẩu truy cập server"}), 401
    return None


@app.route('/api/get_eta/<flight_code>', methods=['GET'])
def get_eta(flight_code):
    auth_error = require_api_key()
    if auth_error is not None:
        return auth_error

    try:
        fr_api = FlightRadar24API()
        flight_code = flight_code.strip().upper()

        # Thu hẹp vùng quét: Việt Nam và lân cận.
        # Giảm dữ liệu tải về, hạn chế nguy cơ bị FlightRadar chặn IP.
        bounds_vn = "25.00,5.00,100.00,115.00"
        flights = fr_api.get_flights(bounds=bounds_vn)

        target_flight = None
        for f in flights:
            number = (getattr(f, "number", None) or "").upper()
            callsign = (getattr(f, "callsign", None) or "").upper()
            if number == flight_code or callsign == flight_code:
                target_flight = f
                break

        if not target_flight:
            return jsonify({
                "status": "error",
                "message": f"Không tìm thấy chuyến bay {flight_code} trong vùng bay VN"
            }), 404

        details = fr_api.get_flight_details(target_flight)

        dest_airport = details.get('airport', {}).get('destination', {}).get('code', {}).get('iata')

        if dest_airport != "DAD":
            return jsonify({
                "status": "error",
                "message": f"Chuyến bay {flight_code} không về Đà Nẵng (Đang về {dest_airport})"
            }), 400

        eta_seconds = details.get('time', {}).get('estimated', {}).get('arrival')

        if eta_seconds:
            return jsonify({
                "status": "success",
                "flight_code": flight_code,
                "destination": "Da Nang (DAD)",
                "eta_millis": eta_seconds * 1000
            })

        return jsonify({"status": "error", "message": "Chuyến bay đã về DAD hoặc chưa có ETA"}), 400

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
