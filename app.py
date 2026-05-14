from flask import Flask, jsonify
from FlightRadar24 import FlightRadar24API
import os

app = Flask(__name__)
fr_api = FlightRadar24API()

@app.route('/api/get_eta/<flight_code>', methods=['GET'])
def get_eta(flight_code):
    try:
        # Chuẩn hóa mã chuyến bay (Viết hoa và xóa khoảng trắng)
        flight_code = flight_code.strip().upper()
        
        # Lấy danh sách TOÀN BỘ chuyến bay đang bay trên bầu trời
        flights = fr_api.get_flights()
        
        # Dò tìm chuyến bay có số hiệu khớp với mã của bạn (VD: VN159)
        target_flight = None
        for f in flights:
            if f.number.upper() == flight_code or f.callsign.upper() == flight_code:
                target_flight = f
                break
                
        if not target_flight:
            return jsonify({"status": "error", "message": "Không tìm thấy chuyến bay trên radar"}), 404
        
        # Lấy chi tiết chuyến bay để quét ra giờ hạ cánh (ETA)
        details = fr_api.get_flight_details(target_flight)
        eta_seconds = details.get('time', {}).get('estimated', {}).get('arrival')
        
        if eta_seconds:
            # Trả về giờ ETA tính bằng Milliseconds cho App Android
            return jsonify({
                "status": "success",
                "flight_code": flight_code,
                "eta_millis": eta_seconds * 1000
            })
        else:
            return jsonify({"status": "error", "message": "Chưa có giờ ETA"}), 400

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
