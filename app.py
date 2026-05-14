from flask import Flask, jsonify
from FlightRadar24 import FlightRadar24API
import os

app = Flask(__name__)

@app.route('/api/get_eta/<flight_code>', methods=['GET'])
def get_eta(flight_code):
    try:
        fr_api = FlightRadar24API()
        flight_code = flight_code.strip().upper()
        
        # 1. Thu hẹp vùng quét (chỉ tập trung vào không phận Việt Nam và lân cận)
        # Việc này giúp giảm lượng dữ liệu tải về, tránh bị FlightRadar chặn IP
        bounds_vn = "23.39,8.55,102.14,109.46"
        flights = fr_api.get_flights(bounds=bounds_vn)
        
        target_flight = None
        for f in flights:
            # Kiểm tra xem mã chuyến bay có khớp không
            if f.number.upper() == flight_code or f.callsign.upper() == flight_code:
                target_flight = f
                break
                
        if not target_flight:
            return jsonify({
                "status": "error", 
                "message": f"Không tìm thấy chuyến bay {flight_code} trong vùng bay VN"
            }), 404
        
        # 2. Lấy chi tiết để kiểm tra điểm đến và giờ hạ cánh (ETA)
        details = fr_api.get_flight_details(target_flight)
        
        # Lấy mã sân bay đến (IATA code)
        dest_airport = details.get('airport', {}).get('destination', {}).get('code', {}).get('iata')
        
        # Lọc: Chỉ xử lý nếu điểm đến là Đà Nẵng (DAD)
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
        else:
            return jsonify({"status": "error", "message": "Chuyến bay đã về DAD hoặc chưa có ETA"}), 400

    except Exception as e:
        # Nếu bị lỗi 429 hoặc lỗi mạng, trả về thông báo để App biết và thử lại sau
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
