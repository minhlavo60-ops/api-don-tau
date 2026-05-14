from flask import Flask, jsonify
from FlightRadar24 import FlightRadar24API
import os

app = Flask(__name__)
fr_api = FlightRadar24API()

@app.route('/api/get_eta/<flight_code>', methods=['GET'])
def get_eta(flight_code):
    try:
        flights = fr_api.get_flights(flight=flight_code)
        if not flights:
            return jsonify({"status": "error", "message": "Không tìm thấy chuyến bay"}), 404
        
        flight = flights[0]
        details = fr_api.get_flight_details(flight)
        eta_seconds = details.get('time', {}).get('estimated', {}).get('arrival')
        
        if eta_seconds:
            # Trả về giờ ETA tính bằng Milliseconds cho Android
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
