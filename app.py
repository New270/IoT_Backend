from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import paho.mqtt.client as mqtt
import json
from flask_cors import CORS

app = Flask(__name__)
CORS(app) 

# ==========================================
# CẤU HÌNH DATABASE MYSQL
# ==========================================
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:@localhost/iot_db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- ĐỊNH NGHĨA BẢNG ---
class User(db.Model):
    __tablename__ = 'users'
    user_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    full_name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), unique=True, nullable=False)
    phone = db.Column(db.String(15), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Device(db.Model):
    __tablename__ = 'devices'
    device_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    device_code = db.Column(db.String(50), unique=True, nullable=False)
    device_name = db.Column(db.String(100), nullable=False)
    device_type = db.Column(db.String(50), nullable=False)
    status = db.Column(db.String(20), nullable=False, default='OFF')

class Sensor(db.Model):
    __tablename__ = 'sensors'
    sensor_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    sensor_code = db.Column(db.String(50), unique=True, nullable=False)
    sensor_name = db.Column(db.String(100), nullable=False)
    sensor_type = db.Column(db.String(50), nullable=False)
    unit = db.Column(db.String(20), nullable=False)
    status = db.Column(db.String(20), nullable=False, default='ACTIVE')

class SensorData(db.Model):
    __tablename__ = 'sensor_data'
    data_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    sensor_id = db.Column(db.Integer, db.ForeignKey('sensors.sensor_id'), nullable=False)
    value = db.Column(db.Numeric(10, 2), nullable=False)
    recorded_at = db.Column(db.DateTime, default=datetime.utcnow)

class ActionHistory(db.Model):
    __tablename__ = 'action_history'
    action_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.user_id'), nullable=False)
    device_id = db.Column(db.Integer, db.ForeignKey('devices.device_id'), nullable=False)
    action = db.Column(db.String(10), nullable=False)
    result = db.Column(db.String(20), nullable=False) # SUCCESS / FAILED / PENDING
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ==========================================
# CẤU HÌNH MQTT
# ==========================================
MQTT_BROKER = "127.0.0.1" 
MQTT_PORT = 1883
TOPIC_SENSOR = "home/sensor/data"
TOPIC_STATUS = "home/device/status"
TOPIC_CONTROL = "home/device/control"
TOPIC_SYNC = "home/device/sync_request"

def on_connect(client, userdata, flags, rc):
    client.subscribe(TOPIC_SENSOR)
    client.subscribe(TOPIC_STATUS)
    client.subscribe(TOPIC_SYNC)
    print("MQTT Connected!")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        topic = msg.topic
        with app.app_context():
            if topic == TOPIC_SYNC:
                devices = Device.query.all()
                for dev in devices:
                    command_payload = json.dumps({"device_id": dev.device_code, "action": dev.status})
                    mqtt_client.publish(TOPIC_CONTROL, command_payload)
                return 

            data = json.loads(payload)
            if topic == TOPIC_SENSOR:
                db.session.add(SensorData(sensor_id=1, value=data['temperature']))
                db.session.add(SensorData(sensor_id=2, value=data['humidity']))
                db.session.add(SensorData(sensor_id=3, value=data['light']))
                db.session.commit()
            
            elif topic == TOPIC_STATUS:
                device_code = data.get('device_id')
                device = Device.query.filter_by(device_code=device_code).first()
                if device:
                    device.status = data.get('status')
                    # Tìm lệnh PENDING gần nhất và đánh dấu SUCCESS vì mạch đã phản hồi
                    pending_action = ActionHistory.query.filter_by(device_id=device.device_id, result="PENDING").order_by(ActionHistory.action_id.desc()).first()
                    if pending_action:
                        pending_action.result = "SUCCESS"
                    db.session.commit()
    except Exception as e:
        print(f"Lỗi MQTT: {e}")

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()


# ==========================================
# REST API - CÁC HÀM XỬ LÝ DỮ LIỆU
# ==========================================
@app.route('/api/auth/register', methods=['POST'])
def api_register():
    username = request.json.get('username')
    if User.query.filter_by(username=username).first():
        return jsonify({"error": "Tài khoản đã tồn tại!"}), 400
    db.session.add(User(username=username, full_name=username, email=f"{username}@gmail.com"))
    db.session.commit()
    return jsonify({"status": "success"}), 200

@app.route('/api/auth/login', methods=['POST'])
def api_login():
    user = User.query.filter_by(username=request.json.get('username')).first()
    if user: return jsonify({"status": "success", "user_id": user.user_id, "username": user.username}), 200
    return jsonify({"error": "Tài khoản không tồn tại!"}), 401

@app.route('/api/sensors/latest', methods=['GET'])
def get_latest_sensors():
    temp = SensorData.query.filter_by(sensor_id=1).order_by(SensorData.data_id.desc()).first()
    hum = SensorData.query.filter_by(sensor_id=2).order_by(SensorData.data_id.desc()).first()
    light = SensorData.query.filter_by(sensor_id=3).order_by(SensorData.data_id.desc()).first()
    return jsonify({
        "temperature": float(temp.value) if temp else 0.0,
        "humidity": float(hum.value) if hum else 0.0,
        "light": int(light.value) if light else 0
    }), 200

# API mới: Lấy lịch sử Cảm biến (DataSensors tab)
@app.route('/api/sensors/history', methods=['GET'])
def get_sensor_history():
    data = SensorData.query.order_by(SensorData.data_id.desc()).all()
    result = []
    for item in data:
        sensor = Sensor.query.get(item.sensor_id)
        result.append({
            "id": f"DATA-{item.data_id}",
            "name": sensor.sensor_name if sensor else "Unknown",
            "type": sensor.sensor_type if sensor else "Unknown",
            "value": float(item.value),
            "unit": sensor.unit if sensor else "",
            "time": item.recorded_at.strftime("%Y-%m-%d %H:%M:%S")
        })
    return jsonify(result), 200

# API mới: Lấy trạng thái hiện tại của đèn
@app.route('/api/devices/status', methods=['GET'])
def get_device_status():
    devices = Device.query.all()
    status_dict = {dev.device_code: dev.status for dev in devices}
    return jsonify(status_dict), 200

@app.route('/api/history', methods=['GET'])
def get_action_history():
    history = ActionHistory.query.order_by(ActionHistory.action_id.desc()).all()
    result = []
    for item in history:
        device = Device.query.get(item.device_id)
        result.append({
            "id": f"ACT-{item.action_id}",
            "device": device.device_name if device else "Unknown",
            "action": item.action, "result": item.result,
            "time": item.created_at.strftime("%Y-%m-%d %H:%M:%S")
        })
    return jsonify(result), 200

@app.route('/api/devices/<device_code>/control', methods=['POST'])
def control_device(device_code):
    data = request.json
    action, user_id = data.get('action'), data.get('user_id')

    device = Device.query.filter_by(device_code=device_code).first()
    if not device: return jsonify({"error": "Device not found"}), 404

    # Ghi trạng thái là PENDING chờ mạch xác nhận
    new_action = ActionHistory(user_id=user_id, device_id=device.device_id, action=action, result="PENDING")
    db.session.add(new_action)
    db.session.commit()

    mqtt_client.publish(TOPIC_CONTROL, json.dumps({"device_id": device_code, "action": action}))
    return jsonify({"status": "success"}), 200


@app.route('/api/devices/bulk_control', methods=['POST'])
def bulk_control_devices():
    try:
        data = request.json
        commands = data.get('commands', []) 
        user_id = data.get('user_id')

        if not commands:
            return jsonify({"error": "No commands provided"}), 400
        
        valid_commands_to_send = []

        for cmd in commands:
            device_code = cmd.get('device_code')
            action = cmd.get('action')

            device = Device.query.filter_by(device_code=device_code).first()
            if device:
                # Ghi lịch sử PENDING vào cơ sở dữ liệu
                new_action = ActionHistory(user_id=user_id, device_id=device.device_id, action=action, result="PENDING")
                db.session.add(new_action)
                # Đưa vào mảng dữ liệu để gửi MQTT
                valid_commands_to_send.append({"device_id": device_code, "action": action})
                
        # Publish toàn bộ mảng lệnh qua MQTT trong 1 lần duy nhất
        if valid_commands_to_send:
            command_payload = json.dumps(valid_commands_to_send)
            mqtt_client.publish(TOPIC_CONTROL, command_payload)

        db.session.commit()
        return jsonify({"status": "success", "message": "Bulk commands sent"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500



# API mới: Đánh dấu lệnh là FAILED nếu sau 4 giây Timeout
@app.route('/api/devices/<device_code>/fail', methods=['POST'])
def mark_action_fail(device_code):
    device = Device.query.filter_by(device_code=device_code).first()
    if device:
        pending_action = ActionHistory.query.filter_by(device_id=device.device_id, result="PENDING").order_by(ActionHistory.action_id.desc()).first()
        if pending_action:
            pending_action.result = "FAILED"
            db.session.commit()
    return jsonify({"status": "updated"}), 200

with app.app_context():
    db.create_all() 
    # Seed data (giữ nguyên)
    devices = [{"code": "LED1", "name": "LED Trắng", "type": "LED"}, {"code": "LED2", "name": "LED Xanh Dương", "type": "LED"}, {"code": "LED3", "name": "LED Xanh Lá", "type": "LED"}]
    for d in devices:
        if not Device.query.filter_by(device_code=d["code"]).first():
            db.session.add(Device(device_code=d["code"], device_name=d["name"], device_type=d["type"], status="OFF"))

    sensors = [{"code": "SEN_TEMP", "name": "Nhiệt độ", "type": "temperature", "unit": "°C"}, {"code": "SEN_HUM", "name": "Độ ẩm", "type": "humidity", "unit": "%"}, {"code": "SEN_LIGHT", "name": "Ánh sáng", "type": "light", "unit": "lux"}]
    for s in sensors:
        if not Sensor.query.filter_by(sensor_code=s["code"]).first():
            db.session.add(Sensor(sensor_code=s["code"], sensor_name=s["name"], sensor_type=s["type"], unit=s["unit"], status="ACTIVE"))
    db.session.commit()

if __name__ == '__main__':
    app.run(debug=True, port=5000, use_reloader=False)
