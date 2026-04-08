import multiprocessing
import json
import time
import ssl
from pymongo import MongoClient
import paho.mqtt.client as mqtt
from datetime import datetime, timezone

# ================= CONFIG =================
BROKER = "snkmqttws.fostec-energy.net"
PORT = 443
USER = "mqttsnk"
PW = "fostec-snk"

MONGO_URI = "mongodb://192.168.100.198:27017/"
DB_NAME = "SNK-MQTT"

# ================= TOPIC MAP =================
TOPIC_MAP = {
    "FOSTEC_AC101_data": {"type":"aircom","line":"5.5","factory":"1"},
    "FOSTEC_AC102_data":  {"type":"aircom","line":"5.5","factory":"1"},
    "FOSTEC_AC103_data": {"type":"aircom","line":"5.5","factory":"1"},
    "FOSTEC_STB101_data": {"type":"aircom","line":"5.5","factory":"1"},

    "FOSTEC_AC104_data": {"type":"aircom","line":"6.5","factory":"1"},
    "FOSTEC_AC105_data": {"type":"aircom","line":"6.5","factory":"1"},
    "FOSTEC_AC106_data": {"type":"aircom","line":"6.5","factory":"1"},
    "FOSTEC_AC107_data": {"type":"aircom","line":"6.5","factory":"1"},
    "FOSTEC_STB102_data": {"type":"aircom","line":"6.5","factory":"1"},

    "FOSTEC_PowerAC-101_data": {"type":"power","line":"5.5","factory":"1"},
    "FOSTEC_PowerAC-102_data": {"type":"power","line":"5.5","factory":"1"},
    "FOSTEC_PowerAC-103_data": {"type":"power","line":"5.5","factory":"1"},
    "FOSTEC_PowerSTB-101_data": {"type":"power","line":"5.5","factory":"1"},

    "FOSTEC_PowerAC-104_data": {"type":"power","line":"6.5","factory":"1"},
    "FOSTEC_PowerAC-105_data": {"type":"power","line":"6.5","factory":"1"},
    "FOSTEC_PowerAC-106_data": {"type":"power","line":"6.5","factory":"1"},
    "FOSTEC_PowerAC-107_data": {"type":"power","line":"6.5","factory":"1"},
    "FOSTEC_PowerSTB-102_data": {"type":"power","line":"6.5","factory":"1"},

    "FOSTEC_Flow-5.5_data": {"type":"flow","line":"5.5","factory":"1"},

    "FOSTEC_Flow-6.5_data": {"type":"flow","line":"6.5","factory":"1"},

    "FOSTEC_Pressure-5.5_data": {"type":"pressure","line":"5.5","factory":"1"},

    "FOSTEC_Pressure-6.5_data": {"type":"pressure","line":"6.5","factory":"1"},




    #--------------------------------------------------------------------------------------------

    "FOSTEC_AC201_data": {"type":"aircom","line":"7","factory":"2"},
    "FOSTEC_AC202_data":  {"type":"aircom","line":"7","factory":"2"},
    "FOSTEC_STB201_data": {"type":"aircom","line":"7","factory":"2"},

    "FOSTEC_PowerAC-201_data": {"type":"power","line":"7","factory":"2"},
    "FOSTEC_PowerAC-202_data": {"type":"power","line":"7","factory":"2"},
    "FOSTEC_PowerSTB-201_data": {"type":"power","line":"7","factory":"2"},

    "FOSTEC_Flow-7.0_1_data": {"type":"flow","line":"7","factory":"2"},
    "FOSTEC_Flow-7.0_No.2_data": {"type":"flow","line":"7","factory":"2"},

    "FOSTEC_Pressure-7.0_data": {"type":"pressure","line":"7","factory":"1"},
}

# ================= MQTT PROCESS =================
def mqtt_process(queue):

    def on_connect(client, userdata, flags, rc):
        print("[+] MQTT Connected, rc =", rc)

        # subscribe เฉพาะที่ map
        for topic in TOPIC_MAP.keys():
            client.subscribe(topic)

    def on_message(client, userdata, msg):
        try:
            topic = msg.topic
            payload = msg.payload.decode()

            meta = TOPIC_MAP.get(topic)
            if not meta:
                return  # ไม่เอา topic ที่ไม่ต้องการ

            data = json.loads(payload)

            # 🔥 inject metadata
            data["timestamp"] = datetime.now(timezone.utc)
            data["type"] = meta["type"]
            data["line"] = meta["line"]
            data["factory"] = meta["factory"]

            queue.put(data)

        except Exception as e:
            print("❌ MQTT error:", e)

    client = mqtt.Client(transport="websockets")
    client.username_pw_set(USER, PW)
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        print("[*] Connecting MQTT...")
        client.connect(BROKER, PORT, 60)
        client.loop_forever()
    except Exception as e:
        print("❌ MQTT connect error:", e)


# ================= DB WORKER =================
def db_worker(queue):

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    buffers = {}
    BATCH_SIZE = 100
    last_flush = time.time()

    while True:
        try:
            data = queue.get(timeout=1)

            col_name = f"{data['type']}_{data['line']}"
            buffers.setdefault(col_name, []).append(data)

        except:
            pass

        now = time.time()

        # 🔥 flush ทุก collection
        for col_name in list(buffers.keys()):
            buf = buffers[col_name]

            if len(buf) >= BATCH_SIZE or (buf and now - last_flush > 1):
                try:
                    db[col_name].insert_many(buf)
                    print(f"[DB] {col_name} -> {len(buf)} docs")

                except Exception as e:
                    print("❌ DB Error:", e)

                buffers[col_name].clear()

        last_flush = now


# ================= MAIN =================
if __name__ == "__main__":

    print("[+] Start MQTT → Queue → Batch → Mongo System")

    queue = multiprocessing.Queue(maxsize=50000)

    mqtt_p = multiprocessing.Process(target=mqtt_process, args=(queue,))

    workers = [
        multiprocessing.Process(target=db_worker, args=(queue,))
        for _ in range(4)
    ]

    mqtt_p.start()

    for w in workers:
        w.start()

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[!] Stopping...")
        mqtt_p.terminate()
        for w in workers:
            w.terminate()