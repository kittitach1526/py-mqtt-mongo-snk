import paho.mqtt.client as mqtt
import ssl
import database
import json

BROKER = "snkmqttws.fostec-energy.net"
PORT = 443
USER = "mqttsnk"
PW = "fostec-snk"

# กำหนด Topic และฟังก์ชันที่จะใช้จัดการ (Mapping)
TOPIC_HANDLERS = {

    "FOSTEC_AC101_data": {"group":"aircom","pressure_line":"5.5","factory":"1"},
    "FOSTEC_AC102_data":  {"group":"aircom","pressure_line":"5.5","factory":"1"},
    "FOSTEC_AC103_data": {"group":"aircom","pressure_line":"5.5","factory":"1"},
    "FOSTEC_STB101_data": {"group":"aircom","pressure_line":"5.5","factory":"1"},

    "FOSTEC_AC104_data": {"group":"aircom","pressure_line":"6.5","factory":"1"},
    "FOSTEC_AC105_data": {"group":"aircom","pressure_line":"6.5","factory":"1"},
    "FOSTEC_AC106_data": {"group":"aircom","pressure_line":"6.5","factory":"1"},
    "FOSTEC_AC107_data": {"group":"aircom","pressure_line":"6.5","factory":"1"},
    "FOSTEC_STB102_data": {"group":"aircom","pressure_line":"6.5","factory":"1"},

    "FOSTEC_PowerAC-101_data": {"group":"power","pressure_line":"5.5","factory":"1"},
    "FOSTEC_PowerAC-102_data": {"group":"power","pressure_line":"5.5","factory":"1"},
    "FOSTEC_PowerAC-103_data": {"group":"power","pressure_line":"5.5","factory":"1"},
    "FOSTEC_PowerSTB-101_data": {"group":"power","pressure_line":"5.5","factory":"1"},

    "FOSTEC_PowerAC-104_data": {"group":"power","pressure_line":"6.5","factory":"1"},
    "FOSTEC_PowerAC-105_data": {"group":"power","pressure_line":"6.5","factory":"1"},
    "FOSTEC_PowerAC-106_data": {"group":"power","pressure_line":"6.5","factory":"1"},
    "FOSTEC_PowerAC-107_data": {"group":"power","pressure_line":"6.5","factory":"1"},
    "FOSTEC_PowerSTB-102_data": {"group":"power","pressure_line":"6.5","factory":"1"},

    "FOSTEC_Flow-5.5_data": {"group":"flow","pressure_line":"5.5","factory":"1"},

    "FOSTEC_Flow-6.5_data": {"group":"flow","pressure_line":"6.5","factory":"1"},

    "FOSTEC_Pressure-5.5_data": {"group":"pressure","pressure_line":"5.5","factory":"1"},

    "FOSTEC_Pressure-6.5_data": {"group":"pressure","pressure_line":"6.5","factory":"1"},




    #--------------------------------------------------------------------------------------------

    "FOSTEC_AC201_data": {"group":"aircoms","pressure_line":"7","factory":"2"},
    "FOSTEC_AC202_data":  {"group":"aircoms","pressure_line":"7","factory":"2"},
    "FOSTEC_STB201_data": {"group":"aircoms","pressure_line":"7","factory":"2"},

    "FOSTEC_PowerAC-201_data": {"group":"power","pressure_line":"7","factory":"2"},
    "FOSTEC_PowerAC-202_data": {"group":"power","pressure_line":"7","factory":"2"},
    "FOSTEC_PowerSTB-201_data": {"group":"power","pressure_line":"7","factory":"2"},

    "FOSTEC_Flow-7.0_1_data": {"group":"flow","pressure_line":"7","factory":"2"},
    "FOSTEC_Flow-7.0_No.2_data": {"group":"flow","pressure_line":"7","factory":"2"},

    "FOSTEC_Pressure-7.0_data": {"group":"pressure","pressure_line":"6.5","factory":"1"},

    #--------------------------------------------------------------------------------------------

}

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[+] SNK MQTT Connected!")
        # Subscribe ทุกตัวที่อยู่ใน Dictionary
        topics = [(t, 1) for t in TOPIC_HANDLERS.keys()]
        client.subscribe(topics)
    else:
        print(f"[-] Failed (code {rc})")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode('utf-8')

    print(payload)
    data = json.loads(payload)
    group = TOPIC_HANDLERS.get(topic)
    # print("group :",group['group'])
    # print("pressure_line :",group['pressure_line'])
    database.save_to_mongo(payload=data,location="SNK",collection=group['pressure_line'],factory=group['factory'],group=group['group'])
    
    # # ส่งต่อข้อมูลไปที่ไฟล์ที่รับผิดชอบ Topic นั้นๆ
    # handler = TOPIC_HANDLERS.get(topic)
    # if handler:
    #     print(handler)
    # else:
    #     print(f"[-] No handler for topic: {topic}")

def process():
    # --- ส่วนการเชื่อมต่อ (เหมือนเดิม) ---
    print('[+] Start SSI MQTT System ..... ')
    client = mqtt.Client(transport="websockets")
    client.username_pw_set(USER, PW)
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER, PORT, 60)
        client.loop_forever()
    except Exception as e:
        print(f"💥 Error: {e}")
