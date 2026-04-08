import mqtt_ssi as mqtt_ssi
import threading
import time
import mqtt_snk as mqtt_snk

print("[+] Start Main MQTT System")
thread1 = threading.Thread(target=mqtt_ssi.process , daemon=True)
thread2 = threading.Thread(target=mqtt_snk.process , daemon=True)
thread1.start()
thread2.start()


# ต้องมี loop หรือคำสั่งอะไรบางอย่างค้างไว้ที่ Main Thread
try:
    while True:
        time.sleep(1) # เลี้ยงโปรแกรมหลักไว้
except KeyboardInterrupt:
    print("Exit")