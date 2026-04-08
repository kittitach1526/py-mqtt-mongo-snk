from pymongo import MongoClient
from datetime import datetime

# 1. เชื่อมต่อกับ MongoDB Server (กรณีรันในเครื่องตัวเองใช้ localhost)
# หรือใช้ Connection String จาก MongoDB Atlas: "mongodb+srv://user:pass@cluster.mongodb.net/"
client = MongoClient("mongodb://192.168.100.198:27017/")

# 2. เลือก Database (ถ้ายังไม่มี ระบบจะสร้างให้เองอัตโนมัติ)
db = client["SSI-MQTT"]
db_snk = client["SNK-MQTT"]


def save_to_mongo(collection: str,payload,factory : str,group:str,location :str):

    if location == "SSI":
        # 1. สมมติว่าแปลง payload เป็น dict แล้ว
        collection = db[collection]
        # data = {"presAC1": 6.7, ...} หรือ {"presAC2": 5.3, ...}
        
        # 2. กำหนดช่วงเวลา (เช่น ทุกๆ 1 นาที ให้เก็บก้อนเดียวกัน)
        # เราจะตัดวินาทีทิ้งเพื่อให้เป็นนาทีเดียวกัน
        # now = datetime.now().replace(second=0, microsecond=0)
        now = datetime.now().replace(microsecond=0)

        payload['factory']=factory
        payload['type'] = group
        # 3. ใช้คำสั่ง update_one
        collection.update_one(
            {"timestamp": now},            # เงื่อนไข: หาเอกสารที่มีเวลาเดียวกัน (นาทีเดียวกัน)
            {"$set": payload},             # ข้อมูล: เอาข้อมูลใหม่ไป "แปะเพิ่ม" หรือ "ทับ" ของเดิม
            upsert=True                    # สำคัญ: ถ้าไม่เจอ timestamp นี้ ให้สร้างใหม่เลย
        )
        # print(f"✅ Merged data for {now}")

    if location == "SNK":
        # 1. สมมติว่าแปลง payload เป็น dict แล้ว
        collection = db_snk[collection]
        # data = {"presAC1": 6.7, ...} หรือ {"presAC2": 5.3, ...}
        
        # 2. กำหนดช่วงเวลา (เช่น ทุกๆ 1 นาที ให้เก็บก้อนเดียวกัน)
        # เราจะตัดวินาทีทิ้งเพื่อให้เป็นนาทีเดียวกัน
        # now = datetime.now().replace(second=0, microsecond=0)
        now = datetime.now().replace(microsecond=0)

        payload['factory']=factory
        payload['type'] = group
        # 3. ใช้คำสั่ง update_one
        collection.update_one(
            {"timestamp": now},            # เงื่อนไข: หาเอกสารที่มีเวลาเดียวกัน (นาทีเดียวกัน)
            {"$set": payload},             # ข้อมูล: เอาข้อมูลใหม่ไป "แปะเพิ่ม" หรือ "ทับ" ของเดิม
            upsert=True                    # สำคัญ: ถ้าไม่เจอ timestamp นี้ ให้สร้างใหม่เลย
        )
        # print(f"✅ Merged data for {now}")

    