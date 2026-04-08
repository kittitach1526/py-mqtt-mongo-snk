# ใช้เวอร์ชันเดียวกับเครื่องที่พัฒนา
FROM python:3.14.3

WORKDIR /app

# ติดตั้ง Library
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ก๊อบปี้โค้ด
COPY . .

CMD ["python", "main.py"]