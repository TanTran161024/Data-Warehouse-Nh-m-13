import pandas as pd
import mysql.connector

# --- Kết nối tới database ---
conn = mysql.connector.connect(
    host="localhost",      # hoặc 127.0.0.1
    user="root",           # user MySQL
    password="",  # thay bằng mật khẩu thật
    database="bonbanh_staging"
)
cursor = conn.cursor()

# --- Đọc dữ liệu từ CSV ---
csv_file = "data/bonbanh_staging.csv"
df = pd.read_csv(csv_file, encoding="utf-8-sig")

# --- Chuẩn hóa tên cột (phòng trường hợp bị lệch) ---
df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]

# --- Xóa hết dữ liệu cũ trong bảng staging ---
print("🧹 Đang xóa dữ liệu cũ trong bảng 'xe_bonbanh'...")
cursor.execute("DELETE FROM xe_bonbanh")
conn.commit()

# --- Insert dữ liệu mới ---
print("🚀 Đang nạp dữ liệu mới vào bảng 'xe_bonbanh'...")
for _, row in df.iterrows():
    sql = """
        INSERT INTO xe_bonbanh (loai_xe_nam_sx, ten_xe, gia_xe, noi_ban, lien_he, link_xe, ngay_dang, luot_xem)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        row.get("loại_xe_+_năm_sx", ""),
        row.get("tên_xe", ""),
        row.get("giá_xe", ""),
        row.get("nơi_bán", ""),
        row.get("liên_hệ", ""),
        row.get("link_xe", ""),
        row.get("ngày_đăng", ""),
        row.get("lượt_xem", "")
    )
    cursor.execute(sql, values)

conn.commit()
cursor.close()
conn.close()

print(f"✅ Đã nạp lại toàn bộ dữ liệu ({len(df)} dòng) vào database staging.")
