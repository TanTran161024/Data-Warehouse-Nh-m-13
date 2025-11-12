# load_to_datawarehouse.py
# ==========================================
# Script để load dữ liệu từ bonbanh_transform.csv vào Data Warehouse
# - Sử dụng MySQL (giả sử database 'bonbanh_datawarehouse')
# - Áp dụng SCD Type 1 cho các Dimension (cập nhật/xóa bản cũ nếu thay đổi, không giữ lịch sử)
# - Fact table: fact_danh_sach_xe
# - Dimensions: dim_mau_xe, dim_vi_tri, dim_nguoi_ban, dim_xuat_xu, dim_tinh_trang, dim_kieu_dang
# ==========================================

import pandas as pd
import mysql.connector
import hashlib  # Để tạo business key hash nếu cần
from datetime import datetime

# --- Kết nối tới MySQL server (không chỉ định database để tạo DB nếu cần) ---
def create_database_if_not_exists():
    conn_temp = mysql.connector.connect(
        host="localhost",      # hoặc 127.0.0.1
        user="root",           # user MySQL
        password=""  # thay bằng mật khẩu thật
    )
    cursor_temp = conn_temp.cursor()
    
    # Tạo database nếu chưa tồn tại
    cursor_temp.execute("CREATE DATABASE IF NOT EXISTS bonbanh_datawarehouse")
    conn_temp.commit()
    cursor_temp.close()
    conn_temp.close()

create_database_if_not_exists()

# --- Kết nối tới Data Warehouse DB ---
conn = mysql.connector.connect(
    host="localhost",      # hoặc 127.0.0.1
    user="root",           # user MySQL
    password="",  # thay bằng mật khẩu thật
    database="bonbanh_datawarehouse"
)
cursor = conn.cursor()

# --- Tạo bảng từ file SQL riêng ---
def create_tables_from_sql(sql_file):
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    for statement in sql_script.split(';'):
        if statement.strip():
            cursor.execute(statement)
    conn.commit()

# --- Hàm xử lý SCD Type 1 cho một dimension ---
def handle_scd_type1(table_name, business_key, attributes, cursor):
    # Tạo hash cho business_key để lưu
    bk_hash = hashlib.md5(business_key.encode()).hexdigest()

    # Kiểm tra bản ghi tồn tại
    sql_check = f"""
        SELECT surrogate_key, {', '.join(attributes.keys())}
        FROM {table_name}
        WHERE business_key = %s
    """
    cursor.execute(sql_check, (bk_hash,))
    existing = cursor.fetchone()

    if not existing:
        # Insert mới
        sql_insert = f"""
            INSERT INTO {table_name} (business_key, {', '.join(attributes.keys())})
            VALUES (%s, {', '.join(['%s'] * len(attributes))})
        """
        values = [bk_hash] + list(attributes.values())
        cursor.execute(sql_insert, values)
        return cursor.lastrowid

    else:
        sk = existing[0]
        existing_attrs = existing[1:]

        # Kiểm tra thay đổi
        changed = False
        for i, attr in enumerate(attributes.values()):
            if str(attr) != str(existing_attrs[i]):
                changed = True
                break

        if changed:
            # Update bản cũ
            sql_update = f"""
                UPDATE {table_name}
                SET {', '.join([f"{k} = %s" for k in attributes.keys()])}
                WHERE surrogate_key = %s
            """
            values = list(attributes.values()) + [sk]
            cursor.execute(sql_update, values)

        return sk

# --- Đọc dữ liệu từ CSV transform ---
csv_file = "data/bonbanh_transform.csv"
df = pd.read_csv(csv_file, encoding="utf-8-sig")

# --- Xử lý từng dòng ---
print("🚀 Đang load dữ liệu vào Data Warehouse...")
for _, row in df.iterrows():
    # --- Dim Mau Xe ---
    car_bk = f"{row.get('Tên xe', '')}_{row.get('Năm sản xuất:', '')}"  # Business key đơn giản
    so_cho_value = row.get('Số chỗ ngồi:', '')
    so_cho_ngoi = int(so_cho_value.split()[0]) if so_cho_value and pd.notnull(so_cho_value) else None
    
    so_cua_value = row.get('Số cửa:', '')
    so_cua = int(so_cua_value.split()[0]) if so_cua_value and pd.notnull(so_cua_value) else None
    
    car_attrs = {
        'ten_xe': row.get('Tên xe', ''),
        'loai_xe_nam_sx': row.get('Loại xe + Năm SX', ''),
        'nam_san_xuat': int(row.get('Năm sản xuất:', 0)) if pd.notnull(row.get('Năm sản xuất:')) else None,
        'dong_co': row.get('Động cơ:', ''),
        'mau_ngoai_that': row.get('Màu ngoại thất:', ''),
        'mau_noi_that': row.get('Màu nội thất:', ''),
        'so_cho_ngoi': so_cho_ngoi,
        'so_cua': so_cua
    }
    mau_xe_sk = handle_scd_type1('dim_mau_xe', car_bk, car_attrs, cursor)

    # --- Dim Vi Tri ---
    loc_bk = row.get('Nơi bán', '')
    loc_attrs = {'noi_ban': row.get('Nơi bán', '')}
    vi_tri_sk = handle_scd_type1('dim_vi_tri', loc_bk, loc_attrs, cursor)

    # --- Dim Nguoi Ban ---
    seller_bk = row.get('Liên hệ', '')
    seller_attrs = {'lien_he': row.get('Liên hệ', '')}
    nguoi_ban_sk = handle_scd_type1('dim_nguoi_ban', seller_bk, seller_attrs, cursor)

    # --- Dim Xuat Xu ---
    origin_bk = row.get('Xuất xứ:', '')
    origin_attrs = {'xuat_xu': row.get('Xuất xứ:', '')}
    xuat_xu_sk = handle_scd_type1('dim_xuat_xu', origin_bk, origin_attrs, cursor)

    # --- Dim Tinh Trang ---
    cond_bk = row.get('Tình trạng:', '')
    cond_attrs = {'tinh_trang': row.get('Tình trạng:', '')}
    tinh_trang_sk = handle_scd_type1('dim_tinh_trang', cond_bk, cond_attrs, cursor)

    # --- Dim Kieu Dang ---
    style_bk = row.get('Kiểu dáng:', '')
    style_attrs = {'kieu_dang': row.get('Kiểu dáng:', '')}
    kieu_dang_sk = handle_scd_type1('dim_kieu_dang', style_bk, style_attrs, cursor)

    # --- Insert vào Fact ---
    ngay_dang_str = row.get('Ngày đăng', '')
    ngay_dang = datetime.strptime(ngay_dang_str, '%d/%m/%Y').date() if ngay_dang_str else None

    sql_fact = """
        INSERT INTO fact_danh_sach_xe (
            mau_xe_sk, vi_tri_sk, nguoi_ban_sk, xuat_xu_sk, tinh_trang_sk, kieu_dang_sk,
            gia_xe, so_km, ngay_dang, luot_xem, link_xe
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values_fact = (
        mau_xe_sk, vi_tri_sk, nguoi_ban_sk, xuat_xu_sk, tinh_trang_sk, kieu_dang_sk,
        int(row.get('Giá xe (VNĐ)', 0)) if pd.notnull(row.get('Giá xe (VNĐ)')) else None,
        int(row.get('Số Km (số)', 0)) if pd.notnull(row.get('Số Km (số)')) else None,
        ngay_dang,
        int(row.get('Lượt xem', 0)) if pd.notnull(row.get('Lượt xem')) else None,
        row.get('Link xe', '')
    )
    cursor.execute(sql_fact, values_fact)

conn.commit()
cursor.close()
conn.close()

print(f"✅ Đã load {len(df)} bản ghi vào Data Warehouse.")