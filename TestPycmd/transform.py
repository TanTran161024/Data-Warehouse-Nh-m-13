# transform.py
# ==========================================
# Chuẩn hóa dữ liệu bonbanh_staging.csv để import vào DB
# Cập nhật dữ liệu mới vào bonbanh_transform.csv (giống get_data.py)
# ==========================================

import pandas as pd
import re
import os


# ==== Hàm chuẩn hóa giá xe ====
def convert_price(price_str):
    """
    Chuyển giá dạng '1 Tỷ 250 Tr.' → số (VNĐ)
    Ví dụ:
        '4 Tỷ 350 Tr.' → 4350000000
        '695 Triệu' → 695000000
    """
    if not isinstance(price_str, str):
        return None
    price_str = price_str.strip()

    ty = re.search(r"(\d+)[\s\.]*T[ỷi]", price_str)
    tr = re.search(r"(\d+)[\s\.]*Tr", price_str)

    ty_val = int(ty.group(1)) * 1_000_000_000 if ty else 0
    tr_val = int(tr.group(1)) * 1_000_000 if tr else 0
    total = ty_val + tr_val

    if total == 0 and re.search(r"Triệu", price_str):
        m = re.search(r"(\d+)", price_str)
        total = int(m.group(1)) * 1_000_000 if m else None

    return total if total > 0 else None


# ==== Hàm chuẩn hóa số km ====
def convert_km(km_str):
    """
    Loại bỏ 'Km', dấu ',' → số nguyên
    Ví dụ: '64,000 Km' → 64000
    """
    if not isinstance(km_str, str):
        return None
    km_str = km_str.replace("Km", "").replace(",", "").strip()
    return int(re.sub(r"[^\d]", "", km_str)) if re.search(r"\d", km_str) else None


def main():
    # ==== Cấu hình đường dẫn ====
    DATA_DIR = "data"
    os.makedirs(DATA_DIR, exist_ok=True)
    input_path = os.path.join(DATA_DIR, "bonbanh_staging.csv")
    csv_path = os.path.join(DATA_DIR, "bonbanh_transform.csv")

    if not os.path.exists(input_path):
        print(f"❌ Không tìm thấy file {input_path}")
        return

    # ==== Đọc dữ liệu staging ====
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    # ==== Chuẩn hóa cột giá và km ====
    df["Giá xe (VNĐ)"] = df["Giá xe"].apply(convert_price)
    if "Số Km đã đi:" in df.columns:
        df["Số Km (số)"] = df["Số Km đã đi:"].apply(convert_km)

    # ==== Chọn cột cần thiết ====
    cols = [
        "Tên xe", "Loại xe + Năm SX", "Năm sản xuất:", "Giá xe (VNĐ)", "Số Km (số)",
        "Nơi bán", "Liên hệ", "Ngày đăng", "Lượt xem", "Link xe", "Tình trạng:",
        "Xuất xứ:", "Kiểu dáng:", "Động cơ:", "Màu ngoại thất:", "Màu nội thất:",
        "Số chỗ ngồi:", "Số cửa:"
    ]
    df_new = df[[c for c in cols if c in df.columns]]

    # ==== Nếu file transform đã tồn tại → gộp dữ liệu ====
    if os.path.exists(csv_path):
        df_existing = pd.read_csv(csv_path, encoding="utf-8-sig")
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        # Giữ lại dòng mới nhất theo "Link xe"
        if "Ngày đăng" in df_combined.columns:
            df_combined = df_combined.sort_values(by="Ngày đăng").drop_duplicates(
                subset="Link xe", keep="last"
            )
        else:
            df_combined = df_combined.drop_duplicates(subset="Link xe", keep="last")
    else:
        df_combined = df_new

    # ==== Ghi kết quả ra CSV ====
    df_combined.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"✅ Đã cập nhật file transform: {csv_path}")
    print(f"→ Tổng số bản ghi: {len(df_combined)}")


if __name__ == "__main__":
    main()
