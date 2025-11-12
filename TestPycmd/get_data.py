# get_data_bonbanh.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from datetime import datetime
import re

BASE_URL = "https://bonbanh.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


# ==== Hàm tải HTML ====
def get_page(url):
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


# ==== Hàm lấy danh sách xe từ trang ngoài ====
def parse_list_page(html):
    soup = BeautifulSoup(html, "html.parser")
    cars = []
    for item in soup.select(".car-item"):
        try:
            # --- Link chi tiết ---
            a_tag = item.select_one("a")
            link = ""
            if a_tag and a_tag.get("href"):
                href = a_tag["href"].strip()
                if not href.startswith("/"):
                    href = "/" + href
                link = BASE_URL + href

            # --- Lấy thông tin cơ bản ---
            cb1 = item.select_one(".cb1")
            loai_xe = cb1.contents[0].strip() if cb1 and cb1.contents else ""
            nam_sx = cb1.select_one("b").get_text(strip=True) if cb1 and cb1.select_one("b") else ""
            info = f"{loai_xe} - {nam_sx}".strip(" -")

            ten_xe = item.select_one(".cb2 b").get_text(strip=True) if item.select_one(".cb2 b") else ""
            gia = item.select_one(".cb3 b").get_text(strip=True) if item.select_one(".cb3 b") else ""
            noi_ban = item.select_one(".cb4 b").get_text(strip=True) if item.select_one(".cb4 b") else ""
            lien_he = item.select_one(".cb7").get_text(" ", strip=True) if item.select_one(".cb7") else ""

            cars.append({
                "Loại xe + Năm SX": info,
                "Tên xe": ten_xe,
                "Giá xe": gia,
                "Nơi bán": noi_ban,
                "Liên hệ": lien_he,
                "Link xe": link
            })
        except Exception as e:
            print(f"Lỗi parse list item: {e}")
            continue
    return cars


# ==== Hàm lấy dữ liệu chi tiết từ link ====
def parse_detail_page(url):
    try:
        html = get_page(url)
        soup = BeautifulSoup(html, "html.parser")

        # ==== Lấy ghi chú (ngày đăng + lượt xem) ====
        notes = soup.find("div", class_="notes")
        notes_text = notes.get_text(strip=True) if notes else ""

        ngay_dang = ""
        luot_xem = ""
        if notes_text:
            m1 = re.search(r"Đăng\s+ngày\s+(\d{1,2}/\d{1,2}/\d{4})", notes_text)
            if m1:
                ngay_dang = m1.group(1)
            m2 = re.search(r"Xem\s+(\d+)\s+lượt", notes_text)
            if m2:
                luot_xem = m2.group(1)

        # ==== Lấy thông tin kỹ thuật ====
        details = {}
        for row in soup.select("div#mail_parent.row"):
            label = row.find("label")
            value = row.find("span", class_="inp")
            if label and value:
                details[label.get_text(strip=True)] = value.get_text(strip=True)

        # Gom dữ liệu
        data = {
            "Ngày đăng": ngay_dang,
            "Lượt xem": luot_xem
        }
        data.update(details)
        return data

    except Exception as e:
        print(f"Lỗi khi lấy chi tiết {url}: {e}")
        return {}


if __name__ == "__main__":
    all_cars = []

    # Crawl danh sách xe (ví dụ trang 1 → tăng page nếu muốn)
    for page in range(1, 2):
        url = f"{BASE_URL}/oto/page,{page}/" if page > 1 else BASE_URL
        print(f"Đang tải trang danh sách {page}...")
        html = get_page(url)
        car_list = parse_list_page(html)
        print(f"✅ Tìm thấy {len(car_list)} xe trên trang {page}")

        # Lấy chi tiết từng xe
        for car in car_list:
            link = car["Link xe"]
            if link:
                print(f"→ Lấy chi tiết: {link}")
                detail_data = parse_detail_page(link)
                car.update(detail_data)
                time.sleep(1)  # nghỉ giữa các request tránh bị chặn

            all_cars.append(car)

    # ==== Lưu kết quả ra CSV, cập nhật dữ liệu cũ ====
    csv_path = os.path.join(DATA_DIR, "bonbanh_transform.csv")

    # Nếu file đã tồn tại, đọc vào DataFrame
    if os.path.exists(csv_path):
        df_existing = pd.read_csv(csv_path, encoding="utf-8-sig")
    else:
        df_existing = pd.DataFrame()

    # Chuyển danh sách mới thành DataFrame
    df_new = pd.DataFrame(all_cars)

    if not df_existing.empty:
        # Gộp dữ liệu theo 'Link xe'
        df_combined = pd.concat([df_existing, df_new])
        
        # Giữ dòng mới nhất cho mỗi Link xe
        df_combined = df_combined.sort_values(by="Ngày đăng").drop_duplicates(subset="Link xe", keep="last")
    else:
        df_combined = df_new

    # Lưu lại CSV
    df_combined.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ Đã lưu {len(df_combined)} xe → {csv_path}")
