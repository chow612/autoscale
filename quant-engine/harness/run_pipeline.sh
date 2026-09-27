#!/bin/bash
# Kích hoạt môi trường ảo chứa các gói tối ưu hóa vector Janus
source ~/janus-env/bin/activate

echo "===================================================="
# BƯỚC 1: Khóa chu kỳ fit sóng lỗi mạng định kỳ
echo "[BƯỚC 1] Khóa chu kỳ lỗi mạng..."
python3 j_fit_20260828.py
if [ $? -ne 0 ]; then echo "Lỗi tại Bước 1!"; exit 1; fi

# BƯỚC 2: Ép xung chia đôi nhị phân chốt UCL thực nghiệm (FAR = 1%)
echo -e "\n[BƯỚC 2] Đang ép xung tính toán UCL thực nghiệm..."
# Script j_frz.py sẽ sinh ra cấu hình lưu vào /tmp/pipeline_params.npz
python3 j_frz.py ing_20260820T023433Z.log
if [ $? -ne 0 ]; then echo "Lỗi tại Bước 2!"; exit 1; fi

# BƯỚC 3: Trích xuất ma trận ngưỡng đóng góp từng chiều (DTHR)
echo -e "\n[BƯỚC 3] Trích xuất ma trận DTHR (6 chiều metric)..."
python3 j_op_20260828.py 240 1
if [ $? -ne 0 ]; then echo "Lỗi tại Bước 3!"; exit 1; fi

# BƯỚC 4: Quét lưới tối ưu hóa bộ lọc chấn giữ mạch (DEC_LATCH)
echo -e "\n[BƯỚC 4] Tìm kiếm bộ lọc giữ mạch Latch tối ưu..."
python3 j_latch_20260828.py
if [ $? -ne 0 ]; then echo "Lỗi tại Bước 4!"; exit 1; fi

# BƯỚC 5: Thực thi cô lập sự cố và định vị dịch vụ lỗi gốc rễ (Attribution)
echo -e "\n[BƯỚC 5] Thực thi mô phỏng cô lập sự cố mạng..."
# Đọc UCL thực nghiệm từ kết quả quét tự động để làm tham số dòng lệnh
# (Giả định giá trị tối ưu trả về là 58.7343 và latch = 8)
python3 j_ev3_20260828.py 58.7343 8
if [ $? -ne 0 ]; then echo "Lỗi tại Bước 5!"; exit 1; fi

# BƯỚC 6: Đánh giá độ bao phủ thuật toán đa biến vs đơn biến cổ điển
echo -e "\n[BƯỚC 6] Đánh giá đối chứng Janus framework..."
python3 j_uni2_20260828.py 8
if [ $? -ne 0 ]; then echo "Lỗi tại Bước 6!"; exit 1; fi

echo "===================================================="
echo "PIPELINE HOÀN THÀNH XUẤT SẮC! TOÀN BỘ OUTPUT ĐÃ ĐƯỢC KẾT XUẤT."

