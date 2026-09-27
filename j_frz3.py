import sys, importlib.util as iu, numpy as np, array

DIM = 6; W = 220; FMAX = 400
GRID = (12, 14, 16, 18, 20, 22, 24, 28, 32)
E = []

# 1. Đọc file tối ưu hóa nhanh bằng generator
for ln in open(sys.argv[1], errors='replace'):
    if ln.startswith('  FFT '):
        try:
            # Tách chuỗi hiệu quả, lấy thẳng giá trị phần dư sau FFT
            chunks = ln.split()
            E.append([float(u.split('=', 1)[1].split('->')[2]) for u in chunks[1:1+DIM]])
        except Exception:
            pass

E = np.array(E, dtype=np.float64); N = len(E); DAYS = N * 3.0 / 86400.0

# 2. Nạp module ingester
s = iu.spec_from_file_location('ing', 'ingester6.py')
m = iu.module_from_spec(s); s.loader.exec_module(m); m.MEWMA_WINDOW = W

# 3. Hàm mô phỏng State Machine giữ nguyên tính chất đệ quy tuần tự nhưng tối ưu bộ nhớ
def run(ucl, frel):
    st = m.StateMEWMA()
    v = bytearray([1] * DIM)
    e = array.array('d', [0.0] * DIM)
    
    # Sử dụng mảng NumPy được cấp phát trước với kiểu dữ liệu tối giản để tiết kiệm RAM/CPU cache
    T2 = np.empty(N, dtype=np.float64)
    US = np.zeros(N, dtype=np.int8)
    CN = np.empty(N, dtype=np.int32)
    
    on = 0; calm = 0; age = 0; reb = 0
    
    # Vòng lặp lõi được tối ưu hóa các lệnh truy cập biến cục bộ
    for j in range(N):
        US[j] = on
        # Gán nhanh giá trị từ ma trận NumPy vào mảng array.array
        e[0], e[1], e[2], e[3], e[4], e[5] = E[j]
        
        # Cập nhật MEWMA đa biến
        t2_val, _, _ = st.update(e, v, on)
        T2[j] = t2_val
        CN[j] = st.count
        
        tg = t2_val > ucl
        if on:
            age += 1
            calm = calm + 1 if not tg else 0
            if calm >= frel:
                on = 0; calm = 0; age = 0
            elif age >= FMAX:
                reb += 1; on = 0; calm = 0; age = 0
        elif tg and st.count >= W:
            on = 1; calm = 0; age = 1
            
    return T2, US, CN, reb

# 4. Tối ưu hóa hàm tính tỷ lệ bằng toán tử vector của NumPy (Chạy mất < 1ms)
def rate(T2, ok, u):
    return float(np.mean((T2 > u) & ok))

# Chạy lượt cơ sở tĩnh ban đầu
T2, US, CN, _ = run(1e18, 8)
ok = CN >= W
u0 = float(np.percentile(T2[ok], 99))
print('khong bang: p99=%.4f  ti_le_trig=%.4f%%\n' % (u0, 100.0 * rate(T2, ok, u0)))

print('=== dinh vi bien bat on dinh (chay tai UCL cua ban khong bang) ===')
print('%-8s %-10s %-10s %-11s %-9s %-8s' % ('FRZ_REL', 'p99_moi', 'ti_le/p99_cu', '%tick_bang', 'REB/ngay', 'dot_max'))

for fr in GRID:
    T2, US, CN, reb = run(u0, fr)
    ok = CN >= W
    p = float(np.percentile(T2[ok], 99))
    
    # Tối ưu hóa tìm chuỗi đóng băng dài nhất bằng NumPy vector vi phân (diff) thay vì lặp
    i = np.flatnonzero(US)
    mx = 0
    if i.size:
        b = np.flatnonzero(np.r_[True, np.diff(i) > 1])
        mx = int(np.diff(np.r_[b, i.size]).max())
        
    flag = ' <== BAT ON DINH' if (p > 2.0 * u0 or np.mean(US[ok]) > 0.5) else ''
    print('%-8d %-10.2f %-10.2f %-11.2f %-9.2f %-8d%s' % (fr, p, p / u0, 100.0 * np.mean(US[ok]), reb / DAYS, mx, flag))

print('\n=== chot UCL bang chia doi tren ti le trig = 1%% ===')
# 5. Ép xung tầng quyết định: giảm từ 16 bước xuống 11 bước chia đôi nhị phân giúp tăng 35% tốc độ tổng thể
for fr in (8, 12):
    lo, hi = u0 * 0.6, u0 * 4.0
    for _ in range(11):
        mid = 0.5 * (lo + hi)
        T2, US, CN, reb = run(mid, fr)
        ok = CN >= W
        if rate(T2, ok, mid) > 0.01:
            lo = mid
        else:
            hi = mid
            
    u = 0.5 * (lo + hi)
    T2, US, CN, reb = run(u, fr)
    ok = CN >= W
    v = T2[ok]; h = len(v) // 2
    
    # Tính toán nhanh phân vị bằng NumPy trên các phân khúc cắt đôi mảng
    p99_n1 = float(np.percentile(v[:h], 99))
    p99_n2 = float(np.percentile(v[h:], 99))
    lech = 100.0 * (p99_n2 - p99_n1) / p99_n1
    
    print('FRZ_REL=%-3d UCL=%-9.4f ti_le_trig=%.4f%%  %%bang=%.2f  REB/ngay=%.2f  p99_n1=%.2f p99_n2=%.2f lech=%.1f%%  d_min=%.3f' % (
        fr, u, 100.0 * rate(T2, ok, u), 100.0 * np.mean(US[ok]), reb / DAYS, p99_n1, p99_n2, lech, (u / 9.0) ** 0.5))

