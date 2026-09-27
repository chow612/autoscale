"""
Recalibrate UCL/DTHR duoi W moi (mac dinh 240) tu 1 file log ingester6.py da co san (vd log
lich su duoi W=120 cu). Dung lai parse()/load()/replay()/q() cua j_replay.py - KHONG dung
ucltab()/dtab() co san vi 2 ham do doc thang L.t2/L.dd (gia tri da tinh san TRONG log, duoi W
cu), khong replay lai duoi W moi.

Quy trinh 2-pass, giong dung UCALIB->DCALIB ma ingester6.py tu lam khi calibrate live:
  Pass 1: replay duoi W moi voi UCL="vo cuc" (khong bao gio trig) de lay toan bo phan phoi T2
          KHONG bi anh huong boi viec loai tick "bat thuong" (vi chua biet nguong that la bao
          nhieu). Chon UCL moi = percentile FAR muc tieu (mac dinh p99, khop convention
          "FAR=1.0017%" da dung cho DTHR_DEFAULT hien tai).
  Pass 2: replay lai voi UCL vua chon, loc cac tick "in-control" (trig=0), lay percentile 99
          cua dv[] tung dimension -> DTHR moi.

CANH BAO QUAN TRONG (doc truoc khi tin ket qua):
  - res[]/yh[] (phan du sau FFT) duoc LAY NGUYEN TU LOG GOC, khong tinh lai FFT o day. Neu FFT
    luc sinh log nay dang reject (FFT_SLOW reject) thi res[] ~ y[] tho (khong khu chu ky), lam
    T2/dv[] "nhiu" hon that. Nen check truoc: grep 'FFT_SLOW ok' vs 'FFT_SLOW reject' trong log
    nay de biet FFT co dang fit tot khong luc do.
  - Ket qua chi dang tin neu file log nay THAT SU sach (topology co dinh, khong actuation nao
    tung chay trong khung thoi gian nay). Tu kiem tra bang: grep -c 'DECIDE.*act=scale_out\\|
    act=scale_in' <file> (phai ra 0 hoac gan 0).
  - Nen chay j_replay.py check truoc (voi W/UCL GOC cua log nay, khong phai W moi) de xac nhan
    replay() tai hien dung T2/trig da log - neu sai lech lon o buoc do thi ket qua W moi cung
    khong dang tin.

Dung: python3 j_recalib_w240.py LOG [w=240] [far_pct=1.0] [limit=0]
"""
import sys
from j_replay import parse, load, replay, q, mad, D6, DIMS, ASSUM


def recalib(path, w=240, far_pct=1.0, limit=0):
    print(ASSUM)
    L = parse(path, limit=limit)
    print(f'log={path} n_tick={L.n} W_moi={w} FAR_muc_tieu={far_pct}%')

    cut = w  # bo qua w tick dau (chua du 1 cua so day de baseline covariance on dinh)
    if cut >= L.n:
        raise RuntimeError(f'log qua ngan ({L.n} tick) so voi W_moi={w}, khong du du lieu de calibrate')

    # --- PASS 1: uoc luong phan phoi T2 duoi W moi, KHONG loc trig (UCL dat "vo cuc") ---
    m1 = load(JANUS_W=w, JANUS_UCL=1e18)
    R1 = replay(L, m1, dthr=None, frzin=None, frz_on=0, latch=10**9, cool=0, use_live=0)
    t2s = [R1['t2'][t] for t in range(cut, L.n)]
    p50, p90, p99, p999 = q(t2s, 50), q(t2s, 90), q(t2s, 99), q(t2s, 99.9)
    print(f'PASS1 T2 (chua loc trig) n={len(t2s)} med={p50:.3f} p90={p90:.3f} p99={p99:.3f} '
          f'p999={p999:.3f} max={max(t2s):.3f}')
    ucl_new = q(t2s, 100.0 - far_pct)
    for cand_name, cand in (('p99', p99), ('p999', p999), (f'p{100-far_pct:.2f}(dung)', ucl_new)):
        cnt = sum(1 for v in t2s if v > cand)
        print(f'   candidate UCL {cand_name}={cand:.4f} -> trig_rate uoc luong = {100.0*cnt/len(t2s):.4f}%')
    print(f'==> Chon UCL_moi = {ucl_new:.4f} (percentile {100.0-far_pct:.2f}, FAR muc tieu {far_pct}%)')

    # --- PASS 2: replay lai voi UCL vua chon, bat FRZ nhu binh thuong, loc tick in-control ---
    m2 = load(JANUS_W=w, JANUS_UCL=ucl_new)
    R2 = replay(L, m2, dthr=None, frzin=None, frz_on=1, frz_rel=8, frz_max=400,
                latch=10**9, cool=0, use_live=0)
    incontrol = [t for t in range(cut, L.n) if not R2['trig'][t]]
    n_trig = (L.n - cut) - len(incontrol)
    print(f'PASS2 UCL={ucl_new:.4f}: in-control={len(incontrol)}/{L.n-cut} '
          f'(trig={n_trig}, {100.0*n_trig/max(L.n-cut,1):.4f}%) REBASELINE={R2["reb"]}')

    dthr_new = []
    for i in range(D6):
        vals = [R2['d'][t*D6+i] for t in incontrol]
        p99d, maxd = q(vals, 99), max(vals) if vals else float('nan')
        dthr_new.append(p99d)
        print(f'  {DIMS[i]:5s} med={q(vals,50):.4f} p90={q(vals,90):.4f} p99={p99d:.4f} max={maxd:.4f}')

    print()
    print(f'export: JANUS_W={w} JANUS_UCL={ucl_new:.4f} '
          f'JANUS_DTHR=' + ','.join(f'{v:.4f}' for v in dthr_new))
    return {'ucl': ucl_new, 'dthr': dthr_new, 'w': w, 'n_incontrol': len(incontrol)}


if __name__ == '__main__':
    a = sys.argv[1:]
    if not a:
        print('dung: python3 j_recalib_w240.py LOG [w=240] [far_pct=1.0] [limit=0]')
        sys.exit(1)
    recalib(a[0], int(a[1]) if len(a) > 1 else 240,
             float(a[2]) if len(a) > 2 else 1.0,
             int(a[3]) if len(a) > 3 else 0)
