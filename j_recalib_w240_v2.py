"""
Ban mo rong cua j_recalib_w240.py: PASS1 (khong FRZ) chi dung de lay khoang UCL ban dau, sau do
LAP (bisection) nhieu lan PASS2 (co FRZ=1, dung dieu kien giong production that: JANUS_FRZ=1,
JANUS_FRZ_REL=8, JANUS_FRZ_MAX=400) cho toi khi FAR THAT (duoi FRZ) hoi tu ve dung target, thay
vi chi dung UCL=p99 tu PASS1 (ma FAR that duoi FRZ lai ra khac, vd 1.687% thay vi 1.000% muc tieu
- do FRZ tao vong phan hoi phi tuyen: dong bang baseline luc dang bat thuong lam tick SAU do de
vuot nguong hon).

Dung: python3 j_recalib_w240_v2.py LOG [w=240] [far_pct=1.0] [tol=0.03] [max_iter=15] [limit=0]
"""
import sys
from j_replay import parse, load, replay, q, D6, DIMS, ASSUM


def pass2_far(L, w, ucl, cut):
    """Chay 1 lan PASS2 (FRZ bat, giong production) voi UCL cho truoc, tra ve (far_thuc_te%, R)."""
    m = load(JANUS_W=w, JANUS_UCL=ucl)
    R = replay(L, m, dthr=None, frzin=None, frz_on=1, frz_rel=8, frz_max=400,
               latch=10 ** 9, cool=0, use_live=0)
    n_total = L.n - cut
    n_trig = sum(1 for t in range(cut, L.n) if R['trig'][t])
    far = 100.0 * n_trig / max(n_total, 1)
    return far, R


def recalib(path, w=240, far_pct=1.0, tol=0.03, max_iter=15, limit=0):
    print(ASSUM)
    L = parse(path, limit=limit)
    print(f'log={path} n_tick={L.n} W_moi={w} FAR_muc_tieu={far_pct}% tol={tol}pp max_iter={max_iter}')

    cut = w
    if cut >= L.n:
        raise RuntimeError(f'log qua ngan ({L.n} tick) so voi W_moi={w}')

    # --- PASS1: uoc luong tho (khong FRZ) de lay khoang [lo,hi] ban dau cho bisection ---
    m1 = load(JANUS_W=w, JANUS_UCL=1e18)
    R1 = replay(L, m1, dthr=None, frzin=None, frz_on=0, latch=10 ** 9, cool=0, use_live=0)
    t2s = [R1['t2'][t] for t in range(cut, L.n)]
    p_lo_guess = q(t2s, 100.0 - far_pct)       # uoc luong ban dau (khong FRZ)
    p_hi_guess = q(t2s, 100.0 - far_pct * 0.1)  # can tren rong hon nhieu (FAR duoi FRZ luon >= khong FRZ)
    print(f'PASS1 (chua FRZ) T2 n={len(t2s)} med={q(t2s,50):.3f} p99={q(t2s,99):.3f} '
          f'max={max(t2s):.3f} -> UCL khoi tao [{p_lo_guess:.4f}, {p_hi_guess:.4f}]')

    # --- Bisection tren PASS2 (co FRZ=1, dung dieu kien nhu production that) ---
    lo, hi = p_lo_guess, p_hi_guess
    far_lo, _ = pass2_far(L, w, lo, cut)
    print(f'  kiem tra can duoi UCL={lo:.4f} -> FAR(FRZ)={far_lo:.4f}%')
    far_hi, _ = pass2_far(L, w, hi, cut)
    print(f'  kiem tra can tren UCL={hi:.4f} -> FAR(FRZ)={far_hi:.4f}%')
    tries = 0
    while far_hi > far_pct and tries < 10:
        hi *= 1.8
        far_hi, _ = pass2_far(L, w, hi, cut)
        tries += 1
        print(f'  can tren chua du cao, mo rong -> UCL={hi:.4f} -> FAR(FRZ)={far_hi:.4f}%')
    if far_lo < far_pct:
        print(f'  CANH BAO: can duoi da FAR={far_lo:.4f}% < muc tieu - dung luon can duoi, khong bisect them')
        ucl_final, far_final, R_final = lo, far_lo, None
    else:
        ucl_final, far_final, R_final = None, None, None
        for it in range(1, max_iter + 1):
            mid = (lo + hi) / 2.0
            far_mid, R_mid = pass2_far(L, w, mid, cut)
            print(f'  iter={it} UCL={mid:.4f} -> FAR(FRZ)={far_mid:.4f}% (target={far_pct}%, tol={tol})')
            if abs(far_mid - far_pct) <= tol:
                ucl_final, far_final, R_final = mid, far_mid, R_mid
                print(f'  HOI TU sau {it} vong lap.')
                break
            if far_mid > far_pct:
                lo = mid  # FAR con cao hon target -> can tang UCL -> day can duoi len
            else:
                hi = mid  # FAR da thap hon target -> co the giam UCL -> keo can tren xuong
        if ucl_final is None:
            ucl_final, far_final, R_final = mid, far_mid, R_mid
            print(f'  KHONG hoi tu du tol sau {max_iter} vong lap, dung gia tri gan nhat: '
                  f'UCL={ucl_final:.4f} FAR={far_final:.4f}%')

    if R_final is None:
        _, R_final = pass2_far(L, w, ucl_final, cut)

    print(f'\n==> UCL_moi (FAR duoi FRZ hoi tu) = {ucl_final:.4f}  (FAR that = {far_final:.4f}%, '
          f'muc tieu {far_pct}%)')

    incontrol = [t for t in range(cut, L.n) if not R_final['trig'][t]]
    print(f'in-control={len(incontrol)}/{L.n-cut} REBASELINE={R_final["reb"]}')
    dthr_new = []
    for i in range(D6):
        vals = [R_final['d'][t * D6 + i] for t in incontrol]
        p99d = q(vals, 99)
        dthr_new.append(p99d)
        print(f'  {DIMS[i]:5s} med={q(vals,50):.4f} p90={q(vals,90):.4f} p99={p99d:.4f} '
              f'max={max(vals) if vals else float("nan"):.4f}')

    print()
    print(f'export: JANUS_W={w} JANUS_UCL={ucl_final:.4f} '
          f'JANUS_DTHR=' + ','.join(f'{v:.4f}' for v in dthr_new))
    return {'ucl': ucl_final, 'far': far_final, 'dthr': dthr_new, 'w': w}


if __name__ == '__main__':
    a = sys.argv[1:]
    if not a:
        print('dung: python3 j_recalib_w240_v2.py LOG [w=240] [far_pct=1.0] [tol=0.03] [max_iter=15] [limit=0]')
        sys.exit(1)
    recalib(a[0], int(a[1]) if len(a) > 1 else 240,
             float(a[2]) if len(a) > 2 else 1.0,
             float(a[3]) if len(a) > 3 else 0.03,
             int(a[4]) if len(a) > 4 else 15,
             int(a[5]) if len(a) > 5 else 0)
