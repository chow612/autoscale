#!/usr/bin/env python3
# j_monN_sweep.py (v2) - sweep M cho co che "M trong N" thay the DEC_LATCH hien tai.
#
# KHAC BAN TRUOC: khong can chay lai ingester6.py/j_replay.py qua server song. Parse TRUC TIEP
# tu file log lich su (vd ing_20260820T023433Z.log) - dung file NAY DA CHUA du du lieu tho
# (dong MEWMA, dong 'D cpu=... ram=...', va 6 dong per-dependency co 'z=a/b/c/d/e/f') de tu tai
# tao wi/wdepa/dkey theo DUNG cong thuc trong ingester6.py (dong ~1155-1178), khong sai lech
# tick voi ground truth vi CUNG DOC 1 FILE ma j_ev3_20260828.py da dung de tao out_ev3.txt.
#
# GIOI HAN CAN GHI RO TRONG BAO CAO: dv[]/z[] trong file log nay duoc TINH SAN luc chay that
# (voi MEWMA_WINDOW/FRZ... cua LAN CHAY DO, co the khac voi tham so calibrate 28/08 dang dung
# lam default moi). Suy luan wi/wdepa/dkey o day dung DTHR moi (calibrate 28/08) ap len dv[] cu
# - la xap xi tot nhat co the tu du lieu offline, KHONG tuong duong chay lai that voi W=240.
# Day la han che chung cua moi phan tich offline tu log co san, khong rieng gi script nay.
#
# CACH DUNG:
#   python3 j_monN_sweep.py --log ing_20260820T023433Z.log --truth out_ev3.txt \
#       --n 16 --cool 100 --max-latency 400
#
# OUTPUT: bang sweep M=1..N, giong phong cach j_latch_20260828.py.

import argparse
import re
import sys
from collections import deque, Counter

DIMS = ('cpu', 'ram', 'rps', 'err', 'asym', 'slow')
DEP_LIST = ('balancereader', 'contacts', 'frontend', 'ledgerwriter', 'transactionhistory', 'userservice')
NDEP = len(DEP_LIST)
DTHR_DEFAULT = (24.8372, 6.6410, 11.5795, 7.0127, 26.4767, 21.2709)  # calibrate 20260828

MEWMA_RE = re.compile(r'^MEWMA T2=[\d.]+ ucl=[\d.]+ trig=(True|False)')
D_RE = re.compile(
    r'^\s*D cpu=([-\d.]+) ram=([-\d.]+) rps=([-\d.]+) err=([-\d.]+) asym=([-\d.]+) slow=([-\d.]+)'
)
DEP_RE = {
    dep: re.compile(rf'^\s*{dep} cpu=.*?z=([-+\d.]+)/([-+\d.]+)/([-+\d.]+)/([-+\d.]+)/([-+\d.]+)/([-+\d.]+)')
    for dep in DEP_LIST
}


def dkey_of(wi, wdep):
    if wi < 0 or wdep < 0:
        return -1
    return wi * NDEP + wdep


def compute_dkey(dv, dep_z, dthr, trig):
    """dv: list 6 gia tri (theo thu tu DIMS). dep_z: dict dep_name -> list 6 gia tri z (theo thu tu DIMS).
    Tra ve (dkey, wi_name, wdep_name, sg) - dung DUNG cong thuc trong ingester6.py."""
    rat = all(d > 0.0 for d in dthr)
    wi = -1
    wd = 0.0
    ws = 0.0
    if rat:
        for i in range(6):
            if dv[i] > ws * dthr[i]:
                ws = dv[i] / dthr[i]
                wi = i
                wd = dv[i]
    else:
        for i in range(6):
            if dv[i] > wd:
                wd = dv[i]
                wi = i
    sg = wi >= 0 and (dthr[wi] <= 0.0 or wd > dthr[wi])
    wdepa = -1
    wz = 0.0
    if wi >= 0:
        for j, dep in enumerate(DEP_LIST):
            az = abs(dep_z[dep][wi])
            if az > wz:
                wz = az
                wdepa = j
    wdep = wdepa if sg else -1
    dkey = dkey_of(wi, wdep) if (trig and sg and wdep >= 0) else -1
    wi_name = DIMS[wi] if wi >= 0 else '-'
    wdep_name = DEP_LIST[wdep] if wdep >= 0 else '-'
    return dkey, wi_name, wdep_name, sg


def parse_history_log(path, dthr):
    """Doc log lich su, tra ve list (tick, dkey) theo dung thu tu tick ma j_ev3 da dung (dem
    tang dan moi lan gap 1 dong MEWMA moi, giong cach j_ev3/j_replay tu duyet log)."""
    rows = []
    tick = -1
    trig = None
    dv = None
    dep_z = {}
    incomplete = 0

    def flush():
        nonlocal dv, dep_z
        if trig is None or dv is None or len(dep_z) < NDEP:
            return False
        dkey, wi_name, wdep_name, sg = compute_dkey(dv, dep_z, dthr, trig)
        rows.append((tick, dkey))
        return True

    with open(path, 'r', errors='replace') as f:
        for line in f:
            m = MEWMA_RE.search(line)
            if m:
                if tick >= 0:
                    if not flush():
                        incomplete += 1
                tick += 1
                trig = (m.group(1) == 'True')
                dv = None
                dep_z = {}
                continue
            m = D_RE.search(line)
            if m:
                dv = [float(x) for x in m.groups()]
                continue
            for dep, rx in DEP_RE.items():
                m = rx.search(line)
                if m:
                    dep_z[dep] = [float(x) for x in m.groups()]
                    break

    if tick >= 0:
        if not flush():
            incomplete += 1

    if not rows:
        sys.exit(f'khong parse duoc tick nao tu {path} - kiem tra lai dinh dang log/regex')
    if incomplete:
        print(f'CANH BAO: {incomplete} tick bi thieu du lieu (D-line hoac 1 trong 6 dep-line), da bo qua', file=sys.stderr)
    return rows


def load_truth(path):
    events = []
    with open(path, 'r', errors='replace') as f:
        first = True
        for line in f:
            parts = line.split()
            if not parts:
                continue
            if first:
                first = False
                if not parts[1].isdigit():
                    continue
            if len(parts) < 9 or parts[-1] != 'THAT':
                continue
            try:
                tick = int(parts[1])
                dim = parts[2]
                dep = parts[3]
            except (ValueError, IndexError):
                continue
            wi = DIMS.index(dim) if dim in DIMS else -1
            wdep = DEP_LIST.index(dep) if dep in DEP_LIST else -1
            dkey = dkey_of(wi, wdep)
            if dkey >= 0:
                events.append((tick, dkey))
    if not events:
        sys.exit(f'khong tim thay su kien THAT nao trong {path} - kiem tra dinh dang cot')
    events.sort(key=lambda x: x[0])
    return events


def simulate(rows, N, M, cool):
    window = deque(maxlen=N)
    counts = Counter()
    fires = []
    cool_left = 0
    for tick, dkey in rows:
        if len(window) == N:
            old = window[0]
            if old >= 0:
                counts[old] -= 1
                if counts[old] <= 0:
                    del counts[old]
        window.append(dkey)
        if dkey >= 0:
            counts[dkey] += 1

        if cool_left > 0:
            cool_left -= 1
            continue

        if counts:
            best_key, best_n = counts.most_common(1)[0]
            if best_n >= M:
                fires.append((tick, best_key))
                cool_left = cool
    return fires


def evaluate(fires, truth_events, max_latency):
    detected = 0
    latencies = []
    matched_fire_idx = set()
    fires_by_tick = sorted(fires)
    for ev_tick, ev_key in truth_events:
        best = None
        for i, (f_tick, f_key) in enumerate(fires_by_tick):
            if f_key != ev_key:
                continue
            if ev_tick <= f_tick <= ev_tick + max_latency:
                if best is None or f_tick < best[1]:
                    best = (i, f_tick)
        if best is not None:
            detected += 1
            latencies.append(best[1] - ev_tick)
            matched_fire_idx.add(best[0])

    false_fires = 0
    for i, (f_tick, f_key) in enumerate(fires_by_tick):
        if i in matched_fire_idx:
            continue
        near_any = any(abs(f_tick - ev_tick) <= max_latency for ev_tick, _ in truth_events)
        if not near_any:
            false_fires += 1

    return detected, latencies, false_fires


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--log', required=True, help='file log lich su (vd ing_20260820T023433Z.log) - CUNG file j_ev3 da dung')
    ap.add_argument('--truth', required=True, help='file ground truth dinh dang out_ev3.txt')
    ap.add_argument('--n', type=int, default=16)
    ap.add_argument('--m-min', type=int, default=1)
    ap.add_argument('--m-max', type=int, default=None, help='mac dinh = N')
    ap.add_argument('--cool', type=int, default=100, help='don vi tick, mac dinh 100 khop JANUS_COOL')
    ap.add_argument('--max-latency', type=int, default=400, help='don vi tick, mac dinh 400 khop FRZ_MAX')
    ap.add_argument('--tick-seconds', type=float, default=3.0)
    ap.add_argument('--dthr', default=None, help='6 so cach nhau boi dau phay, mac dinh dung calibrate 28/08')
    args = ap.parse_args()

    dthr = DTHR_DEFAULT
    if args.dthr:
        dthr = tuple(float(x) for x in args.dthr.split(','))
        if len(dthr) != 6:
            sys.exit('--dthr phai co dung 6 gia tri')

    m_max = args.m_max or args.n

    rows = parse_history_log(args.log, dthr)
    truth = load_truth(args.truth)
    total_ticks = rows[-1][0] - rows[0][0] + 1
    total_days = total_ticks * args.tick_seconds / 86400.0

    print(f'DOC XONG: {len(rows)} tick tu {args.log} (tick {rows[0][0]}..{rows[-1][0]}, ~{total_days:.2f} ngay), '
          f'{len(truth)} su kien THAT trong {args.truth}')
    print(f'DTHR dung de suy luan wi/wdepa: {dthr}')
    print(f'N={args.n} cool={args.cool} max_latency={args.max_latency}')
    print()
    print(f'{"M":>3} {"nfire":>6} {"phat_hien":>10} {"do_tre_med":>11} {"do_tre_p95":>11} {"bao_dong_gia":>13} {"bao_dong/ngay":>14}')

    best_m = None
    for M in range(args.m_min, m_max + 1):
        fires = simulate(rows, args.n, M, args.cool)
        detected, latencies, false_fires = evaluate(fires, truth, args.max_latency)
        lat_sorted = sorted(latencies)
        med = lat_sorted[len(lat_sorted) // 2] if lat_sorted else float('nan')
        p95 = lat_sorted[int(len(lat_sorted) * 0.95)] if lat_sorted else float('nan')
        alarms_per_day = false_fires / total_days if total_days > 0 else float('nan')
        print(f'{M:>3} {len(fires):>6} {detected:>4}/{len(truth):<5} {med:>11} {p95:>11} '
              f'{false_fires:>13} {alarms_per_day:>14.2f}')
        if detected == len(truth) and best_m is None:
            best_m = M

    print()
    if best_m is not None:
        print(f'=> M NHO NHAT dat phat hien 100% ({len(truth)}/{len(truth)}): M={best_m} (voi N={args.n})')
    else:
        print(f'=> KHONG gia tri M nao trong [{args.m_min},{m_max}] dat phat hien 100%.')


if __name__ == '__main__':
    main()
