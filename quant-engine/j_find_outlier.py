"""
Tim N tick co T2 (replay duoi W moi) lon nhat, in ra dung so dong trong file log GOC de
sed -n 'A,Bp' xem context that (khong grep truc tiep duoc vi T2 nay la gia tri REPLAY, khong
ton tai san trong text log goc voi W cu).

Dung: python3 j_find_outlier.py LOG [w=240] [topn=10]
"""
import sys, array
from j_replay import load, replay, q, D6, DIMS, MASK_FULL


class LogLN:
    """Ban sao rut gon cua Log (j_replay.py) nhung KHONG dung __slots__, de them duoc
    lineno (Log goc dung __slots__ nen khong gan them thuoc tinh moi vao duoc)."""
    def __init__(s):
        for a in ('res', 'dd', 'sd', 'mu', 'xx', 'zz', 'dz', 't2', 'ucl', 'tr', 'dw'):
            setattr(s, a, array.array('d'))
        for a in ('live', 'trig', 'frz'):
            setattr(s, a, bytearray())
        for a in ('cnt', 'pp', 'nl', 'calm', 'tuoi', 'lineno'):
            setattr(s, a, array.array('i'))
        s.n = 0;s.mask = 0;s.seen = 0


def parse_with_lineno(path, limit=0):
    """Ban sao parse() cua j_replay.py, them L.lineno = so dong (1-based) cua dong
    '--- window' MO DAU tick do trong file goc, de map nguoc tick -> vi tri that."""
    L = LogLN()
    L.lineno = array.array('i')
    need = MASK_FULL
    L.mask = need
    r = array.array('d', [0.0] * D6);dv = array.array('d', [0.0] * D6)
    sv = array.array('d', [0.0] * D6);mv = array.array('d', [0.0] * D6)
    xv = array.array('d', [0.0] * D6);zv = array.array('d', [0.0] * D6)
    lv = bytearray(D6);dz = array.array('d', [0.0] * (D6 * 6))
    st = {'have': 0, 'dw': 0.0, 't2': 0.0, 'ucl': 0.0, 'tr': 0.0, 'trig': 0, 'cnt': 0, 'pp': 0,
          'nl': 0, 'frz': 0, 'calm': 0, 'tuoi': 0, 'lineno': 0}

    def flush():
        L.res.extend(r);L.dd.extend(dv);L.sd.extend(sv);L.mu.extend(mv)
        L.xx.extend(xv);L.zz.extend(zv);L.dz.extend(dz);L.live.extend(lv)
        L.trig.append(st['trig']);L.frz.append(st['frz'])
        L.calm.append(st['calm']);L.tuoi.append(st['tuoi'])
        L.t2.append(st['t2']);L.ucl.append(st['ucl'])
        L.tr.append(st['tr']);L.dw.append(st['dw'])
        L.cnt.append(st['cnt']);L.pp.append(st['pp']);L.nl.append(st['nl'])
        L.lineno.append(st['lineno'])
        L.n += 1

    def _v(t):
        return t[t.index('=') + 1:]

    DIDX = {n: i for i, n in enumerate(('balancereader', 'contacts', 'frontend', 'ledgerwriter',
                                         'transactionhistory', 'userservice'))}
    ND = 6

    f = open(path, 'r', errors='replace')
    lno = 0
    for ln in f:
        lno += 1
        if ln[:3] == '---':
            if (st['have'] & need) == need:
                flush()
                if limit and L.n >= limit:break
            L.seen |= st['have']
            st['have'] = 0
            if ln[:10] == '--- window':
                st['lineno'] = lno  # dong nay la dong BAT DAU tick moi
                for t in ln.split():
                    if t[:3] == 'dw=':
                        st['dw'] = float(t[3:-1]);break
                st['have'] = 1
            continue
        if ln[:6] == 'MEWMA ':
            k = ln.split()
            st['t2'] = float(_v(k[1]));st['ucl'] = float(_v(k[2]))
            st['trig'] = 1 if _v(k[3]) == 'True' else 0
            st['cnt'] = int(_v(k[4]));st['pp'] = int(_v(k[5]));st['tr'] = float(_v(k[6]))
            st['have'] |= 2
            continue
        if ln[:2] != '  ':continue
        k = ln.split()
        h = k[0]
        if h == 'FFT':
            for i in range(D6):r[i] = float(k[i + 1].rsplit('>', 1)[1])
            st['have'] |= 4
        elif h == 'Z':
            for i in range(D6):
                t = k[i + 1]
                if t[-1] == '*':
                    lv[i] = 0;t = t[:-1]
                else:
                    lv[i] = 1
                a, b = _v(t).split('/')
                xv[i] = float(a);zv[i] = float(b)
            st['have'] |= 8
        elif h == 'SD':
            for i in range(D6):
                t = _v(k[i + 1]);j = t.index('(')
                sv[i] = float(t[:j]);mv[i] = float(t[j + 3:-1])
            st['have'] |= 16
        elif h == 'D':
            for i in range(D6):dv[i] = float(_v(k[i + 1]))
            st['nl'] = int(_v(k[7]));st['have'] |= 32
        elif h == 'FRZstate':
            st['frz'] = int(_v(k[1]));st['calm'] = int(_v(k[2]));st['tuoi'] = int(_v(k[3]))
            st['have'] |= 64
        elif h in DIDX:
            t = k[-1]
            if t[:2] == 'z=':
                j = DIDX[h];p = _v(t).split('/')
                for i in range(D6):dz[i * ND + j] = float(p[i])
    else:
        if (st['have'] & need) == need:flush()
    L.seen |= st['have']
    f.close()
    if L.n == 0:
        raise RuntimeError('0 tick parse duoc tu %s' % path)
    return L


def find_outliers(path, w=240, topn=10):
    L = parse_with_lineno(path)
    print(f'log={path} n_tick={L.n} W_moi={w}')
    m = load(JANUS_W=w, JANUS_UCL=1e18)
    R = replay(L, m, dthr=None, frzin=None, frz_on=0, latch=10 ** 9, cool=0, use_live=0)
    t2 = R['t2']
    order = sorted(range(len(t2)), key=lambda i: t2[i], reverse=True)[:topn]
    print(f'\nTop {topn} tick co T2 (replay W={w}) lon nhat:')
    for rank, t in enumerate(order, 1):
        ln0 = L.lineno[t]
        # tick tiep theo (neu co) de biet pham vi dong can xem, uoc luong ~30 dong/tick
        ln1 = L.lineno[t + 1] - 1 if t + 1 < L.n else ln0 + 40
        dvals = ' '.join(f'{DIMS[i]}={L.dd[t*D6+i]:.3f}' for i in range(D6))
        print(f'  #{rank} tick={t} T2={t2[t]:.3f} dong_bat_dau_trong_log_goc={ln0} '
              f'(xem: sed -n \'{ln0},{ln1}p\' {path})')
        print(f'       nl(log cu)={L.nl[t]} D(log cu, chua normalize)={dvals}')
    print(f'\nGoi y: chay "sed -n \'{L.lineno[order[0]]},{(L.lineno[order[0]+1]-1) if order[0]+1<L.n else L.lineno[order[0]]+40}p\' {path}"')
    print('de xem toan bo cac dong (MEWMA/FFT/Z/SD/D/REP/dep...) cua dung tick outlier lon nhat.')


if __name__ == '__main__':
    a = sys.argv[1:]
    if not a:
        print('dung: python3 j_find_outlier.py LOG [w=240] [topn=10]')
        sys.exit(1)
    find_outliers(a[0], int(a[1]) if len(a) > 1 else 240, int(a[2]) if len(a) > 2 else 10)
