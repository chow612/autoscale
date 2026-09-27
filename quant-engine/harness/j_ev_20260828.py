import sys,time,hashlib,j_replay as J

P='/home/chau/quant-engine/ing_20260820T023433Z.log'
T0=1787193273
DW=3.002
C0=1787629023
C1=1787888223
B0=2820.0
BP=3636.00
HW=300.0
W=240
COOL=100
TH=(24.8372,6.6410,11.5795,7.0127,26.4767,21.2709)

def mask(s0,s1):
    n=s1-s0
    m=bytearray(b'\x01'*n)
    k=int((T0+s0*DW-B0-HW)/BP)
    while 1:
        c=B0+k*BP
        k+=1
        if c-HW>T0+s1*DW:break
        a=int(round((c-HW-T0)/DW))-s0;b=int(round((c+HW-T0)/DW))-s0
        if b<0 or a>=n:continue
        if a<0:a=0
        if b>=n:b=n-1
        for t in range(a,b+1):m[t]=0
    return m

def near(ep):
    k=round((ep-B0)/BP)
    return ep-(B0+k*BP)

def main():
    src=open(__file__,'rb').read()
    print('SCRIPT j_ev_20260828.py md5=%s dong=%d'%(hashlib.md5(src).hexdigest(),src.count(b'\n')))
    u=float(sys.argv[1]);l=int(sys.argv[2])
    L=J.parse(P)
    s0=int(round((C0-T0)/DW));s1=int(round((C1-T0)/DW))
    n=s1-s0
    sel=mask(s0,s1)
    warm=W+180
    i0=s0-warm;koff=s0-i0
    m=J.load(JANUS_W=W,JANUS_UCL=u)
    R=J.replay(L,m,dthr=TH,frz_on=1,frz_rel=8,frz_max=400,latch=l,cool=COOL,i0=i0,i1=s1)
    dc=R['dec'];t2=R['t2'];wi=R['wi'];wd=R['wdep']
    print('UCL=%.4f latch=%d cool=%d'%(u,l,COOL))
    print('')
    print('gio_UTC              tick    trong_mat_na  chieu  dich_vu             T2       lech_moc(s)')
    pv=0
    for j in range(n):
        v=dc[koff+j]
        if v and not pv:
            ep=T0+(s0+j)*DW
            k=koff+j
            print('%s  %-7d %-13s %-6s %-19s %-8.2f %+.1f'%(
                time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime(ep)),
                s0+j,
                'ngoai' if sel[j] else 'TRONG',
                J.DIMS[wi[k]] if wi[k]>=0 else '-',
                J.DEPS[wd[k]] if wd[k]>=0 else 'UNATTRIBUTED',
                t2[k],near(ep)))
        pv=v

if __name__=='__main__':
    main()
