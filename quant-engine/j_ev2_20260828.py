import sys,time,bisect,hashlib,j_replay as J

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
D6=6
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

def main():
    src=open(__file__,'rb').read()
    print('SCRIPT j_ev2_20260828.py md5=%s dong=%d'%(hashlib.md5(src).hexdigest(),src.count(b'\n')))
    u=float(sys.argv[1]);l=int(sys.argv[2])
    L=J.parse(P)
    s0=int(round((C0-T0)/DW));s1=int(round((C1-T0)/DW))
    n=s1-s0
    sel=mask(s0,s1)
    res=L.res
    cpu=sorted(res[(s0+j)*D6] for j in range(n))
    rps=sorted(res[(s0+j)*D6+2] for j in range(n))
    p99=cpu[int(round(0.99*(n-1)))]
    med=cpu[n//2]
    print('cua so sach: tcpu trung vi=%.4f  p99=%.4f'%(med,p99))
    warm=W+180
    i0=s0-warm;koff=s0-i0
    m=J.load(JANUS_W=W,JANUS_UCL=u)
    R=J.replay(L,m,dthr=TH,frz_on=1,frz_rel=8,frz_max=400,latch=l,cool=COOL,i0=i0,i1=s1)
    dc=R['dec'];wi=R['wi'];wd=R['wdep'];t2=R['t2']
    print('')
    print('gio_UTC              tick    chieu  dich_vu             T2      tcpu    pct   rps     ket')
    pv=0;nreal=0;ntot=0
    for j in range(n):
        v=dc[koff+j]
        if v and not pv and sel[j]:
            ntot+=1
            ep=T0+(s0+j)*DW
            k=koff+j
            c=res[(s0+j)*D6];r=res[(s0+j)*D6+2]
            pc=100.0*bisect.bisect_left(cpu,c)/n
            ok=c>p99
            if ok:nreal+=1
            print('%s  %-7d %-6s %-19s %-7.1f %-7.3f %-5.1f %-7.1f %s'%(
                time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime(ep)),s0+j,
                J.DIMS[wi[k]] if wi[k]>=0 else '-',
                J.DEPS[wd[k]] if wd[k]>=0 else 'UNATTRIBUTED',
                t2[k],c,pc,r,'THAT' if ok else '?'))
        pv=v
    print('')
    print('tieu chi chot truoc: THAT <=> tcpu tai tick bat > p99 cua toan cua so sach')
    print('=> %d/%d su kien ngoai mat na vuot p99'%(nreal,ntot))
    if nreal==ntot:
        print('   KET LUAN: KHONG co duong tinh gia nao o cau hinh nay')
    else:
        print('   KET LUAN: %d su kien chua giai thich duoc, phai soi tung cai'%(ntot-nreal))

if __name__=='__main__':
    main()
