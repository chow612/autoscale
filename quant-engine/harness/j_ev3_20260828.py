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
    print('SCRIPT j_ev3_20260828.py md5=%s dong=%d  ban truoc: j_ev2_20260828.py'%(
        hashlib.md5(src).hexdigest(),src.count(b'\n')))
    u=float(sys.argv[1]);l=int(sys.argv[2])
    L=J.parse(P)
    s0=int(round((C0-T0)/DW));s1=int(round((C1-T0)/DW))
    n=s1-s0
    sel=mask(s0,s1)
    res=L.res
    col=[];p99=[]
    for i in range(D6):
        a=sorted(res[(s0+j)*D6+i] for j in range(n))
        col.append(a);p99.append(a[int(round(0.99*(n-1)))])
    print('p99 tung chieu tren cua so sach:')
    print('  '+' '.join('%s=%.4f'%(J.DIMS[i],p99[i]) for i in range(D6)))
    warm=W+180
    i0=s0-warm;koff=s0-i0
    m=J.load(JANUS_W=W,JANUS_UCL=u)
    R=J.replay(L,m,dthr=TH,frz_on=1,frz_rel=8,frz_max=400,latch=l,cool=COOL,i0=i0,i1=s1)
    dc=R['dec'];wi=R['wi'];wd=R['wdep'];t2=R['t2']
    print('')
    print('gio_UTC              tick    chieu  dich_vu             T2      gia_tri   p99_chieu pct    ket')
    pv=0;nreal=0;ntot=0
    for j in range(n):
        v=dc[koff+j]
        if v and not pv and sel[j]:
            ntot+=1
            ep=T0+(s0+j)*DW
            k=koff+j;d=wi[k]
            x=res[(s0+j)*D6+d] if d>=0 else 0.0
            pc=100.0*bisect.bisect_left(col[d],x)/n if d>=0 else 0.0
            ok=d>=0 and x>p99[d]
            if ok:nreal+=1
            print('%s  %-7d %-6s %-19s %-7.1f %-9.4f %-9.4f %-6.1f %s'%(
                time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime(ep)),s0+j,
                J.DIMS[d] if d>=0 else '-',
                J.DEPS[wd[k]] if wd[k]>=0 else 'UNATTRIBUTED',
                t2[k],x,p99[d] if d>=0 else 0.0,pc,'THAT' if ok else '?'))
        pv=v
    print('')
    print('tieu chi chot truoc: THAT <=> gia tri tho cua CHIEU DUOC QUY KET > p99 cua chieu do')
    print('=> %d/%d su kien ngoai mat na dat'%(nreal,ntot))
    if nreal==ntot:
        print('   KET LUAN: KHONG co duong tinh gia nao o cau hinh nay')
    else:
        print('   KET LUAN: %d su kien chua giai thich duoc'%(ntot-nreal))

if __name__=='__main__':
    main()
