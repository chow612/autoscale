import sys,time,array,hashlib,j_replay as J

P='/home/chau/quant-engine/ing_20260820T023433Z.log'
T0=1787193273
DW=3.002
C0=1787629023
C1=1787888223
B0=2820.0
BP=3636.00
HW=300.0
W=240
UCL=58.7343
COOL=100
D6=6
TH=(24.8372,6.6410,11.5795,7.0127,26.4767,21.2709)

def q(a,p):
    b=sorted(a)
    if not b:return float('nan')
    return b[int(round(p*0.01*(len(b)-1)))]

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
    print('SCRIPT j_uni2_20260828.py md5=%s dong=%d'%(hashlib.md5(src).hexdigest(),src.count(b'\n')))
    l=int(sys.argv[1]) if len(sys.argv)>1 else 6
    L=J.parse(P)
    s0=int(round((C0-T0)/DW));s1=int(round((C1-T0)/DW))
    n=s1-s0
    sel=mask(s0,s1)
    warm=W+180
    i0=s0-warm;koff=s0-i0
    m=J.load(JANUS_W=W,JANUS_UCL=UCL)
    R=J.replay(L,m,dthr=TH,frz_on=1,frz_rel=8,frz_max=400,latch=l,cool=COOL,i0=i0,i1=s1)
    fz=R['frz'];dc=R['dec'];wi=R['wi'];wd=R['wdep'];t2=R['t2']
    S=m.StateMEWMA()
    e=array.array('d',[0.0]*D6);vb=bytearray(b'\x01'*D6)
    res=L.res
    mz=array.array('d',[0.0]*(s1-i0))
    for t in range(i0,s1):
        k=t-i0;b=t*D6
        for i in range(D6):e[i]=res[b+i]
        S.update(e,vb,fz[k])
        z=S.z;lv=S.live;u=0.0
        for i in range(D6):
            if not lv[i]:continue
            a=z[i]
            if a<0.0:a=-a
            if a>u:u=a
        mz[k]=u
    ft=100.0*sum(1 for j in range(n) if sel[j] and t2[koff+j]>UCL)/sum(1 for j in range(n) if sel[j])
    K=q([mz[koff+j] for j in range(n) if sel[j]],100.0-ft)
    fu=100.0*sum(1 for j in range(n) if sel[j] and mz[koff+j]>K)/sum(1 for j in range(n) if sel[j])
    print('W=%d UCL=%.4f  FAR(T2)=%.4f%%   K*=%.4f  FAR(UNI)=%.4f%%'%(W,UCL,ft,K,fu))
    print('tieu chi: UNI bat <=> ton tai tick trong [bat-%d, bat+%d] co max|z| > K*'%(l,l))
    print('')
    print('gio_UTC              tick    chieu  dich_vu             T2      max|z|_dinh  UNI')
    pv=0;tot=0;hit=0
    for j in range(n):
        v=dc[koff+j]
        if v and not pv and sel[j]:
            tot+=1
            a=j-l;b=j+l
            if a<0:a=0
            if b>=n:b=n-1
            pk=max(mz[koff+x] for x in range(a,b+1))
            ok=pk>K
            if ok:hit+=1
            ep=T0+(s0+j)*DW
            k=koff+j
            print('%s  %-7d %-6s %-19s %-7.1f %-12.4f %s'%(
                time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime(ep)),s0+j,
                J.DIMS[wi[k]] if wi[k]>=0 else '-',
                J.DEPS[wd[k]] if wd[k]>=0 else 'UNATTRIBUTED',
                t2[k],pk,'BAT' if ok else 'BO SOT'))
        pv=v
    print('')
    print('=> UNI bat %d/%d su kien ma T2 bat (o cung ti le bao dong)'%(hit,tot))
    if hit<tot:
        print('   %d su kien chi T2 bat duoc -> phat bieu ve DO BAO PHU, khong ve tinh THAT'%(tot-hit))

if __name__=='__main__':
    main()
