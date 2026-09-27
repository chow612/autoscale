import sys,hashlib,j_replay as J

P='/home/chau/quant-engine/ing_20260820T023433Z.log'
T0=1787193273
DW=3.002
C0=1787629023
C1=1787888223
B0=2820.0
BP=3636.00
HW=300.0
D6=6
ENV=(24.9689,6.5488,6.9570,6.8484,27.9837,20.1079)

def q(a,p):
    b=sorted(a)
    if not b:return float('nan')
    return b[int(round(p*0.01*(len(b)-1)))]

def main():
    src=open(__file__,'rb').read()
    print('SCRIPT j_dthr_20260828.py md5=%s dong=%d'%(hashlib.md5(src).hexdigest(),src.count(b'\n')))
    W=int(sys.argv[1]) if len(sys.argv)>1 else 240
    L=J.parse(P)
    s0=int(round((C0-T0)/DW));s1=int(round((C1-T0)/DW))
    if L.n<s1:
        print('CHUA DU');return
    n=s1-s0
    sel=bytearray(b'\x01'*n)
    k=int((T0+s0*DW-B0-HW)/BP)
    while 1:
        c=B0+k*BP
        k+=1
        if c-HW>T0+s1*DW:break
        a=int(round((c-HW-T0)/DW))-s0;b=int(round((c+HW-T0)/DW))-s0
        if b<0 or a>=n:continue
        if a<0:a=0
        if b>=n:b=n-1
        for t in range(a,b+1):sel[t]=0
    warm=W+180
    i0=s0-warm
    m=J.load(JANUS_W=W)
    R=J.replay(L,m,frz_on=0,i0=i0,i1=s1)
    koff=s0-i0
    t2=R['t2'];tr=R['tr'];d=R['d']
    u=q([t2[koff+j] for j in range(n) if sel[j]],99.0)
    print('W=%d  UCL=%.4f  (p99 tren tap da loai su kien)'%(W,u))
    g=[j for j in range(n) if sel[j] and t2[koff+j]<=u and tr[koff+j]<m.TR_MAX]
    print('tick qua ba cong DCALIB: %d/%d (%.1f%%)  TR_MAX=%.1f'%(
        len(g),n,100.0*len(g)/n,m.TR_MAX))
    TH=[q([d[(koff+j)*D6+i] for j in g],99.0) for i in range(D6)]
    print('')
    print('DTHR = '+','.join('%.4f'%v for v in TH))
    print('delta so env: '+' '.join('%s=%+.1f%%'%(J.DIMS[i],100.0*(TH[i]-ENV[i])/ENV[i])
                                    for i in range(D6)))
    print('')
    c=[0]*D6;tot=0
    for j in range(n):
        if not sel[j]:continue
        b=(koff+j)*D6;ws=0.0;wi=-1
        for i in range(D6):
            if d[b+i]>ws*TH[i]:ws=d[b+i]/TH[i];wi=i
        if wi>=0:c[wi]+=1;tot+=1
    print('do tap trung argmax(d_i/DTHR_i) tren %d tick:'%tot)
    print('  '+' '.join('%s=%.1f%%'%(J.DIMS[i],100.0*c[i]/tot) for i in range(D6)))
    mx=max(100.0*c[i]/tot for i in range(D6))
    print('cong 40%%: %s (max=%.1f%%)'%('DAT' if mx<=40.0 else 'TRUOT',mx))

if __name__=='__main__':
    main()
