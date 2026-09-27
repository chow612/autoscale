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
PT=1.00
ITER=12
ENV=(24.9689,6.5488,6.9570,6.8484,27.9837,20.1079)

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

def far(t2,koff,n,sel,u):
    nu=0;nt=0
    for j in range(n):
        if not sel[j]:continue
        nu+=1
        if t2[koff+j]>u:nt+=1
    return 100.0*nt/nu

def main():
    src=open(__file__,'rb').read()
    print('SCRIPT j_op_20260828.py md5=%s dong=%d  ban truoc: j_dthr_20260828.py'%(
        hashlib.md5(src).hexdigest(),src.count(b'\n')))
    W=int(sys.argv[1]) if len(sys.argv)>1 else 240
    FZ=int(sys.argv[2]) if len(sys.argv)>2 else 1
    L=J.parse(P)
    s0=int(round((C0-T0)/DW));s1=int(round((C1-T0)/DW))
    if L.n<s1:
        print('CHUA DU');return
    n=s1-s0
    sel=mask(s0,s1)
    warm=W+180
    i0=s0-warm;koff=s0-i0
    m=J.load(JANUS_W=W)
    R=J.replay(L,m,frz_on=0,i0=i0,i1=s1)
    u=q([R['t2'][koff+j] for j in range(n) if sel[j]],100.0-PT)
    if FZ:
        lo=0.4*u;hi=2.5*u
        for _ in range(ITER):
            u=0.5*(lo+hi)
            m=J.load(JANUS_W=W,JANUS_UCL=u)
            R=J.replay(L,m,frz_on=1,i0=i0,i1=s1)
            if far(R['t2'],koff,n,sel,u)>PT:lo=u
            else:hi=u
    print('W=%d  FRZ=%d  UCL=%.4f  FAR=%.4f%%'%(W,FZ,u,far(R['t2'],koff,n,sel,u)))
    t2=R['t2'];tr=R['tr'];d=R['d']
    g=[j for j in range(n) if sel[j] and t2[koff+j]<=u and tr[koff+j]<m.TR_MAX]
    print('tick qua ba cong DCALIB: %d/%d (%.1f%%)'%(len(g),n,100.0*len(g)/n))
    TH=[q([d[(koff+j)*D6+i] for j in g],99.0) for i in range(D6)]
    print('')
    print('DTHR = '+','.join('%.4f'%v for v in TH))
    print('delta so env: '+' '.join('%s=%+.1f%%'%(J.DIMS[i],100.0*(TH[i]-ENV[i])/ENV[i])
                                    for i in range(D6)))
    c=[0]*D6;tot=0
    for j in range(n):
        if not sel[j]:continue
        b=(koff+j)*D6;ws=0.0;wi=-1
        for i in range(D6):
            if d[b+i]>ws*TH[i]:ws=d[b+i]/TH[i];wi=i
        if wi>=0:c[wi]+=1;tot+=1
    mx=max(100.0*c[i]/tot for i in range(D6))
    print('do tap trung: '+' '.join('%s=%.1f%%'%(J.DIMS[i],100.0*c[i]/tot) for i in range(D6)))
    print('cong 40%%: %s (max=%.1f%%)'%('DAT' if mx<=40.0 else 'TRUOT',mx))
    print('')
    print('CAM VAO ENV: JANUS_W=%d JANUS_UCL=%.4f JANUS_DTHR=%s'%(
        W,u,','.join('%.4f'%v for v in TH)))

if __name__=='__main__':
    main()
