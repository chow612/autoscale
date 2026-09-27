import sys,math,hashlib,j_replay as J,j_arl as A

P='/home/chau/quant-engine/ing_20260820T023433Z.log'
T0=1787193273
DW=3.002
C0=1787629023
C1=1787888223
B0=2820.0
BP=3636.00
BHW=300.0
PT=1.00
NB=3
ITER=11
D6=6
DLT=2.5
HOR=250
NON=60

def ix(ep):return int(round((ep-T0)/DW))

def sel_burst(s0,n,hw):
    m=bytearray(b'\x01'*n)
    k=int((T0+s0*DW-B0-hw)/BP)
    nex=0
    while 1:
        c=B0+k*BP
        if c-hw>T0+(s0+n)*DW:break
        a=ix(c-hw)-s0;b=ix(c+hw)-s0
        k+=1
        if b<0 or a>=n:continue
        if a<0:a=0
        if b>=n:b=n-1
        for t in range(a,b+1):
            if m[t]:m[t]=0;nex+=1
    return m,nex

def runs(t2,koff,n,sel,u):
    nu=0;nt=0;rn=0;pv=0
    for j in range(n):
        if not sel[j]:
            pv=0;continue
        nu+=1
        if t2[koff+j]>u:
            nt+=1
            if not pv:rn+=1
            pv=1
        else:pv=0
    return nu,nt,rn

def blocks(t2,koff,n,sel,u,nb):
    out=[]
    for j in range(nb):
        a=j*n//nb;b=(j+1)*n//nb
        nu=0;nt=0
        for t in range(a,b):
            if not sel[t]:continue
            nu+=1
            if t2[koff+t]>u:nt+=1
        out.append(100.0*nt/nu if nu else float('nan'))
    return out

def base(L,w,s0,s1,warm,sel):
    i0=s0-warm
    if i0<0:i0=0
    n=s1-s0;koff=s0-i0
    m=J.load(JANUS_W=w)
    R=J.replay(L,m,frz_on=0,i0=i0,i1=s1)
    v=sorted(R['t2'][koff+j] for j in range(n) if sel[j])
    return v[int(round(0.99*(len(v)-1)))],R,koff,n,i0

def calib(L,w,s0,s1,warm,sel,frz_on):
    u,R,koff,n,i0=base(L,w,s0,s1,warm,sel)
    if not frz_on:return u,R,koff,n
    lo=0.4*u;hi=2.5*u
    for _ in range(ITER):
        u=0.5*(lo+hi)
        m=J.load(JANUS_W=w,JANUS_UCL=u)
        R=J.replay(L,m,frz_on=1,i0=i0,i1=s1)
        nu,nt,rn=runs(R['t2'],koff,n,sel,u)
        if 100.0*nt/nu>PT:lo=u
        else:hi=u
    return u,R,koff,n

def delay(L,m,ucl,ons,warm):
    import array
    sh=[DLT*A.SIG_SDF[i]*A.SDW[i] for i in range(D6)]
    e=array.array('d',[0.0]*D6);vb=bytearray(b'\x01'*D6)
    res=L.res;out=[]
    for t0 in ons:
        S=m.StateMEWMA()
        s=t0-warm
        if s<0:s=0
        en=t0+HOR
        if en>L.n:en=L.n
        d=-1
        for t in range(s,en):
            q=t*D6
            if t>=t0:
                for i in range(D6):e[i]=res[q+i]+sh[i]
            else:
                for i in range(D6):e[i]=res[q+i]
            v,u,g=S.update(e,vb,0)
            if t>=t0 and v>ucl:
                d=t-t0;break
        out.append(d)
    return out

def main():
    src=open(__file__,'rb').read()
    print('SCRIPT j_w2_20260828.py md5=%s dong=%d  ban truoc: j_w.py'%(
        hashlib.md5(src).hexdigest(),src.count(b'\n')))
    a=sys.argv[1:]
    grid=[int(x) for x in a[0].split(',')] if a else [120,180,240,300,360]
    hw=float(a[1]) if len(a)>1 else BHW
    s0=ix(C0);s1=ix(C1)
    L=J.parse(P)
    print(J.ASSUM)
    print('cua so sach [%d..%d) n=%d  L.n=%d'%(s0,s1,s1-s0,L.n))
    if L.n<s1:
        print('CHUA DU: thieu %d tick = %.1f phut. DUNG.'%(s1-L.n,(s1-L.n)*DW/60.0))
        return
    n=s1-s0
    selA=bytearray(b'\x01'*n)
    selB,nex=sel_burst(s0,n,hw)
    print('loai 3622s: hw=%.0fs  tick loai=%d (%.2f%%)'%(hw,nex,100.0*nex/n))
    print('tieu chi chot truoc: TOL(W)=200*sqrt(.01*.99/((n_dung/%d)/L1(W)))'%NB)
    print('')
    print('W    nhanh  UCL       FAR%    kh1    kh2    kh3    max|lech| L1     TOL    ket    dtrig')
    R1={}
    for w in grid:
        warm=w+180
        for nm,fz,sl in (('FRZ0',0,selA),('FRZ0x',0,selB),('FRZ1',1,selA),('FRZ1x',1,selB)):
            u,R,koff,nn=calib(L,w,s0,s1,warm,sl,fz)
            nu,nt,rn=runs(R['t2'],koff,nn,sl,u)
            far=100.0*nt/nu
            bl=blocks(R['t2'],koff,nn,sl,u,NB)
            l1=float(nt)/rn if rn else float('nan')
            tol=200.0*math.sqrt(0.01*0.99/((nu/float(NB))/l1)) if rn else float('nan')
            mx=max(abs(x-PT) for x in bl)
            ok='DAT' if mx<=tol else 'TRUOT'
            dt=''
            if nm=='FRZ0x':
                m=J.load(JANUS_W=w)
                lo=warm+50;hi=L.n-HOR
                st=(hi-lo)//NON
                ons=[lo+i*st for i in range(NON)]
                dd=delay(L,m,u,ons,warm)
                gd=[x for x in dd if x>=0]
                dt='%.1f(kd%.0f%%)'%(J.q(gd,50) if gd else float('nan'),
                                     100.0*(len(dd)-len(gd))/len(dd))
                R1[w]=(mx,tol,u,J.q(gd,50) if gd else 1e9,R,koff,nn)
            print('%-4d %-6s %-9.4f %-7.4f %-6.3f %-6.3f %-6.3f %-9.4f %-6.3f %-6.3f %-6s %s'%(
                w,nm,u,far,bl[0],bl[1],bl[2],mx,l1,tol,ok,dt))
            sys.stdout.flush()
    print('')
    pa=[w for w in grid if R1[w][0]<=R1[w][1]]
    print('nhanh quyet dinh: FRZ0x (FRZ=0, loai 3622s)')
    print('thoa (a) max|lech|<=TOL: %s'%(pa or 'KHONG CO'))
    if not pa:
        print('=> KHONG W nao thoa. Noi luoi va GHI RO da noi.');return
    bd=min(R1[w][3] for w in pa)
    sel=[w for w in pa if R1[w][3]<=bd+2.0]
    W=sel[0]
    print('thoa (b) dtrig<=%.1f+2: %s'%(bd,sel))
    print('=> W = %d   UCL = %.4f'%(W,R1[W][2]))
    mx,tol,u,dd,R,koff,nn=R1[W]
    d=R['d'];selB2=selB
    TH=[]
    for i in range(D6):
        v=sorted(d[(koff+j)*D6+i] for j in range(nn) if selB2[j])
        TH.append(v[int(round(0.99*(len(v)-1)))])
    print('=> DTHR = %s'%(','.join('%.4f'%x for x in TH)))
    c=[0]*D6;tot=0
    for j in range(nn):
        if not selB2[j]:continue
        b=(koff+j)*D6;ws=0.0;wi=-1
        for i in range(D6):
            if d[b+i]>ws*TH[i]:ws=d[b+i]/TH[i];wi=i
        if wi>=0:c[wi]+=1;tot+=1
    print('do tap trung argmax(d_i/DTHR_i): %s'%(' '.join(
        '%s=%.1f%%'%(J.DIMS[i],100.0*c[i]/tot) for i in range(D6))))
    mxc=max(100.0*c[i]/tot for i in range(D6))
    print('cong 40%%: %s (max=%.1f%%)'%('DAT' if mxc<=40.0 else 'TRUOT',mxc))

if __name__=='__main__':
    main()
