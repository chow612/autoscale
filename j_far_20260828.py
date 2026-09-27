import hashlib,j_replay as J

P='/home/chau/quant-engine/ing_20260820T023433Z.log'
T0=1787193273
DW=3.002
C0=1787629023
C1=1787888223
B0=2820.0
BP=3636.00
HW=300.0
W=240
D6=6
FRZ_REL=8
FRZ_MAX=400
COOL=100
ITER=10
FG=(1.0,0.5,0.2,0.1,0.05)
LG=(6,8,10)

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

def rate(t2,koff,n,sel,u):
    nu=0;nt=0
    for j in range(n):
        if not sel[j]:continue
        nu+=1
        if t2[koff+j]>u:nt+=1
    return 100.0*nt/nu

def main():
    src=open(__file__,'rb').read()
    print('SCRIPT j_far_20260828.py md5=%s dong=%d'%(hashlib.md5(src).hexdigest(),src.count(b'\n')))
    L=J.parse(P)
    s0=int(round((C0-T0)/DW));s1=int(round((C1-T0)/DW))
    if L.n<s1:
        print('CHUA DU');return
    n=s1-s0
    sel=mask(s0,s1)
    warm=W+180
    i0=s0-warm;koff=s0-i0
    days=n*DW/86400.0
    print('W=%d FRZ=1 cool=%d  cua so sach %.2f ngay'%(W,COOL,days))
    print('luat: FAR LON NHAT co A=0 va B>0; trong do latch nho nhat')
    print('')
    print('FAR%   UCL       latch  nfire  A_ngoai  B_trong  A_moi_ngay')
    good=[]
    for ft in FG:
        m=J.load(JANUS_W=W)
        R=J.replay(L,m,frz_on=0,i0=i0,i1=s1)
        u=q([R['t2'][koff+j] for j in range(n) if sel[j]],100.0-ft)
        lo=0.4*u;hi=2.5*u
        for _ in range(ITER):
            u=0.5*(lo+hi)
            m=J.load(JANUS_W=W,JANUS_UCL=u)
            R=J.replay(L,m,frz_on=1,i0=i0,i1=s1)
            if rate(R['t2'],koff,n,sel,u)>ft:lo=u
            else:hi=u
        t2=R['t2'];tr=R['tr'];d=R['d']
        g=[j for j in range(n) if sel[j] and t2[koff+j]<=u and tr[koff+j]<m.TR_MAX]
        TH=[q([d[(koff+j)*D6+i] for j in g],99.0) for i in range(D6)]
        for l in LG:
            R2=J.replay(L,m,dthr=TH,frz_on=1,frz_rel=FRZ_REL,frz_max=FRZ_MAX,
                        latch=l,cool=COOL,i0=i0,i1=s1)
            dc=R2['dec']
            a=0;b=0;pv=0
            for j in range(n):
                v=dc[koff+j]
                if v and not pv:
                    if sel[j]:a+=1
                    else:b+=1
                pv=v
            print('%-6.2f %-9.4f %-6d %-6d %-8d %-8d %.2f'%(
                ft,u,l,R2['nfire'],a,b,a/days))
            if a==0 and b>0:good.append((ft,l,u,TH))
        print('       DTHR = '+','.join('%.4f'%v for v in TH))
    print('')
    if good:
        ft,l,u,TH=max(good,key=lambda r:(r[0],-r[1]))
        print('=> FAR=%.2f%%  DEC_LATCH=%d  UCL=%.4f'%(ft,l,u))
        print('   JANUS_W=%d JANUS_UCL=%.4f JANUS_LATCH=%d JANUS_DTHR=%s'%(
            W,u,l,','.join('%.4f'%v for v in TH)))
    else:
        print('=> KHONG o nao dat A=0 va B>0.')
        print('   KET LUAN: tang quyet dinh khong the sach tren du lieu nay o bat ky')
        print('   cap (FAR, LATCH) nao trong luoi. Day la KET QUA, khong phai loi hieu chuan.')

if __name__=='__main__':
    main()
