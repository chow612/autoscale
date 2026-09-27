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
UCL=58.7343
TH=(24.8372,6.6410,11.5795,7.0127,26.4767,21.2709)
FRZ_REL=8
FRZ_MAX=400
LG=(2,4,6,8,10,12,16)
CG=(0,100)

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
    print('SCRIPT j_latch_20260828.py md5=%s dong=%d'%(hashlib.md5(src).hexdigest(),src.count(b'\n')))
    L=J.parse(P)
    s0=int(round((C0-T0)/DW));s1=int(round((C1-T0)/DW))
    if L.n<s1:
        print('CHUA DU');return
    n=s1-s0
    sel=mask(s0,s1)
    warm=W+180
    i0=s0-warm;koff=s0-i0
    m=J.load(JANUS_W=W,JANUS_UCL=UCL)
    print('W=%d UCL=%.4f FRZ=1 rel=%d max=%d  cua so sach %d tick = %.2f ngay'%(
        W,UCL,FRZ_REL,FRZ_MAX,n,n*DW/86400.0))
    print('tieu chi chot truoc: A = su kien quyet dinh NGOAI cua so 3622s, muc tieu 0')
    print('')
    print('latch cool  nfire  nev_tong  A_ngoai  B_trong  A_moi_ngay')
    res={}
    for c in CG:
        for l in LG:
            R=J.replay(L,m,dthr=TH,frz_on=1,frz_rel=FRZ_REL,frz_max=FRZ_MAX,
                       latch=l,cool=c,i0=i0,i1=s1)
            dc=R['dec']
            a=0;b=0;pv=0
            for j in range(n):
                v=dc[koff+j]
                if v and not pv:
                    if sel[j]:a+=1
                    else:b+=1
                pv=v
            print('%-5d %-5d %-6d %-9d %-8d %-8d %.2f'%(
                l,c,R['nfire'],a+b,a,b,a/(n*DW/86400.0)))
            if c==100:res[l]=a
    print('')
    ok=[l for l in LG if res.get(l,1)==0]
    if ok:
        print('=> DEC_LATCH = %d   (A = 0, gia tri nho nhat trong luoi)'%ok[0])
    else:
        bl=min(LG,key=lambda l:res[l])
        print('=> KHONG latch nao cho A = 0. Nho nhat: latch=%d voi A=%d'%(bl,res[bl]))
        print('   GHI RO trong bao cao: tieu chi da noi, va noi cai gi')

if __name__=='__main__':
    main()
