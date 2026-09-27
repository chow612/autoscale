import hashlib,math,j_replay as J

P='/home/chau/quant-engine/ing_20260820T023433Z.log'
T0=1787193273
DW=3.002
CLEAN=1787629023
PT=0.01

def main():
    src=open(__file__,'rb').read()
    print('SCRIPT md5=%s dong=%d'%(hashlib.md5(src).hexdigest(),src.count(b'\n')))
    L=J.parse(P)
    a=int(round((CLEAN-T0)/DW))
    b=L.n-1
    if a<0:a=0
    n=b-a+1
    tr=L.trig
    nt=0;runs=0;prev=0
    for t in range(a,b+1):
        v=tr[t]
        if v:
            nt+=1
            if not prev:runs+=1
        prev=v
    print('cua so sach idx=[%d..%d] n=%d'%(a,b,n))
    print('trig=%d far=%.4f%% chuoi=%d'%(nt,100.0*nt/n,runs))
    if runs==0:
        print('KHONG co chuoi trig nao -> khong tinh duoc do dinh chum');return
    L1=float(nt)/runs
    ne=n/L1
    sd0=100.0*math.sqrt(PT*(1.0-PT)/n)
    sd1=100.0*math.sqrt(PT*(1.0-PT)/ne)
    print('do dai chum trung binh=%.3f tick  n_eff=%.0f'%(L1,ne))
    print('sd nhi thuc thuan =%.4f pp   2sd=%.4f pp'%(sd0,2.0*sd0))
    print('sd co dinh chum   =%.4f pp   2sd=%.4f pp'%(sd1,2.0*sd1))
    print('TOL CHOT = %.3f pp'%(2.0*sd1))

if __name__=='__main__':
    main()
