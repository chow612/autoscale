import array,hashlib

TSV='/tmp/uni.tsv'
T0=1787193273
DW=3.002
C0=1787629023
C1=1787888223
U=117.1920
HW=150.0
BIN=15.0
P_LO=3560.0
P_HI=3700.0
P_ST=0.25
NB=3

def q(a,p):
    b=sorted(a)
    if not b:return float('nan')
    return b[int(round(p*0.01*(len(b)-1)))]

def main():
    src=open(__file__,'rb').read()
    print('SCRIPT j_fit_20260828.py md5=%s dong=%d'%(hashlib.md5(src).hexdigest(),src.count(b'\n')))
    t2=array.array('d')
    with open(TSV) as fh:
        for ln in fh:t2.append(float(ln.split('\t')[1]))
    a=int(round((C0-T0)/DW));b=int(round((C1-T0)/DW))
    ep=array.array('d')
    for i in range(a,b):
        if t2[i]>U:ep.append(T0+i*DW)
    n=len(ep)
    print('tick vuot nguong: %d   luoi chu ky [%.0f..%.0f] buoc %.2f'%(n,P_LO,P_HI,P_ST))
    bw=int(HW*2.0/BIN)
    best=(-1,0.0,0.0)
    p=P_LO
    while p<=P_HI:
        nbins=int(p/BIN)+1
        h=[0]*nbins
        for e in ep:
            r=e%p
            h[int(r/BIN)]+=1
        s=0
        for j in range(bw):s+=h[j%nbins]
        bs=s;bj=0
        for j in range(1,nbins):
            s+=h[(j+bw-1)%nbins]-h[(j-1)%nbins]
            if s>bs:bs=s;bj=j
        if bs>best[0]:best=(bs,p,(bj+bw*0.5)*BIN)
        p+=P_ST
    cov,BP,ph=best
    print('')
    print('CHU KY FIT      = %.2f s   (mo hinh cu 3622.50)'%BP)
    print('PHA (mod chu ky)= %.1f s'%ph)
    print('do bao phu      = %d/%d tick = %.1f%% nam trong +-%.0f s quanh moc'%(
        cov,n,100.0*cov/n,HW))
    print('')
    ex=[i for i in range(a,b) if t2[i]>U]
    w=int(round(1800.0/DW))
    k=int(((T0+a*DW)-ph)/BP)
    rows=[]
    while 1:
        c=ph+k*BP
        k+=1
        m=int(round((c-T0)/DW))
        if m>=b:break
        if m<a:continue
        bd=None
        for i in ex:
            d=i-m
            if d<-w or d>w:continue
            if bd is None or abs(d)<abs(bd):bd=d
        rows.append((m,bd))
    print('kiem lai bang moc MOI:')
    print('khoi  n_moc  n_khop  lech_trung_vi_s')
    nn=b-a
    md=[]
    for j in range(NB):
        lo=a+j*nn//NB;hi=a+(j+1)*nn//NB
        sub=[d for m,d in rows if lo<=m<hi]
        ok=[d for d in sub if d is not None]
        if not ok:
            print('%-5d %-6d %-7d -'%(j+1,len(sub),0));continue
        v=q(ok,50)*DW
        md.append(v)
        print('%-5d %-6d %-7d %+.1f'%(j+1,len(sub),len(ok),v))
    if len(md)==NB:
        sp=max(md)-min(md)
        print('')
        print('do tan lech giua ba khoi = %.1f s  (mo hinh cu: 534.3 s)'%sp)
        print('KET LUAN: %s'%('MOC MOI DUNG - dung cho j_uni/j_w2' if sp<120.0
              else 'VAN TROI - khong phai mot chu ky don thuan'))

if __name__=='__main__':
    main()
