import os,array,hashlib

TSV='/tmp/uni.tsv'
T0=1787193273
DW=3.002
C0=1787629023
C1=1787888223
B0=1787200186
BP=3622.5
U=117.1920
WIN=1800.0
NB=3

def q(a,p):
    b=sorted(a)
    if not b:return float('nan')
    return b[int(round(p*0.01*(len(b)-1)))]

def main():
    src=open(__file__,'rb').read()
    print('SCRIPT j_lag_20260828.py md5=%s dong=%d'%(hashlib.md5(src).hexdigest(),src.count(b'\n')))
    t2=array.array('d')
    with open(TSV) as fh:
        for ln in fh:t2.append(float(ln.split('\t')[1]))
    a=int(round((C0-T0)/DW));b=int(round((C1-T0)/DW))
    ex=[i for i in range(a,b) if t2[i]>U]
    print('so tick vuot nguong trong cua so sach: %d'%len(ex))
    w=int(round(WIN/DW))
    k=int((T0+a*DW-B0)/BP)
    rows=[]
    while 1:
        c=B0+k*BP
        k+=1
        m=int(round((c-T0)/DW))
        if m>=b:break
        if m<a:continue
        best=None
        for i in ex:
            d=i-m
            if d<-w or d>w:continue
            if best is None or abs(d)<abs(best):best=d
        rows.append((m,best))
    print('so moc du doan trong cua so: %d'%len(rows))
    print('')
    print('khoi  n_moc  n_khop  lech_trung_vi_tick  lech_trung_vi_s  p10     p90')
    n=b-a
    for j in range(NB):
        lo=a+j*n//NB;hi=a+(j+1)*n//NB
        sub=[d for m,d in rows if lo<=m<hi]
        ok=[d for d in sub if d is not None]
        if not ok:
            print('%-5d %-6d %-7d khong khop duoc moc nao'%(j+1,len(sub),0));continue
        md=q(ok,50)
        print('%-5d %-6d %-7d %-19.1f %-16.1f %-7.1f %.1f'%(
            j+1,len(sub),len(ok),md,md*DW,q(ok,10),q(ok,90)))
    print('')
    ok=[(m,d) for m,d in rows if d is not None]
    if len(ok)>=6:
        h=len(ok)//2
        d1=q([d for _,d in ok[:h]],50);d2=q([d for _,d in ok[h:]],50)
        print('nua dau vs nua sau: %+.1f -> %+.1f tick (%+.1f -> %+.1f s)'%(
            d1,d2,d1*DW,d2*DW))
        print('KET LUAN: %s'%('(A) MAT NA TROI PHA' if abs(d2-d1)>=w*0.25
              else '(B) KHONG TROI PHA -> khoi 3 co su kien moi'))
    else:
        print('qua it moc khop -> khong ket luan duoc')

if __name__=='__main__':
    main()
