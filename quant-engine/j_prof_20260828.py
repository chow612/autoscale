import os,array,hashlib

P='/home/chau/quant-engine/ing_20260820T023433Z.log'
TSV='/tmp/uni.tsv'
T0=1787193273
DW=3.002
C0=1787629023
C1=1787888223
K=2.4980
U=117.1920

def load():
    um=array.array('d');t2=array.array('d')
    if os.path.exists(TSV) and os.path.getsize(TSV)>0:
        with open(TSV) as fh:
            for ln in fh:
                p=ln.split('\t')
                um.append(float(p[0]));t2.append(float(p[1]))
        return um,t2
    with open(P,'r',errors='replace') as fh, open(TSV,'w') as out:
        for ln in fh:
            if not ln.startswith('  UNI '):continue
            u=0.0;v=0.0
            for tk in ln.split():
                if tk.startswith('z='):u=float(tk[2:])
                elif tk.startswith('t2='):v=float(tk[3:])
            um.append(u);t2.append(v)
            out.write('%.3f\t%.3f\n'%(u,v))
    return um,t2

def main():
    src=open(__file__,'rb').read()
    print('SCRIPT j_prof_20260828.py md5=%s dong=%d'%(hashlib.md5(src).hexdigest(),src.count(b'\n')))
    um,t2=load()
    a=int(round((C0-T0)/DW));b=int(round((C1-T0)/DW))
    if len(um)<b:
        print('CHUA DU');return
    print('K*=%.4f U*=%.4f  cua so [%d..%d)'%(K,U,a,b))
    print('gio_utc            n     UNI%%     T2%%')
    t=a
    while t<b:
        e=t+1200
        if e>b:e=b
        nu=0;nt=0
        for i in range(t,e):
            if um[i]>K:nu+=1
            if t2[i]>U:nt+=1
        n=e-t
        ep=T0+t*DW
        h=int(ep//3600*3600)
        d=(h-1787193600)//3600
        print('%s+%03dh  %5d  %7.3f  %7.3f'%(
            '2026-08-20T02Z',d,n,100.0*nu/n,100.0*nt/n))
        t=e
    print('')
    print('20 phut cuoi cua tung gio co T2>2%: xem cot tren, moi dong = 1 gio')

if __name__=='__main__':
    main()
