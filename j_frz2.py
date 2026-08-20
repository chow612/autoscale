import sys,importlib.util as iu,numpy as np,array
DIM=6;W=220;FMAX=400
GRID=(12,14,16,18,20,22,24,28,32)
E=[]
for ln in open(sys.argv[1],errors='replace'):
    if ln.startswith('  FFT '):
        try:E.append([float(u.split('=',1)[1].split('->')[2]) for u in ln.split()[1:1+DIM]])
        except Exception:pass
E=np.array(E);N=len(E);DAYS=N*3.0/86400.0
s=iu.spec_from_file_location('ing','ingester6.py')
m=iu.module_from_spec(s);s.loader.exec_module(m);m.MEWMA_WINDOW=W
def run(ucl,frel):
    st=m.StateMEWMA();v=bytearray([1]*DIM);e=array.array('d',[0.0]*DIM)
    T2=np.empty(N);US=np.zeros(N,dtype=np.int8);CN=np.empty(N,dtype=np.int32)
    on=0;calm=0;age=0;reb=0
    for j in range(N):
        US[j]=on
        for i in range(DIM):e[i]=E[j,i]
        T2[j]=st.update(e,v,on)[0];CN[j]=st.count
        tg=T2[j]>ucl
        if on:
            age+=1
            calm=calm+1 if not tg else 0
            if calm>=frel:on=0;calm=0;age=0
            elif age>=FMAX:reb+=1;on=0;calm=0;age=0
        elif tg and st.count>=W:on=1;calm=0;age=1
    return T2,US,CN,reb
def rate(T2,ok,u):return float(((T2>u)&ok).mean())
T2,US,CN,_=run(1e18,8)
ok=CN>=W
u0=float(np.percentile(T2[ok],99))
print('khong bang: p99=%.4f  ti_le_trig=%.4f%%\n'%(u0,100.0*rate(T2,ok,u0)))
print('=== dinh vi bien bat on dinh (chay tai UCL cua ban khong bang) ===')
print('%-8s %-10s %-10s %-11s %-9s %-8s'%('FRZ_REL','p99_moi','ti_le/p99_cu','%tick_bang','REB/ngay','dot_max'))
for fr in GRID:
    T2,US,CN,reb=run(u0,fr)
    ok=CN>=W
    p=float(np.percentile(T2[ok],99))
    i=np.flatnonzero(US)
    mx=0
    if i.size:
        b=np.flatnonzero(np.r_[True,np.diff(i)>1]);mx=int(np.diff(np.r_[b,i.size]).max())
    flag=' <== BAT ON DINH' if (p>2.0*u0 or US[ok].mean()>0.5) else ''
    print('%-8d %-10.2f %-10.2f %-11.2f %-9.2f %-8d%s'%(fr,p,p/u0,100.0*US[ok].mean(),reb/DAYS,mx,flag))
print('\n=== chot UCL bang chia doi tren ti le trig = 1%% ===')
for fr in (8,12):
    lo,hi=u0*0.6,u0*4.0
    for _ in range(16):
        mid=0.5*(lo+hi)
        T2,US,CN,reb=run(mid,fr)
        ok=CN>=W
        if rate(T2,ok,mid)>0.01:lo=mid
        else:hi=mid
    u=0.5*(lo+hi)
    T2,US,CN,reb=run(u,fr)
    ok=CN>=W
    v=T2[ok];h=len(v)//2
    print('FRZ_REL=%-3d UCL=%-9.4f ti_le_trig=%.4f%%  %%bang=%.2f  REB/ngay=%.2f  p99_n1=%.2f p99_n2=%.2f lech=%.1f%%  d_min=%.3f'%(
        fr,u,100.0*rate(T2,ok,u),100.0*US[ok].mean(),reb/DAYS,
        float(np.percentile(v[:h],99)),float(np.percentile(v[h:],99)),
        100.0*(np.percentile(v[h:],99)-np.percentile(v[:h],99))/np.percentile(v[:h],99),
        (u/9.0)**0.5))
