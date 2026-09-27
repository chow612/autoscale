import sys,importlib.util as iu,numpy as np,array
DIM=6
D_=200
FRELS=(6,8,12,16,24)
FMAX=400
WSW=220
WWIN=(120,220,360,480,720)
NSTART=12
E=[]
for ln in open(sys.argv[1],errors='replace'):
    if ln.startswith('  FFT '):
        try:E.append([float(u.split('=',1)[1].split('->')[2]) for u in ln.split()[1:1+DIM]])
        except Exception:pass
E=np.array(E);N=len(E)
DAYS=N*3.0/86400.0
print('so tick=%d (%.2f ngay)\n'%(N,DAYS))
def load(W):
    s=iu.spec_from_file_location('ing','ingester6.py')
    m=iu.module_from_spec(s);s.loader.exec_module(m)
    m.MEWMA_WINDOW=W
    return m
def replay(m,W,ucl,frel,fmax,Eb=None,step=None):
    st=m.StateMEWMA();v=bytearray([1]*DIM);e=array.array('d',[0.0]*DIM)
    Es=E if Eb is None else Eb
    n=len(Es)
    T2=np.empty(n);US=np.zeros(n,dtype=np.int8);CN=np.empty(n,dtype=np.int32)
    ZS=np.zeros(n)
    on=0;calm=0;age=0;reb=0
    sd0=None
    for j in range(n):
        used=on
        row=Es[j]
        if step is not None:
            di,sig,a0,ln_=step
            if a0<=j<a0+ln_:
                if sd0 is None:sd0=float(st.sd[di])
                row=row.copy();row[di]+=sig*sd0
        for i in range(DIM):e[i]=row[i]
        t2=st.update(e,v,used)[0]
        T2[j]=t2;US[j]=used;CN[j]=st.count
        if step is not None:ZS[j]=float(st.z[step[0]])
        trig=t2>ucl
        if on:
            age+=1
            calm=calm+1 if not trig else 0
            if calm>=frel:on=0;calm=0;age=0
            elif age>=fmax:reb+=1;on=0;calm=0;age=0
        elif trig and st.count>=W:
            on=1;calm=0;age=1
    return T2,US,CN,reb,ZS
def epis(u):
    i=np.flatnonzero(u)
    if i.size==0:return 0,0.0,0.0
    b=np.flatnonzero(np.r_[True,np.diff(i)>1])
    L=np.diff(np.r_[b,i.size])
    return len(L),float(np.median(L)),float(np.percentile(L,95))
print('=== 1: sweep FRZ_REL tai W=%d (diem bat dong cua UCL) ==='%WSW)
m=load(WSW)
T2,US,CN,_,_=replay(m,WSW,1e18,8,FMAX)
ok=CN>=WSW
u0=float(np.percentile(T2[ok],99))
print('khong bang: p99=%.4f'%u0)
print('%-6s %-10s %-10s %-9s %-8s %-8s %-8s %-9s'%('FRZ_REL','UCL_hoi_tu','quy_dao','%tick_bang','dot/ngay','len_med','len_p95','REBASE/ngay'))
for fr in FRELS:
    u=u0;tr=[]
    for it in range(4):
        T2,US,CN,reb,_=replay(m,WSW,u,fr,FMAX)
        ok=CN>=WSW
        un=float(np.percentile(T2[ok],99))
        tr.append(un);u=un
    ne,lm,l95=epis(US)
    print('%-6d %-10.4f %-10s %-9.2f %-8.2f %-8.1f %-8.1f %-9.2f'%(
        fr,u,'/'.join('%.0f'%x for x in tr),100.0*US[ok].mean(),ne/DAYS,lm,l95,reb/DAYS))
print('\n=== 2: cua so duy tri DO DUOC theo W (delta=4 vao slow, khong bang) ===')
print('%-5s %-9s %-11s %-11s %-11s'%('W','UCL_p99','cua_so_med','cua_so_p10','cua_so_p90'))
rng=np.arange(NSTART)
for W in WWIN:
    mm=load(W)
    T2,US,CN,_,_=replay(mm,W,1e18,8,FMAX)
    ok=CN>=W
    u=float(np.percentile(T2[ok],99))
    wins=[]
    for k in rng:
        a0=W+50
        off=int(20000+k*7000)
        if off+a0+D_+10>N:break
        Eb=E[off:off+a0+D_+10]
        t2b,_,cnb,_,_=replay(mm,W,1e18,8,FMAX,Eb=Eb,step=(5,4.0,a0,D_))
        seg=t2b[a0:a0+D_]
        above=seg>u
        if not above.any():wins.append(0);continue
        i=np.flatnonzero(above)
        b=np.flatnonzero(np.r_[True,np.diff(i)>1])
        L=np.diff(np.r_[b,i.size])
        wins.append(int(L.max()))
    wins=np.array(wins)
    print('%-5d %-9.4f %-11.0f %-11.0f %-11.0f'%(W,u,np.median(wins),np.percentile(wins,10),np.percentile(wins,90)))
print('(cua so = so tick lien tiep dai nhat co T2>UCL trong 200 tick soc)')
