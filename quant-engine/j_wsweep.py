import sys,importlib.util as iu,numpy as np,array
GRID=(60,120,180,240,360,480,720,960)
TARGET=6.0
LATS=(4,6,8)
COOL=100
DIM=6
NAMES=('cpu','ram','rps','err','asym','slow')
TAU={'cpu':5.5,'ram':1.3,'rps':1.2,'err':1.0,'asym':10.5,'slow':5.6}
E=[]
for ln in open(sys.argv[1],errors='replace'):
    if ln.startswith('  FFT '):
        t=ln.split()[1:1+DIM]
        try:E.append([float(u.split('=',1)[1].split('->')[2]) for u in t])
        except Exception:pass
E=np.array(E);N=len(E)
DAYS=N*3.0/86400.0
print('so tick=%d  (%.2f ngay)'%(N,DAYS))
def evrate(tg,L,days):
    idx=np.flatnonzero(tg)
    if idx.size==0:return 0.0,0
    b=np.flatnonzero(np.r_[True,np.diff(idx)>1])
    st=idx[b];ln=np.diff(np.r_[b,idx.size])
    c=0;last=-10**9
    for s,l in zip(st,ln):
        if l<L:continue
        t=s+L-1
        if t-last<COOL:continue
        last=t;c+=1
    return c/days,c
def find_u(T2,ok,L,days,target):
    v=T2[ok];lo=float(np.percentile(v,50.0));hi=float(v.max())
    for _ in range(45):
        mid=0.5*(lo+hi)
        r,_=evrate((T2>mid)&ok,L,days)
        if r>target:lo=mid
        else:hi=mid
    return 0.5*(lo+hi)
def rz(d,u):return (1.0-u)/np.sqrt(1.0+d*d*u*(1.0-u))
def ustar(d,rmin):
    lo,hi=0.0,1.0
    for _ in range(60):
        mid=0.5*(lo+hi)
        if rz(d,mid)>rmin:lo=mid
        else:hi=mid
    return 0.5*(lo+hi)
R={}
for W in GRID:
    s=iu.spec_from_file_location('ing','ingester6.py')
    m=iu.module_from_spec(s);s.loader.exec_module(m)
    m.MEWMA_WINDOW=W
    st=m.StateMEWMA();v=bytearray([1]*DIM);e=array.array('d',[0.0]*DIM)
    T2=np.empty(N);TR=np.empty(N);P=np.empty(N,dtype=np.int8);CN=np.empty(N,dtype=np.int32)
    D=np.zeros((N,DIM))
    for j in range(N):
        for i in range(DIM):e[i]=E[j,i]
        T2[j]=st.update(e,v)[0]
        TR[j]=st.tr;P[j]=st.p;CN[j]=st.count
        st.decomp(T2[j])
        for i in range(DIM):D[j,i]=st.d[i]
    ok=(P==DIM)&(CN>=W)&(TR<m.TR_MAX)&np.isfinite(T2)
    vv=T2[ok];h=len(vv)//2
    p99=float(np.percentile(vv,99))
    us=find_u(T2,ok,6,DAYS,TARGET)
    q=100.0*float((vv<=us).mean())
    base=ok&(T2<=us)
    dthr=[float(np.percentile(D[base,i],99)) for i in range(DIM)]
    ev99=[evrate((T2>p99)&ok,L,DAYS)[0] for L in LATS]
    evst=[evrate((T2>us)&ok,L,DAYS)[0] for L in LATS]
    R[W]=dict(n=int(ok.sum()),med=float(np.median(vv)),p99=p99,
              h1=float(np.percentile(vv[:h],99)),h2=float(np.percentile(vv[h:],99)),
              trm=float(np.median(TR[ok])),us=us,q=q,dthr=dthr,ev99=ev99,evst=evst,
              neff=min(W/TAU[k] for k in NAMES))
    print('  xong W=%d'%W,flush=True)
print('\n=== BANG 1: diem van hanh theo W (UCL = p99 thuc nghiem) ===')
print('%-5s %-8s %-9s %-8s %-8s %-8s %-8s %-7s %-6s %-6s %-6s'%(
    'W','n_hople','n_eff_min','med T2','p99','p99_n1','p99_n2','lech%','L4','L6','L8'))
for W in GRID:
    r=R[W]
    print('%-5d %-8d %-9.1f %-8.4f %-8.4f %-8.4f %-8.4f %-7.1f %-6.2f %-6.2f %-6.2f'%(
        W,r['n'],r['neff'],r['med'],r['p99'],r['h1'],r['h2'],
        100.0*(r['h2']-r['h1'])/r['h1'],r['ev99'][0],r['ev99'][1],r['ev99'][2]))
print('\n=== BANG 2: ghim %.1f su kien/ngay tai L6 (so sanh cong bang) ==='%TARGET)
print('%-5s %-9s %-8s %-6s %-6s %-6s  %s'%('W','UCL*','phan vi','L4','L6','L8','DTHR*'))
for W in GRID:
    r=R[W]
    print('%-5d %-9.4f %-8.3f %-6.2f %-6.2f %-6.2f  %s'%(
        W,r['us'],r['q'],r['evst'][0],r['evst'][1],r['evst'][2],
        ','.join('%.4f'%x for x in r['dthr'])))
print('\n=== BANG 3: che lap (dang dong) ===')
print('r(u)=(1-u)/sqrt(1+d^2 u(1-u));  M=T2_dinh/UCL;  yeu cau r>=M^-0.5')
print('%-4s %-9s %-7s %-7s %-8s %-9s %-9s'%('d','T2~9d^2','M','r_can','u*','W>= (D=100)','W>= (D=200)'))
for d in (3.0,4.0,6.0,8.0):
    t2p=9.0*d*d;M=t2p/50.0334
    if M<=1.0:
        print('%-4.0f %-9.1f %-7.2f %-7s %-8s %-9s %-9s'%(d,t2p,M,'-','-','KHONG BAO GIO VUOT UCL',''))
        continue
    rmin=M**-0.5;u=ustar(d,rmin)
    print('%-4.0f %-9.1f %-7.2f %-7.3f %-8.3f %-9.0f %-9.0f'%(d,t2p,M,rmin,u,100.0/u,200.0/u))
print('\n=== BANG 4: che lap thuc te tai tung W (u=D/W) ===')
print('%-5s %-14s %-14s %-14s %-14s'%('W','r(d=3,D=100)','r(d=4,D=100)','r(d=3,D=200)','r(d=4,D=200)'))
for W in GRID:
    o=[]
    for D_ in (100,200):
        for d in (3.0,4.0):
            u=min(1.0,D_/float(W))
            o.append(rz(d,u))
    print('%-5d %-14.3f %-14.3f %-14.3f %-14.3f'%(W,o[0],o[1],o[2],o[3]))
