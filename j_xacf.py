import sys,importlib.util as iu,numpy as np,array
GRID=(60,120,180,240,360,480,720,960)
DIM=6;K=80
NAMES=('cpu','ram','rps','err','asym','slow')
E=[]
for ln in open(sys.argv[1],errors='replace'):
    if ln.startswith('  FFT '):
        t=ln.split()[1:1+DIM]
        try:E.append([float(u.split('=',1)[1].split('->')[2]) for u in t])
        except Exception:pass
E=np.array(E);N=len(E)
print('so tick=%d\n'%N)
def tau_of(y):
    y=y-y.mean()
    v=float((y*y).mean())
    if v<=0:return 1.0,0.0
    ac=np.array([float((y[:-k]*y[k:]).mean())/v for k in range(1,K+1)])
    z=np.flatnonzero(ac<=0.0)
    kc=int(z[0]) if z.size else K
    return 1.0+2.0*float(ac[:kc].sum()),float(ac[0])
rows=[]
for W in GRID:
    s=iu.spec_from_file_location('ing','ingester6.py')
    m=iu.module_from_spec(s);s.loader.exec_module(m)
    m.MEWMA_WINDOW=W
    st=m.StateMEWMA();v=bytearray([1]*DIM);e=array.array('d',[0.0]*DIM)
    X=np.empty((N,DIM));Z=np.empty((N,DIM));T2=np.empty(N);CN=np.empty(N,dtype=np.int32)
    for j in range(N):
        for i in range(DIM):e[i]=E[j,i]
        T2[j]=st.update(e,v)[0]
        CN[j]=st.count
        for i in range(DIM):X[j,i]=st.x[i];Z[j,i]=st.z[i]
    ok=CN>=W
    tx=[];rz1=[]
    for i in range(DIM):
        t,_=tau_of(X[ok,i]);tx.append(t)
        _,r1=tau_of(Z[ok,i]);rz1.append(r1)
    tt2,rt2=tau_of(T2[ok])
    rows.append((W,tx,rz1,tt2,rt2))
    print('  xong W=%d'%W,flush=True)
print('\n=== BANG A: tau_int cua x theo tung W (tick) ===')
print('%-5s '%'W'+' '.join('%-8s'%s for s in NAMES)+' n_eff_min  n_eff/p')
for W,tx,rz1,tt2,rt2 in rows:
    ne=min(W/t for t in tx)
    print('%-5d '%W+' '.join('%-8.2f'%t for t in tx)+' %-10.1f %.2f'%(ne,ne/DIM))
print('\n=== BANG B: rho1 cua z (thiet ke = 0.8) ===')
print('%-5s '%'W'+' '.join('%-8s'%s for s in NAMES))
for W,tx,rz1,tt2,rt2 in rows:
    print('%-5d '%W+' '.join('%-8.3f'%r for r in rz1))
print('\n=== BANG C: tu tuong quan cua T2 ===')
print('%-5s %-10s %-10s'%('W','rho1(T2)','tau(T2)'))
for W,tx,rz1,tt2,rt2 in rows:
    print('%-5d %-10.3f %-10.2f'%(W,rt2,tt2))
