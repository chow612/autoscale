import sys,importlib.util as iu,numpy as np,array
GRID=(120,150,180,200,220,240,260,280,300,360,480)
DIM=6;K=80
NAMES=('cpu','ram','rps','err','asym','slow')
E=[];RPS=[]
for ln in open(sys.argv[1],errors='replace'):
    if ln.startswith('  FFT '):
        t=ln.split()[1:1+DIM]
        try:E.append([float(u.split('=',1)[1].split('->')[2]) for u in t])
        except Exception:pass
    elif ln.startswith('STATE '):
        try:RPS.append(float(ln.split()[3].split('=')[1]))
        except Exception:pass
E=np.array(E);N=len(E)
print('so tick=%d  rps=%d\n'%(N,len(RPS)))
def tau(y):
    y=y-y.mean();v=float((y*y).mean())
    if v<=0:return 1.0
    ac=np.array([float((y[:-k]*y[k:]).mean())/v for k in range(1,K+1)])
    z=np.flatnonzero(ac<=0.0)
    kc=int(z[0]) if z.size else K
    return 1.0+2.0*float(ac[:kc].sum())
A=[];B=[];Cb={}
for W in GRID:
    s=iu.spec_from_file_location('ing','ingester6.py')
    m=iu.module_from_spec(s);s.loader.exec_module(m)
    m.MEWMA_WINDOW=W
    st=m.StateMEWMA();v=bytearray([1]*DIM);e=array.array('d',[0.0]*DIM)
    X=np.empty((N,DIM));CN=np.empty(N,dtype=np.int32)
    for j in range(N):
        for i in range(DIM):e[i]=E[j,i]
        st.update(e,v);CN[j]=st.count
        for i in range(DIM):X[j,i]=st.x[i]
    Xg=X[CN>=W]
    tv=[tau(Xg[:,i]) for i in range(DIM)]
    ne=min(W/t for t in tv)
    A.append((W,tv,ne))
    nb=len(Xg)//W
    lmin=[];lmax=[];dt=[]
    for b in range(nb):
        Y=Xg[b*W:(b+1)*W]
        Y=Y-Y.mean(0)
        C=Y.T@Y/(W-1)
        w=np.linalg.eigvalsh(C)
        lmin.append(float(w[0]));lmax.append(float(w[-1]))
        dt.append(float(np.linalg.det(C)))
    lmin=np.array(lmin);lmax=np.array(lmax);dt=np.array(dt)
    r=ne/DIM
    mp=(1.0-np.sqrt(1.0/r))**2 if r>1.0 else 0.0
    B.append((W,nb,float(np.median(lmin)),float(lmin.std()/max(lmin.mean(),1e-12)),
              float(np.median(lmax)),float(np.median(lmax/np.maximum(lmin,1e-12))),
              float(np.median(dt)),mp,r))
    q=len(Xg)//4
    Cb[W]=[[tau(Xg[k*q:(k+1)*q,i]) for i in range(DIM)] for k in range(4)]
    print('  xong W=%d (nb=%d)'%(W,nb),flush=True)
print('\n=== A: tau(x) luoi min — tim cho chuyen pha cua rps ===')
print('%-5s '%'W'+' '.join('%-8s'%s for s in NAMES)+' n_eff  n_eff/p')
for W,tv,ne in A:
    print('%-5d '%W+' '.join('%-8.2f'%t for t in tv)+' %-6.1f %.2f'%(ne,ne/DIM))
print('\n=== B: phan bo lay mau cua C-hat tren cua so roi nhau ===')
print('%-5s %-5s %-9s %-9s %-9s %-9s %-10s %-9s'%('W','nblk','lmin_med','lmin_rSD','lmax_med','cond_med','det_med','MP_lmin'))
for W,nb,lm,rsd,lx,cd,de,mp,r in B:
    print('%-5d %-5d %-9.4f %-9.3f %-9.4f %-9.1f %-10.3e %-9.4f'%(W,nb,lm,rsd,lx,cd,de,mp))
print('\n=== C: do bat dinh cua tau — 4 khoi ngay roi nhau, W=240 ===')
print('%-6s '%'khoi'+' '.join('%-8s'%s for s in NAMES))
for k,row in enumerate(Cb[240]):
    print('%-6d '%(k+1)+' '.join('%-8.2f'%t for t in row))
mn=np.array(Cb[240])
print('%-6s '%'CV%'+' '.join('%-8.1f'%(100.0*mn[:,i].std()/mn[:,i].mean()) for i in range(DIM)))
print('\n=== D: pho cua rps o phan giai 3s (vung mu cua FFT production) ===')
y=np.array(RPS,dtype=float)
n=len(y)-(len(y)%2)
y=y[:n]
med=float(np.median(y));mad=float(np.median(np.abs(y-med)))
lim=8.0*1.4826*mad
nc=int((np.abs(y-med)>lim).sum())
y=np.clip(y,med-lim,med+lim)
tm=np.arange(n,dtype=float);tm-=tm.mean()
yc=y-med
sl=float((tm*yc).sum()/(tm*tm).sum())
yc=yc-sl*tm;yc=yc-yc.mean()
Y=np.abs(np.fft.rfft(yc))
f=np.arange(len(Y))/(n*3.0)
per=np.empty(len(Y));per[0]=np.inf;per[1:]=1.0/f[1:]
sel=(per>=30.0)&(per<=7200.0)
idx=np.flatnonzero(sel)
o=idx[np.argsort(Y[idx])[::-1][:15]]
print('median=%.2f mad=%.2f clip=%d  trend=%.4g/tick'%(med,mad,nc,sl))
print('%-12s %-12s %-12s'%('chu_ky','phut','bien_do'))
for i in sorted(o,key=lambda z:-Y[z]):
    print('%-12.1fs %-12.2f %-12.2f'%(per[i],per[i]/60.0,2.0*Y[i]/n))
