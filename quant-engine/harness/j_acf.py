import sys,numpy as np
DIM=6;NAMES=('cpu','ram','rps','err','asym','slow')
X=[]
for ln in open(sys.argv[1],errors='replace'):
    if ln.startswith('  Z '):
        t=ln.split()[1:1+DIM]
        try:X.append([float(u.split('=',1)[1].split('/')[0].rstrip('*')) for u in t])
        except Exception:pass
X=np.array(X)
print('so tick =',len(X))
K=60
print('\nchieu   rho1    rho5    rho10   rho20   tau_int   n_eff(W=120)  (240)  (480)  (960)')
for i in range(DIM):
    y=X[:,i].astype(float);y=y-y.mean()
    v=float((y*y).mean())
    if v<=0:
        print('%-6s  (sd=0)'%NAMES[i]);continue
    ac=np.array([float((y[:-k]*y[k:]).mean())/v for k in range(1,K+1)])
    z=np.flatnonzero(ac<=0.0)
    kc=int(z[0]) if z.size else K
    tau=1.0+2.0*float(ac[:kc].sum())
    row=[]
    for W in (120,240,480,960):
        kk=min(kc,W-1)
        s=1.0+2.0*float(((1.0-np.arange(1,kk+1)/W)*ac[:kk]).sum())
        row.append(W/max(s,1e-9))
    print('%-6s  %6.3f  %6.3f  %6.3f  %6.3f  %8.1f   %8.1f %6.1f %6.1f %6.1f'%(
        NAMES[i],ac[0],ac[4],ac[9],ac[19],tau,row[0],row[1],row[2],row[3]))
print('\nC la 6x6 = 21 tham so tu do. Quy tac ngon tay: n_eff >= 5p = 30 la toi thieu, >= 10p = 60 la on dinh.')
