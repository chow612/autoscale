import sys
import numpy as np
DIM=6
W=120
LAM=0.2
RIDGE=1e-6
G=LAM/(2.0-LAM)
SF=np.array([v*v for v in (0.01,1.0e5,0.5,0.05,0.05,0.05)])
def pz(t):
    a=[0.0]*DIM;b=[0.0]*DIM;l=[1.0]*DIM
    for i in range(DIM):
        v=t[i].split('=',1)[1]
        if v.endswith('*'):v=v[:-1];l[i]=0.0
        p=v.split('/')
        a[i]=float(p[0]);b[i]=float(p[1])
    return a,b,l
def pf(t):
    return [float(t[i].split('=',1)[1].split('->')[2]) for i in range(DIM)]
def ps(t):
    a=[0.0]*DIM;b=[0.0]*DIM
    for i in range(DIM):
        s,m=t[i].split('=',1)[1].split('(m=')
        a[i]=float(s);b[i]=float(m.rstrip(')'))
    return a,b
X=[];Zl=[];LV=[];E=[];MU=[];SD=[];T2=[];NN=[];PP=[]
cx=cz=clv=ce=cmu=csd=ct2=None
cn=cp=0
bad=0
with open(sys.argv[1],'r',errors='replace') as fh:
    for line in fh:
        if line.startswith('MEWMA '):
            try:
                t=line.split()
                ct2=float(t[1].split('=')[1]);cn=int(t[4].split('=')[1]);cp=int(t[5].split('=')[1])
            except Exception:ct2=None
        elif line.startswith('  FFT '):
            try:ce=pf(line.split()[1:1+DIM])
            except Exception:ce=None
        elif line.startswith('  Z '):
            try:cx,cz,clv=pz(line.split()[1:1+DIM])
            except Exception:cx=None
        elif line.startswith('  SD '):
            try:csd,cmu=ps(line.split()[1:1+DIM])
            except Exception:csd=None
        elif line.startswith('  D '):
            if ct2 is None or ce is None or cx is None or csd is None:bad+=1
            else:
                X.append(cx);Zl.append(cz);LV.append(clv);E.append(ce)
                MU.append(cmu);SD.append(csd);T2.append(ct2);NN.append(cn);PP.append(cp)
            ct2=ce=cx=csd=None
X=np.array(X);Zl=np.array(Zl);LV=np.array(LV);E=np.array(E)
MU=np.array(MU);SD=np.array(SD);T2=np.array(T2)
NN=np.array(NN);PP=np.array(PP)
N=len(T2)
print(f'ticks={N} bad={bad}')
den=np.sqrt(SD*SD+SF)
ro=[];rn=[];tro=[];trn=[];idx=[]
I=np.eye(DIM)*RIDGE*G
DN=np.full((N,DIM),np.nan)
KI=[[q for q in range(DIM) if q!=a] for a in range(DIM)]
for j in range(W-1,N):
    if NN[j]<W or PP[j]<DIM or LV[j].sum()<DIM:continue
    zv=Zl[j]
    Xw=X[j-W+1:j+1]
    Xn=(E[j-W+1:j+1]-MU[j])/den[j]
    Sn=None;t2n=float('nan')
    for Xa,ra,ta in ((Xw,ro,tro),(Xn,rn,trn)):
        Xc=Xa-Xa.mean(0)
        C=Xc.T@Xc/(W-1)
        ta.append(C.trace()/DIM)
        S=C*G+I
        try:vv=float(zv@np.linalg.solve(S,zv))
        except np.linalg.LinAlgError:vv=float('nan')
        ra.append(vv)
        Sn=S;t2n=vv
    for a in range(DIM):DN[j,a]=0.0
    if t2n==t2n and t2n>0.0:
        for a in range(DIM):
            ki=KI[a];zs=zv[ki]
            try:DN[j,a]=t2n-float(zs@np.linalg.solve(Sn[np.ix_(ki,ki)],zs))
            except np.linalg.LinAlgError:DN[j,a]=0.0
    idx.append(j)
ro=np.array(ro);rn=np.array(rn);tro=np.array(tro);trn=np.array(trn);idx=np.array(idx)
lg=T2[idx]
d=np.abs(ro-lg)
print(f'VALIDATE n={len(idx)} err_med={np.median(d):.4f} err_p99={np.percentile(d,99):.4f} err_max={d.max():.4f} rel_med={np.median(d/np.maximum(lg,1e-9)):.5f}')
for nm,r,t in (('OLD',ro,tro),('NEW',rn,trn)):
    m=(t<2.0)&np.isfinite(r)
    v=r[m]
    h=len(v)//2
    print(f'{nm} n={int(m.sum())} med={np.median(v):.4f} p95={np.percentile(v,95):.4f} p99={np.percentile(v,99):.4f} p999={np.percentile(v,99.9):.4f} max={v.max():.4f} tr_med={np.median(t[m]):.4f}')
    print(f'{nm} split p99_nua1={np.percentile(v[:h],99):.4f} p99_nua2={np.percentile(v[h:],99):.4f}')
    print(f'{nm} vuot_F(18.5652)={100.0*(v>18.5652).mean():.2f}% vuot_p99cu(46.5076)={100.0*(v>46.5076).mean():.2f}%')
sdw=np.abs(SD[idx]-SD[idx-W+1])/np.maximum(SD[idx],1e-12)
sdm=sdw.mean(1)
df=rn-ro
qs=np.percentile(sdm,[20,40,60,80])
bk=np.digitize(sdm,qs)
print('DIAG sd_troi -> delta_T2')
for k in range(5):
    m=bk==k
    print(f'  q{k+1} n={int(m.sum())} sd_troi_med={np.median(sdm[m]):.4f} dT2_med={np.median(df[m]):+.4f} dT2_p95={np.percentile(df[m],95):+.4f} dT2_max={df[m].max():+.4f}')
print(f'DIAG corr(sd_troi,dT2)={np.corrcoef(sdm,df)[0,1]:+.4f} corr(T2_old,dT2)={np.corrcoef(ro,df)[0,1]:+.4f}')
T2N=np.full(N,np.nan);TRN=np.full(N,np.nan)
T2N[idx]=rn;TRN[idx]=trn
np.savez('j_new.npz',t2=T2N,d=DN,tr=TRN)
print(f'SAVED j_new.npz n_ok={len(idx)}')
