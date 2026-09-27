import sys
from array import array
import numpy as np

DIMS=('cpu','ram','rps','err','asym','slow')
DEPS=('userservice','ledgerwriter','balancereader','transactionhistory','frontend','contacts')
DSET=set(DEPS)
DTHR=np.array([12.4348,6.7844,8.0606,6.2051,13.2100,10.6277])
DI={d:i for i,d in enumerate(DIMS)};PI={d:i for i,d in enumerate(DEPS)}
W,TRMAX,UCL0,COOL=120,2.0,18.5652,100

src=sys.argv[1]
t2=array('d');trg=array('b');nv=array('i');pv=array('b');trv=array('d');nlv=array('b')
dc=[array('d') for _ in range(6)];st=[array('d') for _ in range(6)]
zz=[array('d') for _ in range(36)]
lad=array('b');lap=array('b')
ti=-1
for ln in open(src,'r',errors='replace'):
    if ln.startswith('MEWMA '):
        w=ln.split()
        t2.append(float(w[1][3:]));trg.append(1 if w[3][5:]=='True' else 0)
        nv.append(int(w[4][2:]));pv.append(int(w[5][2:]));trv.append(float(w[6][3:]))
        nlv.append(-1);lad.append(-1);lap.append(-2)
        for k in range(6):dc[k].append(float('nan'));st[k].append(float('nan'))
        for k in range(36):zz[k].append(0.0)
        ti+=1
    elif ti<0:continue
    elif ln.startswith('STATE '):
        w=ln.split()
        for k in range(6):st[k][ti]=float(w[k+1].split('=')[1].split('(')[0])
    elif ln.startswith('  D '):
        w=ln.split()
        for k in range(6):dc[k][ti]=float(w[k+1].split('=')[1])
        nlv[ti]=int(w[7][3:])
    elif ln.startswith('  ATTR '):
        w=ln.split()
        lad[ti]=DI[w[1][4:]];lap[ti]=PI.get(w[5][4:],-1)
    else:
        w=ln.split()
        if len(w)==8 and w[0] in DSET and w[7].startswith('z='):
            j=PI[w[0]];v=w[7][2:].split('/')
            for k in range(6):zz[j*6+k][ti]=float(v[k])

n=ti+1
T2=np.frombuffer(t2,dtype=np.float64);TR=np.frombuffer(trg,dtype=np.int8)
N=np.frombuffer(nv,dtype=np.int32);P=np.frombuffer(pv,dtype=np.int8)
TRC=np.frombuffer(trv,dtype=np.float64);NL=np.frombuffer(nlv,dtype=np.int8)
D=np.vstack([np.frombuffer(c,dtype=np.float64) for c in dc]).T
S=np.vstack([np.frombuffer(c,dtype=np.float64) for c in st]).T
Z=np.vstack([np.frombuffer(c,dtype=np.float64) for c in zz]).T.reshape(n,6,6)
LD=np.frombuffer(lad,dtype=np.int8);LP=np.frombuffer(lap,dtype=np.int8)

print('REPEAT_RATE (tick lien tiep trung khit bit)')
for k in range(6):
    v=S[:,k];g=np.isfinite(v[1:])&np.isfinite(v[:-1])
    print('  %-5s %.4f'%(DIMS[k],float((v[1:][g]==v[:-1][g]).mean())))

ok=(P==6)&(N>=W)&(TRC<TRMAX)&(NL>=3)&np.isfinite(D).all(1)
Dm=np.where(ok[:,None],D,-1.0)
dim=Dm.argmax(1)
sig=ok&(Dm[np.arange(n),dim]>DTHR[dim])
za=np.abs(Z[np.arange(n),:,dim])
dep=za.argmax(1)
hasdep=za[np.arange(n),dep]>0
key=np.where(sig&hasdep,dim*6+dep,-1)
key_u=np.where(sig&~hasdep,-2,key)

v=TR==1
mm_dim=int((LD[v]!=dim[v]).sum())
mm_dep=int((LP[v]!=np.where(hasdep[v],dep[v],-1)).sum())
print('\nVALIDATE trig_ticks=%d mismatch_dim=%d mismatch_dep=%d'%(int(v.sum()),mm_dim,mm_dep))

x=T2[(P==6)&(N>=W)&(TRC<TRMAX)]
QS=(0.50,0.75,0.90,0.95,0.975,0.99,0.995,0.999)
UCLS=[UCL0]+[float(np.quantile(x,q)) for q in QS]
LAT=(1,2,3,4,6,8,12,16,24,32)
days=n*3.0/86400.0

def runs(u):
    k=np.where(T2>u,key_u,-1).astype(np.int64)
    b=np.flatnonzero(np.r_[True,k[1:]!=k[:-1]])
    L=np.diff(np.r_[b,len(k)]);V=k[b]
    s=V>=0
    return b[s],L[s],V[s]

def fire(b,L,V,lat):
    last={};c=0;per={}
    for s,l,q in zip(b,L,V):
        if l<lat:continue
        t=s+lat-1
        if q in last and t-last[q]<COOL:continue
        last[q]=t;c+=1;per[q]=per.get(q,0)+1
    return c,per

print('\nSWEEP  su kien/ngay (COOL=%d)'%COOL)
print('%-9s %-8s %-7s '%('ucl','q','trig%')+' '.join('L%-5d'%l for l in LAT))
for i,u in enumerate(UCLS):
    b,L,V=runs(u)
    tg=float((T2>u).mean())
    lbl='F-dist' if i==0 else '%g'%QS[i-1]
    row=[]
    for l in LAT:
        c,_=fire(b,L,V,l);row.append('%-6.2f'%(c/days))
    print('%-9.4f %-8s %-7.3f '%(u,lbl,tg)+' '.join(row))

print('\nSWEEP  slow/frontend su kien/ngay')
kf=DI['slow']*6+PI['frontend']
print('%-9s %-8s '%('ucl','q')+' '.join('L%-5d'%l for l in LAT))
for i,u in enumerate(UCLS):
    b,L,V=runs(u)
    lbl='F-dist' if i==0 else '%g'%QS[i-1]
    row=[]
    for l in LAT:
        _,per=fire(b,L,V,l);row.append('%-6.2f'%(per.get(kf,0)/days))
    print('%-9.4f %-8s '%(u,lbl)+' '.join(row))

print('\nUNATTRIBUTED_ticks=%d  no_sig_ticks=%d  invalid=%d'%(
    int((key_u==-2).sum()),int((ok&~sig).sum()),int((~ok).sum())))
