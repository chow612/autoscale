import sys
from array import array
import numpy as np

DIMS=('cpu','ram','rps','err','asym','slow')
DEPS=('userservice','ledgerwriter','balancereader','transactionhistory','frontend','contacts','UNATTRIBUTED')
DTHR=(12.4348,6.7844,8.0606,6.2051,13.2100,10.6277)
DI={d:i for i,d in enumerate(DIMS)}
PI={d:i for i,d in enumerate(DEPS)}
W,TRMAX,UCL0=120,2.0,18.5652

src,dst=sys.argv[1],sys.argv[2]
t2=array('d');trg=array('b');nv=array('i');pv=array('b');trv=array('d');nlv=array('b')
dc=[array('d') for _ in range(6)]
ut2=array('d');umw=array('b')
at=array('i');ad=array('b');ap=array('b');asg=array('b');av=array('d')
ti=-1
for ln in open(src,'r',errors='replace'):
    if ln.startswith('MEWMA '):
        w=ln.split()
        t2.append(float(w[1][3:]));trg.append(1 if w[3][5:]=='True' else 0)
        nv.append(int(w[4][2:]));pv.append(int(w[5][2:]));trv.append(float(w[6][3:]))
        nlv.append(-1);ut2.append(float('nan'));umw.append(-1)
        for k in range(6):dc[k].append(float('nan'))
        ti+=1
    elif ti<0:continue
    elif ln.startswith('  D '):
        w=ln.split()
        for k in range(6):dc[k][ti]=float(w[k+1].split('=')[1])
        nlv[ti]=int(w[7][3:])
    elif ln.startswith('  ATTR '):
        w=ln.split()
        at.append(ti);ad.append(DI[w[1][4:]]);av.append(float(w[2][2:]))
        asg.append(int(w[4][4:]));ap.append(PI.get(w[5][4:],-1))
    elif ln.startswith('  UNI '):
        w=ln.split()
        umw[ti]=int(w[6][6:]);ut2[ti]=float(w[7][3:])

T2=np.frombuffer(t2,dtype=np.float64);TR=np.frombuffer(trg,dtype=np.int8)
N=np.frombuffer(nv,dtype=np.int32);P=np.frombuffer(pv,dtype=np.int8)
TRC=np.frombuffer(trv,dtype=np.float64);NL=np.frombuffer(nlv,dtype=np.int8)
D=np.vstack([np.frombuffer(c,dtype=np.float64) for c in dc]).T
UT=np.frombuffer(ut2,dtype=np.float64);UM=np.frombuffer(umw,dtype=np.int8)
AT=np.frombuffer(at,dtype=np.int32);AD=np.frombuffer(ad,dtype=np.int8)
AP=np.frombuffer(ap,dtype=np.int8);AS=np.frombuffer(asg,dtype=np.int8)
AV=np.frombuffer(av,dtype=np.float64)
np.savez_compressed(dst,T2=T2,TR=TR,N=N,P=P,TRC=TRC,NL=NL,D=D,UT=UT,UM=UM,AT=AT,AD=AD,AP=AP,AS=AS,AV=AV)

bad=np.nansum(np.abs(UT-T2)>1e-3)
print('ticks=%d attr=%d parser_mismatch_t2=%d mismatch_mewma=%d'%(len(T2),len(AT),bad,int(np.sum((UM>=0)&(UM!=TR)))))
print('p_hist',{int(k):int(v) for k,v in zip(*np.unique(P,return_counts=True))})
fin=np.isfinite(D).all(1)
print('D_finite=%d  D_finite&trig0=%d  D_finite&trig1=%d'%(fin.sum(),int((fin&(TR==0)).sum()),int((fin&(TR==1)).sum())))
print('D_nonzero&trig0=%d'%int((fin&(TR==0)&(D.sum(1)>0)).sum()))
print('attr_on_trig0=%d'%int((TR[AT]==0).sum()))

m=(P==6)&(N>=W)&(TRC<TRMAX)&np.isfinite(T2)
x=T2[m]
print('\ncalib_n=%d  mean=%.4f  median=%.4f  max=%.4f'%(len(x),x.mean(),np.median(x),x.max()))
print('exceed_rate@%.4f = %.4f'%(UCL0,float((x>UCL0).mean())))
for q in (0.90,0.95,0.99,0.995,0.999,0.9995,0.9999):
    v=float(np.quantile(x,q))
    print('q=%-7g ucl=%8.4f  arl0=%8.1f tick = %7.1f min'%(q,v,1/(1-q),1/(1-q)*3/60))

y=x-x.mean();v=float((y*y).mean());n=len(y)
print('\nacf_t2 '+' '.join('%d:%.3f'%(k,float((y[:n-k]*y[k:]).mean()/v)) for k in (1,2,3,5,8,10,15,20,30,50,100)))
b=(x>UCL0).astype(np.float64);b-=b.mean();vb=float((b*b).mean())
print('acf_ind '+' '.join('%d:%.3f'%(k,float((b[:n-k]*b[k:]).mean()/vb)) for k in (1,2,3,5,8,10,15,20,30,50,100)))

sig=(AS==1)&(AP>=0)&(AP<6)
key=AD.astype(np.int32)*7+AP.astype(np.int32)
run={}
for t,k in zip(AT[sig],key[sig]):run.setdefault(k,[]).append(int(t))
print('\nrun_len @ucl=%.4f  latch=8'%UCL0)
for k in sorted(run,key=lambda k:-len(run[k])):
    ts=run[k];L=[];c=1
    for i in range(1,len(ts)):
        if ts[i]==ts[i-1]+1:c+=1
        else:L.append(c);c=1
    L.append(c);L=np.array(L)
    print('%-14s %-20s ticks=%6d runs=%5d maxrun=%4d p50=%3d p95=%3d n>=8=%4d n>=16=%3d'%(
        DIMS[k//7],DEPS[k%7],len(ts),len(L),L.max(),int(np.percentile(L,50)),int(np.percentile(L,95)),
        int((L>=8).sum()),int((L>=16).sum())))
