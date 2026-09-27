import sys,importlib.util as iu,numpy as np,array
W=220;FREL=8;FMAX=400;UCL=61.9109;DIM=6
E=[]
for ln in open(sys.argv[1],errors='replace'):
    if ln.startswith('  FFT '):
        try:E.append([float(u.split('=',1)[1].split('->')[2]) for u in ln.split()[1:1+DIM]])
        except Exception:pass
E=np.array(E);N=len(E)
s=iu.spec_from_file_location('ing','ingester6.py')
m=iu.module_from_spec(s);s.loader.exec_module(m);m.MEWMA_WINDOW=W
st=m.StateMEWMA();v=bytearray([1]*DIM);e=array.array('d',[0.0]*DIM)
T2=np.full(N,np.nan);TR=np.full(N,np.nan);D=np.full((N,DIM),np.nan)
on=0;calm=0;age=0;reb=0;nf=0
for j in range(N):
    used=on
    for i in range(DIM):e[i]=E[j,i]
    t2=st.update(e,v,used)[0]
    if st.count>=W:
        T2[j]=t2;TR[j]=st.tr
        st.decomp(t2)
        for i in range(DIM):D[j,i]=st.d[i]
        nf+=1
    tg=t2>UCL
    if on:
        age+=1
        calm=calm+1 if not tg else 0
        if calm>=FREL:on=0;calm=0;age=0
        elif age>=FMAX:reb+=1;on=0;calm=0;age=0
    elif tg and st.count>=W:on=1;calm=0;age=1
np.savez('j_frz.npz',t2=T2,d=D,tr=TR)
print('SAVED j_frz.npz  n_hople=%d  REBASELINE=%d'%(nf,reb))
