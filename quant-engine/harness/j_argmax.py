import sys,numpy as np
DIM=6
NAMES=('cpu','ram','rps','err','asym','slow')
SETS={'W120_ghim':(25.8387,6.5558,7.0026,6.8761,29.2725,20.7678),
      'q099_prod':(24.9689,6.5488,6.9570,6.8484,27.9837,20.1079),
      'W360_ghim':(26.8985,6.8378,12.6282,7.2688,33.3542,28.3862)}
Dl=[];T2l=[];NLl=[];ct2=None
for ln in open(sys.argv[1],errors='replace'):
    if ln.startswith('MEWMA '):
        try:ct2=float(ln.split()[1].split('=')[1])
        except Exception:ct2=None
    elif ln.startswith('  D '):
        w=ln.split()
        if ct2 is None:continue
        try:
            d=[float(w[i+1].split('=')[1]) for i in range(DIM)]
            nl=int(w[7].split('=')[1])
        except Exception:
            ct2=None;continue
        Dl.append(d);NLl.append(nl);T2l.append(ct2);ct2=None
D=np.array(Dl);T2=np.array(T2l);NL=np.array(NLl)
n=len(D)
print('so tick=%d'%n)
base=(NL==DIM)&np.isfinite(T2)&np.isfinite(D).all(1)
top=base&(T2>46.5076)
print('tick hop le=%d   tick top-1%% (T2>46.5076)=%d\n'%(base.sum(),top.sum()))
for name,thr in SETS.items():
    T=np.array(thr,dtype=float)
    R=D/T
    wr=D.argmax(1);wt=R.argmax(1)
    exc=D>T
    anye=exc.any(1)
    wine=exc[np.arange(n),wr]
    fn=anye&(~wine)
    mis=wine&(wt!=wr)
    for tag,m in (('tat ca tick hop le',base),('chi top-1%% T2',top)):
        k=int(m.sum())
        if not k:continue
        print('%-11s | %-20s n=%-7d  co_chieu_vuot=%5.2f%%  quy_ket_duoc=%5.2f%%  AM_TINH_GIA=%5.2f%%  QUY_KET_LECH=%5.2f%%'%(
            name,tag,k,100.0*anye[m].mean(),100.0*wine[m].mean(),100.0*fn[m].mean(),100.0*mis[m].mean()))
    print('   argmax tho :  '+'  '.join('%s=%.1f%%'%(NAMES[i],100.0*(wr[base]==i).mean()) for i in range(DIM)))
    print('   argmax ty le:  '+'  '.join('%s=%.1f%%'%(NAMES[i],100.0*(wt[base]==i).mean()) for i in range(DIM)))
    f=fn&base
    if f.sum():
        lost=np.zeros(DIM)
        ex=exc[f]
        for i in range(DIM):lost[i]=100.0*ex[:,i].mean()
        print('   chieu bi bo qua khi am tinh gia: '+'  '.join('%s=%.1f%%'%(NAMES[i],lost[i]) for i in range(DIM)))
    print()
