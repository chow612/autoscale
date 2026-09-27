import sys,array,j_replay as J,j_arl as A

D6=6
FAR=1.0
DLT=2.5
HOR=250


def clean_t2(L,m):
    S=m.StateMEWMA()
    e=array.array('d',[0.0]*D6);vb=bytearray(b'\x01'*D6)
    res=L.res
    tk=array.array('i');t2=array.array('d')
    for t in range(L.n):
        b=t*D6
        for i in range(D6):e[i]=res[b+i]
        v,u,g=S.update(e,vb,0)
        if S.count<m.MEWMA_WINDOW:continue
        tk.append(t);t2.append(v)
    return tk,t2


def far_blocks(tk,t2,ucl,nb):
    lo=tk[0];hi=tk[-1]
    sp=(hi-lo+1)/float(nb)
    c=[0]*nb;n=[0]*nb
    for k in range(len(tk)):
        j=int((tk[k]-lo)/sp)
        if j>=nb:j=nb-1
        n[j]+=1
        if t2[k]>ucl:c[j]+=1
    return [(100.0*c[j]/n[j] if n[j] else float('nan')) for j in range(nb)],n


def delayA(L,m,ucl,sh,ons,warm):
    mk=m.StateMEWMA
    e=array.array('d',[0.0]*D6);vb=bytearray(b'\x01'*D6)
    res=L.res
    out=[]
    for t0 in ons:
        S=mk()
        s=t0-warm
        if s<0:s=0
        end=t0+HOR
        if end>L.n:end=L.n
        d=-1
        for t in range(s,end):
            q=t*D6
            if t>=t0:
                for i in range(D6):e[i]=res[q+i]+sh[i]
            else:
                for i in range(D6):e[i]=res[q+i]
            v,u,g=S.update(e,vb,0)
            if t>=t0 and v>ucl:
                d=t-t0;break
        out.append(d)
    return out


def main():
    a=sys.argv[1:]
    path=a[0] if a else 'ing_20260819T122931Z.log'
    nb=int(a[1]) if len(a)>1 else 4
    non=int(a[2]) if len(a)>2 else 60
    grid=[int(x) for x in a[3].split(',')] if len(a)>3 else [120,180,240,300,360,480]
    L=J.parse(path)
    wmax=max(grid)+180
    lo=wmax+50;hi=L.n-HOR
    if hi<=lo:
        print('log qua ngan cho luoi nay');return
    step=(hi-lo)//non
    ons=[lo+i*step for i in range(non)]
    sh=[DLT*A.SIG_SDF[i]*A.SDW[i] for i in range(D6)]
    print(J.ASSUM)
    print('log=%s n=%d khoi=%d onset=%d (chung cho moi W) delta=%.1f horizon=%d'%(
        path,L.n,nb,non,DLT,HOR))
    print('W     UCL       FAR%%    '+' '.join('kh%d'%(j+1) for j in range(nb))
          +'   max|lech|  d_trig med  mad   kd%')
    R=[]
    for w in grid:
        m=J.load(JANUS_W=w)
        tk,t2=clean_t2(L,m)
        ucl=J.q(t2,100.0-FAR)
        tot=100.0*sum(1 for v in t2 if v>ucl)/len(t2)
        fb,nn=far_blocks(tk,t2,ucl,nb)
        mx=max(abs(x-FAR) for x in fb)
        dd=delayA(L,m,ucl,sh,ons,w+180)
        ok=[x for x in dd if x>=0]
        md=J.q(ok,50) if ok else float('nan')
        ma=J.mad(ok) if ok else float('nan')
        kd=100.0*(len(dd)-len(ok))/len(dd)
        print('%-5d %-9.4f %-7.3f '%(w,ucl,tot)+' '.join('%.2f'%x for x in fb)
              +'   %-10.3f %-11.1f %-5.1f %.0f'%(mx,md,ma,kd))
        R.append((w,mx,md))
        sys.stdout.flush()
    best=min(r[2] for r in R)
    print()
    print('luat chon: W nho nhat thoa (a) max|lech| <= 0.25 pp VA (b) d_trig <= %.1f + 2'%best)
    sel=[r for r in R if r[1]<=0.25 and r[2]<=best+2.0]
    if sel:
        print('=> W = %d'%sel[0][0])
    else:
        pa=[r for r in R if r[1]<=0.25]
        print('=> KHONG W nao thoa ca hai. thoa (a): %s'%([r[0] for r in pa] or 'khong co'))
        print('   neu khong W nao thoa (a) thi noi luoi len tren va GHI RO da noi')


if __name__=='__main__':
    main()
