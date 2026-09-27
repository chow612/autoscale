import os, json, hmac, hashlib, time, asyncio, logging
from pathlib import Path
from fastapi import FastAPI, Request, Response

# ============================================================================
# Config
# ============================================================================
LISTEN_HOST = os.environ.get('ACT_LISTEN_HOST', '0.0.0.0')
LISTEN_PORT = int(os.environ.get('ACT_LISTEN_PORT', '8080'))
# LUU Y: khong co TLS o tang app nay. Phai dat sau VPN/Tailscale hoac reverse
# proxy co TLS - khong bao gio expose port nay ra internet public.

ACT_SECRET = os.environ.get('JANUS_ACT_SECRET', '').encode()
if not ACT_SECRET:
    raise RuntimeError('JANUS_ACT_SECRET rong - tu choi khoi dong, khong chay webhook khong co secret')

KUBECTL_BIN = os.environ.get('ACT_KUBECTL_BIN', 'kubectl')
KUBECONFIG = os.environ.get('ACT_KUBECONFIG', '')  # rong = dung default (~/.kube/config hoac KUBECONFIG env co san)
# Ngan sach thoi gian: worker1 doi toi da JANUS_ACT_TIMEOUT=5.0s (tinh ca round-trip mang).
# Danh 1.8s cho moi lenh kubectl (GET + PATCH = toi da 3.6s), con lai la du cho mang + overhead.
KUBECTL_TIMEOUT = float(os.environ.get('ACT_KUBECTL_TIMEOUT', '0') or 0) or 1.8

AUDIT_LOG_PATH = os.environ.get('ACT_AUDIT_LOG', '/var/log/janus-actuator/audit.jsonl')

# Whitelist CUNG - nguon that duy nhat cho quyen han actuation, khong tin bat ky
# gia tri nao tu request. Doi xung voi NS_FILTER/MAX_REP trong ingester6.py,
# nhung duoc khai bao lai o day doc lap - day la lop chan cuoi cung co quyen that (kubeconfig).
WHITELIST = {
    'bank-of-anthos': {
        'frontend': 20,
        'contacts': 15,
        'userservice': 15,
        'ledgerwriter': 10,
        'balancereader': 10,
        'transactionhistory': 10,
    }
}

# ============================================================================
# State
# ============================================================================
_locks = {}  # (namespace, deployment) -> asyncio.Lock, tao khi can (chay tren 1 event loop nen an toan)
_logger = logging.getLogger('janus-actuator')


def _get_lock(namespace, deployment):
    key = (namespace, deployment)
    lk = _locks.get(key)
    if lk is None:
        lk = _locks[key] = asyncio.Lock()
    return lk


def _audit(record: dict):
    record['ts'] = time.time()
    line = json.dumps(record, separators=(',', ':'))
    _logger.info(line)
    try:
        p = Path(AUDIT_LOG_PATH)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open('a', buffering=1) as f:
            f.write(line + '\n')
    except OSError as e:
        # Audit log la best-effort - loi ghi file KHONG duoc lam fail request actuation.
        _logger.warning(f'audit log write failed: {e!r}')


class KubectlError(Exception):
    pass


async def _run_kubectl(args, timeout=KUBECTL_TIMEOUT):
    """Chay kubectl qua subprocess async (khong block event loop). Tra ve stdout (str).
    Nem KubectlError neu timeout hoac returncode != 0."""
    env = os.environ.copy()
    if KUBECONFIG:
        env['KUBECONFIG'] = KUBECONFIG
    proc = await asyncio.create_subprocess_exec(
        KUBECTL_BIN, *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise KubectlError(f'timeout sau {timeout}s: kubectl {" ".join(args)}')
    if proc.returncode != 0:
        raise KubectlError(f'kubectl {" ".join(args)} exit={proc.returncode} stderr={stderr.decode(errors="replace")[:300]}')
    return stdout.decode(errors='replace').strip()


async def get_replicas(namespace, deployment):
    out = await _run_kubectl([
        'get', 'deployment', deployment, '-n', namespace,
        '--subresource=scale', '-o', 'jsonpath={.spec.replicas}',
    ])
    if not out:
        raise KubectlError(f'khong doc duoc spec.replicas cho {namespace}/{deployment} (rong)')
    return int(out)


async def patch_replicas(namespace, deployment, desired):
    await _run_kubectl([
        'patch', 'deployment', deployment, '-n', namespace,
        '--subresource=scale', '--type=merge',
        '-p', json.dumps({'spec': {'replicas': desired}}),
    ])
    # kubectl patch --subresource=scale khong luon in lai object moi qua jsonpath truc tiep
    # tren cung 1 lenh mot cach nhat quan giua cac phien ban kubectl -> doc lai rieng de xac nhan.
    return await get_replicas(namespace, deployment)


# ============================================================================
# App
# ============================================================================
app = FastAPI()


@app.post('/scale')
async def scale(request: Request):
    body = await request.body()
    sig = request.headers.get('x-signature', '')
    src = request.client.host if request.client else '?'

    expected_sig = hmac.new(ACT_SECRET, body, hashlib.sha256).hexdigest()
    if not sig or not hmac.compare_digest(expected_sig, sig):
        _audit({'event': 'reject', 'reason': 'bad_signature', 'src': src})
        return Response(status_code=401, content=json.dumps({'error': 'bad signature'}), media_type='application/json')

    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        _audit({'event': 'reject', 'reason': 'bad_json', 'src': src})
        return Response(status_code=400, content=json.dumps({'error': 'bad json'}), media_type='application/json')

    namespace = payload.get('namespace')
    deployment = payload.get('deployment')
    desired = payload.get('desired')
    expected_cur = payload.get('expected_cur')  # chi de log doi chieu, khong dung de chan quyet dinh (theo thiet ke da chot)

    if not isinstance(namespace, str) or not isinstance(deployment, str) or not isinstance(desired, int):
        _audit({'event': 'reject', 'reason': 'bad_types', 'src': src, 'payload': payload})
        return Response(status_code=400, content=json.dumps({'error': 'namespace/deployment phai la string, desired phai la int'}), media_type='application/json')

    dep_whitelist = WHITELIST.get(namespace)
    max_rep = dep_whitelist.get(deployment) if dep_whitelist else None
    if max_rep is None:
        _audit({'event': 'reject', 'reason': 'not_whitelisted', 'src': src, 'namespace': namespace, 'deployment': deployment})
        return Response(status_code=403, content=json.dumps({'error': 'namespace/deployment khong nam trong whitelist'}), media_type='application/json')

    if desired < 1 or desired > max_rep:
        _audit({'event': 'reject', 'reason': 'out_of_bounds', 'src': src, 'namespace': namespace, 'deployment': deployment, 'desired': desired, 'max': max_rep})
        return Response(status_code=400, content=json.dumps({'error': f'desired phai trong [1,{max_rep}]'}), media_type='application/json')

    lock = _get_lock(namespace, deployment)
    async with lock:
        try:
            frm = await get_replicas(namespace, deployment)
        except (KubectlError, ValueError) as e:
            _audit({'event': 'fail', 'reason': 'get_failed', 'src': src, 'namespace': namespace, 'deployment': deployment, 'detail': str(e)})
            return Response(status_code=502, content=json.dumps({'error': f'khong doc duoc replicas hien tai: {e}'}), media_type='application/json')

        if expected_cur is not None and isinstance(expected_cur, int) and expected_cur != frm:
            _audit({'event': 'mismatch', 'src': src, 'namespace': namespace, 'deployment': deployment,
                    'expected_cur': expected_cur, 'actual_cur': frm})
            # Khong chan actuation vi mismatch - chi ghi lai de doi chieu ben ngoai (nhu da thong nhat).

        if frm == desired:
            _audit({'event': 'noop', 'src': src, 'namespace': namespace, 'deployment': deployment, 'from': frm, 'to': desired})
            return {'applied': True, 'from': frm, 'to': frm}

        try:
            to = await patch_replicas(namespace, deployment, desired)
        except (KubectlError, ValueError) as e:
            _audit({'event': 'fail', 'reason': 'patch_failed', 'src': src, 'namespace': namespace, 'deployment': deployment,
                    'from': frm, 'desired': desired, 'detail': str(e)})
            return Response(status_code=502, content=json.dumps({'error': f'patch that bai: {e}'}), media_type='application/json')

        _audit({'event': 'applied', 'src': src, 'namespace': namespace, 'deployment': deployment, 'from': frm, 'to': to})
        return {'applied': True, 'from': frm, 'to': to}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    import uvicorn
    uvicorn.run(app, host=LISTEN_HOST, port=LISTEN_PORT)

