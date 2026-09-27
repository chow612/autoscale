import hmac, hashlib, json, os, sys, urllib.request, urllib.error

URL = os.environ.get('ACT_URL', 'http://127.0.0.1:8080/scale')
SECRET = os.environ.get('JANUS_ACT_SECRET', '').encode()
if not SECRET:
    sys.exit('JANUS_ACT_SECRET rong - source /etc/actuator/env truoc khi chay script nay')


def call(payload, label):
    body = json.dumps(payload, separators=(',', ':')).encode()
    sig = hmac.new(SECRET, body, hashlib.sha256).hexdigest()
    req = urllib.request.Request(URL, data=body, method='POST',
                                  headers={'Content-Type': 'application/json', 'X-Signature': sig})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            print(f'[{label}] status={r.status} body={r.read().decode()}')
    except urllib.error.HTTPError as e:
        print(f'[{label}] status={e.code} body={e.read().decode()}')
    except urllib.error.URLError as e:
        print(f'[{label}] loi ket noi: {e}')


# Case 1: deployment khong whitelist -> ky vong 403
call({'namespace': 'bank-of-anthos', 'deployment': 'khong-ton-tai', 'desired': 3, 'expected_cur': 5},
     'not_whitelisted')

# Case 2: desired ngoai bien [1, max] (frontend max=20) -> ky vong 400
call({'namespace': 'bank-of-anthos', 'deployment': 'frontend', 'desired': 999, 'expected_cur': 5},
     'out_of_bounds')

# Case 3: hop le, desired khac cur hien tai -> ky vong 200, applied=true
call({'namespace': 'bank-of-anthos', 'deployment': 'frontend', 'desired': 6, 'expected_cur': 5},
     'valid_scale')

# Case 4: goi lai y het case 3 (desired da la gia tri hien tai) -> ky vong 200, to==from (no-op)
call({'namespace': 'bank-of-anthos', 'deployment': 'frontend', 'desired': 6, 'expected_cur': 6},
     'idempotent_noop')

print('\nNho patch lai replicas ve 5 (gia tri goc) sau khi test xong:')
print('kubectl patch deployment frontend -n bank-of-anthos --subresource=scale --type=merge -p \'{"spec":{"replicas":5}}\'')
