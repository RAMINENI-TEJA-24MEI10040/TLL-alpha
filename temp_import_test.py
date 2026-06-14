import requests

BASE = 'http://localhost:8000/api/v1'

print('health', requests.get('http://localhost:8000/health').status_code)
create = requests.post(
    BASE + '/scans',
    json={'name': 'petstore-test', 'target': 'https://petstore3.swagger.io/api/v3/openapi.json', 'config': {}}
)
print('create status', create.status_code)
print(create.text[:1200])
if create.ok:
    scan = create.json()
    scan_id = scan['id']
    print('scan id', scan_id)
    discover = requests.post(
        BASE + f'/scans/{scan_id}/discover',
        json={'spec_source': 'https://petstore3.swagger.io/api/v3/openapi.json', 'base_url': 'https://petstore3.swagger.io/api/v3'}
    )
    print('discover status', discover.status_code)
    print(discover.text[:2000])
