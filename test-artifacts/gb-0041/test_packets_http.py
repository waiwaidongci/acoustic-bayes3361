import os,sys,random,json,math
sys.path.insert(0,'/workspace/backend')
os.environ['DATABASE_URL']='sqlite:////tmp/http.db'
os.environ['ARTIFACT_DIR']='/tmp/artifacts'
os.environ['REDIS_URL']='redis://127.0.0.1:6399/0'
os.environ['JWT_SECRET']='test-secret'
for f in ('/tmp/http.db',):
    if os.path.exists(f):os.remove(f)
# Fake redis: in-memory list so /health and enqueues work without a server.
class FakeQueue:
    def __init__(self):self.lists={}
    def rpush(self,key,*values):self.lists.setdefault(key,[]).extend(values);return len(self.lists[key])
    def ping(self):return True
import src.services.runs as runs_mod
import src.services.packets as packets_mod
fake=FakeQueue()
runs_mod.queue=fake
packets_mod.queue=fake
from src.config.database import Base,engine,Session
from src.models import packet as pm,user,source,record,run,note,audit
from src.services.algorithm import calculate
from src.utils.security import hash_password
from src.main import app
Base.metadata.create_all(engine)
with Session() as db:
    for role in ('admin','analyst','viewer'):
        db.add(user.User(email=role+'@t.demo',password_hash=hash_password('pw123456'),role=role))
    db.commit()
from fastapi.testclient import TestClient
client=TestClient(app)

def login(email):
    r=client.post('/auth/login',json={'email':email,'password':'pw123456'});assert r.status_code==200,r.text
    return {'Authorization':'Bearer '+r.json()['token']}
A=login('analyst@t.demo');AD=login('admin@t.demo');V=login('viewer@t.demo')

rng=random.Random(3);names=['正常','轴承磨损','转子偏心'];n=48
recs=[{'reference':f'TX-{i:04d}','values':{'rms':round(max(.1,rng.gauss([1.2,3.,2.][i%3],.35)),3),'centroid':round(max(100,rng.gauss([900,1600,2300][i%3],170)),2),'kurtosis':round(max(.1,rng.gauss([2.8,5.2,3.9][i%3],.45)),3),'label':names[i%3]}} for i in range(n)]
params={'folds':3,'smoothing':1e-9}
res=calculate(recs,params);fold_of={}
for f in res['folds']:
    for i in f['validation_indices']:fold_of[i]=f['fold']
items=[{'position':i+1,'reference':row['reference'],'values':recs[i]['values'],'claim':{'fold':fold_of[i],'predicted':row['predicted'],'posterior':row['posterior']}} for i,row in enumerate(res['rows'])]

# create packet
r=client.post('/packets',headers=A,json={'book_key':'BOOK-HTTP-1','name':'HTTP判读册','description':'','expected_count':n,'parameters':params})
assert r.status_code==200,r.text;pid=r.json()['id']

# viewer cannot write
r=client.post(f'/packets/{pid}/shards',headers=V,json={'shard_key':'x','items':[items[0]]});assert r.status_code==403,r.text
# shards out of order
order=list(range(0,n,9));rng.shuffle(order)
for k in order:
    r=client.post(f'/packets/{pid}/shards',headers=A,json={'shard_key':f's{k//9:03d}','items':items[k:k+9]})
    assert r.status_code==200,(k,r.text)
# duplicate retry
r=client.post(f'/packets/{pid}/shards',headers=A,json={'shard_key':f's{order[0]//9:03d}','items':items[order[0]:order[0]+9]})
assert r.json()['status']=='duplicate',r.text
d=client.get(f'/packets/{pid}',headers=A).json()
assert d['received_count']==n and d['missing_count']==0,(d['received_count'],d['missing_count'])
# seal
r=client.post(f'/packets/{pid}/seal',headers=A,json={});assert r.json()['sealed']==pid,r.text
assert fake.lists['packet:verify']==[pid],fake.lists
# worker step
from src.services.packet_worker import verify_once
with Session() as db: out=verify_once(db,pid)
assert out['status']=='verified',out
d=client.get(f'/packets/{pid}',headers=A).json();assert d['state']=='verified',d['state']
# viewer cannot admit; admin can
r=client.post(f'/packets/{pid}/admit',headers=V,json={});assert r.status_code==403
r=client.post(f'/packets/{pid}/admit',headers=AD,json={});assert r.json()['admitted'],r.text
r=client.post(f'/packets/{pid}/admit',headers=AD,json={});assert r.json()['admitted'] is False
# re-create same book_key returns same packet
r=client.post('/packets',headers=A,json={'book_key':'BOOK-HTTP-1','name':'HTTP判读册','description':'','expected_count':n,'parameters':params})
assert r.json()['id']==pid and r.json()['state']=='admitted'
# admissions
r=client.get('/packets/admissions',headers=A);assert len(r.json())==1
# items listing via API
r=client.get(f'/packets/{pid}/items?offset=0&limit=10',headers=A);assert len(r.json())==10
# audit has packet events
r=client.get('/audit',headers=A);actions={a['action'] for a in r.json()}
assert {'packet.created','packet.shard','packet.sealed','packet.admitted'} <= actions,actions
print('HTTP FLOW PASSED, packet',pid)
