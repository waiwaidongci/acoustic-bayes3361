import os,sys,random,json,math
sys.path.insert(0,'/workspace/backend')
os.environ['DATABASE_URL']='sqlite:////tmp/pkt_it.db'
for f in ('/tmp/pkt_it.db',):
    if os.path.exists(f):os.remove(f)
from src.config.database import Base,engine,Session
from src.models import packet as packet_models,user,source,record,run,note,audit
from src.services.algorithm import calculate
from src.services import packets,packet_worker,verification
packets._enqueue=lambda pid: True  # no redis in test harness
from src.utils.security import hash_password
Base.metadata.create_all(engine)

class U:
    def __init__(self,email,role):self.email=email;self.role=role
analyst=U('analyst@x','analyst');admin=U('admin@x','admin')

def gen(seed,n=72,classes=3):
    rng=random.Random(seed)
    names=['正常','轴承磨损','转子偏心','松动'][:classes]
    recs=[]
    for i in range(n):
        c=i%classes
        recs.append({'reference':f'P-{i+1:04d}','values':{
            'rms':round(max(.1,rng.gauss([1.2,3.0,2.0,4.0][c],.35)),3),
            'centroid':round(max(100,rng.gauss([900,1600,2300,3000][c],170)),2),
            'kurtosis':round(max(.1,rng.gauss([2.8,5.2,3.9,6.1][c],.45)),3),
            'label':names[c]}})
    return recs

def produce(recs,params):
    result=calculate(recs,params)
    fold_of={}
    for fold in result['folds']:
        for i in fold['validation_indices']:fold_of[i]=fold['fold']
    items=[]
    for i,row in enumerate(result['rows']):
        items.append({'position':i+1,'reference':row['reference'],
                      'values':recs[i]['values'],
                      'claim':{'fold':fold_of[i],'predicted':row['predicted'],
                               'posterior':row['posterior']}})
    return items,result

def shards(items,size,rng,prefix='s'):
    order=list(range(len(items)))
    out=[]
    for k in range(0,len(items),size):
        chunk=items[k:k+size]
        out.append({'shard_key':f'{prefix}{k//size:03d}','items':chunk})
    rng.shuffle(out)
    return out


def shard_obj(spec):
    items=[type('I',(),{'position':it['position'],'reference':it['reference'],'values':it['values'],
        'claim':type('C',(),{'fold':it['claim']['fold'],'predicted':it['claim']['predicted'],
        'posterior':it['claim']['posterior']})()}) for it in spec['items']]
    return type('D',(),{'shard_key':spec['shard_key'],'expected_version':None,'items':items})()

def make_packet(db,book_key,recs,params,shard_specs,user=analyst):
    created=packets.create_packet(db,user,type('D',(),{'book_key':book_key,'name':'测试判读册',
        'description':'','expected_count':len(recs),'parameters':params})())
    pid=created['id']
    for spec in shard_specs:
        packets.receive_shard(db,user,pid,shard_obj(spec))
    return pid

def run_worker(pid):
    with Session() as db:
        return packet_worker.verify_once(db,pid)

def seal(db,pid,user=analyst):
    return packets.seal_packet(db,user,pid,type('D',(),{'expected_version':None})())

# ============ case 1: all shards received out of order, full match ============
params={'folds':3,'smoothing':1e-9}
recs=gen(1,90)
items,alg=produce(recs,params)
with Session() as db:
    pid=make_packet(db,'BK-1',recs,params,shards(items,7,random.Random(3)))
    sealed=seal(db,pid);assert sealed['sealed']==pid
    out=run_worker(pid)
    assert out['status']=='verified',out
    pkt=packets.get_packet(db,pid)
    assert pkt['state']=='verified'
    res=pkt['result']
    assert res['all_matched'] and res['checked']==len(recs)
    print('case1 ok acc',round(res['accuracy'],4),'alg',round(alg['summary'][0]['value'],4))
    adm=packets.admit_packet(db,admin,pid)
    assert adm['admitted']
    again=packets.admit_packet(db,admin,pid)
    assert again['admitted'] is False
    print('case1 admission idempotent ok')

# ============ case 2: duplicate shard retry & conflicting value isolated ============
with Session() as db:
    recs2=gen(2,66);items2,_=produce(recs2,params)
    specs=shards(items2,6,random.Random(5))
    pid=make_packet(db,'BK-2',recs2,params,specs)
    # retry identical first shard
    spec=specs[0]
    r=packets.receive_shard(db,analyst,pid,shard_obj(spec))
    assert r['status']=='duplicate' and r['accepted']==0,r
    before=packets.get_packet(db,pid)
    # same shard key different content -> 409
    bad=json.loads(json.dumps(spec));bad['items'][0]['reference']='CHANGED'
    try:packets.receive_shard(db,analyst,pid,shard_obj(bad));raise AssertionError('should reject')
    except packets.HTTPException as e:assert e.status_code==409
    # a new shard re-sending an existing position with different value -> isolated conflict
    target=items2[3]
    conflict_item={'position':target['position'],'reference':target['reference'],
                   'values':{**target['values'],'rms':target['values']['rms']+5},
                   'claim':target['claim']}
    data_c=shard_obj({'shard_key':'conflict-shard','items':[conflict_item]})
    rc=packets.receive_shard(db,analyst,pid,data_c)
    assert rc['conflicts']==1 and rc['accepted']==0,rc
    pkt=packets.get_packet(db,pid)
    assert pkt['open_conflicts']==1 and pkt['received_count']==before['received_count']
    # seal must be blocked
    try:seal(db,pid);raise AssertionError('seal should fail')
    except packets.HTTPException as e:assert e.status_code==409
    conflicts=packets.list_conflicts(db,pid)
    assert conflicts[0]['position']==target['position']
    # keep existing -> can seal and matches
    packets.resolve_conflict(db,admin,pid,conflicts[0]['id'],
        type('D',(),{'keep':'existing','expected_version':None})())
    seal(db,pid)
    out=run_worker(pid)
    assert out['status']=='verified',out
    print('case2 ok duplicate/conflict/isolation')

# ============ case 3: tampered claim -> divergence at first wrong item ============
with Session() as db:
    recs3=gen(3,90);items3,_=produce(recs3,params)
    tampered=json.loads(json.dumps(items3))
    idx=37
    tampered[idx]['claim']['posterior']=min(1.0,tampered[idx]['claim']['posterior']+0.25)
    pid=make_packet(db,'BK-3',recs3,params,shards(tampered,10,random.Random(2)))
    seal(db,pid)
    out=run_worker(pid)
    assert out['status']=='diverged',out
    div=out['divergence']
    assert div['position']==idx+1 and div['kind']=='posterior',div
    pkt=packets.get_packet(db,pid)
    assert pkt['state']=='failed'
    try:packets.admit_packet(db,admin,pid);raise AssertionError('admit should fail')
    except packets.HTTPException as e:assert e.status_code==409
    print('case3 ok first divergence at position',div['position'])

# ============ case 4: wrong predicted label -> divergence 'predicted' ============
with Session() as db:
    recs4=gen(4,72);items4,_=produce(recs4,params)
    tampered=json.loads(json.dumps(items4))
    tampered[10]['claim']['predicted']='错误类型'
    pid=make_packet(db,'BK-4',recs4,params,shards(tampered,9,random.Random(9)))
    seal(db,pid);out=run_worker(pid)
    assert out['status']=='diverged' and out['divergence']['kind']=='predicted',out
    print('case4 ok predicted divergence')

# ============ case 5: interrupted run resumes from checkpoint ============
recs5=gen(5,120);items5,_=produce(recs5,params)
with Session() as db:
    pid=make_packet(db,'BK-5',recs5,params,shards(items5,11,random.Random(7)))
    seal(db,pid)
# run with interruption after some items: crash at the start of the 3rd 20-item batch
orig=verification.verify_items
packet_worker.CHECKPOINT_EVERY=20
limit={'n':40,'seen':0}
def interrupted(source,assigner,models,state,tolerance=verification.POSTERIOR_TOLERANCE):
    def capped():
        for item in source():
            if limit['seen']>=limit['n']:
                raise KeyboardInterrupt('simulated crash')
            limit['seen']+=1
            yield item
    return orig(capped,assigner,models,state,tolerance=tolerance)
packet_worker.verification.verify_items=interrupted
try:
    run_worker(pid)
except KeyboardInterrupt:
    pass
finally:
    packet_worker.verification.verify_items=orig
    packet_worker.CHECKPOINT_EVERY=1000
with Session() as db:
    pkt=packets.get_packet(db,pid)
    assert pkt['verified_through']>=40,(pkt['verified_through'],pkt['state'])
    packets.retry_verification(db,analyst,pid)
    resumed=run_worker(pid)
    assert resumed['status']=='verified',resumed
    res=packets.get_packet(db,pid)['result']
    assert res['all_matched'] and res['checked']==len(recs5)
    print('case5 ok resumed from',pkt['verified_through'],'-> checked',res['checked'])

# ============ case 6: 100k items bounded memory ============
import tracemalloc,time
recs6=gen(6,100000,3)
# generate claims directly with independent model (not sklearn rows for memory)
items6,_=produce(recs6,params)
with Session() as db:
    t=time.time()
    pid=make_packet(db,'BK-6',recs6,params,shards(items6,2000,random.Random(11)))
    print('100k intake',round(time.time()-t,1),'s')
    seal(db,pid)
    tracemalloc.start()
    t=time.time()
    out=run_worker(pid)
    cur,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    print('100k verify',round(time.time()-t,1),'s status',out['status'],'peak MB',round(peak/1e6,1))
    assert out['status']=='verified',out['status']
    res=packets.get_packet(db,pid)['result']
    assert res['checked']==100000 and res['all_matched']
print('ALL CASES PASSED')
