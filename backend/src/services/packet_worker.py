"""Sealed-packet verification worker.

A sealed packet is verified in four streamed passes over its items:

1. ``structure`` — count items/classes, check ranges, re-hash every item and
   confirm the frozen content hash still matches (catches any post-seal drift).
2. ``scalers``   — training-only standardisation statistics of every fold.
3. ``models``    — per-fold GaussianNB means, variances and priors.
4. ``items``     — item-by-item comparison against the claimed fold, prediction
   and posterior, stopping at the first divergence.

Each phase checkpoints to the database, and phase 4 checkpoints every
``CHECKPOINT_EVERY`` items, so an interrupted run resumes from the last
confirmed position instead of starting over. Every pass streams from the
database in batches, so memory stays flat regardless of packet size.

Concurrency: a worker claims the packet through a short lease (``verify_token``
plus ``verify_lease_until``). A second worker can only take over once the lease
has expired, i.e. the first worker was interrupted. Older workers' checkpoints
are rejected by the token guard, so two runs can never interleave writes.
"""
import logging
import math
import time
from collections import defaultdict

from sqlalchemy import select,update

from ..models.packet import Packet,PacketItem
from ..utils import canonical
from . import verification
from .audit import log
from .verification import FoldAssigner,StructuralError,Welford,features,fold_sequence

CHECKPOINT_EVERY=1000
SCAN_BATCH=2000
LEASE_SECONDS=120


class Superseded(Exception):
    """Another worker holds the lease, or the packet version changed."""


def _rows(db,packet_id,after,limit):
    statement=(select(PacketItem.position,PacketItem.reference,PacketItem.values,
                      PacketItem.claimed_fold,PacketItem.claimed_predicted,
                      PacketItem.claimed_posterior)
               .where(PacketItem.packet_id==packet_id,PacketItem.position>after)
               .order_by(PacketItem.position).limit(limit))
    for row in db.execute(statement):
        yield (row[0],row[1],row[2],row[3],row[4],row[5])


def _batches(db,packet_id,size=SCAN_BATCH):
    """Yield items in bounded blocks so a full-book scan never loads the book."""
    after=0
    while True:
        block=list(_rows(db,packet_id,after,size))
        if not block:return
        yield from block
        after=block[-1][0]


class Run:
    def __init__(self,db,packet,token,heartbeat=None):
        self.db=db
        self.packet=packet
        self.id=packet.id
        self.token=token
        self.heartbeat=heartbeat
        self.parameters=packet.parameters
        self.folds=int(packet.parameters['folds'])

    def items(self,after=0,limit=1000000):
        return _rows(self.db,self.id,after,limit)

    def beat(self):
        if self.heartbeat:self.heartbeat()

    def save(self,state,phase,through):
        result=self.db.execute(update(Packet)
                               .where(Packet.id==self.id,Packet.verify_token==self.token)
                               .values(verify_state=_strip(state),verify_phase=phase,
                                       verified_through=int(through),
                                       verify_lease_until=time.time()+LEASE_SECONDS))
        self.db.commit()
        self.beat()
        if result.rowcount==0:raise Superseded()


def _strip(state):
    return {key:value for key,value in state.items() if not key.startswith('_')}


def _claim(db,packet_id):
    """Atomically take the verification lease; returns (packet,token) or None.

    Only sealed (never started) and verifying (interrupted, lease expired) books
    may be claimed. A ``failed`` book has a confirmed divergence and is final;
    re-running it would reproduce the same first divergence.
    """
    token=f'{time.time():.6f}-{packet_id}'
    result=db.execute(update(Packet)
                      .where(Packet.id==packet_id,
                             Packet.state.in_(('sealed','verifying')),
                             (Packet.verify_lease_until.is_(None))|
                             (Packet.verify_lease_until<time.time()))
                      .values(state='verifying',verify_token=token,
                              verify_lease_until=time.time()+LEASE_SECONDS,error=''))
    db.commit()
    if result.rowcount==0:return None
    db.expire_all()
    return db.get(Packet,packet_id),token


def verify_once(db,packet_id,heartbeat=None):
    claimed=_claim(db,packet_id)
    if claimed is None:
        db.expire_all()
        current=db.get(Packet,packet_id)
        return {'status':'busy' if current and current.state=='verifying' else
                (current.state if current else 'missing')}
    packet,token=claimed
    run=Run(db,packet,token,heartbeat)
    try:
        return _drive(run,packet)
    except Superseded:
        return {'status':'superseded'}
    except StructuralError as exc:
        _reject(db,packet,str(exc))
        return {'status':'rejected','error':str(exc)}
    except Exception as exc:
        logging.exception('packet verification failed')
        db.rollback()
        db.execute(update(Packet).where(Packet.id==packet_id,Packet.verify_token==token)
                   .values(state='sealed',verify_lease_until=0,error=str(exc)[:2000]))
        db.commit()
        return {'status':'error','error':str(exc)[:2000]}
    finally:
        # A crash (BaseException) cannot run this; the lease then simply expires.
        if run.db is not None:
            try:run.db.rollback()
            except Exception:pass


def _drive(run,packet):
    state=packet.verify_state or {'phase':'structure'}
    phase=state.get('phase','structure')

    if phase=='structure':
        profile,digest=scan_structure(run)
        if profile is None:return {'status':'rejected'}
        if digest!=packet.content_hash:
            _reject(run.db,packet,'封包内容哈希与已入库样本不一致，判读册在封包后被改动')
            return {'status':'rejected'}
        state={'phase':'scalers','counts':profile['counts'],'order':profile['order'],
               'n':profile['n']}
        run.save(state,'scalers',0)
        phase='scalers'

    if phase=='scalers':
        means,scales=build_scalers(run,state)
        state={'phase':'models','counts':state['counts'],'order':state['order'],
               'means':means,'scales':scales}
        run.save(state,'models',0)
        phase='models'

    if phase=='models':
        models=build_models(run,state)
        state={'counts':state['counts'],'order':state['order'],
               'means':state['means'],'scales':state['scales'],'model':models,
               **verification.init_state(models)}
        run.save(state,'items',0)
        return verify_items(run,packet,state)

    return verify_items(run,packet,state)


# --------------------------------------------------------------------------
# pass 1: structure + content hash
# --------------------------------------------------------------------------
def scan_structure(run):
    profile=verification.count_items(lambda:_batches(run.db,run.id))
    try:verification.check_structure(profile,run.folds)
    except StructuralError as exc:
        _reject(run.db,run.packet,str(exc))
        return None,None
    hasher=canonical.ContentHasher(run.parameters)
    for position,reference,values,fold,predicted,posterior in _batches(run.db,run.id):
        hasher.update(canonical.item_digest(canonical.item_payload(
            position,reference,values,fold,predicted,posterior)))
    return profile,hasher.hexdigest()


# --------------------------------------------------------------------------
# pass 2: fold scalers
# --------------------------------------------------------------------------
def build_scalers(run,state):
    sequence=fold_sequence(state['counts'],state['order'],run.folds)
    assigner=FoldAssigner(sequence)
    accumulators=[Welford(len(verification.FEATURES)) for _ in range(run.folds)]
    for item in _batches(run.db,run.id):
        own=assigner.fold(str(item[2]['label']))
        sample=features(item[2])
        for index in range(run.folds):
            if index!=own:accumulators[index].add(sample)
    means=[];scales=[]
    for accumulator in accumulators:
        means.append(accumulator.mean)
        scales.append([math.sqrt(value) if value>0 else 1.0
                       for value in accumulator.variance()])
    return means,scales


# --------------------------------------------------------------------------
# pass 3: fold models
# --------------------------------------------------------------------------
def build_models(run,state):
    sequence=fold_sequence(state['counts'],state['order'],run.folds)
    assigner=FoldAssigner(sequence)
    means=state['means'];scales=state['scales']
    global_stats=[Welford(len(verification.FEATURES)) for _ in range(run.folds)]
    class_stats=[defaultdict(lambda:Welford(len(verification.FEATURES)))
                 for _ in range(run.folds)]
    for item in _batches(run.db,run.id):
        label=str(item[2]['label'])
        own=assigner.fold(label)
        sample=features(item[2])
        for index in range(run.folds):
            if index==own:continue
            scaled=[(sample[j]-means[index][j])/scales[index][j]
                    for j in range(len(verification.FEATURES))]
            global_stats[index].add(scaled)
            class_stats[index][label].add(scaled)
    smoothing=float(run.parameters['smoothing'])
    models=[]
    for index in range(run.folds):
        epsilon=smoothing*max(global_stats[index].variance())
        classes=sorted(class_stats[index])
        total=sum(stat.count for stat in class_stats[index].values())
        models.append({'fold':index+1,'classes':classes,
                       'theta':[class_stats[index][label].mean for label in classes],
                       'var':[[value+epsilon for value in class_stats[index][label].variance()]
                              for label in classes],
                       'prior':[class_stats[index][label].count/total for label in classes],
                       'train_count':total,'mean':means[index],'scale':scales[index],
                       'epsilon':epsilon})
    return models


# --------------------------------------------------------------------------
# pass 4: item-by-item verification
# --------------------------------------------------------------------------
def verify_items(run,packet,state):
    """Pass 4: batch-by-batch item comparison with a checkpoint per batch.

    Each batch is its own short transaction so a crash loses at most
    ``CHECKPOINT_EVERY`` items; the fold cursor is persisted with the batch, so
    the resume reads exactly the remaining items.
    """
    sequence=fold_sequence(state['counts'],state['order'],run.folds)
    assigner=FoldAssigner(sequence,cursor=state.get('cursor'))
    models=state['model']
    state['through']=int(state.get('through') or 0)
    while True:
        start=state['through']
        batch=list(run.items(start,CHECKPOINT_EVERY))
        if not batch:break
        result,ok=verification.verify_items(lambda:iter(batch),assigner,models,state)
        if not ok:
            _fail(run,result)
            return {'status':'diverged','divergence':result['first_divergence']}
        run.save(result,'items',result['through'])
    _verified(run,packet,state)
    return {'status':'verified'}


def _fail(run,state):
    divergence=state['first_divergence']
    run.db.execute(update(Packet)
                   .where(Packet.id==run.id,Packet.verify_token==run.token)
                   .values(state='failed',verify_phase='diverged',divergence=divergence,
                           verify_state=_strip(state),verify_lease_until=0,
                           error='第 '+str(divergence['position'])+' 件首次出现分歧（'
                                 +divergence['reference']+'）；已逐件核对至第 '
                                 +str(state['checked'])+' 件。整册不入库'))
    log(run.db,'worker','packet.diverged',run.id,divergence['reference'])
    run.db.commit()


def _verified(run,packet,state):
    summary=verification.summary(state,state['model'],packet.expected_count,
                                 packet.content_hash)
    run.db.execute(update(Packet)
                   .where(Packet.id==run.id,Packet.verify_token==run.token)
                   .values(state='verified',verify_phase='done',result=summary,
                           verified_through=packet.expected_count,
                           verify_state=_strip(state),verify_lease_until=0,error=''))
    log(run.db,'worker','packet.verified',run.id,packet.content_hash)
    run.db.commit()


def _reject(db,packet,message):
    db.execute(update(Packet).where(Packet.id==packet.id)
               .values(state='rejected',error=message[:2000],verify_phase='rejected',
                       result=None,divergence=None,verify_lease_until=0))
    db.commit()
