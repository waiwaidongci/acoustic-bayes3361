"""Judgement-book intake, sealing, conflicts and admission.

Workflow
--------
collecting --(all positions received, no open conflicts)--> seal
seal       --(independent worker逐件核对)--> verified | failed | rejected
verified   --(all items matched)--> admitted

The same ``book_key`` may be re-submitted; intake is idempotent (a retried shard
with identical content is a no-op, a retried item with identical content is a
no-op) and admission is idempotent (only one book per ``book_key`` is ever
stored). A different value arriving for an already-received position is never
silently overwritten: it is isolated as an open conflict until a reviewer
resolves it.
"""
from fastapi import HTTPException
from redis import Redis
from sqlalchemy import func,select
from sqlalchemy.exc import IntegrityError

from ..config.settings import REDIS_URL
from ..models.packet import Packet,PacketAdmission,PacketConflict,PacketItem,PacketShard
from ..utils import canonical
from ..utils.serialization import now
from .audit import log
from .domain_validation import validate_parameters,validate_record

queue=Redis.from_url(REDIS_URL)


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def _packet_or_404(db,id):
    packet=db.get(Packet,id)
    if not packet:raise HTTPException(404,'未找到判读册')
    db.refresh(packet)
    return packet


def _locked_packet(db,id,expected_version,label='判读册'):
    """Row-lock the packet and enforce optimistic concurrency."""
    packet=db.get(Packet,id,with_for_update=True)
    if not packet:raise HTTPException(404,'未找到判读册')
    if expected_version is not None and int(expected_version)!=int(packet.version):
        db.rollback()
        raise HTTPException(409,f'{label}已被其他操作更新（版本 {packet.version}），请重新读取后重试')
    return packet


def _touch(packet):
    packet.version=int(packet.version or 0)+1
    packet.updated_at=now()


def _item_payload(item):
    return canonical.item_payload(item.position,item.reference,item.values,item.claimed_fold,
                                  item.claimed_predicted,item.claimed_posterior)


def _public(packet,drop=('manifest','verify_state','checkpoint')):
    payload={column.name:getattr(packet,column.name) for column in packet.__table__.columns}
    for key in drop:payload.pop(key,None)
    return payload


def _open_conflicts(db,id):
    return int(db.scalar(select(func.count()).select_from(PacketConflict)
                         .where(PacketConflict.packet_id==id,
                                PacketConflict.state=='open')) or 0)


def _finite(value):
    try:number=float(value)
    except (TypeError,ValueError):return False
    return number==number and abs(number)!=float('inf')


def _validate_claim(item):
    if item.claim.fold<1:raise HTTPException(422,'判读折号必须为正整数')
    if not _finite(item.claim.posterior) or not 0<=float(item.claim.posterior)<=1:
        raise HTTPException(422,'后验概率必须是 0 到 1 之间的有限数')
    if not item.claim.predicted.strip():raise HTTPException(422,'预测类型不能为空')


# --------------------------------------------------------------------------
# create / read
# --------------------------------------------------------------------------
def create_packet(db,user,data):
    try:parameters=validate_parameters(data.parameters)
    except ValueError as exc:raise HTTPException(422,str(exc))
    existing=db.scalar(select(Packet).where(Packet.book_key==data.book_key))
    if existing:
        if (existing.name,existing.description,existing.parameters,existing.expected_count)!=(
                data.name,data.description or '',parameters,data.expected_count):
            raise HTTPException(409,'同一判读册编号已存在，但名称、参数或样本数不一致')
        return _public(existing)
    packet=Packet(book_key=data.book_key,name=data.name,description=data.description or '',
                  expected_count=data.expected_count,parameters=parameters,state='collecting',
                  version=0,received_count=0,conflict_count=0,created_at=now(),updated_at=now())
    db.add(packet)
    try:db.flush()
    except IntegrityError:
        db.rollback()
        again=db.scalar(select(Packet).where(Packet.book_key==data.book_key))
        if again:return _public(again)
        raise
    log(db,user,'packet.created',packet.id,data.book_key)
    db.commit()
    return _public(packet)


def list_packets(db):
    return [_public(packet) for packet in db.scalars(select(Packet).order_by(Packet.id.desc()))]


def get_packet(db,id):
    packet=_packet_or_404(db,id)
    payload=_public(packet)
    # Aggregate instead of materialising every position: a 100k book must not
    # allocate a 100k-element set on each status poll.
    stats=db.execute(select(func.count(PacketItem.id),func.min(PacketItem.position),
                            func.max(PacketItem.position))
                     .where(PacketItem.packet_id==id)).one()
    count,low,high=int(stats[0] or 0),stats[1],stats[2]
    contiguous=(count==packet.expected_count and low==1 and high==packet.expected_count)
    missing_count=0 if contiguous else max(0,packet.expected_count-count)
    payload.update({'open_conflicts':_open_conflicts(db,id),
                    'received_count':count,
                    'missing_count':missing_count,
                    'positions_contiguous':contiguous,
                    'missing_preview':[] if contiguous else _missing_preview(db,id,packet.expected_count),
                    'verify_state_summary':_state_summary(packet)})
    return payload


def _missing_preview(db,id,expected,cap=50):
    """Up to ``cap`` missing positions, streamed so memory stays bounded."""
    found=[]
    seen=0
    cursor=0
    while len(found)<cap:
        row=db.execute(select(PacketItem.position).where(PacketItem.packet_id==id,
                                                         PacketItem.position>cursor)
                       .order_by(PacketItem.position).limit(1)).first()
        following=row[0] if row else expected+1
        for position in range(cursor+1,min(following,expected+1)):
            found.append(position)
            if len(found)>=cap:break
        if not row or following>expected:break
        cursor=following
    return found


def _state_summary(packet):
    state=packet.verify_state or {}
    return {'phase':state.get('phase',''),'checked':int(state.get('through') or 0),
            'verified_through':int(packet.verified_through or 0)}


def list_items(db,id,offset=0,limit=50):
    _packet_or_404(db,id)
    items=db.execute(select(PacketItem).where(PacketItem.packet_id==id)
                     .order_by(PacketItem.position).offset(max(0,offset))
                     .limit(min(200,max(1,limit)))).scalars()
    return [{column.name:getattr(item,column.name) for column in item.__table__.columns}
            for item in items]


def list_conflicts(db,id):
    _packet_or_404(db,id)
    return [{column.name:getattr(item,column.name) for column in item.__table__.columns}
            for item in db.scalars(select(PacketConflict).where(PacketConflict.packet_id==id)
                                   .order_by(PacketConflict.id.desc()))]


# --------------------------------------------------------------------------
# shard intake
# --------------------------------------------------------------------------
def receive_shard(db,user,id,data):
    packet=_locked_packet(db,id,data.expected_version)
    if packet.state!='collecting':
        raise HTTPException(409,f'判读册已封包（{packet.state}），不再接收新分片')
    positions=[item.position for item in data.items]
    if len(set(positions))!=len(positions):raise HTTPException(422,'同一分片内样本位置重复')
    if min(positions)<1:raise HTTPException(422,'样本位置必须从 1 开始')
    if max(positions)>packet.expected_count:
        raise HTTPException(422,f'样本位置超出封包范围 1..{packet.expected_count}')
    for item in data.items:_validate_claim(item)
    normalised=[]
    for item in data.items:
        try:values=validate_record(item.values)
        except ValueError as exc:
            raise HTTPException(422,'位置 '+str(item.position)+'：'+str(exc))
        normalised.append((item,values))
    payloads=[canonical.item_payload(item.position,item.reference,values,
                                     item.claim.fold,item.claim.predicted,item.claim.posterior)
              for item,values in normalised]
    digest=canonical.shard_digest(payloads)
    existing=db.scalar(select(PacketShard).where(PacketShard.packet_id==id,
                                                 PacketShard.shard_key==data.shard_key))
    if existing:
        if existing.digest==digest and existing.item_count==len(payloads):
            return {'status':'duplicate','shard_key':data.shard_key,'accepted':0,
                    'conflicts':0,'received_count':packet.received_count,'version':packet.version}
        raise HTTPException(409,f'分片 {data.shard_key} 已以不同内容收件；重试不得改动分片内容')
    shard=PacketShard(packet_id=id,shard_key=data.shard_key,item_count=len(payloads),
                      digest=digest,created_at=now())
    db.add(shard);db.flush()
    present={row.position:row for row in db.scalars(
        select(PacketItem).where(PacketItem.packet_id==id,
                                 PacketItem.position.in_(positions)))}
    accepted=0;conflicts=0
    for payload,(item,values) in zip(payloads,normalised):
        prior=present.get(item.position)
        if prior is not None:
            if canonical.item_digest(_item_payload(prior))==canonical.item_digest(payload):
                continue
            db.add(PacketConflict(packet_id=id,position=item.position,existing_item_id=prior.id,
                                  incoming=payload,incoming_digest=canonical.item_digest(payload),
                                  incoming_shard_key=data.shard_key,state='open',created_at=now()))
            conflicts+=1
            continue
        db.add(PacketItem(packet_id=id,position=item.position,reference=item.reference,
                          values=values,claimed_fold=item.claim.fold,
                          claimed_predicted=item.claim.predicted,
                          claimed_posterior=float(item.claim.posterior),shard_id=shard.id,
                          created_at=now()))
        accepted+=1
    packet.received_count=int(packet.received_count)+accepted
    packet.conflict_count=int(packet.conflict_count)+conflicts
    _touch(packet)
    log(db,user,'packet.shard',id,f'{data.shard_key}: +{accepted}, open conflicts +{conflicts}')
    try:db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(409,'分片与并发写入冲突，请以相同内容重试')
    return {'status':'accepted' if accepted else ('conflicts' if conflicts else 'duplicate'),
            'shard_key':data.shard_key,'accepted':accepted,'conflicts':conflicts,
            'received_count':packet.received_count,'version':packet.version}


# --------------------------------------------------------------------------
# conflicts
# --------------------------------------------------------------------------
def resolve_conflict(db,user,id,conflict_id,data):
    packet=_locked_packet(db,id,data.expected_version)
    conflict=db.get(PacketConflict,conflict_id,with_for_update=True)
    if not conflict or conflict.packet_id!=id:raise HTTPException(404,'未找到冲突记录')
    if conflict.state!='open':raise HTTPException(409,'该冲突已处理')
    if packet.state!='collecting':raise HTTPException(409,'判读册已封包，冲突须在封包前处理')
    if data.keep=='incoming':
        prior=db.get(PacketItem,conflict.existing_item_id,with_for_update=True)
        if not prior:raise HTTPException(409,'原样本已不存在')
        payload=conflict.incoming
        try:values=validate_record(payload['values'])
        except ValueError as exc:raise HTTPException(422,'来件业务字段无效：'+str(exc))
        prior.values=values
        if payload.get('reference'):prior.reference=payload['reference']
        prior.claimed_fold=int(payload['claim']['fold'])
        prior.claimed_predicted=payload['claim']['predicted']
        prior.claimed_posterior=float(payload['claim']['posterior'])
        prior.created_at=now()
    conflict.state='resolved'
    conflict.resolution=data.keep
    conflict.resolved_by=user.email
    conflict.resolved_at=now()
    packet.conflict_count=max(0,int(packet.conflict_count)-1)
    _touch(packet)
    log(db,user,'packet.conflict',id,f'#{conflict_id} keep {data.keep}')
    db.commit()
    return {'resolved':conflict_id,'keep':data.keep,'version':packet.version,
            'open_conflicts':_open_conflicts(db,id)}


# --------------------------------------------------------------------------
# seal
# --------------------------------------------------------------------------
def seal_packet(db,user,id,data):
    packet=_locked_packet(db,id,data.expected_version)
    if packet.state!='collecting':raise HTTPException(409,f'判读册已封包（{packet.state}）')
    open_conflicts=_open_conflicts(db,id)
    if open_conflicts:raise HTTPException(409,f'仍有 {open_conflicts} 处位置异值未处理')
    hasher=canonical.ContentHasher(packet.parameters)
    count=0
    previous=None
    for item in db.scalars(select(PacketItem).where(PacketItem.packet_id==id)
                           .order_by(PacketItem.position)
                           .execution_options(yield_per=2000)):
        hasher.update(canonical.item_digest(_item_payload(item)))
        if previous is not None and item.position!=previous+1:
            raise HTTPException(409,'封包位置不连续，请检查收件')
        previous=item.position
        count+=1
    if count!=packet.expected_count or previous!=packet.expected_count:
        raise HTTPException(409,f'收件不完整：已收 {count} 件，应为 {packet.expected_count} 件')
    digest=hasher.hexdigest()
    shards=db.scalar(select(func.count()).select_from(PacketShard)
                     .where(PacketShard.packet_id==id)) or 0
    # Compact manifest: the per-item digests must not be materialised, otherwise
    # a 100k-item book would cost tens of MB on every load. The content hash plus
    # a re-stream at verification time is sufficient to detect any post-seal edit.
    packet.content_hash=digest
    packet.manifest={'expected_count':packet.expected_count,'content_hash':digest,
                     'shard_count':int(shards)}
    packet.state='sealed'
    packet.sealed_at=now()
    packet.verify_state={'phase':'structure'}
    packet.verify_phase='structure'
    packet.verify_token=''
    packet.verify_lease_until=0
    packet.verified_through=0
    packet.result=None
    packet.divergence=None
    packet.error=''
    _touch(packet)
    log(db,user,'packet.sealed',id,digest)
    db.commit()
    queued=_enqueue(packet.id)
    if not queued:
        packet.error='核验队列暂不可用，可在研究页面重新发起核验'
        db.commit()
    return {'sealed':packet.id,'content_hash':digest,'items':packet.expected_count,
            'queued':queued,'version':packet.version}


def _enqueue(packet_id):
    try:
        queue.rpush('packet:verify',packet_id)
        return True
    except Exception:
        return False


# --------------------------------------------------------------------------
# resume / retry
# --------------------------------------------------------------------------
def retry_verification(db,user,id):
    packet=_packet_or_404(db,id)
    if packet.state=='failed':
        raise HTTPException(409,'该册已确认首处分歧，需更正来件后按原内容重新送检，不能原地重跑')
    if packet.state not in ('sealed','verifying'):
        raise HTTPException(409,'只有已封包且未入库的判读册可以重新发起核验')
    # An explicit retry supersedes a crashed/interrupted worker: release the old
    # lease. The token guard makes any still-running old worker stop writing, so
    # only one pass can persist progress.
    packet.verify_lease_until=0
    log(db,user,'packet.retry',id)
    db.commit()
    if not _enqueue(packet.id):raise HTTPException(503,'核验队列暂不可用，请稍后重试')
    return {'queued':True,'id':packet.id,'state':packet.state,'resume_from':packet.verified_through}


# --------------------------------------------------------------------------
# admission
# --------------------------------------------------------------------------
def admit_packet(db,user,id):
    packet=_locked_packet(db,id,None)
    if db.scalar(select(PacketAdmission.id).where(PacketAdmission.packet_id==packet.id)):
        return {'admitted':False,'packet_id':packet.id,'book_key':packet.book_key,
                'reason':'此判读册已经入库'}
    if packet.state!='verified' or not (packet.result or {}).get('all_matched'):
        raise HTTPException(409,'逐件核对未通过，整册不可入库')
    prior=db.scalar(select(PacketAdmission).where(PacketAdmission.book_key==packet.book_key))
    if prior:
        raise HTTPException(409,'同一包编号 {0} 已有入库判读册 #{1}'.format(
            packet.book_key,prior.packet_id))
    db.add(PacketAdmission(packet_id=packet.id,book_key=packet.book_key,
                           content_hash=packet.content_hash,item_count=packet.expected_count,
                           admitted_at=now()))
    try:db.flush()
    except IntegrityError:
        db.rollback()
        raise HTTPException(409,'同一包编号已有入库判读册')
    packet.state='admitted'
    _touch(packet)
    log(db,user,'packet.admitted',id,packet.book_key)
    db.commit()
    return {'admitted':True,'packet_id':packet.id,'book_key':packet.book_key,
            'content_hash':packet.content_hash,'items':packet.expected_count}


def list_admissions(db):
    return [{column.name:getattr(item,column.name) for column in item.__table__.columns}
            for item in db.scalars(select(PacketAdmission).order_by(PacketAdmission.id.desc()))]
