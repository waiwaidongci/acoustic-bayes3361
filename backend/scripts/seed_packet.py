"""Seed one sealed demo judgement book from the first acoustic source.

The book is produced with the real algorithm, then handed over in shuffled
shards so the intake path (out-of-order, per-shard) is exercised. The seed is
idempotent: a second run finds the same ``book_key`` and does nothing.
"""
import json,logging,random

from sqlalchemy import select

from src.config.database import Session
from src.config.settings import CONFIG
from src.models.packet import Packet
from src.models.record import Record
from src.models.source import Source
from src.services.algorithm import calculate
from src.services.packets import create_packet,receive_shard,seal_packet

BOOK_KEY='BOOK-DEMO-202611-01'
SHARD_SIZE=40


class _User:
    email='seed@'+CONFIG['slug']+'.demo'
    role='admin'


class _Items(list):
    pass


def _shard(key,rows):
    attributes={'shard_key':key,'expected_version':None,'items':[]}
    for position,reference,values,fold,predicted,posterior in rows:
        claim=type('Claim',(),{'fold':fold,'predicted':predicted,'posterior':posterior})()
        attributes['items'].append(type('Item',(),{'position':position,'reference':reference,
                                                   'values':values,'claim':claim})())
    return type('Shard',(),attributes)()


def seed_packet(db):
    if db.scalar(select(Packet.id).where(Packet.book_key==BOOK_KEY)):return None
    source=db.scalar(select(Source).where(Source.key==CONFIG['code']+'-202611-01'))
    if not source:return None
    records=list(db.scalars(select(Record).where(Record.source_id==source.id)
                            .order_by(Record.id)))
    if len(records)<12:return None
    parameters=CONFIG['default_parameters']
    payload=[{'reference':record.reference,'values':record.values} for record in records]
    try:result=calculate(payload,parameters)
    except ValueError as exc:
        logging.warning('seed packet skipped: %s',exc)
        return None
    fold_of={}
    for fold in result['folds']:
        for index in fold['validation_indices']:fold_of[index]=fold['fold']
    rows=[(index+1,row['reference'],records[index].values,fold_of[index],
           row['predicted'],row['posterior']) for index,row in enumerate(result['rows'])]
    rng=random.Random(CONFIG.get('seed_count',42))
    order=list(range(0,len(rows),SHARD_SIZE))
    rng.shuffle(order)
    packet=create_packet(db,_User(),type('Input',(),{
        'book_key':BOOK_KEY,'name':'合作站首册 · 声检判读',
        'description':'由首份来源的真实算法结果切分而来，仅用于演示逐件核对流程。',
        'expected_count':len(rows),'parameters':parameters})())
    for index in order:
        receive_shard(db,_User(),packet['id'],
                      _shard(f'demo-{index//SHARD_SIZE:03d}',rows[index:index+SHARD_SIZE]))
    seal_packet(db,_User(),packet['id'],type('Seal',(),{'expected_version':None})())
    return packet['id']
