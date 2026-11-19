from sqlalchemy import Column,Integer,String,Text,JSON,Float,ForeignKey,UniqueConstraint
from ..config.database import Base
from ..config.settings import CONFIG


class Packet(Base):
    __tablename__=CONFIG['packet_table']
    id=Column(Integer,primary_key=True)
    book_key=Column(String(160),unique=True,nullable=False)
    name=Column(String(160),nullable=False)
    description=Column(Text,default='')
    expected_count=Column(Integer,nullable=False)
    parameters=Column(JSON,nullable=False)
    state=Column(String(20),default='collecting',nullable=False)
    version=Column(Integer,default=0,nullable=False)
    received_count=Column(Integer,default=0,nullable=False)
    conflict_count=Column(Integer,default=0,nullable=False)
    content_hash=Column(String(64),default='')
    manifest=Column(JSON,nullable=True)
    sealed_at=Column(String(40),default='')
    verify_phase=Column(String(20),default='')
    verify_token=Column(String(64),default='')
    verify_lease_until=Column(Float,default=0,nullable=False)
    verify_state=Column(JSON,nullable=True)
    verified_through=Column(Integer,default=0,nullable=False)
    result=Column(JSON,nullable=True)
    divergence=Column(JSON,nullable=True)
    error=Column(Text,default='')
    created_at=Column(String(40),nullable=False)
    updated_at=Column(String(40),default='')


class PacketShard(Base):
    __tablename__=CONFIG['shard_table']
    __table_args__=(UniqueConstraint('packet_id','shard_key',name='uq_packet_shard_key'),)
    id=Column(Integer,primary_key=True)
    packet_id=Column(Integer,ForeignKey(CONFIG['packet_table']+'.id'),nullable=False)
    shard_key=Column(String(80),nullable=False)
    item_count=Column(Integer,nullable=False)
    digest=Column(String(64),nullable=False)
    created_at=Column(String(40),nullable=False)


class PacketItem(Base):
    __tablename__=CONFIG['item_table']
    __table_args__=(UniqueConstraint('packet_id','position',name='uq_packet_item_position'),)
    id=Column(Integer,primary_key=True)
    packet_id=Column(Integer,ForeignKey(CONFIG['packet_table']+'.id'),nullable=False)
    position=Column(Integer,nullable=False)
    reference=Column(String(120),nullable=False)
    values=Column(JSON,nullable=False)
    claimed_fold=Column(Integer,nullable=False)
    claimed_predicted=Column(String(160),nullable=False)
    claimed_posterior=Column(Float,nullable=False)
    shard_id=Column(Integer,ForeignKey(CONFIG['shard_table']+'.id'),nullable=False)
    created_at=Column(String(40),nullable=False)


class PacketConflict(Base):
    __tablename__=CONFIG['conflict_table']
    id=Column(Integer,primary_key=True)
    packet_id=Column(Integer,ForeignKey(CONFIG['packet_table']+'.id'),nullable=False)
    position=Column(Integer,nullable=False)
    existing_item_id=Column(Integer,nullable=False)
    incoming=Column(JSON,nullable=False)
    incoming_digest=Column(String(64),nullable=False)
    incoming_shard_key=Column(String(80),default='')
    state=Column(String(20),default='open',nullable=False)
    resolution=Column(String(20),default='')
    resolved_by=Column(String(120),default='')
    resolved_at=Column(String(40),default='')
    created_at=Column(String(40),nullable=False)


class PacketAdmission(Base):
    __tablename__=CONFIG['admission_table']
    __table_args__=(UniqueConstraint('book_key',name='uq_admission_book_key'),
                    UniqueConstraint('packet_id',name='uq_admission_packet'))
    id=Column(Integer,primary_key=True)
    packet_id=Column(Integer,ForeignKey(CONFIG['packet_table']+'.id'),nullable=False)
    book_key=Column(String(160),nullable=False)
    content_hash=Column(String(64),nullable=False)
    item_count=Column(Integer,nullable=False)
    admitted_at=Column(String(40),nullable=False)
