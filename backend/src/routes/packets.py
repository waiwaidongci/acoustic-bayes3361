from fastapi import APIRouter,Depends
from ..config.database import db_session
from ..schemas.packet import (ConflictResolveInput,PacketInput,SealInput,ShardInput)
from ..services.auth import admin,current_user,writer
from ..services import packets
router=APIRouter(prefix='/packets',tags=['packets'])

@router.get('')
def listing(db=Depends(db_session),user=Depends(current_user)):
    return packets.list_packets(db)

@router.post('')
def create(data:PacketInput,db=Depends(db_session),user=Depends(writer)):
    return packets.create_packet(db,user,data)

@router.get('/admissions')
def admissions(db=Depends(db_session),user=Depends(current_user)):
    return packets.list_admissions(db)

@router.get('/{id}')
def detail(id:int,db=Depends(db_session),user=Depends(current_user)):
    return packets.get_packet(db,id)

@router.get('/{id}/items')
def items(id:int,offset:int=0,limit:int=50,db=Depends(db_session),user=Depends(current_user)):
    return packets.list_items(db,id,offset,limit)

@router.get('/{id}/conflicts')
def conflicts(id:int,db=Depends(db_session),user=Depends(current_user)):
    return packets.list_conflicts(db,id)

@router.post('/{id}/shards')
def receive(id:int,data:ShardInput,db=Depends(db_session),user=Depends(writer)):
    return packets.receive_shard(db,user,id,data)

@router.post('/{id}/conflicts/{conflict_id}/resolve')
def resolve(id:int,conflict_id:int,data:ConflictResolveInput,db=Depends(db_session),
            user=Depends(admin)):
    return packets.resolve_conflict(db,user,id,conflict_id,data)

@router.post('/{id}/seal')
def seal(id:int,data:SealInput,db=Depends(db_session),user=Depends(writer)):
    return packets.seal_packet(db,user,id,data)

@router.post('/{id}/verify')
def reverify(id:int,db=Depends(db_session),user=Depends(writer)):
    return packets.retry_verification(db,user,id)

@router.post('/{id}/admit')
def admit(id:int,db=Depends(db_session),user=Depends(admin)):
    return packets.admit_packet(db,user,id)
