from typing import Literal
from pydantic import BaseModel,ConfigDict,Field

class ClaimInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    fold:int
    predicted:str=Field(min_length=1,max_length=160)
    posterior:float

class ShardItemInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    position:int
    reference:str=Field(min_length=1,max_length=120)
    values:dict
    claim:ClaimInput

class ShardInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    shard_key:str=Field(min_length=1,max_length=80)
    expected_version:int|None=None
    items:list[ShardItemInput]=Field(min_length=1,max_length=2000)

class PacketInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    book_key:str=Field(min_length=1,max_length=160)
    name:str=Field(min_length=2,max_length=160)
    description:str=Field(default='',max_length=2000)
    expected_count:int=Field(ge=1,le=1000000)
    parameters:dict

class SealInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    expected_version:int|None=None

class ConflictResolveInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    keep:Literal['existing','incoming']
    expected_version:int|None=None
