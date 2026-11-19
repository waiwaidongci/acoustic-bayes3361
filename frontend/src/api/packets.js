import {api} from './client.js';
export const packetsApi={
  list:()=>api('/packets'),
  get:id=>api('/packets/'+id),
  items:(id,offset=0,limit=50)=>api('/packets/'+id+'/items?offset='+offset+'&limit='+limit),
  conflicts:id=>api('/packets/'+id+'/conflicts'),
  receive:(id,payload)=>api('/packets/'+id+'/shards',{method:'POST',body:JSON.stringify(payload)}),
  resolve:(id,conflictId,payload)=>api('/packets/'+id+'/conflicts/'+conflictId+'/resolve',{method:'POST',body:JSON.stringify(payload)}),
  seal:(id,payload={})=>api('/packets/'+id+'/seal',{method:'POST',body:JSON.stringify(payload)}),
  verify:id=>api('/packets/'+id+'/verify',{method:'POST',body:JSON.stringify({})}),
  admit:id=>api('/packets/'+id+'/admit',{method:'POST',body:JSON.stringify({})}),
  admissions:()=>api('/packets/admissions')
};
