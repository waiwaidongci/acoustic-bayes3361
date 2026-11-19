import {domain} from '../constants/domain.js';
import {research} from '../stores/research.js';
import {session} from '../stores/session.js';
import {packetsApi} from '../api/packets.js';
import {packetStatus,conflictTable,verificationSummary} from '../components/common/packet.js';
import {esc,fmt} from '../utils/format.js';
import {bind,toast} from '../hooks/actions.js';

export async function packetsPage(){
  const packets=await packetsApi.list();
  research.packets=packets;
  if(research.packetId==null&&packets.length)research.packetId=packets[0].id;
  const packet=research.packetId!=null?await packetsApi.get(research.packetId):null;
  research.activePacket=packet;
  const conflicts=packet?await packetsApi.conflicts(packet.id):[];
  let items=[];
  if(packet&&['verified','failed','admitted'].includes(packet.state)){
    items=await packetsApi.items(packet.id,research.packetOffset,25);
  }
  return '<section class="page-heading"><p class="eyebrow">07 / INTAKE & CROSS-CHECK</p><h1>逐件核对通过，整册才入库。</h1><p>分片可乱序到达、重试不重复收件、同位置异值先隔离；封包后由独立复算逐件比对归折、预测与后验，任何分歧在首处即停并整册退回。</p></section>'+
    '<div class="split"><section><label class="compact">判读册<select id="packet-select">'+
    (packets.length?packets.map(p=>'<option value="'+p.id+'" '+(p.id==research.packetId?'selected':'')+'>#'+p.id+' · '+esc(p.name)+' · '+esc(p.book_key)+'</option>').join(''):'<option value="">尚无判读册</option>')+
    '</select></label>'+(session.writable?'<details><summary>接收分片（JSON）</summary><form id="shard-form"><label>分片编号<input name="shard_key" required placeholder="s-0001"></label><label>分片内容<textarea name="body" rows="8" required placeholder=\'{"items":[{"position":1,"reference":"TX-0001","values":{"rms":1.2,"centroid":900,"kurtosis":2.8,"label":"正常"},"claim":{"fold":1,"predicted":"正常","posterior":0.99}}]}\'></textarea></label><button type="submit">接收分片 →</button></form></details>':'')+
    (!packet?'<div class="empty">选择一个判读册查看收件与核对进度。</div>':'<article class="packet-card"><div class="run-title"><h2>'+esc(packet.name)+'</h2>'+packetStatus(packet.state)+'<small>#'+packet.id+' · 版本 '+packet.version+'</small></div><p class="fine">封包编号 '+esc(packet.book_key)+' · 参数 '+esc(JSON.stringify(packet.parameters))+' · 已收 '+packet.received_count+' / '+packet.expected_count+' 件 · 未处理异值 '+packet.open_conflicts+' 处'+(packet.missing_count?' · 缺 '+packet.missing_count+' 个位置（'+esc((packet.missing_preview||[]).join(', '))+'）':'')+'</p>'+
      (packet.content_hash?'<p class="fine">封包内容 SHA256<br><code>'+esc(packet.content_hash)+'</code></p>':'')+
      (packet.error?'<div class="failure"><p>'+esc(packet.error)+'</p></div>':'')+
      (packet.divergence?'<div class="failure"><h2>首处分歧 · 第 '+packet.divergence.position+' 件</h2><p>样本 '+esc(packet.divergence.reference)+' · 实际类型 '+esc(packet.divergence.actual)+' · 分歧字段 <b>'+esc(packet.divergence.kind)+'</b></p><p>来件值 '+esc(JSON.stringify(packet.divergence.claimed))+' · 复算值 '+esc(JSON.stringify(packet.divergence.computed))+'</p></div>':'')+
      verificationSummary(packet.result)+
      '<div class="packet-actions">'+
      (packet.state==='collecting'&&session.writable?'<button id="seal-packet">封包并送独立核验 →</button>':'')+
      (packet.state==='verifying'&&session.writable?'<button id="retry-packet" class="secondary">中断后按已核位置继续 ↗</button>':'')+
      (packet.state==='sealed'&&session.writable?'<button id="retry-packet" class="secondary">送入核验队列 ↗</button>':'')+
      (packet.state==='failed'?'<p class="fine">已确认首处分歧，整册退回；请合作站更正来件后以原判读册编号重新送检。</p>':'')+
      (packet.state==='rejected'?'<p class="fine">结构校验未通过，整册退回；请修正样本字段或参数后重新封包送检。</p>':'')+
      (packet.state==='verified'&&session.user.role==='admin'?'<button id="admit-packet">逐件核对通过，整册入库 ✓</button>':'')+
      (packet.state==='admitted'?'<p class="fine">本册已入库，同一包编号仅保留这一册。</p>':'')+
      '</div>'+
      (items.length?'<h2>样本明细 <small>第 '+(research.packetOffset+1)+' 起，共 '+packet.expected_count+' 件</small></h2>'+itemTable(items)+'<div class="pagination"><button id="pkt-prev" '+(research.packetOffset===0?'disabled':'')+'>上一页</button><span>'+Math.floor(research.packetOffset/25+1)+' / '+Math.max(1,Math.ceil(packet.expected_count/25))+'</span><button id="pkt-next" '+((research.packetOffset+25)>=packet.expected_count?'disabled':'')+'>下一页</button></div>':'')+
      '<h2>位置异值隔离</h2>'+conflictTable(conflicts)+
      '</article>')+
    '</section><aside class="method-sheet"><p class="eyebrow">CROSS-CHECK / '+esc(domain.code)+'</p><h2>独立复算，不与来件共用实现。</h2><ol><li>乱序分片按位置落库，重试以内容哈希去重</li><li>同位置异值进入隔离区，封包前须处理</li><li>封包冻结次序、参数与内容哈希</li><li>固定内存流式复算每件归折、预测、后验</li><li>发现首处分歧即停，整册退回；全部一致才入库</li></ol><p class="fine">每折仍以包内全部训练样本拟合；后验是模型分布，不是检修结论。</p></aside></div>';
}

function itemTable(items){
  return '<div class="table-wrap"><table><thead><tr><th>位置</th><th>样本编号</th><th>rms</th><th>centroid</th><th>kurtosis</th><th>类型</th><th>来件折</th><th>来件预测</th><th>后验</th></tr></thead><tbody>'+
    items.map(it=>'<tr><td>'+it.position+'</td><td>'+esc(it.reference)+'</td><td>'+esc(fmt(it.values.rms))+'</td><td>'+esc(fmt(it.values.centroid))+'</td><td>'+esc(fmt(it.values.kurtosis))+'</td><td>'+esc(it.values.label)+'</td><td>'+it.claimed_fold+'</td><td>'+esc(it.claimed_predicted)+'</td><td>'+esc(fmt(it.claimed_posterior))+'</td></tr>').join('')+'</tbody></table></div>';
}

export function bindPackets(refresh){
  bind('#packet-select','change',async e=>{research.packetId=Number(e.target.value);research.packetOffset=0;await refresh()});
  bind('#pkt-prev','click',async()=>{research.packetOffset=Math.max(0,research.packetOffset-25);await refresh()});
  bind('#pkt-next','click',async()=>{research.packetOffset+=25;await refresh()});
  bind('#seal-packet','click',async()=>{try{const out=await packetsApi.seal(research.packetId);toast('已封包并送独立核验：'+out.content_hash.slice(0,16)+'…');await refresh()}catch(e){toast(e.message,true)}});
  bind('#retry-packet','click',async()=>{try{await packetsApi.verify(research.packetId);toast('已按已核位置重新发起核验');await refresh()}catch(e){toast(e.message,true)}});  bind('#admit-packet','click',async()=>{try{const out=await packetsApi.admit(research.packetId);toast(out.admitted?'整册已入库':'该册此前已入库');await refresh()}catch(e){toast(e.message,true)}});
  for(const b of document.querySelectorAll('[data-keep-existing],[data-keep-incoming]')){
    b.onclick=async()=>{try{const id=b.dataset.keepExisting||b.dataset.keepIncoming;const keep=b.dataset.keepExisting?'existing':'incoming';await packetsApi.resolve(research.packetId,id,{keep});toast('冲突已处理');await refresh()}catch(e){toast(e.message,true)}};
  }
  bind('#shard-form','submit',async e=>{e.preventDefault();const form=e.target,button=form.querySelector('button');if(button)button.disabled=true;try{const data=Object.fromEntries(new FormData(form));const payload=JSON.parse(data.body);payload.shard_key=data.shard_key;const out=await packetsApi.receive(research.packetId,payload);toast('分片 '+out.status+'：接收 '+out.accepted+' 件，异值 '+out.conflicts+' 处');form.reset();await refresh()}catch(error){toast('分片接收失败：'+error.message,true)}finally{if(button)button.disabled=false}});
}
