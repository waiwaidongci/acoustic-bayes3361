import {esc,fmt} from '../../utils/format.js';
import {packetStates} from '../../constants/enums.js';

export function packetStatus(state){return '<span class="status '+esc(state)+'">'+esc(packetStates[state]||state)+'</span>'}

export function packetLedger(packets,activeId){
  if(!packets.length)return '<div class="empty"><strong>还没有判读册。</strong><p>按合作方封包编号创建判读册后逐片接收样本。</p></div>';
  return '<div class="ledger">'+packets.map(p=>'<a href="#packets" data-packet="'+p.id+'" class="'+(p.id==activeId?'active':'')+'"><small>#'+p.id+'</small><strong>'+esc(p.name)+'</strong>'+packetStatus(p.state)+'<time>'+esc(p.book_key)+' · '+p.received_count+'/'+p.expected_count+'</time></a>').join('')+'</div>';
}

export function conflictTable(conflicts){
  if(!conflicts.length)return '<p class="fine">没有位置异值冲突。</p>';
  return '<div class="table-wrap"><table><thead><tr><th>位置</th><th>原样本</th><th>来件分片</th><th>原值</th><th>来件值</th><th>处理</th><th></th></tr></thead><tbody>'+conflicts.map(c=>{
    const incoming=c.incoming?.values||{};
    return '<tr><td>'+c.position+'</td><td>#'+c.existing_item_id+'</td><td>'+esc(c.incoming_shard_key)+'</td><td class="fine">'+esc(JSON.stringify(c.incoming?.claim||{}))+'</td><td>'+esc(fmt(incoming.rms))+' / '+esc(fmt(incoming.centroid))+' / '+esc(fmt(incoming.kurtosis))+' · '+esc(incoming.label||'')+'</td><td>'+esc(c.state==='open'?'待处理':c.resolution)+'</td><td>'+(c.state==='open'?'<button class="secondary" data-keep-existing="'+c.id+'">保留原值</button> <button class="secondary" data-keep-incoming="'+c.id+'">采用来件</button>':'')+'</td></tr>';
  }).join('')+'</tbody></table></div>';
}

export function verificationSummary(result,classes){
  if(!result)return '';
  const labels=result.classes||classes||[];
  const confusion='<h2>折外混淆矩阵 <small>独立复算</small></h2><div class="table-wrap"><table><thead><tr><th>实际 / 预测</th>'+labels.map(l=>'<th>'+esc(l)+'</th>').join('')+'</tr></thead><tbody>'+result.confusion_matrix.map((row,i)=>'<tr><th>'+esc(labels[i])+'</th>'+row.map(v=>'<td>'+fmt(v)+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>';
  const folds='<h2>每折核对</h2><div class="table-wrap"><table><thead><tr><th>折</th><th>训练件数</th><th>验证件数</th><th>一致</th><th>折准确率</th></tr></thead><tbody>'+result.fold_total.map((total,i)=>'<tr><td>'+(i+1)+'</td><td>'+fmt((result.fold_training_counts||[])[i])+'</td><td>'+fmt(total)+'</td><td>'+fmt(result.fold_correct[i])+'</td><td>'+fmt(result.fold_accuracy[i])+'</td></tr>').join('')+'</tbody></table></div>';
  return '<dl class="metrics"><div><dt>核对件数</dt><dd>'+fmt(result.checked)+' / '+fmt(result.items)+'</dd></div><div><dt>逐件一致</dt><dd>'+(result.all_matched?'是':'否')+'</dd></div><div><dt>核对准确率</dt><dd>'+fmt(result.accuracy)+'</dd></div><div><dt>宏平均 F1</dt><dd>'+fmt(result.macro_f1)+'</dd></div></dl>'+confusion+folds;
}
