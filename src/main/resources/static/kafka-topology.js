let topology=null,topologyBusy=false,selectedTopic=null,selectedGroup=null,topologyKey=null;
let graphSelection=null,graphNodes=[],graphEdges=[],graphScale=null,graphHeight=400,lastShape=null;
const graphWidth=1130;
const number=value=>value===null||value===undefined?'—':Number(value).toLocaleString();
const topicKey=name=>'topic:'+name;
const groupKey=id=>'group:'+id;
const memberKey=(group,member)=>'member:'+JSON.stringify([group,member]);
const assignmentText=a=>a.topic+':P'+a.partition;
const kinds={COMMAND:'요청',RESULT:'결과',DLT:'실패 기록',OTHER:'발견된 토픽'};
const svgElement=(tag,attrs={})=>{const node=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const [key,value] of Object.entries(attrs))node.setAttribute(key,value);return node;};

function selectTopic(name,focus=false){
  selectedTopic=name;renderTopics();
  if(focus){$('topicDetail').focus({preventScroll:true});$('topicDetail').scrollIntoView({behavior:'smooth',block:'nearest'});}
}
function renderTopics(){
  const picker=$('topicPicker');picker.replaceChildren();
  for(const topic of topology.topics){
    const button=element('button','','topic-option'+(topic.kind==='DLT'?' dlt':''));
    button.setAttribute('aria-pressed',String(topic.name===selectedTopic));
    button.append(element('strong',topic.name),element('small',topic.state==='UNKNOWN'?'조회 불가':`${topic.partitions.length}개 파티션 · ${kinds[topic.kind]||'토픽'}${topic.targetNode?' · '+topic.targetNode:''}`));
    button.onclick=()=>{selectTopic(topic.name);graphSelection=topicKey(topic.name);highlightGraph();};picker.append(button);
  }
  const topic=topology.topics.find(t=>t.name===selectedTopic)||topology.topics[0];const detail=$('topicDetail');detail.replaceChildren();
  if(!topic){detail.append(element('p','조회된 일반 토픽이 없습니다.','muted'));return;}
  selectedTopic=topic.name;
  detail.append(element('h3',topic.name));
  if(topic.state==='UNKNOWN'){detail.append(element('p','이 토픽의 현재 정보를 조회하지 못했습니다.','warning'));return;}
  const groups=topology.groups.filter(g=>g.offsets.some(p=>p.topic===topic.name)||g.members.some(m=>m.assignments.some(a=>a.topic===topic.name)));
  if(!groups.some(g=>g.id===selectedGroup))selectedGroup=groups.find(g=>g.id===topic.groupId)?.id||groups[0]?.id||null;
  const group=groups.find(g=>g.id===selectedGroup);
  if(groups.length){
    const row=element('div','','offset-group');const label=element('label','오프셋 조회 그룹');label.htmlFor='offsetGroup';const select=document.createElement('select');select.id='offsetGroup';
    groups.forEach(g=>select.add(new Option(g.id,g.id)));select.value=selectedGroup;select.onchange=()=>{selectedGroup=select.value;renderTopics();};row.append(label,select);detail.append(row);
  }else detail.append(element('p','확인된 Consumer 할당·커밋 이력이 없습니다.','muted'));
  const wrapper=element('div','','partition-table');const table=document.createElement('table');const head=document.createElement('thead');const tr=document.createElement('tr');
  ['파티션','리더','복제 / 동기화','끝 오프셋','커밋 오프셋','미처리 (Lag)'].forEach(label=>{const th=element('th',label);th.scope='col';tr.append(th);});head.append(tr);table.append(head);const body=document.createElement('tbody');
  for(const p of topic.partitions){
    const row=document.createElement('tr');const synced=p.inSyncReplicas.length===p.replicas.length&&p.leader!==null;const progress=group?.offsets.find(o=>o.topic===topic.name&&o.partition===p.id);
    const values=['P'+p.id,p.leader===null?'없음':'Broker '+p.leader,`${p.replicas.length} / ${p.inSyncReplicas.length}${synced?'':' · 확인 필요'}`,number(p.endOffset),group?number(progress?.committedOffset):'해당 없음',group?number(progress?.lag):'해당 없음'];
    values.forEach((value,i)=>row.append(element('td',value,i===2&&!synced?'warning':'')));body.append(row);
  }
  table.append(body);wrapper.append(table);detail.append(wrapper);
  const help=element('details','');help.append(element('summary','오프셋 기준'),element('p','Lag = 로그 끝 − 커밋 오프셋. 보관 메시지 수·고유 작업 수와 다릅니다. 미확인 값은 —로 표시합니다.','topology-note'));detail.append(help);
}

function graphShape(data){
  return new Set([
    ...data.topics.map(t=>'T:'+t.name),
    ...data.groups.map(g=>'G:'+g.id),
    ...data.groups.flatMap(g=>g.members.map(m=>'M:'+memberKey(g.id,m.memberId))),
    ...data.groups.flatMap(g=>g.members.flatMap(m=>m.assignments.map(a=>'A:'+JSON.stringify([g.id,m.memberId,a.topic,a.partition]))))
  ]);
}
function describeChanges(data,recovered){
  if(!data.discoveryComplete||data.status!=='CONNECTED'){
    $('graphChanges').textContent='일부 정보를 확인하지 못했습니다. 조회 불가 항목은 삭제로 판단하지 않습니다.';return;
  }
  const shape=graphShape(data);
  if(lastShape){
    const added=[...shape].filter(key=>!lastShape.has(key)),removed=[...lastShape].filter(key=>!shape.has(key));
    if(added.length||removed.length){
      const counts=(keys,prefix)=>keys.filter(key=>key.startsWith(prefix)).length;
      const parts=[['T:','토픽'],['G:','그룹'],['M:','Consumer']].flatMap(([prefix,label])=>{
        const plus=counts(added,prefix),minus=counts(removed,prefix);return plus||minus?[`${label} +${plus} / −${minus}`]:[];
      });
      if(counts(added,'A:')||counts(removed,'A:'))parts.push('파티션 할당 변경');
      $('graphChanges').textContent=`${new Date(data.checkedAt).toLocaleTimeString()} · ${parts.join(' · ')}`;
    }else if(recovered)$('graphChanges').textContent='연결이 복구되어 최신 구성을 반영했습니다.';
  }else $('graphChanges').textContent='구성 변경 감지 중';
  lastShape=shape;
}

function renderGraph(){
  const canvas=$('graphCanvas');const active=document.activeElement?.dataset.nodeKey;canvas.replaceChildren();graphNodes=[];graphEdges=[];
  const query=$('graphSearch').value.trim().toLowerCase();const matches=value=>value.toLowerCase().includes(query);
  const matchingGroups=new Set(topology.groups.filter(g=>matches(g.id)||g.members.some(m=>matches(m.clientId)||matches(m.memberId))).map(g=>g.id));
  let topics=topology.topics.filter(t=>!query||matches(t.name)||topology.groups.some(g=>matchingGroups.has(g.id)&&g.members.some(m=>m.assignments.some(a=>a.topic===t.name))));
  const visibleTopics=new Set(topics.map(t=>t.name));
  let groups=topology.groups.filter(g=>!query||matchingGroups.has(g.id)||g.members.some(m=>m.assignments.some(a=>visibleTopics.has(a.topic)))||g.offsets.some(o=>visibleTopics.has(o.topic)));
  const order={COMMAND:0,RESULT:1,OTHER:2,DLT:3};topics.sort((a,b)=>(order[a.kind]??2)-(order[b.kind]??2)||a.name.localeCompare(b.name));
  const topicIndex=new Map(topics.map((t,i)=>[t.name,i]));
  const firstAssigned=g=>Math.min(topics.length,...g.members.flatMap(m=>m.assignments.map(a=>topicIndex.get(a.topic)??topics.length)));
  groups.sort((a,b)=>firstAssigned(a)-firstAssigned(b)||a.id.localeCompare(b.id));
  const configured=$('showConfigured').checked;
  const paths=topology.routes.filter(r=>visibleTopics.has(r.targetTopic));
  const hasOutbox=configured&&paths.some(r=>r.sourceKind==='OUTBOX');
  const svg=svgElement('svg',{width:graphWidth,'aria-hidden':'true'});const defs=svgElement('defs');
  for(const [id,color] of [['live','#8c83cf'],['config','#a18a64']]){
    const marker=svgElement('marker',{id:'arrow-'+id,viewBox:'0 0 10 10',refX:9,refY:5,markerWidth:6,markerHeight:6,orient:'auto-start-reverse'});marker.append(svgElement('path',{d:'M 0 0 L 10 5 L 0 10 z',fill:color}));defs.append(marker);
  }
  svg.append(defs);canvas.append(svg);
  for(const [x,label] of [[20,'발행 경로'],[280,`TOPICS · ${topics.length}`],[760,`CONSUMER GROUPS · ${groups.length}`]]){
    const title=element('span',label,'graph-column');title.style.left=x+'px';canvas.append(title);
  }
  const node=(id,type,x,y,width,height,title,subtitle,description,data)=>{
    const button=element('button','','graph-node '+type);button.style.cssText=`left:${x}px;top:${y}px;width:${width}px;height:${height}px`;
    button.dataset.nodeKey=id;button.dataset.nodeType=type.split(' ')[0];button.title=title+'\n'+description;
    button.append(element('small',subtitle),element('strong',title),element('p',description));button.setAttribute('aria-label',subtitle+' '+title+' '+description);
    const entry={id,type,x,y,width,height,button,title,data};graphNodes.push(entry);button.onclick=()=>{graphSelection=graphSelection===id?null:id;if(type.startsWith('topic'))selectTopic(data.name);highlightGraph();};canvas.append(button);return entry;
  };
  if(hasOutbox)node('outbox','outbox',20,65,190,114,'DB Outbox','앱 설정','요청·결과 발행',null);
  else {const empty=element('p',configured?'알려진 발행 경로 없음':'설정 경로 숨김','graph-empty');empty.style.cssText='left:20px;top:70px;width:180px';canvas.append(empty);}
  for(const [index,t] of topics.entries()){
    const linked=topology.groups.flatMap(g=>g.members.filter(m=>m.assignments.some(a=>a.topic===t.name))).length;
    node(topicKey(t.name),'topic'+(t.kind==='DLT'?' dlt':'')+(t.state==='UNKNOWN'?' unknown':''),280,60+index*116,300,94,t.name,kinds[t.kind]||'토픽',
      t.state==='UNKNOWN'?'현재 메타데이터 조회 불가':`${t.partitions.length} partitions · ${linked} consumers`,t);
  }
  let groupBottom=40;
  for(const g of groups){
    const height=94+Math.max(1,g.members.length)*99;
    const groupY=Math.max(groupBottom+22,60+firstAssigned(g)*116-30);
    const bg=element('div','','graph-group');bg.style.cssText=`left:744px;top:${groupY}px;width:362px;height:${height}px`;canvas.append(bg);
    node(groupKey(g.id),'group-title'+(g.state==='UNKNOWN'?' unknown':''),752,groupY+3,346,80,g.id,'GROUP',`${g.state} · ${number(g.memberCount)} consumers · Lag ${number(g.lag)}`,g);
    g.members.forEach((m,index)=>node(memberKey(g.id,m.memberId),'consumer',764,groupY+90+index*99,322,86,m.clientId||m.memberId,'CONSUMER',`${m.assignments.length}개 파티션 할당 · ID ${m.memberId.slice(-10)}`,{...m,group:g.id}));
    if(!g.members.length){const empty=element('p',g.memberCount===null?'Consumer 정보 조회 불가':'활성 Consumer 없음 · 커밋 이력만 보존될 수 있음','graph-empty');empty.style.cssText=`left:768px;top:${groupY+108}px;width:310px`;canvas.append(empty);}
    groupBottom=groupY+height;
  }
  if(!topics.length&&!groups.length){const empty=element('p',query?'검색 결과가 없습니다.':'조회된 토픽과 Consumer 그룹이 없습니다.','graph-empty');empty.style.cssText='left:280px;top:74px';canvas.append(empty);}
  const byId=new Map(graphNodes.map(n=>[n.id,n]));
  function edge(from,to,label,config=false){
    const a=byId.get(from),b=byId.get(to);if(!a||!b)return;
    const forward=a.x<b.x;const x1=forward?a.x+a.width:a.x,x2=forward?b.x:b.x+b.width,y1=a.y+a.height/2,y2=b.y+b.height/2;
    const bend=Math.max(40,Math.abs(x2-x1)*.55);const d=`M ${x1} ${y1} C ${x1+(forward?bend:-bend)} ${y1}, ${x2+(forward?-bend:bend)} ${y2}, ${x2} ${y2}`;
    const path=svgElement('path',{d,class:'graph-edge'+(config?' configured':''),'marker-end':`url(#arrow-${config?'config':'live'})`});path.dataset.from=from;path.dataset.to=to;path.dataset.kind=config?'configured':'assignment';
    const tooltip=svgElement('title');tooltip.textContent=`${a.title} → ${b.title}: ${label}`;path.append(tooltip);
    const text=svgElement('text',{x:(x1+x2)/2,y:(y1+y2)/2-6,'text-anchor':'middle',class:'edge-label'+(config?' configured':'')});text.textContent=label;text.style.display='none';
    svg.append(path,text);graphEdges.push({from,to,path,text});
  }
  if(configured)for(const r of paths)edge(r.sourceKind==='OUTBOX'?'outbox':groupKey(r.sourceId),topicKey(r.targetTopic),r.kind==='FAILURE'?`실패 · 재시도 ${topology.retries}회 후`:'발행 · 앱 설정',true);
  for(const g of groups)for(const m of g.members){
    const assigned=new Map();for(const a of m.assignments){if(!assigned.has(a.topic))assigned.set(a.topic,[]);assigned.get(a.topic).push(a.partition);}
    for(const [topic,partitions] of assigned)edge(topicKey(topic),memberKey(g.id,m.memberId),partitions.map(p=>'P'+p).join(', '));
  }
  graphHeight=Math.max(270,60+topics.length*116,groupBottom+24);canvas.style.width=graphWidth+'px';canvas.style.height=graphHeight+'px';svg.setAttribute('height',graphHeight);
  $('graphCount').textContent=`토픽 ${topics.length} · 그룹 ${groups.length} · Consumer ${groups.reduce((sum,g)=>sum+g.members.length,0)}`;
  if(graphSelection&&!byId.has(graphSelection))graphSelection=null;
  highlightGraph();applyGraphScale();
  if(active)byId.get(active)?.button.focus({preventScroll:true});
}

function highlightGraph(){
  const selected=graphNodes.find(n=>n.id===graphSelection);const related=new Set(selected?[selected.id]:[]);
  if(selected?.type.startsWith('group-title'))graphNodes.filter(n=>n.data?.group===selected.data.id).forEach(n=>related.add(n.id));
  const roots=new Set(related);
  for(const edge of graphEdges)if(roots.has(edge.from)||roots.has(edge.to)){related.add(edge.from);related.add(edge.to);}
  for(const n of graphNodes){n.button.classList.toggle('selected',n.id===graphSelection);n.button.classList.toggle('related',related.has(n.id)&&n.id!==graphSelection);n.button.classList.toggle('dimmed',!!selected&&!related.has(n.id));n.button.setAttribute('aria-pressed',String(n.id===graphSelection));}
  for(const e of graphEdges){const match=roots.has(e.from)||roots.has(e.to);e.path.classList.toggle('selected',!!selected&&match);e.path.classList.toggle('dimmed',!!selected&&!match);e.text.style.display=selected&&match?'':'none';}
  const detail=$('graphDetail');detail.replaceChildren();
  if(!selected){detail.append(element('span','노드를 선택해 연결 정보 확인','muted'));return;}
  detail.append(element('strong',selected.title));
  if(selected.type==='consumer')detail.append(element('p',`그룹: ${selected.data.group} · Member ID: ${selected.data.memberId}`,'muted'));
  if(selected.type.startsWith('group-title'))detail.append(element('p',`상태: ${selected.data.state} · 활성 Consumer ${number(selected.data.memberCount)} · Lag ${number(selected.data.lag)}`,'muted'));
  const edges=graphEdges.filter(e=>roots.has(e.from)||roots.has(e.to));
  if(!edges.length)detail.append(element('p','현재 표시 범위에 확인된 연결이 없습니다.','muted'));
  for(const e of edges){const from=graphNodes.find(n=>n.id===e.from),to=graphNodes.find(n=>n.id===e.to);detail.append(element('p',`${from.title} → ${to.title} · ${e.text.textContent}`,'muted'));}
  if(selected.type.startsWith('topic')){const button=element('button','파티션 상세 보기','secondary');button.onclick=()=>selectTopic(selected.data.name,true);detail.append(button);}
}

function applyGraphScale(){
  if(graphScale===null)graphScale=Math.max(.6,Math.min(1,($('graphViewport').clientWidth-20)/graphWidth));
  $('graphCanvas').style.transform=`scale(${graphScale})`;$('graphSizer').style.width=graphWidth*graphScale+'px';$('graphSizer').style.height=graphHeight*graphScale+'px';$('graphZoom').textContent=Math.round(graphScale*100)+'%';
}

function renderKafka(data){
  if(data.status==='UNAVAILABLE'){markKafkaStale('Kafka 조회 불가');return;}
  const recovered=$('graphPanel').classList.contains('graph-stale')||topology?.status==='PARTIAL';
  topology=data;if(!data.topics.some(t=>t.name===selectedTopic))selectedTopic=data.topics[0]?.name||null;
  $('graphPanel').classList.remove('graph-stale');
  $('kafkaConnection').replaceChildren(element('span','','signal '+(data.status==='PARTIAL'?'partial':'connected')),document.createTextNode(data.status==='PARTIAL'?'연결됨 · 일부 정보 조회 불가':'Kafka 연결됨'));
  $('kafkaUpdated').textContent=`${new Date(data.checkedAt).toLocaleTimeString()} 조회 · 10초마다 갱신`;
  const available=data.topics.filter(t=>t.state==='AVAILABLE');const lag=data.discoveryComplete&&data.groups.every(g=>g.lag!==null)?data.groups.reduce((sum,g)=>sum+g.lag,0):null;
  $('kafkaStats').replaceChildren();
  for(const [label,value,hint] of [['브로커',data.brokers.length,'클러스터에서 조회'],['발견된 토픽',available.length,'내부 관리 토픽 제외'],['활성 Consumer',data.discoveryComplete&&data.groups.every(g=>g.memberCount!==null)?data.groups.reduce((sum,g)=>sum+g.memberCount,0):null,`${data.groups.length}개 Consumer 그룹`],['미처리 메시지',lag,'조회된 그룹 Lag 합계']]){
    const box=element('div','','stat');box.append(element('small',label),element('strong',number(value)),element('small',hint));$('kafkaStats').append(box);
  }
  $('brokerBand').replaceChildren();for(const b of data.brokers){const chip=element('div','','broker-chip');chip.append(element('b','Broker '+b.id),element('span',`${b.host}:${b.port}`));if(b.controller)chip.append(element('span','Controller','muted'));$('brokerBand').append(chip);}
  $('kafkaGroups').replaceChildren();for(const group of data.groups){
    const card=element('div','','group-card');const head=element('div','','group-head');head.append(element('h3',group.id),element('span',group.state==='UNKNOWN'?'조회 불가':group.state,'badge'));card.append(head,element('p',`활성 Consumer ${number(group.memberCount)} · 미처리 메시지 ${number(group.lag)}`));
    if(!group.members.length)card.append(element('span',group.memberCount===0?'활성 Consumer 없음. 커밋 이력은 유지될 수 있습니다.':'Consumer 정보를 조회하지 못했습니다.','muted'));
    for(const member of group.members){const item=element('div',member.clientId||member.memberId,'member');item.append(element('small','Member ID: '+member.memberId),element('small',member.assignments.length?member.assignments.map(assignmentText).join(' · '):'파티션 할당 대기'));card.append(item);}$('kafkaGroups').append(card);
  }
  if(!data.groups.length)$('kafkaGroups').append(element('p','조회된 Consumer 그룹이 없습니다.','muted'));
  describeChanges(data,recovered);renderGraph();renderTopics();
}

function markKafkaStale(message){
  $('kafkaConnection').replaceChildren(element('span','','signal unavailable'),document.createTextNode(message));
  $('kafkaUpdated').textContent=topology?`${new Date(topology.checkedAt).toLocaleTimeString()}의 이전 정보 · 최신 상태 확인 불가`:'10초 후 다시 조회합니다.';
  $('graphPanel').classList.add('graph-stale');$('graphChanges').textContent=topology?'연결도를 이전 조회 상태로 유지합니다. 현재 구성으로 해석하지 마세요.':'Kafka에 연결되면 토픽과 Consumer를 자동으로 표시합니다.';
  for(const el of document.querySelectorAll('#kafkaStats strong'))el.textContent='—';
}

async function refreshKafka(){
  if(topologyBusy)return;topologyBusy=true;$('refreshTopology').disabled=true;const key=$('apiKey').value;
  if(topologyKey!==key){
    topology=null;lastShape=null;graphSelection=null;graphNodes=[];graphEdges=[];topologyKey=key;
    for(const id of ['kafkaStats','brokerBand','graphCanvas','graphDetail','graphCount','graphChanges','kafkaGroups','topicPicker','topicDetail'])$(id).replaceChildren();
    $('graphSizer').style.height='140px';
  }
  try{const data=await api('/api/kafka/topology',{signal:AbortSignal.timeout(8000)});if($('apiKey').value===key)renderKafka(data);}
  catch(error){if($('apiKey').value===key)markKafkaStale(error.name==='TimeoutError'?'Kafka 조회 시간이 초과되었습니다.':error.message);}
  finally{topologyBusy=false;$('refreshTopology').disabled=false;}
}
function showTab(){
  const page=location.hash==='#kafka'?'kafka':location.hash==='#flows'?'flow':location.hash==='#experiments'?'experiment':'work';
  const url=new URL(location);if(page==='work'){url.searchParams.delete('flow');url.searchParams.delete('experiment');}else if(page==='flow'&&activeWorkflow)url.searchParams.set('flow',activeWorkflow);else if(page==='experiment'&&activeExperiment)url.searchParams.set('experiment',activeExperiment);history.replaceState(null,'',url);
  for(const id of ['work','flow','experiment','kafka']){const active=id===page;$(id+'Panel').hidden=!active;$(id+'Tab').setAttribute('aria-selected',String(active));$(id+'Tab').tabIndex=active?0:-1;}
  const [title,eyebrow]={work:['실행','EXECUTIONS'],flow:['협업','WORKFLOWS'],experiment:['비교 실험','EXPERIMENTS'],kafka:['Kafka','INFRASTRUCTURE']}[page];
  $('pageTitle').textContent=title;$('pageCrumb').textContent=title;$('pageEyebrow').textContent=eyebrow;document.title=title+' · Event-driven LLM';
  $('pollLabel').replaceChildren(element('i',''),document.createTextNode(page==='kafka'?'10초마다 갱신':'3초마다 갱신'));
  if(page==='kafka')refreshKafka();else refresh();
}
$('workTab').onclick=()=>{location.hash='work';};$('flowTab').onclick=()=>{location.hash='flows';};$('kafkaTab').onclick=()=>{location.hash='kafka';};$('refreshTopology').onclick=refreshKafka;
$('experimentTab').onclick=()=>{location.hash='experiments';};
$('graphSearch').oninput=()=>{if(topology)renderGraph();};$('showConfigured').onchange=()=>{if(topology)renderGraph();};
$('zoomIn').onclick=()=>{graphScale=Math.min(1.5,(graphScale||1)+.1);applyGraphScale();};$('zoomOut').onclick=()=>{graphScale=Math.max(.4,(graphScale||1)-.1);applyGraphScale();};$('zoomFit').onclick=()=>{graphScale=null;applyGraphScale();};
document.querySelector('.tabs').onkeydown=event=>{
  const tabs=['work','flow','experiment','kafka'];if(!['ArrowLeft','ArrowRight','ArrowUp','ArrowDown','Home','End'].includes(event.key))return;
  event.preventDefault();let index=tabs.findIndex(id=>$(id+'Tab')===event.target.closest('[role=tab]'));
  index=event.key==='Home'?0:event.key==='End'?tabs.length-1:(index+(['ArrowRight','ArrowDown'].includes(event.key)?1:tabs.length-1))%tabs.length;
  location.hash=tabs[index]==='flow'?'flows':tabs[index]==='experiment'?'experiments':tabs[index];$(tabs[index]+'Tab').focus();
};
const mobileNavigation=matchMedia('(max-width:640px)');
const orientNavigation=()=>document.querySelector('.tabs').setAttribute('aria-orientation',mobileNavigation.matches?'horizontal':'vertical');
mobileNavigation.addEventListener('change',orientNavigation);orientNavigation();
if((activeExperiment||activeWorkflow)&&(!location.hash||location.hash==='#work'))history.replaceState(null,'',location.pathname+location.search+(activeExperiment?'#experiments':'#flows'));
window.addEventListener('hashchange',showTab);showTab();connect();
setInterval(()=>{if(!document.hidden&&$('kafkaPanel').hidden)refresh();},3000);
setInterval(()=>{if(!document.hidden&&!$('kafkaPanel').hidden)refreshKafka();},10000);
