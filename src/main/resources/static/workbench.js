const $=id=>document.getElementById(id);
const names={QUEUED:'대기',RUNNING:'처리 중',DELIVERING:'결과 전달 중',SUCCEEDED:'완료',FAILED:'실패'};
let selected=null,busy=false,submitting=false;
let activeBatch=new URLSearchParams(location.search).get('batch')||'';
let runtime={maxBatchSize:50,maxBatchCharacters:256000};
function notice(message,error=false){$('notice').textContent=message;$('notice').className=error?'error':'';}
async function api(path,options={}){
  const headers={'Content-Type':'application/json',...options.headers};
  if($('apiKey').value)headers['X-API-Key']=$('apiKey').value;
  const response=await fetch(path,{signal:AbortSignal.timeout(15000),...options,headers});
  if(!response.ok)throw new Error(response.status===401?'접근 키를 확인해 주세요.':response.status===409?'현재 상태에서는 재시도할 수 없습니다.':response.status===400?'입력 개수·길이와 처리 노드를 확인해 주세요.':response.status===404?'요청한 작업이나 묶음을 찾을 수 없습니다.':`요청을 처리하지 못했습니다 (${response.status}).`);
  return response.json();
}
function element(tag,text,cls){const el=document.createElement(tag);el.textContent=text;if(cls)el.className=cls;return el;}
function duration(start,end){
  if(start===null||start===undefined||end===null||end===undefined)return '—';
  const seconds=Math.max(0,end-start)/1000;
  return seconds<60?seconds.toFixed(1)+'초':`${Math.floor(seconds/60)}분 ${(seconds%60).toFixed(0)}초`;
}
function taskTimes(task){
  const now=Date.now(),terminal=['SUCCEEDED','FAILED'].includes(task.status);
  return [duration(task.queuedAt,task.startedAt??(task.status==='QUEUED'?now:null)),
    duration(task.startedAt,task.inferenceCompletedAt??(task.status==='RUNNING'?now:null)),
    duration(task.createdAt,task.finishedAt??(terminal?null:now))];
}
function show(task){
  const box=$('detail');box.replaceChildren();
  box.append(element('span',names[task.status]||task.status,'badge '+task.status),element('span',`시도 ${task.attempt} · ${task.nodeId||'배정 대기'}`,'muted'));
  const times=element('div','','timings');taskTimes(task).forEach((value,i)=>{const cell=element('div','');cell.append(element('small',['대기','추론','전체 경과'][i]),element('strong',value));times.append(cell);});box.append(times);
  box.append(element('small','대기·추론은 최근 추론 시도 기준입니다. 전체 시간은 재시도·전달을 포함하며, 이전 기록의 미측정 시간은 —로 표시합니다.','muted'));
  box.append(element('p',task.prompt),element('small',task.taskId,'muted'));
  if(task.lastError)box.append(element('p',task.failureStage==='DELIVERY'?'결과 전달에 실패했습니다. 재시도 시 저장된 결과를 다시 전달합니다.':'처리에 실패했습니다. 설정을 확인한 뒤 다시 시도하세요.'));
  if(task.output!==null){const out=element('pre',task.output);out.id='result';box.append(out);}
  if(task.status==='FAILED'){
    const retry=element('button','다시 시도','row');retry.onclick=async()=>{retry.disabled=true;try{await api(`/api/tasks/${task.taskId}/retry`,{method:'POST'});notice('재시도를 요청했습니다.');await refresh();}catch(e){notice(e.message,true);retry.disabled=false;}};box.append(retry);
  }
}
function chooseBatch(id){
  activeBatch=id;selected=null;$('filter').value='';const url=new URL(location.href);
  if(id)url.searchParams.set('batch',id);else url.searchParams.delete('batch');
  history.replaceState(null,'',url);$('detail').replaceChildren(element('p','목록에서 작업을 선택하세요.','muted'));refresh();
}
function renderBatch(batch){
  const box=$('batchSummary');box.hidden=!batch;box.replaceChildren();if(!batch)return;
  const s=batch.summary,terminal=s.succeeded+s.failed;
  const head=element('div','','listhead');head.append(element('strong',`작업 묶음 · ${s.total}개`));const link=element('a','이 묶음 링크');link.href='/?batch='+encodeURIComponent(s.batchId)+'#work';head.append(link);box.append(head);
  const cells=element('div','','batch-numbers');for(const [label,value] of [['완료',s.succeeded],['실패',s.failed],['대기·처리 중',s.total-terminal]]){const cell=element('div','');cell.append(element('span',label),element('strong',value));cells.append(cell);}box.append(cells);
  const progress=document.createElement('progress');progress.max=s.total;progress.value=terminal;progress.setAttribute('aria-label','처리가 끝난 작업');box.append(progress);
  const generated=batch.tasks.filter(t=>t.startedAt!==null&&t.inferenceCompletedAt!==null);
  const average=generated.length?generated.reduce((sum,t)=>sum+Math.max(0,t.inferenceCompletedAt-t.startedAt),0)/generated.length:null;
  const allFinished=terminal===s.total&&batch.tasks.every(t=>t.finishedAt!==null);
  const end=allFinished?Math.max(...batch.tasks.map(t=>t.finishedAt)):Date.now();
  const timing=element('p',`전체 경과 ${duration(s.createdAt,end)} · 평균 추론 ${duration(0,average)} (${generated.length}개 측정)`);timing.id='batchTiming';box.append(timing);
  box.append(element('p','묶음은 함께 접수되지만 완료 순서는 달라질 수 있습니다. 각 결과를 선택해 확인하세요.','muted'));
}
async function refresh(){
  if(busy)return;busy=true;const batchId=activeBatch,key=$('apiKey').value;
  try{
    const [batches,data]=await Promise.all([api('/api/batches?limit=10'),api(batchId?'/api/batches/'+encodeURIComponent(batchId):'/api/tasks?limit=30'+($('filter').value?'&status='+$('filter').value:''))]);
    if(batchId!==activeBatch||key!==$('apiKey').value)return;
    const batchSelect=$('batchFilter');batchSelect.replaceChildren(new Option('전체 최근 작업',''));
    batches.forEach(b=>batchSelect.add(new Option(`${new Date(b.createdAt).toLocaleString()} · ${b.total}개 (${b.succeeded}개 완료)`,b.batchId)));
    if(batchId&&!batches.some(b=>b.batchId===batchId))batchSelect.add(new Option('선택한 작업 묶음',batchId));batchSelect.value=batchId;
    renderBatch(batchId?data:null);$('taskListTitle').textContent=batchId?'묶음의 작업':'최근 작업';
    const tasks=batchId?data.tasks.filter(t=>!$('filter').value||t.status===$('filter').value):data;
    const list=$('tasks');list.replaceChildren();if(!tasks.length)list.append(element('p','표시할 작업이 없습니다.','muted'));
    for(const task of tasks){
      const button=element('button','','task');button.setAttribute('aria-pressed',String(task.taskId===selected));
      button.append(element('span',names[task.status]||task.status,'badge '+task.status),element('span',task.prompt.slice(0,100)),element('small',`${new Date(task.createdAt).toLocaleString()} · ${task.targetNode||'자동 배정'}`));
      const times=taskTimes(task);button.append(element('div',`대기 ${times[0]} · 추론 ${times[1]} · 전체 ${times[2]}`,'task-time'));
      button.onclick=()=>{selected=task.taskId;show(task);for(const item of list.children)item.setAttribute('aria-pressed',String(item===button));};list.append(button);
    }
    if(selected){const current=tasks.find(t=>t.taskId===selected);if(current)show(current);else{const id=selected;const task=await api('/api/tasks/'+id);if(selected===id&&key===$('apiKey').value)show(task);}}
  }catch(e){notice(e.message,true);}finally{busy=false;if(batchId!==activeBatch||key!==$('apiKey').value)queueMicrotask(refresh);}
}
async function connect(){
  try{
    const [nodes,info]=await Promise.all([api('/api/nodes'),api('/api/runtime')]);runtime=info;
    const select=$('node'),previous=select.value;select.replaceChildren(new Option('자동 배정',''));nodes.forEach(n=>select.add(new Option(n,n)));if(nodes.includes(previous))select.value=previous;
    const box=$('modelInfo');box.replaceChildren(element('span',info.mode==='DEMO'?'DEMO 응답':'실제 LLM','badge'+(info.mode==='DEMO'?' demo':'')),element('span',info.mode==='DEMO'?'실제 모델 추론을 사용하지 않습니다.':`${info.model} · 응답 최대 ${info.maxOutputTokens} 토큰`));
    notice('연결되었습니다.');validateInput();await refresh();if(!$('kafkaPanel').hidden)await refreshKafka();
  }catch(e){$('modelInfo').textContent='모델 설정을 확인하지 못했습니다.';notice(e.message,true);}
}
function inputPrompts(){return $('requestMode').value==='batch'?$('prompt').value.split(/^\s*---\s*$/m).map(p=>p.trim()):[$('prompt').value.trim()];}
function validateInput(){
  const prompts=inputPrompts(),batch=$('requestMode').value==='batch';let error='';
  if(prompts.length>runtime.maxBatchSize)error=`최대 ${runtime.maxBatchSize}개까지 접수할 수 있습니다.`;
  else if(prompts.some(p=>!p))error=batch?'구분선 사이에 비어 있는 요청이 있습니다.':'요청 내용을 입력해 주세요.';
  else if(prompts.some(p=>p.length>32000))error='각 요청은 32,000자 이하여야 합니다.';
  else if(prompts.reduce((sum,p)=>sum+p.length,0)>runtime.maxBatchCharacters)error='전체 요청은 256,000자 이하여야 합니다.';
  $('inputCount').textContent=$('prompt').value?(error||(batch?`${prompts.length}개 작업을 한 번에 접수합니다.`:`${prompts[0].length.toLocaleString()}자`)):'';
  $('inputCount').className=error&&$('prompt').value?'invalid':'';$('submit').disabled=submitting||!!error;
  $('submit').textContent=submitting?'접수 중…':batch?`${prompts.length}개 작업 접수`:'작업 요청';return {prompts,error};
}
$('requestMode').onchange=()=>{
  const batch=$('requestMode').value==='batch';$('sampleBatch').hidden=!batch;$('prompt').maxLength=batch?257000:32000;
  $('inputHelp').textContent=batch?'요청 사이를 --- 한 줄로 구분하세요. 최대 50개, 각 32,000자·전체 256,000자까지 접수합니다.':'한 작업에 최대 32,000자까지 입력할 수 있습니다.';
  $('prompt').placeholder=batch?'첫 번째 요청\n---\n두 번째 요청\n---\n세 번째 요청':'예: 아래 글의 핵심 내용을 세 문장으로 정리해 주세요.';validateInput();
};
$('sampleBatch').onclick=()=>{
  $('prompt').value=[
    '다음 문의를 배송, 환불, 상품 중 한 단어로 분류하세요: 주문한 물건이 아직 도착하지 않았습니다.',
    '다음 문의를 배송, 환불, 상품 중 한 단어로 분류하세요: 구매를 취소하고 돈을 돌려받고 싶습니다.',
    '다음 문의를 배송, 환불, 상품 중 한 단어로 분류하세요: 이 가방은 방수가 되나요?',
    '한 문장으로 요약하세요: 오늘 회의에서 다음 주 월요일에 새 기능을 출시하기로 결정했다.',
    '한 문장으로 요약하세요: 사용자들은 화면이 깔끔하다고 평가했지만 검색 기능은 더 빨라지길 원했다.',
    '할 일만 한 문장으로 정리하세요: 민수는 금요일까지 고객 설문 결과를 정리한다.',
    '다음 리뷰의 감정을 긍정, 부정, 중립 중 한 단어로 분류하세요: 배송이 빠르고 제품도 마음에 듭니다.',
    '다음 리뷰의 감정을 긍정, 부정, 중립 중 한 단어로 분류하세요: 제품이 고장 나서 사용할 수 없습니다.',
    '재사용 가능한 물병의 상품 소개 문구를 한국어 한 문장으로 작성하세요.',
    '회의 시간이 오후 2시에서 3시로 변경되었다는 안내를 한국어 한 문장으로 작성하세요.'
  ].join('\n---\n');validateInput();$('prompt').focus();
};
$('prompt').oninput=validateInput;
$('requestForm').onsubmit=async event=>{
  event.preventDefault();if(submitting)return;const {prompts,error}=validateInput();if(error){notice(error,true);return;}
  const batch=$('requestMode').value==='batch';submitting=true;validateInput();
  try{
    const targetNode=$('node').value||null;
    const accepted=await api(batch?'/api/commands/batch':'/api/commands',{method:'POST',body:JSON.stringify(batch?{prompts,targetNode}:{prompt:prompts[0],targetNode})});
    if(batch){chooseBatch(accepted.summary.batchId);notice(`${accepted.summary.total}개 작업을 한 묶음으로 접수했습니다.`);}
    else{chooseBatch('');selected=accepted.taskId;notice('작업을 접수했습니다.');}
    await refresh();
  }catch(e){notice(e.message,true);}finally{submitting=false;validateInput();}
};
$('connect').onclick=connect;$('filter').onchange=refresh;$('batchFilter').onchange=()=>chooseBatch($('batchFilter').value);validateInput();
