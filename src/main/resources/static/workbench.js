const $=id=>document.getElementById(id);
const names={QUEUED:'대기',RUNNING:'처리 중',DELIVERING:'전달 중',SUCCEEDED:'완료',FAILED:'실패'};
let selected=null,busy=false,submitting=false;
let activeBatch=new URLSearchParams(location.search).get('batch')||'';
let activeWorkflow=new URLSearchParams(location.search).get('flow')||'';
let activeExperiment=new URLSearchParams(location.search).get('experiment')||'';
const expandedExperimentDetails=new Set();
const expandedWorkflowSteps=new Set();
let runtime={maxBatchSize:50,maxBatchCharacters:256000};
let jev={mode:'OFF',configured:false,available:false},evaluationSubmitting=false;
const expandedEvaluations=new Set();
const expandedTaskInputs=new Set();
let noticeTimer;
function notice(message,error=false){
  clearTimeout(noticeTimer);$('notice').textContent=message;$('notice').className=error?'error':'';
  if(!error&&message)noticeTimer=setTimeout(()=>{$('notice').textContent='';},6000);
}
function connection(online){
  const state=$('connectionState');state.className='connection-state '+(online?'connected':'error');
  state.querySelector('span').textContent=online?'연결됨':'연결 확인 필요';
}
function disclosure(key,expanded,summary){
  const details=document.createElement('details');details.open=expanded.has(key);
  details.ontoggle=()=>{if(details.isConnected){if(details.open)expanded.add(key);else expanded.delete(key);}};
  details.append(element('summary',summary));return details;
}
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
  const head=element('div','','detail-status');head.append(element('span',names[task.status]||task.status,'badge '+task.status),element('span',task.taskId.slice(0,8),'detail-id'));box.append(head);
  const times=element('div','','timings');taskTimes(task).forEach((value,i)=>{const cell=element('div','');cell.append(element('small',['대기','추론','전체 경과'][i]),element('strong',value));times.append(cell);});box.append(times);
  times.title='대기·추론: 최근 시도 기준 / 전체: 재시도·전달 포함 / —: 미측정';
  if(task.lastError)box.append(element('p',task.failureStage==='DELIVERY'?'전달 실패 · 저장된 결과로 재시도할 수 있습니다.':'실행 실패 · 설정을 확인한 뒤 재시도하세요.','error-message'));
  const outputHead=element('div','OUTPUT','section-label');box.append(outputHead);
  if(task.output!==null&&task.output!==undefined){
    const copy=element('button','복사','result-copy');copy.type='button';
    copy.onclick=async()=>{try{await navigator.clipboard.writeText(task.output);notice('결과를 복사했습니다.');}catch{notice('복사할 수 없습니다. 결과를 직접 선택해 주세요.',true);}};
    outputHead.append(copy);const out=element('pre',task.output);out.id='result';box.append(out);
  }else box.append(element('p',task.status==='FAILED'?'저장된 결과 없음':'결과 대기 중…','muted'));
  const input=disclosure(task.taskId,expandedTaskInputs,'프롬프트 · 실행 정보');input.className='detail-input';
  input.append(element('pre',task.prompt),element('div',`ID ${task.taskId}\n노드 ${task.nodeId||task.targetNode||'자동 배정'} · 시도 ${task.attempt}\n${new Date(task.createdAt).toLocaleString()}`,'detail-metadata'));box.append(input);
  if(task.status==='FAILED'){
    const retry=element('button','다시 시도','row');retry.onclick=async()=>{retry.disabled=true;try{await api(`/api/tasks/${task.taskId}/retry`,{method:'POST'});notice('재시도를 요청했습니다.');await refresh();}catch(e){notice(e.message,true);retry.disabled=false;}};box.append(retry);
  }
  const evaluationBox=element('section','','evaluation');evaluationBox.id='evaluation';box.append(evaluationBox);showEvaluations(task,evaluationBox);
}
const evaluationLabels={PASS:'통과 의견',REJECT:'수정 권고',REVIEW:'검토 권고',QUEUED:'평가 대기',RUNNING:'평가 중',FAILED:'평가 실패'};
const checkLabels={fulfillment:'요청 충족',faithfulness:'원문·근거 일치',constraints:'언어·형식 준수'};
const choiceLabels={pass:'충족',fail:'위반',unknown:'판단 어려움'};
function evaluationError(code){
  if(code==='HTTP_401'||code==='HTTP_403')return 'Jev API 키와 사용 권한을 확인하세요.';
  if(code==='INPUT_TOO_LARGE')return '요청과 답변이 평가 입력 한도(합계 24,000자)를 초과했습니다. 일부만 잘라 평가하지 않습니다.';
  if(code==='INVALID_RESPONSE')return 'Jev 응답 형식이나 확률 값이 올바르지 않아 판정을 저장하지 않았습니다.';
  if(code==='NOT_CONFIGURED')return 'Jev 설정을 확인하세요.';
  return '평가를 완료하지 못했습니다. '+code;
}
async function showEvaluations(task,box){
  const key=$('apiKey').value;
  const head=element('div','','listhead');head.append(element('h3','Jev 평가'));
  const button=element('button','답변 평가','secondary');button.type='button';
  const eligible=task.output!==null&&(task.status==='SUCCEEDED'||task.status==='DELIVERING'||(task.status==='FAILED'&&task.failureStage==='DELIVERY'));
  button.disabled=!jev.available||!eligible||evaluationSubmitting;head.append(button);box.append(head);
  head.title='참고용 평가 · 결과 전달과 별도로 실행';
  if(!jev.available)button.title=!jev.configured?'Jev API 키 미설정':'Jev 평가 꺼짐';
  else if(!eligible)button.title='답변 저장 후 평가 가능';
  const content=element('div','평가 이력 확인 중…','muted');box.append(content);
  button.onclick=async()=>{
    if(evaluationSubmitting)return;evaluationSubmitting=true;button.disabled=true;
    try{await api(`/api/tasks/${task.taskId}/evaluations`,{method:'POST'});notice('저장된 답변의 평가를 접수했습니다.');}
    catch(e){notice(e.message,true);}finally{evaluationSubmitting=false;await refresh();}
  };
  try{
    const items=await api(`/api/tasks/${task.taskId}/evaluations`);
    if(!box.isConnected||key!==$('apiKey').value||selected!==task.taskId)return;
    content.replaceChildren();content.className='';
    if(!items.length){if(!jev.available)box.hidden=true;else content.append(element('p','평가 이력 없음','muted'));return;}
    if(items.some(e=>e.status==='QUEUED'||e.status==='RUNNING'))button.disabled=true;
    items.forEach((item,index)=>{
      const entry=element('div','','evaluation-entry');
      const label=item.status==='COMPLETED'?item.report.verdict:item.status;
      entry.append(element('span',evaluationLabels[label]||label,'badge eval-'+label));
      entry.append(element('small',`${new Date(item.createdAt).toLocaleString()} · 작업 시도 ${item.taskAttempt} · 평가 ${item.attempts}/3회`,'muted'));
      if(item.lastError)entry.append(element('p',evaluationError(item.lastError)));
      if(item.report){
        const report=item.report;
        entry.append(element('p',`${report.model} · ${report.rubricVersion} · 판정 기준 ${report.threshold.toFixed(2)}`,'muted'));
        const checks=element('div','');
        report.checks.forEach(check=>{
          const line=element('div',`${checkLabels[check.id]||check.id} · ${choiceLabels[check.choice]||check.choice}`,'evaluation-check');
          line.append(element('small',`확신도 ${check.confidence.toFixed(2)} · 충족 ${(check.probabilities.pass*100).toFixed(1)}% / 위반 ${(check.probabilities.fail*100).toFixed(1)}% / 판단 어려움 ${(check.probabilities.unknown*100).toFixed(1)}%`));checks.append(line);
        });
        if(index===items.findIndex(e=>e.report))entry.append(checks);else{
          const detail=document.createElement('details');detail.open=expandedEvaluations.has(item.id);
          detail.ontoggle=()=>{if(detail.isConnected){if(detail.open)expandedEvaluations.add(item.id);else expandedEvaluations.delete(item.id);}};
          detail.append(element('summary','항목별 판정'),checks);entry.append(detail);
        }
      }
      content.append(entry);
    });
    const help=element('details','');help.append(element('summary','평가 기준'),element('p','확신도는 정답률이 아닙니다. 작업 완료와 평가 통과는 별개이며, 각 이력은 당시 저장된 답변 기준입니다. 최근 20개 표시.','muted'));content.append(help);
  }catch(e){if(box.isConnected)content.textContent=e.message;}
}
function chooseBatch(id){
  activeBatch=id;selected=null;$('filter').value='';const url=new URL(location.href);
  if(id)url.searchParams.set('batch',id);else url.searchParams.delete('batch');
  history.replaceState(null,'',url);$('detail').replaceChildren(element('div','목록에서 실행을 선택하세요.','empty-state'));refresh();
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
}
function renderRunStats(tasks){
  const states=[['목록 내 실행',tasks.length,''],['진행 중',tasks.filter(t=>['QUEUED','RUNNING','DELIVERING'].includes(t.status)).length,'running'],['완료',tasks.filter(t=>t.status==='SUCCEEDED').length,'succeeded'],['실패',tasks.filter(t=>t.status==='FAILED').length,'failed']];
  $('runStats').replaceChildren();
  for(const [label,value,state] of states){const cell=element('div','','run-stat'),title=element('small','');title.append(element('i','','status-dot '+state),document.createTextNode(label));cell.append(title,element('strong',String(value)));$('runStats').append(cell);}
  $('runCount').textContent=String(tasks.length);$('runCount').title='현재 목록의 실행 수';$('listCount').textContent=tasks.length+'개';
}
async function refresh(){
  if(busy)return;busy=true;const batchId=activeBatch,key=$('apiKey').value,status=$('filter').value;
  try{
    const [batches,data]=await Promise.all([api('/api/batches?limit=10'),api(batchId?'/api/batches/'+encodeURIComponent(batchId):'/api/tasks?limit=30'+(status?'&status='+status:''))]);
    if(batchId!==activeBatch||key!==$('apiKey').value||status!==$('filter').value)return;
    connection(true);
    const batchSelect=$('batchFilter');batchSelect.replaceChildren(new Option('최근 30개 실행',''));
    batches.forEach(b=>batchSelect.add(new Option(`${new Date(b.createdAt).toLocaleString()} · ${b.total}개 (${b.succeeded}개 완료)`,b.batchId)));
    if(batchId&&!batches.some(b=>b.batchId===batchId))batchSelect.add(new Option('선택한 작업 묶음',batchId));batchSelect.value=batchId;
    renderBatch(batchId?data:null);$('taskListTitle').textContent=batchId?'일괄 실행':'최근 실행';
    const tasks=batchId?data.tasks.filter(t=>!status||t.status===status):data;renderRunStats(tasks);
    if(!selected&&tasks.length)selected=tasks[0].taskId;
    const focused=document.activeElement?.closest('.task')?.dataset.taskId;
    const list=$('tasks');list.replaceChildren();if(!tasks.length)list.append(element('div',status?'이 상태의 실행이 없습니다.':'아직 실행이 없습니다. 새 실행을 시작하세요.','empty-state'));
    for(const task of tasks){
      const button=element('button','','task');button.dataset.taskId=task.taskId;button.setAttribute('aria-pressed',String(task.taskId===selected));
      const info=element('span',''),prompt=element('span',task.prompt,'task-prompt');prompt.title=task.prompt;
      const meta=element('span','','task-meta');meta.append(element('span',task.taskId.slice(0,8),'mono'),element('span',new Date(task.createdAt).toLocaleString('ko-KR',{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false})));
      info.append(prompt,meta);const times=taskTimes(task);button.append(info,element('span',names[task.status]||task.status,'badge '+task.status),element('span',times[2],'task-time'));
      button.onclick=()=>{selected=task.taskId;show(task);for(const item of list.children)item.setAttribute('aria-pressed',String(item===button));if(matchMedia('(max-width:950px)').matches)$('detail').scrollIntoView({behavior:'smooth',block:'start'});};list.append(button);
      if(focused===task.taskId)button.focus({preventScroll:true});
    }
    if(selected){const current=tasks.find(t=>t.taskId===selected);if(current)show(current);else{const id=selected;const task=await api('/api/tasks/'+id);if(selected===id&&key===$('apiKey').value)show(task);}}
    await refreshWorkflows();
    if(!$('experimentPanel').hidden)await refreshExperiments();
  }catch(e){connection(false);notice(e.message,true);}finally{busy=false;if(batchId!==activeBatch||key!==$('apiKey').value||status!==$('filter').value)queueMicrotask(refresh);}
}
async function connect(){
  try{
    const [nodes,info,assessment]=await Promise.all([api('/api/nodes'),api('/api/runtime'),api('/api/evaluations/config')]);runtime=info;jev=assessment;
    const select=$('node'),previous=select.value;select.replaceChildren(new Option('자동 배정',''));nodes.forEach(n=>select.add(new Option(n,n)));if(nodes.includes(previous))select.value=previous;
    const box=$('modelInfo');box.replaceChildren(element('span',info.mode,'badge'),element('span',info.mode==='DEMO'?'모의 응답':`${info.model}${info.maxOutputTokens==null?'':` · 최대 ${info.maxOutputTokens} tokens`}`));
    $('runtimeModel').textContent=info.mode==='DEMO'?'DEMO · 모의 응답':info.model;
    $('jevInfo').textContent=jev.available?'Jev · 활성':'Jev · 비활성';$('jevInfo').title=jev.available?jev.model:!jev.configured?'Jev API 키 미설정':'Jev 평가 꺼짐';
    connection(true);validateInput();await refresh();if(!$('kafkaPanel').hidden)await refreshKafka();return true;
  }catch(e){connection(false);$('modelInfo').textContent='모델 연결 확인 필요';notice(e.message,true);$('accessError').textContent=e.message;$('accessError').hidden=false;return false;}
}
function inputPrompts(){return $('requestMode').value==='batch'?$('prompt').value.split(/^\s*---\s*$/m).map(p=>p.trim()):[$('prompt').value.trim()];}
function validateInput(){
  const prompts=inputPrompts(),batch=$('requestMode').value==='batch',workflow=$('requestMode').value==='workflow',experiment=$('requestMode').value==='experiment';let error='';
  if(prompts.length>runtime.maxBatchSize)error=`최대 ${runtime.maxBatchSize}개까지 접수할 수 있습니다.`;
  else if(prompts.some(p=>!p))error=batch?'구분선 사이에 비어 있는 요청이 있습니다.':'요청 내용을 입력해 주세요.';
  else if(prompts.some(p=>p.length>32000))error='각 요청은 32,000자 이하여야 합니다.';
  else if(prompts.reduce((sum,p)=>sum+p.length,0)>runtime.maxBatchCharacters)error='전체 요청은 256,000자 이하여야 합니다.';
  if((workflow||experiment)&&prompts[0].length>8000)error='협업·비교 요청은 8,000자 이하여야 합니다.';
  if(workflow&&$('useDraft').checked&&(!$('initialDraft').value.trim()||$('initialDraft').value.length>8000))error='기존 초안을 1~8,000자로 입력해 주세요.';
  if(experiment&&(!$('experimentDraft').value.trim()||$('experimentDraft').value.length>8000))error='비교할 초안을 1~8,000자로 입력해 주세요.';
  if(experiment&&$('expectedOutput').value.length>8000)error='기대 답변은 8,000자 이하여야 합니다.';
  $('inputCount').textContent=$('prompt').value?(error||(batch?`${prompts.length}개 요청`:`${prompts[0].length.toLocaleString()}자`)):'';
  $('inputCount').className=error&&$('prompt').value?'invalid':'';$('submit').disabled=submitting||!!error;
  $('submit').textContent=submitting?'접수 중…':batch?`${prompts.length}개 실행`:workflow?'협업 시작':experiment?'비교 시작':'실행';return {prompts,error};
}
$('requestMode').onchange=()=>{
  const batch=$('requestMode').value==='batch',workflow=$('requestMode').value==='workflow',experiment=$('requestMode').value==='experiment';$('sampleBatch').hidden=!batch;$('prompt').maxLength=batch?257000:workflow||experiment?8000:32000;$('workflowOptions').hidden=!workflow;$('experimentOptions').hidden=!experiment;
  $('inputHelp').textContent=batch?'--- 줄로 구분 · 최대 50개':workflow||experiment?'최대 8,000자':'최대 32,000자';
  $('prompt').placeholder=batch?'첫 번째 요청\n---\n두 번째 요청\n---\n세 번째 요청':'무엇을 실행할까요?';validateInput();
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
$('useDraft').onchange=()=>{$('initialDraft').hidden=!$('useDraft').checked;validateInput();};
$('initialDraft').oninput=validateInput;
$('experimentDraft').oninput=validateInput;$('expectedOutput').oninput=validateInput;
$('requestForm').onsubmit=async event=>{
  event.preventDefault();if(submitting)return;const {prompts,error}=validateInput();if(error){notice(error,true);return;}
  const batch=$('requestMode').value==='batch',workflow=$('requestMode').value==='workflow',experiment=$('requestMode').value==='experiment';submitting=true;$('composeError').hidden=true;validateInput();
  try{
    const targetNode=$('node').value||null;
    if(experiment){
      const accepted=await api('/api/experiments',{method:'POST',body:JSON.stringify({prompt:prompts[0],targetNode,initialDraft:$('experimentDraft').value.trim(),expectedOutput:$('expectedOutput').value.trim()||null})});
      chooseExperiment(accepted.experimentId);$('composeDialog').close();location.hash='experiments';notice('같은 초안으로 비교를 시작했습니다.');await refresh();return;
    }
    if(workflow){
      const accepted=await api('/api/workflows',{method:'POST',body:JSON.stringify({prompt:prompts[0],targetNode,initialDraft:$('useDraft').checked?$('initialDraft').value.trim():null})});
      chooseWorkflow(accepted.workflowId);selected=accepted.steps[0].task.taskId;$('composeDialog').close();location.hash='flows';notice('협업을 시작했습니다.');await refresh();return;
    }
    const accepted=await api(batch?'/api/commands/batch':'/api/commands',{method:'POST',body:JSON.stringify(batch?{prompts,targetNode}:{prompt:prompts[0],targetNode})});
    if(batch){chooseBatch(accepted.summary.batchId);notice(`${accepted.summary.total}개 작업을 한 묶음으로 접수했습니다.`);}
    else{chooseBatch('');selected=accepted.taskId;notice('작업을 접수했습니다.');}
    $('composeDialog').close();$('workTab').click();
    await refresh();
  }catch(e){$('composeError').textContent=e.message;$('composeError').hidden=false;}finally{submitting=false;validateInput();}
};
$('accessForm').onsubmit=async event=>{event.preventDefault();$('connect').disabled=true;$('accessError').hidden=true;try{if(await connect()){$('accessDialog').close();notice('연결되었습니다.');}}finally{$('connect').disabled=false;}};
$('openAccess').onclick=()=>{$('accessError').hidden=true;$('accessDialog').showModal();};
$('newRun').onclick=()=>{
  if(!$('prompt').value){$('requestMode').value=location.hash==='#flows'?'workflow':location.hash==='#experiments'?'experiment':'single';$('requestMode').onchange();}
  $('composeError').hidden=true;$('composeDialog').showModal();$('prompt').focus();
};
for(const button of document.querySelectorAll('[data-close]'))button.onclick=()=>$(button.dataset.close).close();
$('requestForm').addEventListener('keydown',event=>{if((event.ctrlKey||event.metaKey)&&event.key==='Enter'&&!$('submit').disabled){event.preventDefault();$('requestForm').requestSubmit();}});
$('filter').onchange=refresh;$('batchFilter').onchange=()=>chooseBatch($('batchFilter').value);validateInput();

function chooseExperiment(id){
  activeExperiment=id;const url=new URL(location);if(id)url.searchParams.set('experiment',id);else url.searchParams.delete('experiment');history.replaceState(null,'',url);
}
$('experimentPicker').onchange=()=>{chooseExperiment($('experimentPicker').value);refreshExperiments();};
async function refreshExperiments(){
  const key=$('apiKey').value;let id=activeExperiment;
  const items=await api('/api/experiments?limit=10');
  if(key!==$('apiKey').value||id!==activeExperiment)return;
  if(!id&&items.length){chooseExperiment(items[0].experimentId);id=activeExperiment;}
  const picker=$('experimentPicker');picker.replaceChildren(new Option('최근 비교 선택',''));
  for(const item of items)picker.add(new Option(`${names[item.status]} · ${item.prompt.slice(0,35)}`,item.experimentId));
  if(id&&!items.some(item=>item.experimentId===id))picker.add(new Option('선택한 비교',id));picker.value=id;
  const box=$('experimentDetail');
  if(!id){box.replaceChildren(element('div','같은 초안으로 두 경로를 비교합니다. 새 실행에서 시작하세요.','empty-state'));return;}
  const experiment=items.find(item=>item.experimentId===id)||await api('/api/experiments/'+id);
  if(key!==$('apiKey').value||id!==activeExperiment)return;
  box.replaceChildren(element('p',experiment.prompt,'workflow-prompt'));
  const metrics=element('div','','workflow-metrics');
  metrics.append(element('span',names[experiment.status],'badge '+experiment.status),element('span','동일 요청 · 동일 초안'));
  const link=element('a','고유 링크 ↗');link.href='/?experiment='+encodeURIComponent(id)+'#experiments';metrics.append(link);box.append(metrics);
  const inputs=element('div','','experiment-inputs');
  for(const [label,value] of [['공통 초안',experiment.initialDraft],['기대 답변',experiment.expectedOutput]]){
    const input=element('div','');input.append(element('h3',label),element('pre',value??'미지정 · 채점 없이 비교'));inputs.append(input);
  }
  box.append(inputs);
  const columns=element('div','','experiment-arms');box.append(columns);
  for(const [kind,label] of [['direct','바로 재작성'],['review','검토 후 수정']]){
    const arm=experiment[kind],flow=arm.workflow,card=element('div','','workflow-step');card.dataset.arm=kind;
    const last=flow.steps.at(-1).task;
    card.append(element('h3',label),element('span',names[flow.status],'badge '+flow.status));
    const score=arm.matchesExpected;
    const scoreLabel=score===true?'기대 답변 일치':score===false?'기대 답변 불일치':!experiment.expectedOutput?'미채점':flow.status==='FAILED'?'실행 실패 · 미채점':'채점 대기';
    card.append(element('span',scoreLabel,'badge experiment-score'+(score===true?' eval-PASS':score===false?' eval-REJECT':'')));
    card.append(element('small',`${flow.inferenceCalls}회 실행 · 추론 ${duration(0,flow.inferenceMillis)} · 전체 ${duration(0,arm.elapsedMillis)}`,'muted'));
    if(score!==null)card.append(element('p',`초안 ${experiment.draftMatchesExpected?'일치':'불일치'} → 수정본 ${score?'일치':'불일치'}`,'muted'));
    card.append(element('pre',arm.output??'결과 대기 중…'));
    const review=flow.steps.find(step=>step.stage==='REVIEW');
    if(review?.task.output){const details=disclosure(id,expandedExperimentDetails,'검토 의견');details.append(element('pre',review.task.output));card.append(details);}
    const models=[...new Set(flow.steps.filter(step=>!step.provided).map(step=>step.model))];
    card.append(element('small',`${models.join(', ')||flow.model} · ${flow.targetNode||'자동 배정'}`,'muted'));
    const detail=element('a','단계 상세 ↗');detail.href='/?flow='+encodeURIComponent(flow.workflowId)+'#flows';detail.className='experiment-detail-link';card.append(detail);
    if(flow.status==='FAILED'){
      card.append(element('p',last.failureStage==='DELIVERY'?'전달 실패 · 결과 저장됨':'모델 실행 실패','error-message'));
      const retry=element('button','이 경로 다시 시도','secondary');retry.onclick=async()=>{retry.disabled=true;try{await api('/api/workflows/'+flow.workflowId+'/retry',{method:'POST'});await refreshExperiments();}catch(e){notice(e.message,true);retry.disabled=false;}};card.append(retry);
    }
    columns.append(card);
  }
  box.append(element('p','일치는 기대 답변과의 문자열 비교입니다. 전체 시간에는 두 경로의 큐 대기와 전달이 포함됩니다.','experiment-note'));
}

function chooseWorkflow(id){
  activeWorkflow=id;const url=new URL(location);if(id)url.searchParams.set('flow',id);else url.searchParams.delete('flow');history.replaceState(null,'',url);
}
$('workflowPicker').onchange=()=>{chooseWorkflow($('workflowPicker').value);refreshWorkflows();};
async function refreshWorkflows(){
  const key=$('apiKey').value;let id=activeWorkflow;
  const items=await api('/api/workflows?limit=10');
  if(key!==$('apiKey').value||id!==activeWorkflow)return;
  $('flowCount').textContent=String(items.length);$('flowCount').title='최근 협업 수 (최대 10개)';
  if(!id&&items.length&&location.hash==='#flows'){chooseWorkflow(items[0].workflowId);id=activeWorkflow;}
  const picker=$('workflowPicker');picker.replaceChildren(new Option('최근 협업 선택',''));
  for(const item of items)picker.add(new Option(`${names[item.status]} · ${item.prompt.slice(0,35)}`,item.workflowId));
  if(id&&!items.some(w=>w.workflowId===id))picker.add(new Option('선택한 협업',id));picker.value=id;
  const box=$('workflowDetail');
  if(!id){box.replaceChildren(element('div',items.length?'협업을 선택하세요.':'아직 협업이 없습니다. 새 실행에서 시작하세요.','empty-state'));return;}
  const workflow=items.find(w=>w.workflowId===id)||await api('/api/workflows/'+id);
  if(key!==$('apiKey').value||id!==activeWorkflow)return;
  box.replaceChildren(element('p',workflow.prompt,'workflow-prompt'));
  const end=workflow.status==='SUCCEEDED'||workflow.status==='FAILED'?workflow.steps.at(-1).task.finishedAt:Date.now();
  const metrics=element('div','','workflow-metrics');const state=element('span',names[workflow.status],'badge '+workflow.status);state.title='절차의 실행 상태 · 답변 품질 판정과 별개';
  metrics.append(state,element('span',workflow.model),element('span',`모델 실행 시도 ${workflow.inferenceCalls}회`),element('span',`추론 ${duration(0,workflow.inferenceMillis)}`),element('span',`전체 ${duration(workflow.createdAt,end)}`));
  const link=element('a','고유 링크 ↗');link.href='/?flow='+encodeURIComponent(id)+'#flows';metrics.append(link);box.append(metrics);
  const direct=workflow.mode==='DIRECT';
  $('workflowSection').querySelector('h2').textContent=direct?'초안 → 바로 재작성':'작성 → 검토 → 수정';
  const columns=element('div','','workflow-steps'+(direct?' direct':''));box.append(columns);
  const stages=direct?[['DRAFT','1. 초안'],['REVISION','2. 바로 재작성']]:[['DRAFT','1. 초안'],['REVIEW','2. 검토 의견'],['REVISION','3. 수정본']];
  for(const [stage,label] of stages){
    const card=element('div','','workflow-step');card.dataset.stage=stage;card.append(element('h3',label));columns.append(card);
    const step=workflow.steps.find(s=>s.stage===stage);
    if(!step){card.append(element('p','앞 단계 완료 후 시작합니다.','muted'));continue;}
    const task=step.task;card.append(element('span',names[task.status],'badge '+task.status));
    card.append(element('small',step.provided?'직접 입력 · 모델 호출 없음':`${step.inferenceCalls}회 실행 · ${duration(0,step.inferenceMillis)}`,'muted'));
    if(task.output!==null)card.append(element('pre',task.output));
    if(step.sourceThreadId)card.append(element('p',`Coral → ${step.sourceReader==='reviewer'?'검토자':'작성자'} · 문맥 전달됨`,'source'));
    if(!step.provided){
      const details=document.createElement('details');details.open=expandedWorkflowSteps.has(task.taskId);
      details.ontoggle=()=>{if(details.isConnected){if(details.open)expandedWorkflowSteps.add(task.taskId);else expandedWorkflowSteps.delete(task.taskId);}};
      details.append(element('summary','입력 · 실행 정보'),element('pre',step.systemPrompt?`[역할 지시]\n${step.systemPrompt}\n\n[사용자 입력]\n${task.prompt}`:task.prompt));
      if(task.threadId)details.append(element('small','Coral thread · '+task.threadId,'mono'));card.append(details);
    }
    if(task.status==='FAILED'){
      card.append(element('p',task.failureStage==='DELIVERY'?'전달 실패 · 결과 저장됨':'모델 실행 실패','error-message'));
      const retry=element('button','이 단계 다시 시도','secondary');retry.onclick=async()=>{retry.disabled=true;try{await api('/api/workflows/'+id+'/retry',{method:'POST'});await refreshWorkflows();}catch(e){notice(e.message,true);retry.disabled=false;}};card.append(retry);
    }
  }
}
