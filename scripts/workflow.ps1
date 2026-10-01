param(
    [string]$BaseUrl = 'http://localhost:18080',
    [string]$ApiKey = '',
    [int]$TimeoutSeconds = 600
)
$ErrorActionPreference = 'Stop'
$headers = @{}
if ($ApiKey) { $headers['X-API-Key'] = $ApiKey }
function Read-WorkflowJson([string]$Uri) {
    # Windows PowerShell 5.1 otherwise decodes JSON without charset as Latin-1.
    $response = Invoke-WebRequest -UseBasicParsing -Uri $Uri -Headers $headers
    return [Text.Encoding]::UTF8.GetString($response.RawContentStream.ToArray()) | ConvertFrom-Json
}
$runtime = Invoke-RestMethod -Uri "$BaseUrl/api/runtime" -Headers $headers
if ($runtime.mode -notin @('OLLAMA', 'CODEX_CLI')) { throw 'This experiment requires a real model.' }
$cases = @(
    @{ name='product-label'; prompt='다음 문의를 배송, 환불, 상품 중 정확히 한 단어로 분류하세요: 이 가방은 방수가 되나요?'; initialDraft='배송'; expected='상품' },
    @{ name='negative-review'; prompt='다음 리뷰의 감정을 긍정, 부정, 중립 중 정확히 한 단어로 분류하세요: 제품이 고장 나서 사용할 수 없습니다.'; initialDraft='긍정'; expected='부정' },
    @{ name='meeting-time'; prompt='회의 시간이 오후 2시에서 오후 3시로 변경되었습니다. 변경 안내를 한국어 한 문장으로 작성하세요.'; initialDraft='회의 시간이 오전 3시로 변경되었습니다.'; expected='오후 3시를 보존한 한 문장' },
    @{ name='correct-control'; prompt='다음 안내를 그대로 출력하세요: 회의는 오후 3시에 시작합니다.'; initialDraft='회의는 오후 3시에 시작합니다.'; expected='회의는 오후 3시에 시작합니다.' },
    @{ name='generated-draft'; prompt='다음 문의를 배송, 환불, 상품 중 정확히 한 단어로 분류하세요: 구매를 취소하고 돈을 돌려받고 싶습니다.'; initialDraft=$null; expected='환불' }
)
$records = @()
$outputDirectory = Join-Path $PSScriptRoot '../build'
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
$outputPath = Join-Path $outputDirectory 'workflow-live-result.json'
foreach ($case in $cases) {
    $body = @{ prompt=$case.prompt; initialDraft=$case.initialDraft } | ConvertTo-Json
    $flow = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/workflows" -Headers $headers -ContentType 'application/json; charset=utf-8' -Body ([Text.Encoding]::UTF8.GetBytes($body))
    Write-Output ("Started {0}: {1}" -f $case.name, $flow.workflowId)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ($flow.status -notin @('SUCCEEDED','FAILED')) {
        if ((Get-Date) -gt $deadline) { throw "Workflow timed out: $($flow.workflowId)" }
        Start-Sleep -Seconds 1
        $flow = Read-WorkflowJson "$BaseUrl/api/workflows/$($flow.workflowId)"
    }
    $records += @{ name=$case.name; expected=$case.expected; suppliedDraft=$case.initialDraft; workflow=$flow }
    @{ runtime=$runtime; capturedAt=(Get-Date).ToUniversalTime().ToString('o'); cases=$records } | ConvertTo-Json -Depth 15 | Set-Content -LiteralPath $outputPath -Encoding utf8
    if ($flow.status -ne 'SUCCEEDED') { throw "Workflow failed; retained record: $($flow.workflowId)" }
    if ($flow.steps.Count -ne 3) { throw 'Expected three recorded steps.' }
    if ($flow.steps[1].sourceReader -ne 'reviewer' -or $flow.steps[2].sourceReader -ne 'writer') { throw 'Coral reader roles missing.' }
    if (-not $flow.steps[2].task.prompt.Contains($flow.steps[1].task.output)) { throw 'Review was not included in revision input.' }
    Write-Output ("Completed {0}: {1} calls; draft={2}; final={3}" -f $case.name, $flow.inferenceCalls, $flow.steps[0].task.output, $flow.steps[2].task.output)
}
Write-Output "Saved all observations to $outputPath. Completion checks do not certify answer quality."
