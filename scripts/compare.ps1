param(
    [string]$BaseUrl = 'http://localhost:18080',
    [string]$ApiKey = '',
    [string]$TargetNode = '',
    [ValidateRange(1,10)][int]$Repetitions = 3,
    [int]$TimeoutSeconds = 600
)
$ErrorActionPreference = 'Stop'
$headers = @{}
if ($ApiKey) { $headers['X-API-Key'] = $ApiKey }
function Read-ComparisonJson([string]$Uri) {
    $response = Invoke-WebRequest -UseBasicParsing -Uri $Uri -Headers $headers
    return [Text.Encoding]::UTF8.GetString($response.RawContentStream.ToArray()) | ConvertFrom-Json
}
$runtime = Read-ComparisonJson "$BaseUrl/api/runtime"
if ($runtime.mode -ne 'OLLAMA') { throw 'This experiment requires a real Ollama model.' }
if (-not $TargetNode) {
    $nodes = @(Read-ComparisonJson "$BaseUrl/api/nodes")
    if (-not $nodes.Count) { throw 'A configured worker node is required.' }
    $TargetNode = $nodes[0]
}
$cases = @(
    @{ name='product-label'; prompt='다음 문의를 배송, 환불, 상품 중 정확히 한 단어로 분류하세요: 이 가방은 방수가 되나요?'; draft='배송'; expected='상품' },
    @{ name='negative-review'; prompt='다음 리뷰의 감정을 긍정, 부정, 중립 중 정확히 한 단어로 분류하세요: 제품이 고장 나서 사용할 수 없습니다.'; draft='긍정'; expected='부정' },
    @{ name='meeting-correction'; prompt='다음 문장을 그대로 출력하세요: 회의는 오후 3시에 시작합니다.'; draft='회의는 오전 3시에 시작합니다.'; expected='회의는 오후 3시에 시작합니다.' },
    @{ name='correct-meeting'; prompt='다음 문장을 그대로 출력하세요: 회의는 오후 3시에 시작합니다.'; draft='회의는 오후 3시에 시작합니다.'; expected='회의는 오후 3시에 시작합니다.' },
    @{ name='correct-refund'; prompt='다음 문의를 배송, 환불, 상품 중 정확히 한 단어로 분류하세요: 구매를 취소하고 돈을 돌려받고 싶습니다.'; draft='환불'; expected='환불' }
)
$records = @()
$directory = Join-Path $PSScriptRoot '../build'
New-Item -ItemType Directory -Path $directory -Force | Out-Null
$outputPath = Join-Path $directory 'comparison-live-result.json'
function Save-Comparisons {
    @{ runtime=$runtime; targetNode=$TargetNode; repetitions=$Repetitions; capturedAt=(Get-Date).ToUniversalTime().ToString('o'); cases=$records } |
        ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $outputPath -Encoding utf8
}
for ($round = 1; $round -le $Repetitions; $round++) {
    foreach ($case in $cases) {
        $body = @{ prompt=$case.prompt; initialDraft=$case.draft; expectedOutput=$case.expected; targetNode=$TargetNode } | ConvertTo-Json
        $response = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$BaseUrl/api/experiments" -Headers $headers -ContentType 'application/json; charset=utf-8' -Body ([Text.Encoding]::UTF8.GetBytes($body))
        $experiment = [Text.Encoding]::UTF8.GetString($response.RawContentStream.ToArray()) | ConvertFrom-Json
        $record = @{ name=$case.name; round=$round; experiment=$experiment }
        $records += $record
        Save-Comparisons
        Write-Output ("Started {0}/{1} {2}: {3}" -f $round,$Repetitions,$case.name,$experiment.experimentId)
        $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
        while ($experiment.status -notin @('SUCCEEDED','FAILED')) {
            if ((Get-Date) -gt $deadline) { throw "Experiment timed out; retained ID: $($experiment.experimentId)" }
            Start-Sleep -Seconds 1
            $experiment = Read-ComparisonJson "$BaseUrl/api/experiments/$($experiment.experimentId)"
            $record.experiment = $experiment
        }
        Save-Comparisons
        if ($experiment.status -ne 'SUCCEEDED') { throw "Experiment failed; results retained: $($experiment.experimentId)" }
        if ($experiment.direct.workflow.steps.Count -ne 2 -or $experiment.review.workflow.steps.Count -ne 3) { throw 'Unexpected experiment stages.' }
        if ($experiment.direct.workflow.steps[-1].sourceReader -ne 'writer' -or $experiment.review.workflow.steps[1].sourceReader -ne 'reviewer') { throw 'Coral reader roles missing.' }
        if ($experiment.direct.workflow.steps[-1].systemPrompt -ne $experiment.review.workflow.steps[-1].systemPrompt) { throw 'Revision instructions differ between arms.' }
        if ($experiment.direct.workflow.steps[0].task.output -cne $experiment.review.workflow.steps[0].task.output) { throw 'The two drafts differ.' }
        Write-Output ("Completed {0}: direct match={1}, review match={2}, calls={3}+{4}" -f $case.name,$experiment.direct.matchesExpected,$experiment.review.matchesExpected,$experiment.direct.workflow.inferenceCalls,$experiment.review.workflow.inferenceCalls)
    }
}
$summary = foreach ($arm in @('direct','review')) {
    $completed = @($records | Where-Object { $_.experiment.$arm.workflow.status -eq 'SUCCEEDED' })
    $correctDrafts = @($completed | Where-Object { $_.experiment.draftMatchesExpected -eq $true })
    $incorrectDrafts = @($completed | Where-Object { $_.experiment.draftMatchesExpected -eq $false })
    [pscustomobject]@{
        arm=$arm
        completed=$completed.Count
        matched=@($completed | Where-Object { $_.experiment.$arm.matchesExpected -eq $true }).Count
        corrected=@($incorrectDrafts | Where-Object { $_.experiment.$arm.matchesExpected -eq $true }).Count
        incorrectDrafts=$incorrectDrafts.Count
        preserved=@($correctDrafts | Where-Object { $_.experiment.$arm.matchesExpected -eq $true }).Count
        correctDrafts=$correctDrafts.Count
        calls=($completed | ForEach-Object { $_.experiment.$arm.workflow.inferenceCalls } | Measure-Object -Sum).Sum
        inferenceMillis=($completed | ForEach-Object { $_.experiment.$arm.workflow.inferenceMillis } | Measure-Object -Sum).Sum
    }
}
$summary | Format-Table -AutoSize
$summary | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $directory 'comparison-live-summary.json') -Encoding utf8
Write-Output "Saved to $outputPath. Exact-match observations, not a general accuracy or latency benchmark."
