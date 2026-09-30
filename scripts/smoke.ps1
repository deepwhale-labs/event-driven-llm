param([string]$BaseUrl = 'http://localhost:18080', [string]$ApiKey = $env:APP_API_KEY, [switch]$RequireModel)
$ErrorActionPreference = 'Stop'
$headers = @{}
if ($ApiKey) { $headers['X-API-Key'] = $ApiKey }

function Wait-Json([string]$Uri, [scriptblock]$Ready = { param($value) $true }) {
    $deadline = [DateTime]::UtcNow.AddSeconds(180)
    do {
        try {
            $value = Invoke-RestMethod -Uri $Uri -Headers $headers -TimeoutSec 40
            if (& $Ready $value) { return $value }
        } catch { if ([DateTime]::UtcNow -ge $deadline) { throw } }
        if ([DateTime]::UtcNow -ge $deadline) { throw "Timed out waiting for $Uri" }
        Start-Sleep -Seconds 2
    } while ($true)
}

$health = Wait-Json "$BaseUrl/actuator/health/readiness"
if ($health.status -ne 'UP') { throw 'Database readiness failed.' }
$coral = Wait-Json "$BaseUrl/api/coral"
if ($coral.status -ne 'connected') { throw 'Coral did not connect.' }
$nodes = Invoke-RestMethod "$BaseUrl/api/nodes" -Headers $headers
foreach ($target in @($null, $nodes[0])) {
    $body = @{ prompt = 'Explain Kafka in one short sentence.'; targetNode = $target } | ConvertTo-Json
    $request = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/commands" -Headers $headers -ContentType 'application/json' -Body $body
    $task = Wait-Json "$BaseUrl/api/tasks/$($request.taskId)" { param($value) $value.status -eq 'SUCCEEDED' }
    $result = Invoke-RestMethod "$BaseUrl/api/results/$($request.taskId)" -Headers $headers
    if ($result.taskId -ne $request.taskId -or !$result.threadId -or !$result.output) { throw 'Incomplete result.' }
    if ($target -and $result.nodeId -ne $target) { throw 'Node routing failed.' }
    if ($RequireModel -and $result.output.StartsWith('[DEMO')) { throw 'Expected real model inference.' }
    $result | ConvertTo-Json
}
$metrics = Invoke-RestMethod "$BaseUrl/actuator/metrics/llm.tasks" -Headers $headers
if ($metrics.name -ne 'llm.tasks') { throw 'Task metrics unavailable.' }
$body = @{ prompts = @('Reply briefly with hello.', 'What is 2 plus 3? Answer briefly.'); targetNode = $null } | ConvertTo-Json
$batch = Invoke-RestMethod -Method Post "$BaseUrl/api/commands/batch" -Headers $headers -ContentType 'application/json' -Body $body
if ($batch.summary.total -ne 2) { throw 'Batch was not accepted atomically.' }
$finished = Wait-Json "$BaseUrl/api/batches/$($batch.summary.batchId)" { param($value) $value.summary.succeeded -eq 2 }
foreach ($task in $finished.tasks) {
    if ($task.batchId -ne $batch.summary.batchId -or !$task.output -or !$task.threadId) { throw 'Batch result incomplete.' }
    if ($null -eq $task.queuedAt -or $null -eq $task.startedAt -or $null -eq $task.inferenceCompletedAt -or $null -eq $task.finishedAt) { throw 'Missing task timings.' }
    if ($task.startedAt -lt $task.queuedAt -or $task.inferenceCompletedAt -lt $task.startedAt -or $task.finishedAt -lt $task.inferenceCompletedAt) { throw 'Task timings out of order.' }
    if ($RequireModel -and $task.output.StartsWith('[DEMO')) { throw 'Expected real batch inference.' }
}
$runtime = Invoke-RestMethod "$BaseUrl/api/runtime" -Headers $headers
if ($RequireModel -and $runtime.mode -ne 'OLLAMA') { throw 'Runtime is not configured for real inference.' }
$evaluation = Invoke-RestMethod "$BaseUrl/api/evaluations/config" -Headers $headers
if ($evaluation.mode -notin @('OFF', 'SHADOW')) { throw 'Invalid evaluation configuration.' }
$history = Invoke-RestMethod "$BaseUrl/api/tasks/$($finished.tasks[0].taskId)/evaluations" -Headers $headers
if ($evaluation.PSObject.Properties.Name -contains 'apiKey') { throw 'Evaluation config exposed a credential field.' }
$topology = Invoke-RestMethod "$BaseUrl/api/kafka/topology" -Headers $headers
if ($topology.status -ne 'CONNECTED' -or $topology.brokers.Count -lt 1 -or $topology.topics.Count -lt 4) { throw 'Kafka topology unavailable.' }
Write-Output 'PASS: durable API -> Kafka -> worker -> saved result -> Coral; node routing; atomic batch; timings; runtime; evaluation API; metrics; Kafka topology'
