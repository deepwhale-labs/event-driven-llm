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
$topology = Invoke-RestMethod "$BaseUrl/api/kafka/topology" -Headers $headers
if ($topology.status -ne 'CONNECTED' -or $topology.brokers.Count -lt 1 -or $topology.topics.Count -lt 4) { throw 'Kafka topology unavailable.' }
Write-Output 'PASS: durable API -> Kafka -> worker -> saved result -> Coral; node routing; metrics; Kafka topology'
