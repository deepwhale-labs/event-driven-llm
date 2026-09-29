param([string]$BaseUrl = 'http://localhost:18080', [string]$ProjectName = 'event-driven-llm', [string]$ApiKey = $env:APP_API_KEY)
# Integration test: temporarily stops this Compose project's broker and Coral; restores them in finally.
$ErrorActionPreference = 'Stop'
$headers = @{}
if ($ApiKey) { $headers['X-API-Key'] = $ApiKey }
function Compose([string[]]$Arguments) {
    & docker compose -p $ProjectName @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Compose operation failed: $Arguments" }
}
function Submit {
    return Invoke-RestMethod -Method Post "$BaseUrl/api/commands" -Headers $headers -ContentType 'application/json' -Body '{"prompt":"Recovery verification"}'
}
function Wait-Task([string]$Id, [string]$Status) {
    $deadline = [DateTime]::UtcNow.AddSeconds(150)
    do {
        try {
            $task = Invoke-RestMethod "$BaseUrl/api/tasks/$Id" -Headers $headers -TimeoutSec 10
            if ($task.status -eq $Status) { return $task }
        } catch { if ([DateTime]::UtcNow -ge $deadline) { throw } }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Task $Id did not become $Status"
}
$first = Submit
$saved = Wait-Task $first.taskId 'SUCCEEDED'
try {
    Compose @('stop', 'coral')
    $persisted = Invoke-RestMethod "$BaseUrl/api/results/$($first.taskId)" -Headers $headers
    if ($persisted.output -ne $saved.output) { throw 'Result unavailable while Coral is stopped.' }
    $manual = Submit
    $automatic = Submit
    $failed = Wait-Task $manual.taskId 'FAILED'
    $null = Wait-Task $automatic.taskId 'FAILED'
    if ($failed.failureStage -ne 'DELIVERY' -or !$failed.output) { throw 'Delivery failure did not preserve inference.' }
    Compose @('start', 'coral')
    $null = Invoke-RestMethod -Method Post "$BaseUrl/api/tasks/$($manual.taskId)/retry" -Headers $headers
    $retried = Wait-Task $manual.taskId 'SUCCEEDED'
    $auto = Wait-Task $automatic.taskId 'SUCCEEDED'
    if ($retried.output -ne $failed.output -or $retried.attempt -le 1 -or $auto.attempt -le 1) { throw 'Stage-aware replay failed.' }
    Compose @('stop', 'broker')
    $queued = Submit
    $accepted = Invoke-RestMethod "$BaseUrl/api/tasks/$($queued.taskId)" -Headers $headers
    if ($accepted.status -ne 'QUEUED') { throw 'Offline broker request was not durably queued.' }
    Compose @('start', 'broker')
    $null = Wait-Task $queued.taskId 'SUCCEEDED'
    Compose @('restart', 'app')
    $afterRestart = Wait-Task $first.taskId 'SUCCEEDED'
    if ($afterRestart.output -ne $saved.output) { throw 'Result was lost on app restart.' }
    Write-Output 'PASS: result persistence, manual/automatic delivery replay, offline broker outbox, app restart'
} finally {
    Compose @('start', 'broker', 'coral')
}
