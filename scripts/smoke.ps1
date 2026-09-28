param([string]$BaseUrl = 'http://localhost:18080')
$ErrorActionPreference = 'Stop'

function Wait-Json([string]$Uri) {
    $deadline = [DateTime]::UtcNow.AddSeconds(90)
    do {
        try { return Invoke-RestMethod -Uri $Uri -TimeoutSec 40 }
        catch {
            if ([DateTime]::UtcNow -ge $deadline) { throw }
            Start-Sleep -Seconds 2
        }
    } while ($true)
}

$coral = Wait-Json "$BaseUrl/api/coral"
if ($coral.status -ne 'connected') { throw 'Coral did not connect.' }
$request = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/commands" `
    -ContentType 'application/json' -Body '{"prompt":"Kafka to Coral smoke test"}'
$result = Wait-Json "$BaseUrl/api/results/$($request.taskId)"
if ($result.taskId -ne $request.taskId -or !$result.threadId -or !$result.output) {
    throw 'The Coral delivery receipt is incomplete.'
}
$result | ConvertTo-Json
Write-Output 'PASS: API -> Kafka -> worker -> Kafka -> Coral thread/message'
