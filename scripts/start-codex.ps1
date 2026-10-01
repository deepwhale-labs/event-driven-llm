param([string]$Model = '', [int]$Port = 18081, [switch]$NoBuild, [switch]$BridgeOnly)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$bridgeScript = Join-Path $PSScriptRoot 'codex_bridge.py'
$runtimeDir = Join-Path $projectRoot 'build/codex-bridge'
if ($Model) { & python $bridgeScript prepare --port $Port --model $Model }
else { & python $bridgeScript prepare --port $Port }
if ($LASTEXITCODE -ne 0) { throw 'Cannot configure Codex CLI bridge.' }
$config = Get-Content -Raw -Encoding UTF8 (Join-Path $runtimeDir 'config.json') | ConvertFrom-Json
$headers = @{ 'X-CLI-Token' = $config.token }
$healthUrl = "http://127.0.0.1:$Port/health"
$health = $null
try { $health = Invoke-RestMethod $healthUrl -Headers $headers -TimeoutSec 3 } catch { }
if (!$health) {
    $bridgeProcess = Start-Process -FilePath (Get-Command python).Source -ArgumentList ('"{0}" serve' -f $bridgeScript) `
        -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $runtimeDir 'bridge.log') -RedirectStandardError (Join-Path $runtimeDir 'bridge-error.log')
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    do {
        Start-Sleep -Milliseconds 500
        try { $health = Invoke-RestMethod $healthUrl -Headers $headers -TimeoutSec 2 } catch { }
        if ($bridgeProcess.HasExited) { throw 'CLI bridge exited. Check build/codex-bridge/bridge-error.log.' }
    } while (!$health -and [DateTime]::UtcNow -lt $deadline)
}
if (!$health -or $health.model -ne $config.model) { throw 'CLI bridge is unavailable or has another model. Stop the previous bridge before changing its model.' }
Write-Output ("Codex CLI bridge ready: {0} (PID {1})" -f $health.model, $health.pid)
if ($BridgeOnly) { return }
Push-Location $projectRoot
try {
    $composeArgs = @('compose')
    if (Test-Path -LiteralPath (Join-Path $projectRoot '.env')) { $composeArgs += @('--env-file', '.env') }
    $composeArgs += @('--env-file', 'build/codex-bridge/compose.env', '-f', 'docker-compose.yml', '-f', 'docker-compose.codex.yml', 'up', '-d')
    if (!$NoBuild) { $composeArgs += '--build' }
    $composeArgs += 'app'
    & docker @composeArgs
    if ($LASTEXITCODE -ne 0) { throw 'Codex app deployment failed.' }
} finally { Pop-Location }
Write-Output 'App started with Codex CLI. Open http://localhost:18080.'
