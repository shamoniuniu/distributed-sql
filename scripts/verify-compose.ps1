param(
    [switch]$KeepRunning
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root

try {
    docker compose -f compose.yaml down --volumes --remove-orphans
    docker compose -f compose.yaml config --quiet
    docker build --pull --target coordinator -t distributed-sql-coordinator:0.1.0 .
    docker build --pull --target worker -t distributed-sql-worker:0.1.0 .
    $env:DISTRIBUTED_SQL_COMPOSE_LEASE_TTL_SECONDS = "2"
    $env:DISTRIBUTED_SQL_COMPOSE_LEASE_CHECK_INTERVAL_SECONDS = "0.2"
    $env:DISTRIBUTED_SQL_COMPOSE_HEARTBEAT_INTERVAL_SECONDS = "0.5"
    $env:DISTRIBUTED_SQL_COMPOSE_TASK_START_DELAY_SECONDS = "4"
    docker compose -f compose.yaml up --detach --wait --wait-timeout 180 --no-build
    uv run --frozen python scripts/compose_acceptance.py `
        --timeout 180 `
        --evidence artifacts/task23/compose-results.json
}
catch {
    docker compose -f compose.yaml ps
    docker compose -f compose.yaml logs --no-color --tail 100
    throw
}
finally {
    if (-not $KeepRunning) {
        docker compose -f compose.yaml down --volumes --remove-orphans
    }
    Remove-Item Env:DISTRIBUTED_SQL_COMPOSE_LEASE_TTL_SECONDS -ErrorAction SilentlyContinue
    Remove-Item Env:DISTRIBUTED_SQL_COMPOSE_LEASE_CHECK_INTERVAL_SECONDS -ErrorAction SilentlyContinue
    Remove-Item Env:DISTRIBUTED_SQL_COMPOSE_HEARTBEAT_INTERVAL_SECONDS -ErrorAction SilentlyContinue
    Remove-Item Env:DISTRIBUTED_SQL_COMPOSE_TASK_START_DELAY_SECONDS -ErrorAction SilentlyContinue
    Pop-Location
}
