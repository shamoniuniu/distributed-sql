param(
    [string]$ArtifactRoot = "artifacts/task16",
    [int]$GenerationTimeoutSeconds = 900,
    [int]$QueryTimeoutSeconds = 900,
    [switch]$SkipFast
)

$ErrorActionPreference = "Stop"
$root = [System.IO.Path]::GetFullPath($ArtifactRoot)
New-Item -ItemType Directory -Force -Path $root | Out-Null

if (-not $SkipFast) {
    uv run pytest -m "not stress" --junitxml "$root/fast-junit.xml"
    if ($LASTEXITCODE -ne 0) {
        throw "Task 16 fast test layer failed with exit code $LASTEXITCODE"
    }
}

$env:RUN_TASK16_STRESS = "1"
$env:TASK16_ARTIFACT_ROOT = $root
$env:TASK16_GENERATION_TIMEOUT = "$GenerationTimeoutSeconds"
$env:TASK16_QUERY_TIMEOUT = "$QueryTimeoutSeconds"
try {
    uv run pytest tests/test_task16_acceptance.py -m stress `
        --junitxml "$root/stress-junit.xml" `
        --timeout ([Math]::Max(
            60,
            $GenerationTimeoutSeconds + 3 * $QueryTimeoutSeconds + 120
        ))
    if ($LASTEXITCODE -ne 0) {
        throw "Task 16 1 GiB stress layer failed with exit code $LASTEXITCODE"
    }
}
finally {
    Remove-Item Env:RUN_TASK16_STRESS -ErrorAction SilentlyContinue
    Remove-Item Env:TASK16_ARTIFACT_ROOT -ErrorAction SilentlyContinue
    Remove-Item Env:TASK16_GENERATION_TIMEOUT -ErrorAction SilentlyContinue
    Remove-Item Env:TASK16_QUERY_TIMEOUT -ErrorAction SilentlyContinue
}

Write-Output "Task 16 acceptance passed"
Write-Output "JUnit: $root/fast-junit.xml"
Write-Output "JUnit: $root/stress-junit.xml"
Write-Output "JSON:  $root/task16-results.json"
Write-Output "Summary: $root/task16-summary.md"
