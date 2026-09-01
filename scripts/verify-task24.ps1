param(
    [string]$K3sImage = "rancher/k3s:v1.33.5-k3s1",
    [string]$Evidence = "artifacts/task24/k3s-results-v1.json"
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$root = Split-Path -Parent $PSScriptRoot
$container = "distributed-sql-task24-k3s"
$network = "distributed-sql-task24"
$archive = Join-Path $root ".task24-images.tar"
$kubeconfig = Join-Path $root ".task24-kubeconfig.yaml"
$initialVersion = "task24-v1"
$upgradeVersion = "task24-v2"
$initialCoordinator = "distributed-sql-coordinator:$initialVersion"
$initialWorker = "distributed-sql-worker:$initialVersion"
$upgradeCoordinator = "distributed-sql-coordinator:$upgradeVersion"
$upgradeWorker = "distributed-sql-worker:$upgradeVersion"
$minio = "minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e"
$temporaryImages = @(
    $initialCoordinator,
    $initialWorker,
    $upgradeCoordinator,
    $upgradeWorker
)
$cleanupRecords = [System.Collections.Generic.List[object]]::new()
$failure = $null

function Invoke-CleanupCommand {
    param(
        [string]$Name,
        [scriptblock]$Action
    )

    $nativePreference = $PSNativeCommandUseErrorActionPreference
    $PSNativeCommandUseErrorActionPreference = $false
    try {
        $output = & $Action 2>&1 | Out-String
        $code = $LASTEXITCODE
    }
    finally {
        $PSNativeCommandUseErrorActionPreference = $nativePreference
    }
    $script:cleanupRecords.Add([ordered]@{
        name = $Name
        exit_code = $code
        output = $output.Trim()
    })
    return $code
}

Push-Location $root
try {
    docker info | Out-Null
    $existingContainers = docker container ls --all --format "{{.Names}}"
    if ($existingContainers -contains $container) {
        throw "Temporary K3s container already exists: $container"
    }
    $existingNetworks = docker network ls --format "{{.Name}}"
    if ($existingNetworks -contains $network) {
        throw "Temporary K3s network already exists: $network"
    }

    docker build --pull --target coordinator `
        --build-arg "BUILD_VERSION=$initialVersion" `
        -t $initialCoordinator .
    docker build --target worker `
        --build-arg "BUILD_VERSION=$initialVersion" `
        -t $initialWorker .
    docker build --target coordinator `
        --build-arg "BUILD_VERSION=$upgradeVersion" `
        -t $upgradeCoordinator .
    docker build --target worker `
        --build-arg "BUILD_VERSION=$upgradeVersion" `
        -t $upgradeWorker .
    docker pull $minio

    $initialCoordinatorId = docker image inspect $initialCoordinator --format "{{.Id}}"
    $upgradeCoordinatorId = docker image inspect $upgradeCoordinator --format "{{.Id}}"
    $initialWorkerId = docker image inspect $initialWorker --format "{{.Id}}"
    $upgradeWorkerId = docker image inspect $upgradeWorker --format "{{.Id}}"
    if (
        $initialCoordinatorId -eq $upgradeCoordinatorId -or
        $initialWorkerId -eq $upgradeWorkerId
    ) {
        throw "Initial and upgrade builds produced identical image IDs."
    }

    docker network create $network | Out-Null
    docker run --detach --privileged `
        --name $container `
        --network $network `
        --publish "127.0.0.1::6443" `
        $K3sImage server `
        --disable traefik `
        --tls-san 127.0.0.1 `
        --write-kubeconfig-mode 644 | Out-Null

    $ready = $false
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        $probe = Start-Process docker `
            -ArgumentList @("exec", $container, "kubectl", "get", "nodes") `
            -Wait -PassThru -WindowStyle Hidden
        if ($probe.ExitCode -eq 0) {
            $ready = $true
            break
        }
        Start-Sleep -Seconds 1
    }
    if (-not $ready) {
        docker logs $container
        throw "K3s did not become ready within 120 seconds."
    }

    $published = docker port $container 6443/tcp
    $hostPort = ($published.Trim() -split ":")[-1]
    $config = docker exec $container cat /etc/rancher/k3s/k3s.yaml | Out-String
    $config = $config.Replace(
        "https://127.0.0.1:6443",
        "https://127.0.0.1:$hostPort"
    )
    [System.IO.File]::WriteAllText($kubeconfig, $config)
    $env:KUBECONFIG = $kubeconfig

    docker save --output $archive @temporaryImages $minio
    docker cp $archive "${container}:/tmp/task24-images.tar"
    docker exec $container ctr --namespace k8s.io images import /tmp/task24-images.tar
    docker exec $container rm /tmp/task24-images.tar

    $k3sVersion = (docker exec $container k3s --version | Select-Object -First 1).Trim()
    uv run --frozen python scripts/kubernetes_acceptance.py `
        --initial-coordinator-image $initialCoordinator `
        --initial-worker-image $initialWorker `
        --upgrade-coordinator-image $upgradeCoordinator `
        --upgrade-worker-image $upgradeWorker `
        --initial-build-version $initialVersion `
        --upgrade-build-version $upgradeVersion `
        --k3s-version $k3sVersion `
        --evidence $Evidence
}
catch {
    $failure = $_
}
finally {
    Remove-Item Env:KUBECONFIG -ErrorAction SilentlyContinue
    if (Test-Path $archive) {
        Remove-Item -Force $archive
        $cleanupRecords.Add([ordered]@{
            name = "remove image archive"
            exit_code = 0
            output = $archive
        })
    }
    if (Test-Path $kubeconfig) {
        Remove-Item -Force $kubeconfig
        $cleanupRecords.Add([ordered]@{
            name = "remove kubeconfig"
            exit_code = 0
            output = $kubeconfig
        })
    }
    Invoke-CleanupCommand "remove K3s container and anonymous volumes" {
        docker rm --force --volumes $container
    } | Out-Null
    Invoke-CleanupCommand "remove K3s network" {
        docker network rm $network
    } | Out-Null
    foreach ($image in $temporaryImages) {
        Invoke-CleanupCommand "remove temporary image $image" {
            docker image rm $image
        } | Out-Null
    }

    $cleanupFailed = @($cleanupRecords | Where-Object { $_.exit_code -ne 0 }).Count -gt 0
    $cleanupResult = [ordered]@{
        schema_version = 1
        task = 24
        status = if ($cleanupFailed) { "failed" } else { "passed" }
        commands = $cleanupRecords
    }
    $cleanupPath = Join-Path $root "artifacts/task24/cleanup-v1.json"
    New-Item -ItemType Directory -Force (Split-Path -Parent $cleanupPath) | Out-Null
    [System.IO.File]::WriteAllText(
        $cleanupPath,
        ($cleanupResult | ConvertTo-Json -Depth 8),
        [System.Text.UTF8Encoding]::new($false)
    )

    if ($cleanupFailed -and -not $failure) {
        $failure = [System.Management.Automation.RuntimeException]::new(
            "Task24 cleanup did not complete successfully."
        )
    }
    if ($failure -and (Test-Path (Join-Path $root $Evidence))) {
        $resultPath = Join-Path $root $Evidence
        $result = Get-Content -Raw $resultPath | ConvertFrom-Json
        $result.status = "failed"
        if (-not $result.error) {
            $result | Add-Member -NotePropertyName error -NotePropertyValue ([ordered]@{
                type = "HarnessFailure"
                message = $failure.Exception.Message
            })
        }
        [System.IO.File]::WriteAllText(
            $resultPath,
            ($result | ConvertTo-Json -Depth 100),
            [System.Text.UTF8Encoding]::new($false)
        )
    }
    Pop-Location
}

if ($failure) {
    throw $failure
}
