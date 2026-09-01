param(
    [string]$InitialCoordinatorImage = "distributed-sql-coordinator:0.1.0",
    [string]$InitialWorkerImage = "distributed-sql-worker:0.1.0",
    [Parameter(Mandatory = $true)]
    [string]$UpgradeCoordinatorImage,
    [Parameter(Mandatory = $true)]
    [string]$UpgradeWorkerImage,
    [string]$Namespace = "distributed-sql"
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$root = Split-Path -Parent $PSScriptRoot

function Invoke-DeploymentSmoke {
    param([switch]$RequireExistingCatalog)

    $forward = Start-Process kubectl -ArgumentList @(
        "-n", $Namespace,
        "port-forward", "service/coordinator", "18080:8080",
        "--address", "127.0.0.1"
    ) -PassThru -NoNewWindow
    try {
        $arguments = @(
            "run", "--frozen", "python", "scripts/deployment_smoke.py",
            "--url", "http://127.0.0.1:18080", "--timeout", "120"
        )
        if ($RequireExistingCatalog) {
            $arguments += "--require-existing-catalog"
        }
        uv @arguments
    }
    finally {
        Stop-Process -Id $forward.Id -Force -ErrorAction SilentlyContinue
    }
}

Push-Location $root

try {
    kubectl cluster-info
    kubectl apply -k deploy/kubernetes
    kubectl -n $Namespace set image deployment/coordinator `
        "coordinator=$InitialCoordinatorImage"
    kubectl -n $Namespace set image deployment/worker `
        "worker=$InitialWorkerImage"
    kubectl -n $Namespace rollout status deployment/minio --timeout=180s
    kubectl -n $Namespace rollout status deployment/coordinator --timeout=180s
    kubectl -n $Namespace rollout status deployment/worker --timeout=180s

    $originalCoordinator = kubectl -n $Namespace get deployment coordinator `
        -o=jsonpath='{.spec.template.spec.containers[0].image}'
    $originalWorker = kubectl -n $Namespace get deployment worker `
        -o=jsonpath='{.spec.template.spec.containers[0].image}'
    if (
        $originalCoordinator -eq $UpgradeCoordinatorImage -or
        $originalWorker -eq $UpgradeWorkerImage
    ) {
        throw "Upgrade images must differ from the currently deployed images."
    }

    Invoke-DeploymentSmoke

    kubectl -n $Namespace set image deployment/coordinator `
        "coordinator=$UpgradeCoordinatorImage"
    kubectl -n $Namespace set image deployment/worker `
        "worker=$UpgradeWorkerImage"
    kubectl -n $Namespace rollout status deployment/coordinator --timeout=180s
    kubectl -n $Namespace rollout status deployment/worker --timeout=180s
    Invoke-DeploymentSmoke -RequireExistingCatalog

    kubectl -n $Namespace rollout undo deployment/coordinator
    kubectl -n $Namespace rollout undo deployment/worker
    kubectl -n $Namespace rollout status deployment/coordinator --timeout=180s
    kubectl -n $Namespace rollout status deployment/worker --timeout=180s

    $rolledBackCoordinator = kubectl -n $Namespace get deployment coordinator `
        -o=jsonpath='{.spec.template.spec.containers[0].image}'
    $rolledBackWorker = kubectl -n $Namespace get deployment worker `
        -o=jsonpath='{.spec.template.spec.containers[0].image}'
    if (
        $rolledBackCoordinator -ne $originalCoordinator -or
        $rolledBackWorker -ne $originalWorker
    ) {
        throw "rollout undo did not restore the original image versions."
    }
    Invoke-DeploymentSmoke -RequireExistingCatalog

    kubectl -n $Namespace get deployment,pod,service,pvc
    kubectl -n $Namespace rollout history deployment/coordinator
    kubectl -n $Namespace rollout history deployment/worker
}
finally {
    Pop-Location
}
