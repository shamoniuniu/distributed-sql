param(
    [string[]]$Paths = @("README.md", "docs", "deploy/README.md")
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$markdownFiles = foreach ($item in $Paths) {
    $absolute = Join-Path $root $item
    if (-not (Test-Path $absolute)) {
        throw "Documentation path does not exist: $item"
    }
    if ((Get-Item $absolute) -is [System.IO.DirectoryInfo]) {
        Get-ChildItem $absolute -Recurse -File -Filter "*.md"
    }
    else {
        Get-Item $absolute
    }
}

$failures = [System.Collections.Generic.List[string]]::new()
$linkPattern = '(?<!!)\[[^\]]+\]\((?<target>[^)]+)\)'
$fencePattern = '(?ms)^```powershell[ \t]*\r?\n(?<body>.*?)^```[ \t]*$'

foreach ($file in $markdownFiles) {
    $content = Get-Content -Raw -Encoding UTF8 $file.FullName
    foreach ($match in [regex]::Matches($content, $linkPattern)) {
        $target = $match.Groups["target"].Value.Trim()
        if ($target -match '^(https?://|mailto:|#)') {
            continue
        }
        $pathPart = ($target -split '#', 2)[0]
        if ([string]::IsNullOrWhiteSpace($pathPart)) {
            continue
        }
        $resolved = Join-Path $file.DirectoryName ([uri]::UnescapeDataString($pathPart))
        if (-not (Test-Path $resolved)) {
            $relative = [System.IO.Path]::GetRelativePath($root, $file.FullName)
            $failures.Add("${relative}: missing local link target '$target'")
        }
    }

    foreach ($match in [regex]::Matches($content, $fencePattern)) {
        # Angle-bracket placeholders are documentation values, not PowerShell syntax.
        $body = $match.Groups["body"].Value -replace '<[^>`\r\n]+>', "'placeholder'"
        $tokens = $null
        $parseErrors = $null
        [void][System.Management.Automation.Language.Parser]::ParseInput(
            $body,
            [ref]$tokens,
            [ref]$parseErrors
        )
        foreach ($parseError in $parseErrors) {
            $relative = [System.IO.Path]::GetRelativePath($root, $file.FullName)
            $failures.Add(
                "${relative}: invalid PowerShell block at line " +
                "$($parseError.Extent.StartLineNumber): $($parseError.Message)"
            )
        }
    }
}

if ($failures.Count -gt 0) {
    $failures | ForEach-Object { Write-Error $_ }
    exit 1
}

Write-Output (
    "Documentation static checks passed: {0} Markdown files, local links and PowerShell blocks valid." `
        -f $markdownFiles.Count
)
