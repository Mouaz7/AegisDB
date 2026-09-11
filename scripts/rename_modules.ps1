$modules = @(
    "aegisdb_benchmark",
    "aegisdb_chaos",
    "aegisdb_client",
    "aegisdb_common",
    "aegisdb_integration",
    "aegisdb_management",
    "aegisdb_mvcc",
    "aegisdb_node",
    "aegisdb_observability",
    "aegisdb_protocol",
    "aegisdb_raft",
    "aegisdb_sharding",
    "aegisdb_storage",
    "aegisdb_transaction",
    "aegisdb_transport"
)

# 1. Rename directories
foreach ($mod in $modules) {
    if (Test-Path $mod) {
        $newName = $mod -replace "_", "-"
        Rename-Item $mod $newName
        Write-Host "Renamed $mod to $newName"
    }
}

# 2. Update files
$filesToUpdate = Get-ChildItem -Recurse -Include pom.xml, README.md, *.sh, *.yaml | Where-Object { -not ($_.FullName -match '\\target\\') -and -not ($_.FullName -match '\\\.git\\') }

foreach ($file in $filesToUpdate) {
    $content = Get-Content $file.FullName -Raw
    $changed = $false
    
    foreach ($mod in $modules) {
        if ($content -match $mod) {
            $newName = $mod -replace "_", "-"
            $content = $content -replace $mod, $newName
            $changed = $true
        }
    }
    
    if ($changed) {
        Set-Content -Path $file.FullName -Value $content -NoNewline
        Write-Host "Updated $($file.FullName)"
    }
}
