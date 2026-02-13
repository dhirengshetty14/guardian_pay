param(
  [switch]$Build = $true
)

$cmd = "docker compose --profile core up"
if ($Build) {
  $cmd += " --build"
}
Write-Host "Running: $cmd"
Invoke-Expression $cmd
