param(
  [switch]$Build = $true
)

$cmd = "docker compose --profile full up"
if ($Build) {
  $cmd += " --build"
}
Write-Host "Running: $cmd"
Invoke-Expression $cmd
