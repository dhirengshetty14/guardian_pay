param(
  [string]$Scenario = "ring_expansion",
  [int]$Count = 25000,
  [int]$RatePerSec = 600
)

$body = @{
  scenario = $Scenario
  count = $Count
  rate_per_sec = $RatePerSec
} | ConvertTo-Json

Invoke-RestMethod `
  -Uri "http://localhost:8080/v1/simulations/run" `
  -Method Post `
  -ContentType "application/json" `
  -Body $body
