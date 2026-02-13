param(
  [int]$PollAttempts = 20,
  [int]$PollDelayMs = 1200
)

$ErrorActionPreference = "Stop"

function Get-Health {
  param([string]$Url)
  try {
    $result = Invoke-RestMethod -Uri $Url -Method Get
    return $result.status -eq "ok"
  } catch {
    return $false
  }
}

Write-Host "Checking service health..."
$checks = @(
  @{ Name = "gateway"; Url = "http://localhost:8080/v1/health" },
  @{ Name = "simulator"; Url = "http://localhost:8090/v1/health" },
  @{ Name = "analytics"; Url = "http://localhost:8083/v1/health" }
)

foreach ($check in $checks) {
  if (-not (Get-Health -Url $check.Url)) {
    throw "Health check failed for $($check.Name) at $($check.Url)"
  }
  Write-Host "$($check.Name): ok"
}

Write-Host "Submitting preview transaction..."
$preview = Invoke-RestMethod `
  -Uri "http://localhost:8080/v1/transactions/score-preview" `
  -Method Post `
  -ContentType "application/json" `
  -Body (@{
      account_id = "acct_smoke_0001"
      card_id = "card_smoke_0001"
      merchant_id = "mer_smoke_0001"
      amount = 109.99
      currency = "USD"
      country = "US"
      device_id = "dev_smoke_0001"
      ip = "10.20.30.40"
      mcc = "5999"
      channel = "web"
      is_card_present = $false
      user_agent = "smoke-test-agent"
    } | ConvertTo-Json)

if (-not $preview.tx_id) {
  throw "Preview request did not return tx_id"
}

Write-Host "Polling decision for tx_id=$($preview.tx_id)..."
$decision = $null
for ($i = 0; $i -lt $PollAttempts; $i++) {
  try {
    $decision = Invoke-RestMethod -Uri "http://localhost:8080/v1/transactions/$($preview.tx_id)/decision" -Method Get
    if ($decision.tx_id) {
      break
    }
  } catch {
  }
  Start-Sleep -Milliseconds $PollDelayMs
}

if (-not $decision -or -not $decision.tx_id) {
  throw "Decision not found after polling"
}

Write-Host "Smoke test passed"
$decision
