param(
  [int]$MaxMessages = 200000,
  [double]$MinPrecision = 0.70,
  [double]$MinRecall = 0.75
)

$ErrorActionPreference = "Stop"

$precision = $MinPrecision.ToString("0.00", [System.Globalization.CultureInfo]::InvariantCulture)
$recall = $MinRecall.ToString("0.00", [System.Globalization.CultureInfo]::InvariantCulture)

docker compose --profile tools run --rm `
  -e QUALITY_MAX_MESSAGES=$MaxMessages `
  -e QUALITY_MIN_PRECISION=$precision `
  -e QUALITY_MIN_RECALL=$recall `
  quality-evaluator
