#Requires -Version 5.1
<#
.SYNOPSIS
  Opens the local endpoints the walkthrough needs, and the pages that use them.

.DESCRIPTION
  The query API is private and stays private: `gcloud run services proxy` attaches
  the caller's own identity token to each request, so no credential is stored,
  passed or printed here. Both endpoints bind to the loopback address only.

  Everything this script starts, it stops on exit. It starts nothing else and
  changes no cloud resource; the Google Cloud Console is the control interface.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File tools\observatory.ps1
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File tools\observatory.ps1 -Console
#>
[CmdletBinding()]
param(
  [string]$Project   = 'eventproof-stream-2609',
  [string]$Region    = 'europe-west1',
  [string]$Zone      = 'europe-west1-b',
  [string]$Cluster   = 'eventproof',
  [string]$Namespace = 'eventproof',
  [string]$Service   = 'current-state-api',
  [int]   $ApiPort   = 8080,
  [int]   $FlinkPort = 8081,
  # Also open the Console pages, in the order the recording visits them.
  [switch]$Console,
  [switch]$NoBrowser
)

$ErrorActionPreference = 'Stop'
$started = @()

function Require-Command([string]$Name) {
  # A name can resolve to several commands; gcloud ships gcloud.ps1, gcloud.cmd and a
  # bash script. Start-Process needs the real executable, so skip the scripts.
  $command = Get-Command $Name -All -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandType -eq 'Application' -and $_.Source -match '\.(exe|cmd|bat)$' } |
    Select-Object -First 1
  if (-not $command) { throw "$Name is not on PATH." }
  return $command.Source
}

function Test-PortFree([int]$Port) {
  try {
    $listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, $Port)
    $listener.Start(); $listener.Stop(); return $true
  } catch { return $false }
}

function Wait-Endpoint([string]$Url, [int]$Seconds = 60) {
  for ($i = 0; $i -lt $Seconds; $i++) {
    try {
      Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5 | Out-Null
      return $true
    } catch {
      # A 4xx means something is listening, which is all this waits for.
      if ($_.Exception.Response) { return $true }
      Start-Sleep -Seconds 1
    }
  }
  return $false
}

function Start-Endpoint([string]$Name, [string]$File, [string[]]$Arguments) {
  Write-Host "starting $Name" -ForegroundColor Cyan
  $process = Start-Process -FilePath $File -ArgumentList $Arguments -PassThru -NoNewWindow
  $script:started += $process
  return $process
}

$gcloud  = Require-Command gcloud
$kubectl = Require-Command kubectl

$account = (& $gcloud config get-value account 2>$null | Select-Object -First 1)
if (-not $account -or $account -eq '(unset)') {
  throw "No active gcloud account. Run: gcloud auth login"
}
Write-Host "account  $account"
Write-Host "project  $Project"

# Confirms the project, the region and that the service exists, in one call.
$serviceUrl = (& $gcloud run services describe $Service --project=$Project --region=$Region `
  --format='value(status.url)' 2>$null | Select-Object -First 1)
if (-not $serviceUrl) {
  throw "Cannot read Cloud Run service '$Service' in $Project/$Region. Is the window still open?"
}
Write-Host "service  $serviceUrl"

$context = "gke_${Project}_${Zone}_${Cluster}"
$contexts = & $kubectl config get-contexts -o name 2>$null
if ($contexts -notcontains $context) {
  Write-Host "fetching cluster credentials for $Cluster" -ForegroundColor Cyan
  & $gcloud container clusters get-credentials $Cluster --zone=$Zone --project=$Project --dns-endpoint
}
& $kubectl config use-context $context | Out-Null
Write-Host "context  $context"

foreach ($port in @($ApiPort, $FlinkPort)) {
  if (-not (Test-PortFree $port)) {
    throw "Port $port is already in use. Close what is holding it, or pass -ApiPort/-FlinkPort."
  }
}

try {
  Start-Endpoint 'query API proxy' $gcloud @(
    'run', 'services', 'proxy', $Service,
    "--project=$Project", "--region=$Region", "--port=$ApiPort") | Out-Null

  Start-Endpoint 'Flink REST port-forward' $kubectl @(
    'port-forward', '--address', '127.0.0.1',
    '-n', $Namespace, 'svc/current-state-rest', "${FlinkPort}:8081") | Out-Null

  $dashboard = "http://127.0.0.1:$ApiPort/"
  $flink     = "http://127.0.0.1:$FlinkPort/"
  if (-not (Wait-Endpoint $dashboard)) { throw "The dashboard did not answer on port $ApiPort." }
  if (-not (Wait-Endpoint $flink 30))  { Write-Warning "Flink did not answer on port $FlinkPort yet." }

  Write-Host ""
  Write-Host "dashboard  $dashboard" -ForegroundColor Green
  Write-Host "Flink UI   $flink      (this is Flink's own UI, not Google Cloud)" -ForegroundColor Green

  $consoleLinks = [ordered]@{
    'Cloud Build'   = "https://console.cloud.google.com/cloud-build/builds;region=$Region?project=$Project"
    'Artifacts'     = "https://console.cloud.google.com/artifacts/docker/$Project/$Region/eventproof?project=$Project"
    'GKE workloads' = "https://console.cloud.google.com/kubernetes/workload/overview?project=$Project"
    'Kafka'         = "https://console.cloud.google.com/managedkafka/clusters?project=$Project"
    'Checkpoints'   = "https://console.cloud.google.com/storage/browser/$Project-flink?project=$Project"
    'Bigtable'      = "https://console.cloud.google.com/bigtable/instances?project=$Project"
    'Cloud Run'     = "https://console.cloud.google.com/run?project=$Project"
    'Monitoring'    = "https://console.cloud.google.com/monitoring/dashboards?project=$Project"
    'Scheduler'     = "https://console.cloud.google.com/cloudscheduler?project=$Project"
  }

  if (-not $NoBrowser) {
    Start-Process $dashboard
    Start-Process $flink
    if ($Console) { foreach ($url in $consoleLinks.Values) { Start-Process $url; Start-Sleep -Milliseconds 400 } }
  }

  Write-Host ""
  Write-Host "Console pages, in recording order:"
  foreach ($name in $consoleLinks.Keys) { "  {0,-14} {1}" -f $name, $consoleLinks[$name] | Write-Host }

  Write-Host ""
  Read-Host "Press Enter to stop the endpoints this script started"
}
finally {
  foreach ($process in $started) {
    if ($process -and -not $process.HasExited) {
      Write-Host "stopping pid $($process.Id)"
      # gcloud is a wrapper; the proxy that holds the port is its child, so kill the tree.
      & taskkill.exe /PID $process.Id /T /F 2>$null | Out-Null
    }
  }
}
