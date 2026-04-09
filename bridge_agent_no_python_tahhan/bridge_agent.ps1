$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$configPath = Join-Path $scriptDir "agent_config.json"
if (!(Test-Path $configPath)) { Write-Host "Missing agent_config.json" -ForegroundColor Red; exit 1 }
$config = Get-Content $configPath -Raw | ConvertFrom-Json
$BASE_URL = ($config.base_url).TrimEnd("/")
$DEVICE_TOKEN = $config.device_token
$POLL_SECONDS = [int]$config.poll_seconds

function Invoke-JsonPost {
    param([string]$Url,[object]$Body,[hashtable]$Headers = @{})
    $json = $Body | ConvertTo-Json -Depth 10
    return Invoke-RestMethod -Uri $Url -Method POST -ContentType "application/json" -Headers $Headers -Body $json -TimeoutSec 60
}
function Get-OutlookApp {
    try { return [Runtime.InteropServices.Marshal]::GetActiveObject("Outlook.Application") }
    catch { return New-Object -ComObject Outlook.Application }
}
function Set-SendAccount {
    param($MailItem,[string]$FromEmail)
    if ([string]::IsNullOrWhiteSpace($FromEmail)) { return }
    try {
        $session = $MailItem.Application.Session
        foreach ($account in $session.Accounts) {
            try { if (($account.SmtpAddress + "").ToLower() -eq $FromEmail.ToLower()) { $MailItem.SendUsingAccount = $account; return } } catch {}
        }
    } catch {}
}
function Send-BridgeJob { param([object]$Job)
    $outlook = Get-OutlookApp
    $mail = $outlook.CreateItem(0)
    $mail.To = $Job.to_email
    $mail.Subject = $Job.subject
    $mail.HTMLBody = $Job.html_body
    Set-SendAccount -MailItem $mail -FromEmail $Job.from_email
    $mail.Send()
}
Write-Host ""
Write-Host "=============================================" -ForegroundColor DarkYellow
Write-Host "      Al Tahhan Outlook Bridge Agent" -ForegroundColor Yellow
Write-Host "=============================================" -ForegroundColor DarkYellow
Write-Host ("Base URL: {0}" -f $BASE_URL) -ForegroundColor Gray
Write-Host ("Polling every {0} seconds" -f $POLL_SECONDS) -ForegroundColor Gray
Write-Host "Keep Outlook Desktop open while sending." -ForegroundColor Cyan
Write-Host ""
while ($true) {
    try {
        Invoke-JsonPost -Url "$BASE_URL/api/bridge/heartbeat" -Body @{ device_token = $DEVICE_TOKEN } | Out-Null
        $response = Invoke-RestMethod -Uri "$BASE_URL/api/bridge/jobs" -Method GET -Headers @{ "X-Device-Token" = $DEVICE_TOKEN } -TimeoutSec 60
        foreach ($job in $response.jobs) {
            try {
                Send-BridgeJob -Job $job
                Invoke-JsonPost -Url "$BASE_URL/api/bridge/job-result" -Body @{ device_token = $DEVICE_TOKEN; job_id = $job.id; status = "sent"; message = "Sent via Al Tahhan Outlook Bridge"; provider_message_id = "" } | Out-Null
                Write-Host ("Sent job {0} to {1}" -f $job.id, $job.to_email) -ForegroundColor Green
            } catch {
                $err = $_.Exception.Message
                try { Invoke-JsonPost -Url "$BASE_URL/api/bridge/job-result" -Body @{ device_token = $DEVICE_TOKEN; job_id = $job.id; status = "failed"; message = $err; provider_message_id = "" } | Out-Null } catch {}
                Write-Host ("Failed job {0}: {1}" -f $job.id, $err) -ForegroundColor Red
            }
        }
    } catch {
        Write-Host ("Bridge loop error: {0}" -f $_.Exception.Message) -ForegroundColor Red
    }
    Start-Sleep -Seconds ([Math]::Max($POLL_SECONDS, 5))
}
