# setup_scheduler.ps1
# 키움 자동매매 — Windows 작업 스케줄러 등록
# 실행: PowerShell을 관리자 권한으로 열고
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   .\setup_scheduler.ps1

$TaskName   = "KiwoomTrader"
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$BatFile    = Join-Path $ScriptDir "run_kiwoom.bat"
$LogFile    = Join-Path $ScriptDir "kiwoom_scheduler.log"

# ── 기존 태스크 제거 ─────────────────────
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "기존 태스크 제거 완료"
}

# ── 액션: run_kiwoom.bat 실행 ────────────
$Action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$BatFile`" >> `"$LogFile`" 2>&1" `
    -WorkingDirectory $ScriptDir

# ── 트리거: 로그온 시 + 장 시작 전 (08:50 KST) ──
$TriggerLogon = New-ScheduledTaskTrigger -AtLogOn
$TriggerDaily = New-ScheduledTaskTrigger -Daily -At "08:50"   # 이미 로그온 중인 날 대비

# ── 설정 ─────────────────────────────────
$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit    (New-TimeSpan -Hours 0) `   # 시간 제한 없음
    -RestartCount          5 `
    -RestartInterval       (New-TimeSpan -Minutes 2) ` # 실패 시 2분 후 재시작
    -StartWhenAvailable    $true `                     # 놓친 트리거 즉시 실행
    -RunOnlyIfNetworkAvailable $true `
    -Priority              4                           # 높음

# ── 등록 (현재 사용자, 로그온 상태에서만 실행) ──
Register-ScheduledTask `
    -TaskName  $TaskName `
    -Action    $Action `
    -Trigger   @($TriggerLogon, $TriggerDaily) `
    -Settings  $Settings `
    -RunLevel  Highest `
    -Force

Write-Host ""
Write-Host "===================================="
Write-Host " 태스크 등록 완료: $TaskName"
Write-Host " 트리거: 로그온 시 + 매일 08:50"
Write-Host " 로그: $LogFile"
Write-Host "===================================="
Write-Host ""

# ── 절전/화면보호기 해제 (상시 실행 필수) ─
Write-Host "절전 모드 해제 설정 중..."
powercfg /change standby-timeout-ac 0       # AC 전원: 절전 해제
powercfg /change monitor-timeout-ac 0       # AC 전원: 모니터 꺼짐 해제
powercfg /change hibernate-timeout-ac 0    # 최대 절전 해제
Write-Host "절전 모드 해제 완료"
Write-Host ""

# ── Windows 자동 로그인 안내 ──────────────
Write-Host "[ 자동 로그인 설정 방법 ]"
Write-Host "  1. Win+R → netplwiz 실행"
Write-Host "  2. 계정 선택 후 '이 컴퓨터를 사용하려면 사용자 이름과 암호를 입력해야 합니다' 체크 해제"
Write-Host "  3. 암호 입력 후 확인"
Write-Host ""
Write-Host "설정 완료 후 재부팅하면 자동 로그인 + KiwoomTrader 자동 시작됩니다."
