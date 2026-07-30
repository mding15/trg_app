@echo off
setlocal
REM Use Windows' native OpenSSH client explicitly, not whichever ssh/scp
REM PATH resolves to first (Git for Windows' bundled MSYS build has been
REM breaking local file writes mid-transfer with "Broken pipe" errors).
set SSH=C:\Windows\System32\OpenSSH\ssh.exe
set SCP=C:\Windows\System32\OpenSSH\scp.exe

REM =============================================================================
REM  deploy_prod2.bat — Deploy to prod2 EC2
REM
REM  Copies the following from local to
REM  /home/ec2-user/api/trgapp/models/ on prod2:
REM    *.py      
REM  
REM =============================================================================

set KEY=C:\Users\mgdin\.ssh\id_rsa
set HOST=ec2-user@ec2-54-86-24-102.compute-1.amazonaws.com
set REMOTE_DIR=/home/ec2-user/api/trgapp/models
set LOCAL_DIR=C:\dev\claude\trg_app\models

REM ── Deploy ───────────────────────────────────────────────────────────────────
echo Deploying *.py ...
"%SCP%" -i %KEY% %LOCAL_DIR%\*.py %HOST%:%REMOTE_DIR%/

REM ── Done ─────────────────────────────────────────────────────────────────────
echo.
echo ============================================================
echo  Deploy complete.
echo ============================================================

endlocal
