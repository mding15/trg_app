@echo off
setlocal
REM =============================================================================
REM  deploy_prod2.bat — Deploy process_scheduler to prod2 EC2
REM
REM  Copies the following from local process_scheduler to
REM  /home/ec2-user/api/trgapp/process_scheduler/ on prod2:
REM    *.py      — feed scripts (feed_ms.py, etc.)
REM    *.sh       — shell scripts (pull, test, keygen, etc.)
REM    *.json     — job definitions 
REM  
REM =============================================================================

set KEY=C:\Users\mgdin\.ssh\id_rsa
set HOST=ec2-user@ec2-54-86-24-102.compute-1.amazonaws.com
set REMOTE_DIR=/home/ec2-user/api/trgapp/process_scheduler
set LOCAL_DIR=C:\dev\claude\trg_app\process_scheduler

REM ── Deploy ───────────────────────────────────────────────────────────────────
echo Deploying *.py ...
scp -i %KEY% %LOCAL_DIR%\*.py %HOST%:%REMOTE_DIR%/

echo Deploying *.sh ...
scp -i %KEY% %LOCAL_DIR%\*.sh %HOST%:%REMOTE_DIR%/

echo Deploying *.json ...
scp -i %KEY% %LOCAL_DIR%\*.json %HOST%:%REMOTE_DIR%/

REM ── Done ─────────────────────────────────────────────────────────────────────
echo.
echo ============================================================
echo  Deploy complete.
echo ============================================================

endlocal
