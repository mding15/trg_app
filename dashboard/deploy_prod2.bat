@echo off
setlocal
REM =============================================================================
REM  deploy_prod2.bat — Deploy to prod2 EC2
REM
REM  Copies the following from local to
REM  /home/ec2-user/api/trgapp/dashboard/ on prod2:
REM    *.py      
REM  
REM =============================================================================

set KEY=C:\Users\mgdin\.ssh\id_rsa
set HOST=ec2-user@ec2-54-86-24-102.compute-1.amazonaws.com
set REMOTE_DIR=/home/ec2-user/api/trgapp/dashboard
set LOCAL_DIR=C:\dev\claude\trg_app\dashboard

REM ── Deploy ───────────────────────────────────────────────────────────────────
echo Deploying *.py ...
scp -i %KEY% %LOCAL_DIR%\*.py %HOST%:%REMOTE_DIR%/

REM ── Done ─────────────────────────────────────────────────────────────────────
echo.
echo ============================================================
echo  Deploy complete.
echo ============================================================

endlocal
