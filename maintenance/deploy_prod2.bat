@echo off
setlocal
REM =============================================================================
REM  deploy_prod2.bat -- Deploy to prod2 EC2
REM
REM  Copies *.py from local to
REM  /home/ec2-user/api/trgapp/maintenance/ on prod2
REM =============================================================================

set KEY=C:\Users\mgdin\.ssh\id_rsa
set HOST=ec2-user@ec2-54-86-24-102.compute-1.amazonaws.com
set REMOTE_DIR=/home/ec2-user/api/trgapp/maintenance
set LOCAL_DIR=C:\dev\claude\trg_app\maintenance

echo Deploying *.py ...
scp -i %KEY% %LOCAL_DIR%\*.py %HOST%:%REMOTE_DIR%/

echo.
echo ============================================================
echo  Deploy complete.
echo ============================================================

endlocal
