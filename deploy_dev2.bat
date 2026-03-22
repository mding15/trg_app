@echo off
setlocal
REM =============================================================================
REM  deploy_dev2.bat — Deploy trg_app to dev2 EC2 instance
REM
REM  Steps:
REM    1. Copy source to a local staging folder (excludes .venv, __pycache__, etc.)
REM    2. SCP staging folder to EC2
REM    3. Restart the API service via supervisorctl
REM =============================================================================

set KEY=C:\Users\mgdin\local\AWS\KeyPairs\dev2.pem
set HOST=ec2-user@ec2-100-26-206-138.compute-1.amazonaws.com
set REMOTE_DIR=/home/ec2-user/api/trgapp
set SRC=%~dp0
if "%SRC:~-1%"=="\" set SRC=%SRC:~0,-1%
set STAGE=%TEMP%\trg_app_deploy

REM ── Step 1: Build staging folder ─────────────────────────────────────────────
echo.
echo ============================================================
echo  Step 1: Building staging folder (excluding venv / cache)
echo ============================================================

REM Remove stale staging folder if it exists
if exist "%STAGE%" rd /s /q "%STAGE%"

REM Robocopy: /E = include subdirs, /XD = exclude dirs, /XF = exclude files
robocopy "%SRC%" "%STAGE%" /E /XD .venv __pycache__ .git test_data log /XF *.pyc *.pyo *.log /NFL /NDL /NJH /NJS
REM Robocopy exit codes 0-7 are success (bit flags for copied/skipped/extra)
if %ERRORLEVEL% GTR 7 (
    echo STAGING FAILED ^(robocopy error %ERRORLEVEL%^). Aborting.
    exit /b 1
)
echo   Staging folder ready: %STAGE%

REM ── Step 2: SCP to EC2 ────────────────────────────────────────────────────────
echo.
echo ============================================================
echo  Step 2: Uploading to %HOST%:%REMOTE_DIR%
echo ============================================================
scp -r -i "%KEY%" "%STAGE%\." %HOST%:%REMOTE_DIR%
if %ERRORLEVEL% neq 0 (
    echo SCP FAILED. Aborting.
    exit /b 1
)

REM ── Step 3: Restart service ───────────────────────────────────────────────────
echo.
echo ============================================================
echo  Step 3: Restarting API service on EC2
echo ============================================================
ssh -i "%KEY%" %HOST% "sudo supervisorctl restart api"
if %ERRORLEVEL% neq 0 (
    echo SERVICE RESTART FAILED.
    exit /b 1
)

REM ── Done ─────────────────────────────────────────────────────────────────────
echo.
echo ============================================================
echo  Deploy complete.
echo ============================================================

REM Clean up staging folder
rd /s /q "%STAGE%"

endlocal
