@echo off
setlocal
REM Use Windows' native OpenSSH client explicitly, not whichever ssh/scp
REM PATH resolves to first (Git for Windows' bundled MSYS build has been
REM breaking local file writes mid-transfer with "Broken pipe" errors).
set SSH=C:\Windows\System32\OpenSSH\ssh.exe
set SCP=C:\Windows\System32\OpenSSH\scp.exe


set PROD2_KEY=C:\Users\mgdin\.ssh\id_rsa
set PROD2_HOST=ec2-user@ec2-54-86-24-102.compute-1.amazonaws.com
set DEV2_KEY=C:\Users\mgdin\local\AWS\KeyPairs\dev2.pem
set DEV2_HOST=ec2-user@ec2-100-26-206-138.compute-1.amazonaws.com
set LOCAL=C:\DATA\trgapp_data\public\input_template.xlsx
set REMOTE=/home/ec2-user/api/data/public/input_template.xlsx

echo ============================================================
echo  Uploading input_template.xlsx to prod2
echo ============================================================
"%SCP%" -i "%PROD2_KEY%" "%LOCAL%" %PROD2_HOST%:%REMOTE%
if %ERRORLEVEL% neq 0 (
    echo SCP to prod2 FAILED. Aborting.
    exit /b 1
)

echo.
echo ============================================================
echo  Uploading input_template.xlsx to dev2
echo ============================================================
"%SCP%" -i "%DEV2_KEY%" "%LOCAL%" %DEV2_HOST%:%REMOTE%
if %ERRORLEVEL% neq 0 (
    echo SCP to dev2 FAILED.
    exit /b 1
)

echo.
echo ============================================================
echo  Upload complete (prod2 + dev2).
echo ============================================================
endlocal
