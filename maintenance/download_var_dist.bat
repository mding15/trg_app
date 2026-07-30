@echo off
setlocal
REM Use Windows' native OpenSSH client explicitly, not whichever ssh/scp
REM PATH resolves to first (Git for Windows' bundled MSYS build has been
REM breaking local file writes mid-transfer with "Broken pipe" errors).
set SSH=C:\Windows\System32\OpenSSH\ssh.exe
set SCP=C:\Windows\System32\OpenSSH\scp.exe

REM =============================================================================
REM  download_var_dist.bat -- Download VaR HDF5 distribution file from prod2 EC2
REM
REM  Copies: ec2-user@prod2:/home/ec2-user/api/data/var/VaR.M_20251231.h5
REM       -> C:\DATA\trgapp_data\var\VaR.M_20251231.h5
REM =============================================================================

set KEY=C:\Users\mgdin\.ssh\id_rsa
set HOST=ec2-user@ec2-54-86-24-102.compute-1.amazonaws.com
set LOCAL=C:\DATA\trgapp_data\var\VaR.M_20251231.h5
set REMOTE=/home/ec2-user/api/data/var/VaR.M_20251231.h5

echo.
echo ============================================================
echo  Downloading VaR distribution from %HOST%
echo  %REMOTE%  -^>  %LOCAL%
echo ============================================================

"%SCP%" -i "%KEY%" %HOST%:%REMOTE% "%LOCAL%"
if %ERRORLEVEL% neq 0 (
    echo DOWNLOAD FAILED.
    exit /b 1
)

echo.
echo Download complete.

endlocal
