@echo off
setlocal
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

scp -i "%KEY%" %HOST%:%REMOTE% "%LOCAL%"
if %ERRORLEVEL% neq 0 (
    echo DOWNLOAD FAILED.
    exit /b 1
)

echo.
echo Download complete.

endlocal
