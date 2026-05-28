@echo off
setlocal
REM =============================================================================
REM  upload_var_dist.bat -- Upload VaR HDF5 distribution file to prod2 EC2
REM
REM  Copies: C:\DATA\trgapp_data\var\VaR.M_20251231.h5
REM       -> ec2-user@prod2:/home/ec2-user/api/data/var/VaR.M_20251231.h5
REM =============================================================================

set KEY=C:\Users\mgdin\.ssh\id_rsa
set HOST=ec2-user@ec2-54-86-24-102.compute-1.amazonaws.com
set LOCAL=C:\DATA\trgapp_data\var\VaR.M_20251231.h5
set REMOTE=/home/ec2-user/api/data/var/VaR.M_20251231.h5

echo.
echo ============================================================
echo  Uploading VaR distribution to %HOST%
echo  %LOCAL%  -^>  %REMOTE%
echo ============================================================

scp -i "%KEY%" "%LOCAL%" %HOST%:%REMOTE%
if %ERRORLEVEL% neq 0 (
    echo UPLOAD FAILED.
    exit /b 1
)

echo.
echo Upload complete.

endlocal
