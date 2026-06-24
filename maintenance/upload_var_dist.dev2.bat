@echo off
setlocal
REM =============================================================================
REM  upload_var_dist.bat -- Upload VaR HDF5 distribution file to dev2 EC2
REM
REM  Copies: C:\DATA\trgapp_data\var\VaR.M_20251231.h5
REM       -> ec2-user@dev2:/home/ec2-user/api/data/var/VaR.M_20251231.h5
REM =============================================================================

set KEY=C:\Users\mgdin\local\AWS\KeyPairs\dev2.pem
set HOST=ec2-user@ec2-100-26-206-138.compute-1.amazonaws.com
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
