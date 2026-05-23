@echo off
REM copy_security_pnl.bat
REM Copies security_pnl.h5 from source AWS instance to local, then uploads to dest AWS instance.
REM Usage: copy_security_pnl.bat

SET SOURCE_HOST=ec2-user@54.86.24.102
SET DEST_HOST=ec2-user@100.26.206.138
SET REMOTE_PATH=/home/ec2-user/api/data/var/security_pnl.h5
SET LOCAL_DIR=C:\DATA\trgapp_data\var
SET SOURCE_KEY=C:\Users\mgdin\.ssh\id_rsa
SET DEST_KEY=C:\Users\mgdin\local\AWS\KeyPairs\dev2.pem

echo.
echo Step 1: Downloading security_pnl.h5 from source (%SOURCE_HOST%)...
scp -i "%SOURCE_KEY%" %SOURCE_HOST%:%REMOTE_PATH% "%LOCAL_DIR%\security_pnl.h5"
IF ERRORLEVEL 1 (
    echo ERROR: Download from source failed.
    pause
    exit /b 1
)
echo Download complete.

echo.
echo Step 2: Uploading security_pnl.h5 to destination (%DEST_HOST%)...
scp -i "%DEST_KEY%" "%LOCAL_DIR%\security_pnl.h5" %DEST_HOST%:%REMOTE_PATH%
IF ERRORLEVEL 1 (
    echo ERROR: Upload to destination failed.
    pause
    exit /b 1
)
echo Upload complete.

echo.
echo Step 3: Verifying file on destination...
ssh -i "%DEST_KEY%" %DEST_HOST% "ls -lh %REMOTE_PATH%"

echo.
echo Done.

