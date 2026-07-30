@echo off
REM Use Windows' native OpenSSH client explicitly, not whichever ssh/scp
REM PATH resolves to first (Git for Windows' bundled MSYS build has been
REM breaking local file writes mid-transfer with "Broken pipe" errors).
set SSH=C:\Windows\System32\OpenSSH\ssh.exe
set SCP=C:\Windows\System32\OpenSSH\scp.exe

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
"%SCP%" -i "%SOURCE_KEY%" %SOURCE_HOST%:%REMOTE_PATH% "%LOCAL_DIR%\security_pnl.h5"
IF ERRORLEVEL 1 (
    echo ERROR: Download from source failed.
    pause
    exit /b 1
)
echo Download complete.

echo.
echo Step 2: Uploading security_pnl.h5 to destination (%DEST_HOST%)...
"%SCP%" -i "%DEST_KEY%" "%LOCAL_DIR%\security_pnl.h5" %DEST_HOST%:%REMOTE_PATH%
IF ERRORLEVEL 1 (
    echo ERROR: Upload to destination failed.
    pause
    exit /b 1
)
echo Upload complete.

echo.
echo Step 3: Verifying file on destination...
"%SSH%" -i "%DEST_KEY%" %DEST_HOST% "ls -lh %REMOTE_PATH%"

echo.
echo Done.

