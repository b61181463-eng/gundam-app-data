@echo off
setlocal

chcp 65001 > nul
cd /d C:\Users\b6118\gundam_app

if not exist logs mkdir logs

set PYTHON=C:\Users\b6118\AppData\Local\Programs\Python\Python311\python.exe
set LOGFILE=logs\run_all_kr_log.txt

echo ====================================== >> "%LOGFILE%"
echo [%date% %time%] Start KR prerequisite work >> "%LOGFILE%"

echo [1/4] GundamBase Stars >> "%LOGFILE%"
"%PYTHON%" sync_kr_gundambase.py >> "%LOGFILE%" 2>&1
if errorlevel 1 goto error

echo [2/4] GundamShop Stars >> "%LOGFILE%"
"%PYTHON%" sync_kr_gundamshop.py >> "%LOGFILE%" 2>&1
if errorlevel 1 goto error

echo [3/4] BNKRMALL Stars >> "%LOGFILE%"
"%PYTHON%" sync_kr_bnkrmall.py >> "%LOGFILE%" 2>&1
if errorlevel 1 goto error

echo [4/4] Absorption Stars >> "%LOGFILE%"
"%PYTHON%" merge_kr_crosscheck.py >> "%LOGFILE%" 2>&1
if errorlevel 1 goto error

echo [%date% %time%] KR entire operation successfully completed >> "%LOGFILE%"
echo. >> "%LOGFILE%"
exit /b 0

:error
echo [%date% %time%] An error occurred - work stoppage >> "%LOGFILE%"
echo. >> "%LOGFILE%"
exit /b 1