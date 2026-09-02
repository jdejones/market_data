@echo off
REM The dashboard starts intraday_price_stream.py first and launches current_rvol_gui.py
REM after the configured startup delay. Other market-data scripts are selected there.
REM Filings Stream GUI remains a separate independently launched process.
set "PYTHON_EXE=C:\Users\jdejo\AppData\Local\Programs\Python\Python313\python.exe"
set "LOG_FILE=C:\Users\jdejo\Market_Data_Processing\market_data\scripts\script_error_logs.txt"
set "DETAILED_LOG_FILE=C:\Users\jdejo\Market_Data_Processing\market_data\scripts\scripts_error_logs_detailed.txt"
set "MARKET_DATA_ROOT=C:\Users\jdejo\Market_Data_Processing\market_data"
set "NEWS_TRACKER_ROOT=C:\Users\jdejo\News_Tracker"
set "SYMBOLS_FILE=E:\Market Research\Studies\Sector Studies\Watchlists\High_AvgDV.txt"
set "ETF_SYMBOLS_FILE=E:\Market Research\Studies\Sector Studies\Watchlists\ETFs.txt"
set "HIGH_BETA_SYMBOLS_FILE=E:\Market Research\Studies\Sector Studies\Watchlists\High_Beta.txt"
set "NETWORK_CHECK_HOST=api.polygon.io"
set "NETWORK_CHECK_PORT=443"
set "NETWORK_READY_TIMEOUT_SECONDS=900"
set "NETWORK_RETRY_SECONDS=15"
set "POST_NETWORK_DELAY_SECONDS=180"
set "STARTUP_DELAY_SECONDS=30"

call :wait_for_network_ready
if errorlevel 1 exit /b %errorlevel%

start "Intraday Script Dashboard" /D "%MARKET_DATA_ROOT%" cmd /v:on /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\intraday_script_launcher.py" --python-exe "%PYTHON_EXE%" --symbols-file "%SYMBOLS_FILE%" "%ETF_SYMBOLS_FILE%" "%HIGH_BETA_SYMBOLS_FILE%" --startup-delay-seconds "%STARTUP_DELAY_SECONDS%" 2> "%TEMP%\market_data_dashboard_stderr.log" & set "EXIT_CODE=!errorlevel!" & if not "!EXIT_CODE!"=="0" (type nul > "%DETAILED_LOG_FILE%" & >> "%DETAILED_LOG_FILE%" echo [!date! !time!] intraday_script_launcher.py failed with exit code !EXIT_CODE!. & >> "%DETAILED_LOG_FILE%" echo. & if exist "%TEMP%\intraday_script_launcher_stderr.log" type "%TEMP%\intraday_script_launcher_stderr.log" >> "%DETAILED_LOG_FILE%" & >> "%LOG_FILE%" echo [!date! !time!] intraday_script_launcher.py failed with exit code !EXIT_CODE!. & echo intraday_script_launcher.py failed with exit code !EXIT_CODE!. See "%LOG_FILE%".) & if exist "%TEMP%\intraday_script_launcher_stderr.log" del "%TEMP%\intraday_script_launcher_stderr.log" > nul 2>&1"

echo Waiting %STARTUP_DELAY_SECONDS% seconds for intraday_price_stream.py to start writing data...
timeout /t %STARTUP_DELAY_SECONDS% /nobreak > nul

start "Filings Stream GUI" /D "%NEWS_TRACKER_ROOT%" cmd /v:on /k ""%PYTHON_EXE%" "%NEWS_TRACKER_ROOT%\scripts\filings_stream_gui.py" 2> "%TEMP%\filings_stream_gui_stderr.log" & set "EXIT_CODE=!errorlevel!" & if not "!EXIT_CODE!"=="0" (type nul > "%DETAILED_LOG_FILE%" & >> "%DETAILED_LOG_FILE%" echo [!date! !time!] filings_stream_gui.py failed with exit code !EXIT_CODE!. & >> "%DETAILED_LOG_FILE%" echo. & if exist "%TEMP%\filings_stream_gui_stderr.log" type "%TEMP%\filings_stream_gui_stderr.log" >> "%DETAILED_LOG_FILE%" & >> "%LOG_FILE%" echo [!date! !time!] filings_stream_gui.py failed with exit code !EXIT_CODE!. & echo filings_stream_gui.py failed with exit code !EXIT_CODE!. See "%LOG_FILE%".) & if exist "%TEMP%\filings_stream_gui_stderr.log" del "%TEMP%\filings_stream_gui_stderr.log" > nul 2>&1"

exit /b 0

:wait_for_network_ready
echo Waiting for %NETWORK_CHECK_HOST% DNS and HTTPS connectivity...
set /a "NETWORK_ATTEMPTS=%NETWORK_READY_TIMEOUT_SECONDS% / %NETWORK_RETRY_SECONDS%"
if %NETWORK_ATTEMPTS% lss 1 set "NETWORK_ATTEMPTS=1"

for /l %%A in (1,1,%NETWORK_ATTEMPTS%) do (
    powershell -NoProfile -ExecutionPolicy Bypass -Command "try { [System.Net.Dns]::GetHostAddresses('%NETWORK_CHECK_HOST%') | Out-Null; $client = New-Object System.Net.Sockets.TcpClient; $async = $client.BeginConnect('%NETWORK_CHECK_HOST%', %NETWORK_CHECK_PORT%, $null, $null); if (-not $async.AsyncWaitHandle.WaitOne(5000, $false)) { $client.Close(); exit 1 }; $client.EndConnect($async); $client.Close(); exit 0 } catch { exit 1 }"
    if not errorlevel 1 (
        echo Network is ready for %NETWORK_CHECK_HOST%:%NETWORK_CHECK_PORT%.
        echo Waiting %POST_NETWORK_DELAY_SECONDS% more seconds for post-wake network services to settle...
        timeout /t %POST_NETWORK_DELAY_SECONDS% /nobreak > nul
        exit /b 0
    )

    echo Network is not ready yet; retrying in %NETWORK_RETRY_SECONDS% seconds...
    timeout /t %NETWORK_RETRY_SECONDS% /nobreak > nul
)

type nul > "%DETAILED_LOG_FILE%"
>> "%DETAILED_LOG_FILE%" echo [%date% %time%] Network readiness check failed for %NETWORK_CHECK_HOST%:%NETWORK_CHECK_PORT% after %NETWORK_READY_TIMEOUT_SECONDS% seconds.
>> "%LOG_FILE%" echo [%date% %time%] Network readiness check failed for %NETWORK_CHECK_HOST%:%NETWORK_CHECK_PORT% after %NETWORK_READY_TIMEOUT_SECONDS% seconds.
echo Network readiness check failed for %NETWORK_CHECK_HOST%:%NETWORK_CHECK_PORT%. See "%LOG_FILE%".
exit /b 1
