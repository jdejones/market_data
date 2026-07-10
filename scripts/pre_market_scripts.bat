@echo off
REM Pre-market startup intentionally launches long-running scripts in separate terminals.
REM intraday_price_stream.py must start first because it streams market data into the
REM database used by downstream tools. After a short startup delay, the GUI/monitoring
REM scripts are independent enough to run in parallel. Use `start ... cmd /k` for each
REM process so every infinite-loop script keeps its own visible terminal and can be
REM monitored or closed independently.
set "PYTHON_EXE=C:\Users\jdejo\AppData\Local\Programs\Python\Python313\python.exe"
set "LOG_FILE=C:\Users\jdejo\Market_Data_Processing\market_data\scripts\script_error_logs.txt"
set "DETAILED_LOG_FILE=C:\Users\jdejo\Market_Data_Processing\market_data\scripts\scripts_error_logs_detailed.txt"
set "MARKET_DATA_ROOT=C:\Users\jdejo\Market_Data_Processing\market_data"
set "NEWS_TRACKER_ROOT=C:\Users\jdejo\News_Tracker"
set "SYMBOLS_FILE=E:\Market Research\Studies\Sector Studies\Watchlists\High_AvgDV.txt"
set "NETWORK_CHECK_HOST=api.polygon.io"
set "NETWORK_CHECK_PORT=443"
set "NETWORK_READY_TIMEOUT_SECONDS=900"
set "NETWORK_RETRY_SECONDS=15"
set "POST_NETWORK_DELAY_SECONDS=180"
set "STARTUP_DELAY_SECONDS=30"

call :wait_for_network_ready
if errorlevel 1 exit /b %errorlevel%

start "Intraday Price Stream" /D "%MARKET_DATA_ROOT%" cmd /v:on /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\intraday_price_stream.py" --symbols-file "%SYMBOLS_FILE%" 2> "%TEMP%\intraday_price_stream_stderr.log" & set "EXIT_CODE=!errorlevel!" & if not "!EXIT_CODE!"=="0" (type nul > "%DETAILED_LOG_FILE%" & >> "%DETAILED_LOG_FILE%" echo [!date! !time!] intraday_price_stream.py failed with exit code !EXIT_CODE!. & >> "%DETAILED_LOG_FILE%" echo. & if exist "%TEMP%\intraday_price_stream_stderr.log" type "%TEMP%\intraday_price_stream_stderr.log" >> "%DETAILED_LOG_FILE%" & >> "%LOG_FILE%" echo [!date! !time!] intraday_price_stream.py failed with exit code !EXIT_CODE!. & echo intraday_price_stream.py failed with exit code !EXIT_CODE!. See "%LOG_FILE%".) & if exist "%TEMP%\intraday_price_stream_stderr.log" del "%TEMP%\intraday_price_stream_stderr.log" > nul 2>&1"

echo Waiting %STARTUP_DELAY_SECONDS% seconds for intraday_price_stream.py to start writing data...
timeout /t %STARTUP_DELAY_SECONDS% /nobreak > nul

start "Filings Stream GUI" /D "%NEWS_TRACKER_ROOT%" cmd /v:on /k ""%PYTHON_EXE%" "%NEWS_TRACKER_ROOT%\scripts\filings_stream_gui.py" 2> "%TEMP%\filings_stream_gui_stderr.log" & set "EXIT_CODE=!errorlevel!" & if not "!EXIT_CODE!"=="0" (type nul > "%DETAILED_LOG_FILE%" & >> "%DETAILED_LOG_FILE%" echo [!date! !time!] filings_stream_gui.py failed with exit code !EXIT_CODE!. & >> "%DETAILED_LOG_FILE%" echo. & if exist "%TEMP%\filings_stream_gui_stderr.log" type "%TEMP%\filings_stream_gui_stderr.log" >> "%DETAILED_LOG_FILE%" & >> "%LOG_FILE%" echo [!date! !time!] filings_stream_gui.py failed with exit code !EXIT_CODE!. & echo filings_stream_gui.py failed with exit code !EXIT_CODE!. See "%LOG_FILE%".) & if exist "%TEMP%\filings_stream_gui_stderr.log" del "%TEMP%\filings_stream_gui_stderr.log" > nul 2>&1"

start "Current RVOL GUI" /D "%MARKET_DATA_ROOT%" cmd /v:on /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\current_rvol_gui.py" --symbols-file "%SYMBOLS_FILE%" --update-elevated-table --update-ep-rvol-table 2> "%TEMP%\current_rvol_gui_stderr.log" & set "EXIT_CODE=!errorlevel!" & if not "!EXIT_CODE!"=="0" (type nul > "%DETAILED_LOG_FILE%" & >> "%DETAILED_LOG_FILE%" echo [!date! !time!] current_rvol_gui.py failed with exit code !EXIT_CODE!. & >> "%DETAILED_LOG_FILE%" echo. & if exist "%TEMP%\current_rvol_gui_stderr.log" type "%TEMP%\current_rvol_gui_stderr.log" >> "%DETAILED_LOG_FILE%" & >> "%LOG_FILE%" echo [!date! !time!] current_rvol_gui.py failed with exit code !EXIT_CODE!. & echo current_rvol_gui.py failed with exit code !EXIT_CODE!. See "%LOG_FILE%".) & if exist "%TEMP%\current_rvol_gui_stderr.log" del "%TEMP%\current_rvol_gui_stderr.log" > nul 2>&1"

start "High Short Interest In Play" /D "%MARKET_DATA_ROOT%" cmd /v:on /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\high_short_interest_in_play.py" 2> "%TEMP%\high_short_interest_in_play_stderr.log" & set "EXIT_CODE=!errorlevel!" & if not "!EXIT_CODE!"=="0" (type nul > "%DETAILED_LOG_FILE%" & >> "%DETAILED_LOG_FILE%" echo [!date! !time!] high_short_interest_in_play.py failed with exit code !EXIT_CODE!. & >> "%DETAILED_LOG_FILE%" echo. & if exist "%TEMP%\high_short_interest_in_play_stderr.log" type "%TEMP%\high_short_interest_in_play_stderr.log" >> "%DETAILED_LOG_FILE%" & >> "%LOG_FILE%" echo [!date! !time!] high_short_interest_in_play.py failed with exit code !EXIT_CODE!. & echo high_short_interest_in_play.py failed with exit code !EXIT_CODE!. See "%LOG_FILE%".) & if exist "%TEMP%\high_short_interest_in_play_stderr.log" del "%TEMP%\high_short_interest_in_play_stderr.log" > nul 2>&1"

start "VWAP Bands" /D "%MARKET_DATA_ROOT%" cmd /v:on /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\vwap_bands.py" 2> "%TEMP%\vwap_bands_stderr.log" & set "EXIT_CODE=!errorlevel!" & if not "!EXIT_CODE!"=="0" (type nul > "%DETAILED_LOG_FILE%" & >> "%DETAILED_LOG_FILE%" echo [!date! !time!] vwap_bands.py failed with exit code !EXIT_CODE!. & >> "%DETAILED_LOG_FILE%" echo. & if exist "%TEMP%\vwap_bands_stderr.log" type "%TEMP%\vwap_bands_stderr.log" >> "%DETAILED_LOG_FILE%" & >> "%LOG_FILE%" echo [!date! !time!] vwap_bands.py failed with exit code !EXIT_CODE!. & echo vwap_bands.py failed with exit code !EXIT_CODE!. See "%LOG_FILE%".) & if exist "%TEMP%\vwap_bands_stderr.log" del "%TEMP%\vwap_bands_stderr.log" > nul 2>&1"

start "NHOD" /D "%MARKET_DATA_ROOT%" cmd /v:on /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\nhod.py" 2> "%TEMP%\nhod_stderr.log" & set "EXIT_CODE=!errorlevel!" & if not "!EXIT_CODE!"=="0" (type nul > "%DETAILED_LOG_FILE%" & >> "%DETAILED_LOG_FILE%" echo [!date! !time!] nhod.py failed with exit code !EXIT_CODE!. & >> "%DETAILED_LOG_FILE%" echo. & if exist "%TEMP%\nhod_stderr.log" type "%TEMP%\nhod_stderr.log" >> "%DETAILED_LOG_FILE%" & >> "%LOG_FILE%" echo [!date! !time!] nhod.py failed with exit code !EXIT_CODE!. & echo nhod.py failed with exit code !EXIT_CODE!. See "%LOG_FILE%".) & if exist "%TEMP%\nhod_stderr.log" del "%TEMP%\nhod_stderr.log" > nul 2>&1"

start "NLOD" /D "%MARKET_DATA_ROOT%" cmd /v:on /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\nlod.py" 2> "%TEMP%\nlod_stderr.log" & set "EXIT_CODE=!errorlevel!" & if not "!EXIT_CODE!"=="0" (type nul > "%DETAILED_LOG_FILE%" & >> "%DETAILED_LOG_FILE%" echo [!date! !time!] nlod.py failed with exit code !EXIT_CODE!. & >> "%DETAILED_LOG_FILE%" echo. & if exist "%TEMP%\nlod_stderr.log" type "%TEMP%\nlod_stderr.log" >> "%DETAILED_LOG_FILE%" & >> "%LOG_FILE%" echo [!date! !time!] nlod.py failed with exit code !EXIT_CODE!. & echo nlod.py failed with exit code !EXIT_CODE!. See "%LOG_FILE%".) & if exist "%TEMP%\nlod_stderr.log" del "%TEMP%\nlod_stderr.log" > nul 2>&1"

start "EP Continuation" /D "%MARKET_DATA_ROOT%" cmd /v:on /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\ep_continuation.py" 2> "%TEMP%\ep_continuation_stderr.log" & set "EXIT_CODE=!errorlevel!" & if not "!EXIT_CODE!"=="0" (type nul > "%DETAILED_LOG_FILE%" & >> "%DETAILED_LOG_FILE%" echo [!date! !time!] ep_continuation.py failed with exit code !EXIT_CODE!. & >> "%DETAILED_LOG_FILE%" echo. & if exist "%TEMP%\ep_continuation_stderr.log" type "%TEMP%\ep_continuation_stderr.log" >> "%DETAILED_LOG_FILE%" & >> "%LOG_FILE%" echo [!date! !time!] ep_continuation.py failed with exit code !EXIT_CODE!. & echo ep_continuation.py failed with exit code !EXIT_CODE!. See "%LOG_FILE%".) & if exist "%TEMP%\ep_continuation_stderr.log" del "%TEMP%\ep_continuation_stderr.log" > nul 2>&1"

start "EP 5-9 EMA Cross Below VWAP" /D "%MARKET_DATA_ROOT%" cmd /v:on /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\ep_59ema_cross_below_vwap.py" 2> "%TEMP%\ep_59ema_cross_below_vwap_stderr.log" & set "EXIT_CODE=!errorlevel!" & if not "!EXIT_CODE!"=="0" (type nul > "%DETAILED_LOG_FILE%" & >> "%DETAILED_LOG_FILE%" echo [!date! !time!] ep_59ema_cross_below_vwap.py failed with exit code !EXIT_CODE!. & >> "%DETAILED_LOG_FILE%" echo. & if exist "%TEMP%\ep_59ema_cross_below_vwap_stderr.log" type "%TEMP%\ep_59ema_cross_below_vwap_stderr.log" >> "%DETAILED_LOG_FILE%" & >> "%LOG_FILE%" echo [!date! !time!] ep_59ema_cross_below_vwap.py failed with exit code !EXIT_CODE!. & echo ep_59ema_cross_below_vwap.py failed with exit code !EXIT_CODE!. See "%LOG_FILE%".) & if exist "%TEMP%\ep_59ema_cross_below_vwap_stderr.log" del "%TEMP%\ep_59ema_cross_below_vwap_stderr.log" > nul 2>&1"

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
