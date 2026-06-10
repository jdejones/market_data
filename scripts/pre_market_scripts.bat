@echo off
REM Pre-market startup intentionally launches long-running scripts in separate terminals.
REM intraday_price_stream.py must start first because it streams market data into the
REM database used by downstream tools. After a short startup delay, the GUI/monitoring
REM scripts are independent enough to run in parallel. Use `start ... cmd /k` for each
REM process so every infinite-loop script keeps its own visible terminal and can be
REM monitored or closed independently.
set "PYTHON_EXE=C:\Users\jdejo\AppData\Local\Programs\Python\Python313\python.exe"
set "LOG_FILE=C:\Users\jdejo\Market_Data_Processing\market_data\scripts\script_error_logs.txt"
set "MARKET_DATA_ROOT=C:\Users\jdejo\Market_Data_Processing\market_data"
set "NEWS_TRACKER_ROOT=C:\Users\jdejo\News_Tracker"
set "SYMBOLS_FILE=E:\Market Research\Studies\Sector Studies\Watchlists\High_AvgDV.txt"
set "STARTUP_DELAY_SECONDS=30"

start "Intraday Price Stream" /D "%MARKET_DATA_ROOT%" cmd /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\intraday_price_stream.py" --symbols-file "%SYMBOLS_FILE%" || (>> "%LOG_FILE%" echo [%%date%% %%time%%] intraday_price_stream.py failed with exit code %%errorlevel%%. & echo intraday_price_stream.py failed with exit code %%errorlevel%%. See "%LOG_FILE%".)"

echo Waiting %STARTUP_DELAY_SECONDS% seconds for intraday_price_stream.py to start writing data...
timeout /t %STARTUP_DELAY_SECONDS% /nobreak > nul

start "Filings Stream GUI" /D "%NEWS_TRACKER_ROOT%" cmd /k ""%PYTHON_EXE%" "%NEWS_TRACKER_ROOT%\scripts\filings_stream_gui.py" || (>> "%LOG_FILE%" echo [%%date%% %%time%%] filings_stream_gui.py failed with exit code %%errorlevel%%. & echo filings_stream_gui.py failed with exit code %%errorlevel%%. See "%LOG_FILE%".)"

start "Current RVOL GUI" /D "%MARKET_DATA_ROOT%" cmd /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\current_rvol_gui.py" --symbols-file "%SYMBOLS_FILE%" --update-elevated-table || (>> "%LOG_FILE%" echo [%%date%% %%time%%] current_rvol_gui.py failed with exit code %%errorlevel%%. & echo current_rvol_gui.py failed with exit code %%errorlevel%%. See "%LOG_FILE%".)"

start "High Short Interest In Play" /D "%MARKET_DATA_ROOT%" cmd /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\high_short_interest_in_play.py" || (>> "%LOG_FILE%" echo [%%date%% %%time%%] high_short_interest_in_play.py failed with exit code %%errorlevel%%. & echo high_short_interest_in_play.py failed with exit code %%errorlevel%%. See "%LOG_FILE%".)"

start "VWAP Bands" /D "%MARKET_DATA_ROOT%" cmd /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\vwap_bands.py" || (>> "%LOG_FILE%" echo [%%date%% %%time%%] vwap_bands.py failed with exit code %%errorlevel%%. & echo vwap_bands.py failed with exit code %%errorlevel%%. See "%LOG_FILE%".)"

start "NHOD" /D "%MARKET_DATA_ROOT%" cmd /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\nhod.py" || (>> "%LOG_FILE%" echo [%%date%% %%time%%] nhod.py failed with exit code %%errorlevel%%. & echo nhod.py failed with exit code %%errorlevel%%. See "%LOG_FILE%".)"

start "NLOD" /D "%MARKET_DATA_ROOT%" cmd /k ""%PYTHON_EXE%" "%MARKET_DATA_ROOT%\scripts\nlod.py" || (>> "%LOG_FILE%" echo [%%date%% %%time%%] nlod.py failed with exit code %%errorlevel%%. & echo nlod.py failed with exit code %%errorlevel%%. See "%LOG_FILE%".)"

exit /b 0
