@echo off

:loop
rem Run the app.py script
python app.py

rem Wait for 2 seconds before running the script again
timeout /t 2 /nobreak > nul

goto loop
