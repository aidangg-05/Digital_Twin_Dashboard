@echo off

:loop
rem run the app.py script
python app.py
rem Wait for 2 seconds before running the script again
timeout /t 1 /nobreak > nul

goto loop