@echo off

:loop
rem run the app.py script
python app.py
rem Wait for 0.5 seconds before running the script again
ping 127.0.0.1 -n 1 -w 200 > nul

goto loop
