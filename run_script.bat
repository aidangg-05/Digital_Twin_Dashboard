@echo off

:loop
<<<<<<< HEAD
rem run the app.py script
python app.py
rem Wait for 2 seconds before running the script again
timeout /t 1 /nobreak > nul

goto loop
=======
rem Run the app.py script
python app.py

rem Wait for 2 seconds before running the script again
timeout /t 2 /nobreak > nul

goto loop
>>>>>>> d2c5a1f9805a248532dec7ede85cedfd7d8a03af
