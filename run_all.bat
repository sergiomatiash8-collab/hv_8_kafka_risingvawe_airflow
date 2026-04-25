@echo off
echo Starting Twitter sentiment analysis system...

:: 1. Start Consumer in a new window
start "Consumer_Service" cmd /k "venv\Scripts\activate && python consumer_main.py"

:: 2. Wait 5 seconds for consumer to connect to Kafka and Postgres
timeout /t 5

:: 3. Start Producer in a new window
start "Producer_Service" cmd /k "venv\Scripts\activate && python producer_main.py"

echo Both services have been started. Check the new windows.