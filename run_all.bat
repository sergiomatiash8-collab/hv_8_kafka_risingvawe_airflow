@echo off
echo 🚀 Запуск системи аналізу настрою твітів...

:: 1. Запускаємо Консюмера в новому вікні
start "Consumer_Service" cmd /k "venv\Scripts\activate && python consumer_main.py"

:: 2. Чекаємо 5 секунд, щоб Консюмер встиг підключитися до Kafka та Postgres
timeout /t 5

:: 3. Запускаємо Продюсера в новому вікні
start "Producer_Service" cmd /k "venv\Scripts\activate && python producer_main.py"

echo ✅ Обидва сервіси запущені. Дивись у нові вікна!