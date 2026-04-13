@echo off
echo 🚀 Запуск системи стрімінгу твітів...

:: 1. Запускаємо Консюмера в новому вікні
start "Consumer" cmd /k "venv\Scripts\activate && python consumer_main.py"

:: 2. Чекаємо 3 секунди, щоб база та клієнт встигли ініціалізуватися
timeout /t 3

:: 3. Запускаємо Продюсера в новому вікні
start "Producer" cmd /k "venv\Scripts\activate && python producer_main.py"

echo ✅ Обидва сервіси запущені!