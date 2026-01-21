@echo off
chcp 65001 >nul 2>&1
title ETL-пайплайн ЦБ РФ
color 0A

echo ================================================
echo      ETL-ПАЙПЛАЙН ДЛЯ КУРСОВ ВАЛЮТ ЦБ РФ
echo ================================================
echo Дата запуска: %date% %time%
echo.

REM Проверка существования виртуального окружения
if exist ".venv\Scripts\activate.bat" (
    echo [1/4] Активация виртуального окружения...
    call ".venv\Scripts\activate.bat"
) else (
    echo [1/4] Виртуальное окружение не найдено, используем системный Python
)

REM Проверка Python
echo [2/4] Проверка Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo ОШИБКА: Python не найден!
    echo Установите Python 3.11+ и добавьте в PATH
    pause
    exit /b 1
)

python --version
echo.

REM Проверка зависимостей
echo [3/4] Проверка зависимостей...
python -c "import zeep, psycopg2, requests; print('✓ Основные библиотеки доступны')" 2>nul
if errorlevel 1 (
    echo Предупреждение: Не все зависимости установлены
    echo Запустите: pip install -r requirements.txt
    echo.
)

REM Проверка .env файла
if not exist ".env" (
    echo ВНИМАНИЕ: Файл .env не найден!
    echo Создайте .env из .env.example и заполните настройки
    if exist ".env.example" (
        echo Доступен шаблон: .env.example
    )
    echo.
)

REM Запуск основного скрипта
echo [4/4] Запуск ETL-пайплайна...
echo ================================================
echo.

REM Передаем все аргументы в Python скрипт
python main.py %*

REM Сохраняем код возврата
set EXIT_CODE=%errorlevel%

echo.
echo ================================================
if %EXIT_CODE% EQU 0 (
    echo УСПЕХ: ETL-пайплайн завершен успешно!
) else (
    echo ОШИБКА: ETL-пайплайн завершен с кодом %EXIT_CODE%
)
echo ================================================

REM Деактивация виртуального окружения (если было активировано)
if defined VIRTUAL_ENV (
    echo Деактивация виртуального окружения...
    deactivate
)

REM Пауза если есть ошибка
if not %EXIT_CODE% EQU 0 (
    pause
)

exit /b %EXIT_CODE%