import os
from pathlib import Path
from dotenv import load_dotenv

# Определяем путь к .env файлу (в корне проекта)
env_path = Path(__file__).parent / '.env'  # config.py → папка → .env

if env_path.exists():
    print(f"🔐 Загружаем переменные из {env_path}")
    load_dotenv(dotenv_path=env_path)
else:
    print("⚠️ Файл .env не найден. Используем значения по умолчанию или системные переменные.")


# Конфигурация базы данных
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),  # Значение по умолчанию
    'port': int(os.getenv('DB_PORT', '5432')),  # Конвертируем в int
    'database': os.getenv('DB_NAME', '********'),
    'user': os.getenv('DB_USER', '******'),
    'password': os.getenv('DB_PASSWORD', '******'),  # ⚠️ По умолчанию тоже 'admin'
}

# Конфигурация API
API_CONFIG = {
        # REST API для ежемесячных данных
    'rest_base_url': 'http://www.cbr.ru/dataservice/',

        # SOAP API для ежедневных данных
    'soap_wsdl_url': 'http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL',

        # Параметры для ежемесячных данных (документация на
        # https://cbr.ru/statistics/data-service/APIdocumentation/)
    'monthly_publication_id': 33,
    'monthly_dataset_id': 128,

        # Настройки запросов
    'request_timeout': int(os.getenv('CBR_API_TIMEOUT', '30')),  # таймаут в секундах
    'max_retries': int(os.getenv('CBR_API_MAX_RETRIES', '3'))    # количество попыток при ошибках
}

ETL_CONFIG = {
    'default_start_date': '2024-01-01',  # Начальная дата для сбора данных
    'batch_size': 100,  # Сколько строк загружать за раз
    'retry_attempts': 3,  # Попытки при ошибках сети
    'log_level': os.getenv('LOG_LEVEL', 'INFO'),   # Уровень логирования
    'environment': os.getenv('ENVIRONMENT', 'development')
}