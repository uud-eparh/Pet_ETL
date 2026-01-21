# ETL-пайплайн для курсов валют ЦБ РФ

Автоматический сбор ежедневных и ежемесячных курсов валют из API Центробанка РФ.

## 📋 Функциональность

### Основные возможности:
- 📅 **Ежедневные курсы валют** через SOAP API
- 📊 **Ежемесячные средние курсы** через REST API
- 🗄️ **Загрузка в PostgreSQL** с поддержкой UPSERT
- 📈 **Историческая загрузка** за любой период
- 📝 **Детальное логирование** в файлы и консоль
- 📄 **Автоматические отчеты** о выполнении
- 🐳 **Docker-контейнеризация** БД

### Режимы работы:
- `full` - полная загрузка (ежедневные + ежемесячные)
- `daily-only` - только ежедневные данные
- `monthly-only` - только ежемесячные данные
- `historical` - историческая загрузка за период

## 🚀 Быстрый старт

### Требования:
- Python 3.11+
- PostgreSQL 15+ (или Docker)
- Docker Desktop (опционально)

### Установка:

1. **Клонируйте репозиторий:**
bash
git clone <your-repo-url>
cd Pet_001_ETL
Настройте виртуальное окружение:

bash
python -m venv .venv

### Windows:
.venv\Scripts\activate

### Linux/Mac:
source .venv/bin/activate
Установите зависимости:

bash
pip install -r requirements.txt
Запустите базу данных:

bash
docker-compose up -d
Настройте конфигурацию (при необходимости):

bash
### Отредактируйте config.py если нужно изменить настройки
Использование:
Базовый запуск:

bash
python main.py
Загрузка исторических данных:

bash
### За последние 30 дней
python main.py --mode historical --historical-last-days 30

### За конкретный период
python main.py --mode historical --start-date 2024-01-01 --end-date 2024-01-31
Только ежедневные данные:

bash
python main.py --mode daily-only --target-date 2024-01-15
Скрипты для разных ОС:

bash
### Windows
run_etl.bat

### Linux/Mac
./run_etl.sh --mode historical --historical-last-days 7

### 🗄️ Структура проекта
text
Pet_001_ETL/
├── src/                    # Исходный код
│   ├── extractors/         # Модули извлечения данных
│   │   ├── daily_extractor.py     # SOAP API (ежедневные)
│   │   ├── monthly_extractor.py   # REST API (ежемесячные)
│   │   └── historical_extractor.py # Исторические данные
│   └── loaders/            # Модули загрузки
│       └── db_loader.py    # Загрузка в PostgreSQL
├── data/                   # Тестовые данные и примеры
├── logs/                   # Логи выполнения
├── reports/                # Отчеты о загрузке
├── init_db/                # SQL скрипты инициализации БД
├── config.py               # Основная конфигурация
├── historical_config.py    # Конфигурация исторической загрузки
├── logging_config.py       # Настройка логирования
├── main.py                 # Главный скрипт ETL
├── docker-compose.yml      # Конфигурация Docker
├── requirements.txt        # Зависимости Python
└── README.md              # Документация

🗄️ Структура базы данных
Таблица exchange_rates:

Поле	Тип	Описание
id	SERIAL	Первичный ключ
currency_code	VARCHAR(3)	Код валюты (USD, EUR)
currency_name	VARCHAR(100)	Название валюты
exchange_rate	DECIMAL(12,6)	Курс к рублю
rate_date	DATE	Дата курса
rate_type	VARCHAR(10)	Тип: 'daily' или 'monthly'
nominal	INTEGER	Номинал валюты
load_timestamp	TIMESTAMP	Время загрузки
Индексы:
Уникальный индекс: (currency_code, rate_date, rate_type)
Индекс по дате: rate_date
Индекс по валюте: currency_code

### 🔧 Конфигурация
Основные файлы конфигурации:
config.py - основные настройки:
Параметры подключения к БД
URL API Центробанка
Таймауты и лимиты
historical_config.py - предопределенные периоды:
last_week, last_month, last_quarter
Годовые периоды (2023, 2024)
docker-compose.yml - настройка PostgreSQL контейнера

## 🐛 Устранение неполадок
Частые проблемы:
Не удается подключиться к БД:

bash
### Проверьте запущен ли Docker
docker ps

### Проверьте порт
netstat -an | grep 6432
Ошибки API Центробанка:
Проверьте интернет-подключение
API может быть недоступно в выходные
Используйте --skip-weekends для пропуска выходных

Не хватает прав:

bash
### Для Linux скрипта
chmod +x run_etl.sh

### Для директорий
chmod 755 logs reports data
📈 Дальнейшее развитие
Потенциальные улучшения:
Мониторинг: Добавить Prometheus/Grafana
Уведомления: Slack/Telegram бот при ошибках
Кэширование: Redis для временного хранения данных
Тесты: pytest для модульного тестирования
Airflow: Миграция на Apache Airflow для оркестрации