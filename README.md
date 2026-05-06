# Проект ETL-пайплайна для курсов валют ЦБ РФ

## 📋 Описание проекта

Автоматизированный ETL-пайплайн для сбора, обработки и визуализации курсов валют Центрального банка Российской Федерации. Проект включает в себя полный цикл данных: от извлечения из API до построения аналитических дашбордов.

## 🏗 Архитектура

### Компоненты системы:

* **Источники данных**: SOAP API (ежедневные курсы) и REST API (ежемесячные курсы) ЦБ РФ

* **Оркестрация**: Apache Airflow 2.10.3

* **Хранилище**: PostgreSQL 15

* **Разработка**: VSCode в браузере (code-server)

* **Визуализация**: Grafana

* **Инфраструктура**: Docker + Docker Compose

### Структура базы данных:

* `cbr_raw` - сырые данные (таблица exchange\_rates)

* `cbr_dm` - витрины данных (материализованные представления)

## 🚀 Быстрый старт

### Предварительные требования:

* Docker и Docker Compose

* Git

* 4+ GB свободной оперативной памяти

## Установка и запуск:

### Клонировать репозиторий

`git clone https://github.com/uud-eparh/Pet_ETL.git`

### Запустить все сервисы

docker-compose up -d

### Проверить статус

docker-compose ps

### Просмотр логов (опционально)

docker-compose logs -f

### Доступ к сервисам:

| Сервис  | URL                     | Логин/Пароль |
| :------ | :---------------------- | :----------- |
| Grafana | <http://localhost:3000> | admin/admin  |
| Airflow | <http://localhost:7005> | admin/admin  |
| VSCode  | <http://localhost:7090> | admin        |

## 📊 Дашборды Grafana

## После запуска автоматически создается дашборд "Аналитика курсов валют ЦБ РФ" с панелями:

### Ключевые метрики:

Общее количество записей

Количество уникальных валют

Статистика загрузок за 7 дней

### Динамика курсов:

Курс USD за 30 дней

Сравнение USD, EUR, CNY с флагами стран

### Аналитика:

Топ-10 волатильных валют

Процентные изменения

Статистика загрузок ETL

## 🔄 ETL процессы (Airflow)

```bash
Регулярные DAGs:
cbr_exchange_rates_etl - ежедневный сбор данных (в 18:00)

cbr_refresh_marts - обновление витрин (в 18:30)

Ручные DAGs:
cbr_historical_load - загрузка за произвольный период с параметрами:

start_date: начальная дата

end_date: конечная дата

skip_weekends: пропускать выходные

load_type: daily_only / monthly_only / both
```

## /📁 Структура проекта

```bash
text
pet_etl_airflow/
├── docker-compose.yml
├── .env
├── requirements.txt
├── README.md
├── src/
│   ├── extractors/
│   │   ├── daily_extractor.py      # SOAP API
│   │   ├── monthly_extractor.py     # REST API
│   │   └── historical_extractor.py  # Исторические данные
│   └── loaders/
│       └── db_loader.py             # Загрузка в PostgreSQL
├── dags/
│   ├── cbr_etl_dag.py               # Регулярный ETL
│   ├── cbr_historical_load_dag.py    # Историческая загрузка
│   └── cbr_refresh_marts_dag.py      # Обновление витрин
├── init_db/
│   ├── 01_create_schemas.sql
│   ├── 02_create_raw_tables.sql
│   └── 03_create_dm_tables.sql       # Витрины данных
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── postgres.yaml
│   │   └── dashboards/
│   │       └── dashboards.yaml
│   └── dashboards/
│       └── cbr_analytics.json
├── logs/
├── data/
└── reports/
```

## 📊 Витрины данных (cbr\_dm)

```bash
Витрина	Описание	Записей
mv_daily_changes	Ежедневные изменения курсов	~19,600
mv_weekly_stats	Недельная статистика	~2,800
mv_monthly_stats	Месячная статистика	~700
mv_quarterly_stats	Квартальная статистика	~250
mv_top_volatile	Топ-10 волатильных валют	10
mv_correlation_with_usd	Корреляция с USD	54
mv_load_stats	Статистика загрузок	по дням
```

## 🔧 Управление проектом

```bash
Полезные команды:

# Остановить все сервисы

docker-compose down

# Перезапустить конкретный сервис

docker-compose restart airflow-webserver

# Просмотр логов

docker-compose logs -f [service-name]

# Зайти в контейнер

docker exec -it etl-postgres psql -U admin -d etl_airflow

# Обновить витрины вручную

docker exec -it etl-postgres psql -U admin -d etl_airflow -c "SELECT cbr_dm.refresh_all_mviews();"
```

## 📈 Примеры SQL запросов

Топ-5 самых волатильных валют за последние 30 дней:

```sql
SELECT currency_code, volatility, avg_rate, min_rate, max_rate
FROM cbr_dm.mv_top_volatile
LIMIT 5;
```

Динамика USD за последнюю неделю:

```sql
SELECT rate_date, exchange_rate, pct_change
FROM cbr_dm.mv_daily_changes
WHERE currency_code = 'USD'
AND rate_date > CURRENT_DATE - INTERVAL '7 days'
ORDER BY rate_date;
```

Статистика загрузок:

```sql
SELECT * FROM cbr_dm.mv_load_stats ORDER BY load_date DESC;
```

## 🐛 Устранение неполадок

Проблема: Не стартует контейнер

```bash

# Проверить логи

docker-compose logs [service-name]

# Пересоздать контейнер

docker-compose rm -f [service-name]
docker-compose up -d [service-name]
Проблема: Нет данных в Grafana
bash

# Проверить подключение к БД

docker exec -it etl-grafana bash
curl <http://localhost:3000/api/datasources>

# Проверить наличие данных

docker exec -it etl-postgres psql -U admin -d etl_airflow -c "SELECT COUNT(*) FROM cbr_raw.exchange_rates;"
```

Проблема: DAG не выполняется

```bash

# Проверить статус scheduler

docker logs etl-airflow-scheduler --tail 50

# Перезапустить scheduler

docker-compose restart airflow-scheduler
```

## 📝 Примечания

Данные обновляются: ежедневно в 18:00 (курсы за предыдущий день)

Витрины обновляются: ежедневно в 18:30

Хранение: PostgreSQL том монтируется для сохранения данных между перезапусками

Логи: хранятся в папке logs/ на хосте

## 🎯 Возможности для расширения

Telegram бот для уведомлений о достижении целевых курсов

Экспорт данных в Excel/CSV через Airflow

Добавление новых валют в анализ

Мониторинг через Prometheus

ML-прогнозирование курсов валют

Alerting в Grafana при аномальных изменениях

## 👨‍💻 Автор

Проект разработан в учебных целях для демонстрации навыков Data Engineering:

Сбор данных из API

ETL-процессы

Оркестрация с Airflow

Работа с PostgreSQL

Визуализация в Grafana

Docker-контейнеризация
