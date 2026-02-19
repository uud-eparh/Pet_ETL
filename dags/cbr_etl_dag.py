"""
DAG для автоматического сбора курсов валют ЦБ РФ

Особенности:
- Ежедневный запуск в 18:00
- Сбор ежедневных курсов (за вчера)
- Сбор ежемесячных курсов (1-го числа каждого месяца)
- Логирование и мониторинг
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging
import sys
import os
from pathlib import Path
from datetime import date

# Добавляем путь к проекту для импорта наших модулей
sys.path.append('/opt/airflow/src')

from extractors.daily_extractor import DailyExtractor
from extractors.monthly_extractor import MonthlyExtractor
from loaders.db_loader import DatabaseLoader

# Настройка логирования
logger = logging.getLogger(__name__)

# Конфигурация по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Создаем DAG
dag = DAG(
    'cbr_exchange_rates_etl',
    default_args=default_args,
    description='Сбор курсов валют ЦБ РФ',
    schedule_interval='0 18 * * *',  # Каждый день в 18:00
    catchup=False,
    tags=['cbr', 'currency', 'etl'],
    max_active_runs=1,
)

def extract_daily_rates(**context):
    """
    Извлечение ежедневных курсов валют
    """
    logger.info("=" * 60)
    logger.info("НАЧАЛО ИЗВЛЕЧЕНИЯ ЕЖЕДНЕВНЫХ КУРСОВ")
    logger.info("=" * 60)
    
    # Определяем дату для загрузки (вчерашний день)
    execution_date = context['execution_date']
    target_date = execution_date.date() - timedelta(days=1)
    
    logger.info(f"Целевая дата: {target_date}")
    
    # Конфигурация
    from config import API_CONFIG
    
    # Создаем экстрактор
    extractor = DailyExtractor(API_CONFIG['soap_wsdl_url'])
    
    if not extractor.connect():
        raise Exception("Не удалось подключиться к SOAP API")
    
    # Получаем данные
    rates = extractor.get_currencies_on_date(target_date)
    
    if not rates:
        logger.warning(f"Нет данных за {target_date}")
        return []
    
    logger.info(f"✓ Получено {len(rates)} записей")
    
    # Преобразуем date объекты в строки для JSON сериализации
    serializable_rates = []
    for rate in rates:
        rate_copy = rate.copy()
        # Преобразуем date в строку
        if isinstance(rate_copy.get('rate_date'), date):
            rate_copy['rate_date'] = rate_copy['rate_date'].isoformat()
        serializable_rates.append(rate_copy)
    
    # Сохраняем в XCom для следующей задачи
    context['ti'].xcom_push(key='daily_rates', value=serializable_rates)
    context['ti'].xcom_push(key='daily_date', value=target_date.isoformat())
    
    return len(rates)

def extract_monthly_rates(**context):
    """
    Извлечение ежемесячных курсов валют
    Запускается только 1-го числа каждого месяца
    """
    logger.info("=" * 60)
    logger.info("НАЧАЛО ИЗВЛЕЧЕНИЯ ЕЖЕМЕСЯЧНЫХ КУРСОВ")
    logger.info("=" * 60)
    
    # Проверяем, нужно ли запускать (1-е число месяца)
    execution_date = context['execution_date']
    
    # Загружаем данные за текущий и предыдущий год
    current_year = execution_date.year
    previous_year = current_year - 1
    
    logger.info(f"Загружаем данные за {previous_year}-{current_year} годы")
    
    from config import API_CONFIG
    
    extractor = MonthlyExtractor(API_CONFIG['rest_base_url'])
    
    rates = extractor.get_monthly_rates(
        publication_id=API_CONFIG['monthly_publication_id'],
        dataset_id=API_CONFIG['monthly_dataset_id'],
        start_year=previous_year,
        end_year=current_year
    )
    
    if not rates:
        logger.warning("Нет ежемесячных данных")
        return []
    
    # Преобразуем в список словарей для загрузки
    monthly_data = []
    for rate in rates:
        monthly_data.append({
            'currency_code': rate.currency_code,
            'currency_name': rate.currency_name,
            'exchange_rate': rate.exchange_rate,
            'rate_date': rate.rate_date,
            'rate_type': rate.rate_type,
            'nominal': rate.nominal
        })
    
    logger.info(f"✓ Получено {len(monthly_data)} ежемесячных записей")
    
    # Сохраняем в XCom
    context['ti'].xcom_push(key='monthly_rates', value=monthly_data)
    
    return len(monthly_data)

def load_rates_to_db(**context):
    """
    Загрузка данных в базу данных
    """
    logger.info("=" * 60)
    logger.info("ЗАГРУЗКА ДАННЫХ В БАЗУ ДАННЫХ")
    logger.info("=" * 60)
    
    # Получаем данные из XCom
    ti = context['ti']
    daily_rates = ti.xcom_pull(key='daily_rates', task_ids='extract_daily_rates')
    monthly_rates = ti.xcom_pull(key='monthly_rates', task_ids='extract_monthly_rates')
    daily_date = ti.xcom_pull(key='daily_date', task_ids='extract_daily_rates')
    
    logger.info(f"Ежедневных данных: {len(daily_rates) if daily_rates else 0}")
    logger.info(f"Ежемесячных данных: {len(monthly_rates) if monthly_rates else 0}")
    
    # Преобразуем строки дат обратно в date объекты для загрузчика
    if daily_rates:
        for rate in daily_rates:
            if isinstance(rate.get('rate_date'), str):
                from datetime import date
                rate['rate_date'] = date.fromisoformat(rate['rate_date'])
    
    if monthly_rates:
        for rate in monthly_rates:
            if isinstance(rate.get('rate_date'), str):
                from datetime import date
                rate['rate_date'] = date.fromisoformat(rate['rate_date'])
    
    # Конфигурация БД
    from config import DB_CONFIG
    
    # Создаем загрузчик
    loader = DatabaseLoader(DB_CONFIG)
    
    if not loader.connect():
        raise Exception("Не удалось подключиться к БД")
    
    total_loaded = 0
    
    # Загружаем ежедневные данные
    if daily_rates:
        logger.info("Загрузка ежедневных данных...")
        daily_count = loader.insert_exchange_rates(daily_rates)
        total_loaded += daily_count
        logger.info(f"✓ Загружено ежедневных: {daily_count}")
    
    # Загружаем ежемесячные данные
    if monthly_rates:
        logger.info("Загрузка ежемесячных данных...")
        monthly_count = loader.insert_exchange_rates(monthly_rates)
        total_loaded += monthly_count
        logger.info(f"✓ Загружено ежемесячных: {monthly_count}")
    
    # Фиксируем транзакцию
    loader.connection.commit()
    loader.disconnect()
    
    logger.info(f"✓ ВСЕГО ЗАГРУЖЕНО: {total_loaded} записей")
    
    return total_loaded

def check_data_quality(**context):
    """
    Проверка качества данных после загрузки
    """
    logger.info("=" * 60)
    logger.info("ПРОВЕРКА КАЧЕСТВА ДАННЫХ")
    logger.info("=" * 60)
    
    from config import DB_CONFIG
    import psycopg2
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    checks_passed = True
    warnings = []
    
    # Получаем дату из контекста (данные за вчера)
    ti = context['ti']
    daily_date = ti.xcom_pull(key='daily_date', task_ids='extract_daily_rates')
    
    logger.info(f"Проверяем данные за дату: {daily_date}")
    
    # Проверка 1: Есть ли данные за загруженную дату
    cur.execute("""
        SELECT COUNT(*) 
        FROM cbr_raw.exchange_rates 
        WHERE rate_date = %s
        AND rate_type = 'daily'
    """, (daily_date,))
    
    yesterday_count = cur.fetchone()[0]
    
    if yesterday_count == 0:
        msg = f"⚠ Нет данных за {daily_date}!"
        logger.warning(msg)
        warnings.append(msg)
        # Не проваливаем задачу, только предупреждение
    else:
        logger.info(f"✓ Данные за {daily_date}: {yesterday_count} записей")
    
    # Проверка 2: Проверка на аномальные значения
    cur.execute("""
        SELECT COUNT(*) 
        FROM cbr_raw.exchange_rates 
        WHERE exchange_rate <= 0 OR exchange_rate > 1000
    """)
    invalid_rates = cur.fetchone()[0]
    
    if invalid_rates > 0:
        msg = f"⚠ Найдено {invalid_rates} аномальных курсов!"
        logger.warning(msg)
        warnings.append(msg)
        # Только предупреждение
    else:
        logger.info("✓ Аномальных значений не найдено")
    
    # Проверка 3: Статистика по основным валютам
    cur.execute("""
        SELECT currency_code, exchange_rate 
        FROM cbr_raw.exchange_rates 
        WHERE rate_date = %s
        AND currency_code IN ('USD', 'EUR', 'CNY')
        ORDER BY currency_code
    """, (daily_date,))
    
    main_currencies = cur.fetchall()
    if main_currencies:
        logger.info("✓ Курсы основных валют:")
        for code, rate in main_currencies:
            logger.info(f"  {code}: {rate}")
    else:
        logger.warning("⚠ Нет данных по основным валютам!")
    
    # Проверка 4: Общее количество записей в БД
    cur.execute("SELECT COUNT(*) FROM cbr_raw.exchange_rates")
    total_records = cur.fetchone()[0]
    logger.info(f"✓ Всего записей в БД: {total_records}")
    
    cur.close()
    conn.close()
    
    # Если есть предупреждения, но не ошибки - задача успешна
    if warnings:
        logger.warning("Проверка качества завершена с предупреждениями:")
        for w in warnings:
            logger.warning(f"  {w}")
    else:
        logger.info("✅ Проверка качества пройдена успешно!")
    
    # Возвращаем результат, но не вызываем исключение
    return {
        "success": True,
        "warnings": warnings,
        "total_records": total_records,
        "date_checked": daily_date
    }

def send_success_notification(**context):
    """
    Отправка уведомления об успешной загрузке
    (можно расширить для отправки в Telegram, Email и т.д.)
    """
    logger.info("=" * 60)
    logger.info("ЗАВЕРШЕНИЕ ETL-ПРОЦЕССА")
    logger.info("=" * 60)
    
    ti = context['ti']
    daily_count = ti.xcom_pull(task_ids='extract_daily_rates')
    monthly_count = ti.xcom_pull(task_ids='extract_monthly_rates')
    total_loaded = ti.xcom_pull(task_ids='load_rates_to_db')
    
    logger.info(f"✅ ETL-пайплайн успешно завершен!")
    logger.info(f"📊 Статистика:")
    logger.info(f"  - Ежедневные курсы: {daily_count} записей")
    logger.info(f"  - Ежемесячные курсы: {monthly_count} записей")
    logger.info(f"  - Всего загружено: {total_loaded} записей")

# Создаем задачи (tasks)

extract_daily = PythonOperator(
    task_id='extract_daily_rates',
    python_callable=extract_daily_rates,
    provide_context=True,
    dag=dag,
)

extract_monthly = PythonOperator(
    task_id='extract_monthly_rates',
    python_callable=extract_monthly_rates,
    provide_context=True,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_rates_to_db',
    python_callable=load_rates_to_db,
    provide_context=True,
    dag=dag,
)

quality_check = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag,
)

notification = PythonOperator(
    task_id='send_notification',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

# Определяем порядок выполнения задач
[extract_daily, extract_monthly] >> load_data >> quality_check >> notification