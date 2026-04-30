"""
DAG для загрузки исторических данных с возможностью указания периода
Даты можно указать в интерфейсе Airflow при запуске
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Param
import logging
import sys

sys.path.append('/opt/airflow/src')

from extractors.historical_extractor import HistoricalExtractor
from loaders.db_loader import DatabaseLoader
from config import API_CONFIG, DB_CONFIG

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Определяем параметры DAG
dag_params = {
    'start_date': Param(
        default=(datetime.now().date() - timedelta(days=30)).isoformat(),
        type='string',
        format='date',
        description='Начальная дата периода (YYYY-MM-DD)'
    ),
    'end_date': Param(
        default=(datetime.now().date() - timedelta(days=1)).isoformat(),
        type='string',
        format='date',
        description='Конечная дата периода (YYYY-MM-DD)'
    ),
    'skip_weekends': Param(
        default=False,
        type='boolean',
        description='Пропускать выходные дни?'
    ),
    'load_type': Param(
        default='daily_only',
        enum=['daily_only', 'monthly_only', 'both'],
        description='Тип данных для загрузки'
    ),
}

dag = DAG(
    'cbr_historical_load',
    default_args=default_args,
    description='Загрузка исторических данных за указанный период',
    schedule_interval=None,  # Только ручной запуск
    catchup=False,
    tags=['cbr', 'historical', 'manual'],
    params=dag_params,
    render_template_as_native_obj=True,
)

def validate_dates(**context):
    """Проверка корректности дат"""
    params = context['params']
    
    start_date = datetime.fromisoformat(params['start_date']).date()
    end_date = datetime.fromisoformat(params['end_date']).date()
    
    logger.info("=" * 60)
    logger.info("ПРОВЕРКА ПАРАМЕТРОВ")
    logger.info("=" * 60)
    logger.info(f"Start date: {start_date}")
    logger.info(f"End date: {end_date}")
    logger.info(f"Skip weekends: {params['skip_weekends']}")
    logger.info(f"Load type: {params['load_type']}")
    
    if start_date > end_date:
        raise ValueError(f"Начальная дата {start_date} больше конечной {end_date}")
    
    if end_date > datetime.now().date():
        raise ValueError(f"Конечная дата {end_date} не может быть в будущем")
    
    days_count = (end_date - start_date).days + 1
    logger.info(f"Всего дней в периоде: {days_count}")
    
    if days_count > 365:
        logger.warning(f"⚠ Период больше года ({days_count} дней). Загрузка может занять много времени.")
    
    # Сохраняем в XCom для следующих задач
    context['ti'].xcom_push(key='start_date', value=start_date.isoformat())
    context['ti'].xcom_push(key='end_date', value=end_date.isoformat())
    context['ti'].xcom_push(key='days_count', value=days_count)
    
    return {
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'days_count': days_count,
        'valid': True
    }

def load_historical_data(**context):
    """Загрузка исторических данных за указанный период"""
    ti = context['ti']
    params = context['params']
    
    # Получаем даты из XCom или параметров
    start_date_str = ti.xcom_pull(key='start_date', task_ids='validate_dates')
    end_date_str = ti.xcom_pull(key='end_date', task_ids='validate_dates')
    
    if not start_date_str or not end_date_str:
        start_date_str = params['start_date']
        end_date_str = params['end_date']
    
    start_date = datetime.fromisoformat(start_date_str).date()
    end_date = datetime.fromisoformat(end_date_str).date()
    skip_weekends = params['skip_weekends']
    load_type = params['load_type']
    
    logger.info("=" * 60)
    logger.info("ЗАГРУЗКА ИСТОРИЧЕСКИХ ДАННЫХ")
    logger.info("=" * 60)
    logger.info(f"Период: {start_date} - {end_date}")
    logger.info(f"Пропускать выходные: {skip_weekends}")
    logger.info(f"Тип загрузки: {load_type}")
    
    # Создаем экстрактор
    extractor = HistoricalExtractor(API_CONFIG['soap_wsdl_url'])
    
    # Получаем данные в зависимости от типа
    all_data = []
    
    if load_type in ['daily_only', 'both']:
        logger.info("Загрузка ежедневных данных...")
        daily_data = extractor.get_historical_data_flattened(
            start_date, 
            end_date, 
            skip_weekends=skip_weekends
        )
        
        if daily_data:
            # Убеждаемся, что у всех записей правильный тип
            for record in daily_data:
                record['rate_type'] = 'daily'
            all_data.extend(daily_data)
            logger.info(f"✓ Получено {len(daily_data)} ежедневных записей")
    
    if load_type in ['monthly_only', 'both']:
        logger.info("Загрузка ежемесячных данных...")
        from extractors.monthly_extractor import MonthlyExtractor
        monthly_extractor = MonthlyExtractor(API_CONFIG['rest_base_url'])
        
        monthly_rates = monthly_extractor.get_monthly_rates(
            publication_id=API_CONFIG['monthly_publication_id'],
            dataset_id=API_CONFIG['monthly_dataset_id'],
            start_year=start_date.year,
            end_year=end_date.year
        )
        
        if monthly_rates:
            monthly_data = []
            for rate in monthly_rates:
                # Фильтруем по дате
                if start_date <= rate.rate_date <= end_date:
                    monthly_data.append({
                        'currency_code': rate.currency_code,
                        'currency_name': rate.currency_name,
                        'exchange_rate': rate.exchange_rate,
                        'rate_date': rate.rate_date,
                        'rate_type': rate.rate_type,
                        'nominal': rate.nominal
                    })
            all_data.extend(monthly_data)
            logger.info(f"✓ Получено {len(monthly_data)} ежемесячных записей")
    
    if not all_data:
        logger.error("Не получено никаких данных")
        raise Exception("Нет данных для загрузки")
    
    logger.info(f"✓ ВСЕГО ПОЛУЧЕНО: {len(all_data)} записей")
    
    # Загружаем в БД
    loader = DatabaseLoader(DB_CONFIG)
    if not loader.connect():
        raise Exception("Не удалось подключиться к БД")
    
    logger.info("Загрузка в базу данных...")
    count = loader.insert_exchange_rates(all_data)
    loader.connection.commit()
    loader.disconnect()
    
    logger.info(f"✓ ЗАГРУЖЕНО В БД: {count} записей")
    
    # Сохраняем статистику
    context['ti'].xcom_push(key='records_loaded', value=count)
    context['ti'].xcom_push(key='total_records', value=len(all_data))
    
    return count

def generate_load_report(**context):
    """Генерация отчета о загрузке"""
    ti = context['ti']
    
    start_date = ti.xcom_pull(key='start_date', task_ids='validate_dates')
    end_date = ti.xcom_pull(key='end_date', task_ids='validate_dates')
    days_count = ti.xcom_pull(key='days_count', task_ids='validate_dates')
    records_loaded = ti.xcom_pull(key='records_loaded', task_ids='load_historical_data')
    
    logger.info("=" * 60)
    logger.info("ОТЧЕТ О ЗАГРУЗКЕ")
    logger.info("=" * 60)
    logger.info(f"Период: {start_date} - {end_date}")
    logger.info(f"Дней в периоде: {days_count}")
    logger.info(f"Загружено записей: {records_loaded}")
    
    # Дополнительная статистика из БД
    from config import DB_CONFIG
    import psycopg2
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Общая статистика
        cur.execute("SELECT COUNT(*) FROM cbr_raw.exchange_rates")
        total = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT currency_code) FROM cbr_raw.exchange_rates")
        currencies = cur.fetchone()[0]
        
        cur.execute("SELECT MIN(rate_date), MAX(rate_date) FROM cbr_raw.exchange_rates")
        min_date, max_date = cur.fetchone()
        
        logger.info(f"Всего записей в БД: {total}")
        logger.info(f"Уникальных валют: {currencies}")
        logger.info(f"Диапазон дат в БД: {min_date} - {max_date}")
        
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Не удалось получить статистику из БД: {e}")
    
    logger.info("=" * 60)
    logger.info("✅ ИСТОРИЧЕСКАЯ ЗАГРУЗКА ЗАВЕРШЕНА")
    logger.info("=" * 60)
    
    return {
        'period': f"{start_date} to {end_date}",
        'days': days_count,
        'records_loaded': records_loaded
    }

# Создаем задачи
validate_task = PythonOperator(
    task_id='validate_dates',
    python_callable=validate_dates,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_historical_data',
    python_callable=load_historical_data,
    provide_context=True,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_load_report,
    provide_context=True,
    dag=dag,
)

# Определяем порядок
validate_task >> load_task >> report_task