"""
DAG для регулярного обновления витрин данных (materialized views) в схеме cbr_dm
Запускается после загрузки новых данных
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'cbr_refresh_marts',
    default_args=default_args,
    description='Обновление витрин данных cbr_dm',
    schedule_interval='30 18 * * *',  # Каждый день в 18:30 (после загрузки)
    catchup=False,
    tags=['cbr', 'marts', 'dm'],
)

# SQL для обновления всех материализованных представлений в cbr_dm
REFRESH_ALL_VIEWS_SQL = """
-- Обновляем все материализованные представления конкурентно
REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_daily_changes;
REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_weekly_stats;
REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_monthly_stats;
REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_quarterly_stats;
REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_top_volatile;
REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_correlation_with_usd;
REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.mv_load_stats;

-- Логируем обновление
INSERT INTO cbr_raw.load_logs (log_message, log_time) 
VALUES ('Материализованные представления cbr_dm обновлены', NOW());
"""

# Создаем таблицу для логов, если её нет
CREATE_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cbr_raw.load_logs (
    id SERIAL PRIMARY KEY,
    log_message TEXT,
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def check_marts_status(**context):
    """Проверка статуса витрин в cbr_dm"""
    hook = PostgresHook(postgres_conn_id='postgres_cbr')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    logger.info("=" * 60)
    logger.info("ПРОВЕРКА ВИТРИН ДАННЫХ (cbr_dm)")
    logger.info("=" * 60)
    
    # Проверяем наличие всех представлений
    views_to_check = [
        'mv_daily_changes',
        'mv_weekly_stats', 
        'mv_monthly_stats',
        'mv_quarterly_stats',
        'mv_top_volatile',
        'mv_correlation_with_usd',
        'mv_load_stats'
    ]
    
    for view in views_to_check:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM pg_matviews 
                WHERE schemaname = 'cbr_dm' 
                AND matviewname = %s
            )
        """, (view,))
        exists = cur.fetchone()[0]
        
        if exists:
            # Получаем количество записей
            cur.execute(f"SELECT COUNT(*) FROM cbr_dm.{view}")
            count = cur.fetchone()[0]
            logger.info(f"✅ cbr_dm.{view}: {count} записей")
        else:
            logger.warning(f"⚠ cbr_dm.{view} не существует")
    
    cur.close()
    conn.close()
    
    return "Проверка завершена"

def refresh_single_view(view_name, **context):
    """Обновление одного материализованного представления в cbr_dm"""
    hook = PostgresHook(postgres_conn_id='postgres_cbr')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    logger.info(f"Обновление cbr_dm.{view_name}...")
    
    try:
        cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY cbr_dm.{view_name}")
        conn.commit()
        
        # Получаем количество записей после обновления
        cur.execute(f"SELECT COUNT(*) FROM cbr_dm.{view_name}")
        count = cur.fetchone()[0]
        
        logger.info(f"✅ cbr_dm.{view_name} обновлен: {count} записей")
        
        context['ti'].xcom_push(key=f'{view_name}_count', value=count)
        
    except Exception as e:
        logger.error(f"Ошибка при обновлении cbr_dm.{view_name}: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    
    return count

# Создаем задачи

create_log_table = PostgresOperator(
    task_id='create_log_table',
    postgres_conn_id='postgres_cbr',
    sql=CREATE_LOG_TABLE_SQL,
    dag=dag,
)

check_status = PythonOperator(
    task_id='check_marts_status',
    python_callable=check_marts_status,
    dag=dag,
)

# Задачи для обновления каждого представления отдельно
refresh_daily_changes = PythonOperator(
    task_id='refresh_daily_changes',
    python_callable=refresh_single_view,
    op_kwargs={'view_name': 'mv_daily_changes'},
    dag=dag,
)

refresh_weekly_stats = PythonOperator(
    task_id='refresh_weekly_stats',
    python_callable=refresh_single_view,
    op_kwargs={'view_name': 'mv_weekly_stats'},
    dag=dag,
)

refresh_monthly_stats = PythonOperator(
    task_id='refresh_monthly_stats',
    python_callable=refresh_single_view,
    op_kwargs={'view_name': 'mv_monthly_stats'},
    dag=dag,
)

refresh_quarterly_stats = PythonOperator(
    task_id='refresh_quarterly_stats',
    python_callable=refresh_single_view,
    op_kwargs={'view_name': 'mv_quarterly_stats'},
    dag=dag,
)

refresh_top_volatile = PythonOperator(
    task_id='refresh_top_volatile',
    python_callable=refresh_single_view,
    op_kwargs={'view_name': 'mv_top_volatile'},
    dag=dag,
)

refresh_correlation = PythonOperator(
    task_id='refresh_correlation',
    python_callable=refresh_single_view,
    op_kwargs={'view_name': 'mv_correlation_with_usd'},
    dag=dag,
)

refresh_load_stats = PythonOperator(
    task_id='refresh_load_stats',
    python_callable=refresh_single_view,
    op_kwargs={'view_name': 'mv_load_stats'},
    dag=dag,
)

def generate_marts_report(**context):
    """Генерация отчета о состоянии витрин cbr_dm"""
    ti = context['ti']
    
    logger.info("=" * 60)
    logger.info("ОТЧЕТ О СОСТОЯНИИ ВИТРИН cbr_dm")
    logger.info("=" * 60)
    
    views = [
        'mv_daily_changes',
        'mv_weekly_stats',
        'mv_monthly_stats',
        'mv_quarterly_stats',
        'mv_top_volatile',
        'mv_correlation_with_usd',
        'mv_load_stats'
    ]
    
    total_records = 0
    
    for view in views:
        count = ti.xcom_pull(task_ids=f'refresh_{view}', key=f'{view}_count')
        if count:
            logger.info(f"📊 cbr_dm.{view}: {count} записей")
            total_records += count
    
    logger.info("-" * 40)
    logger.info(f"✅ ВСЕГО ЗАПИСЕЙ В ВИТРИНАХ cbr_dm: {total_records}")
    logger.info("=" * 60)
    
    return total_records

report_task = PythonOperator(
    task_id='generate_marts_report',
    python_callable=generate_marts_report,
    dag=dag,
)

# Порядок выполнения
create_log_table >> check_status >> [
    refresh_daily_changes,
    refresh_weekly_stats,
    refresh_monthly_stats,
    refresh_quarterly_stats,
    refresh_top_volatile,
    refresh_correlation,
    refresh_load_stats
] >> report_task