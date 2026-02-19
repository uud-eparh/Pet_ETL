# src/test_connection.py
from loaders.db_loader import DatabaseLoader
from config import DB_CONFIG
import logging
from datetime import date, timedelta
import sys
import os

# Добавляем путь к проекту в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connection():
    """Тест подключения к БД"""
    print("=" * 50)
    print("ТЕСТ ПОДКЛЮЧЕНИЯ К БАЗЕ ДАННЫХ")
    print("=" * 50)
    
    loader = DatabaseLoader(DB_CONFIG)
    
    if loader.connect():
        print("✓ Подключение к БД успешно")
        
        # Проверяем схемы
        loader.cursor.execute("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname LIKE 'cbr_%'")
        schemas = loader.cursor.fetchall()
        print(f"✓ Найденные схемы: {', '.join(s[0] for s in schemas)}")
        
        # Проверяем таблицы
        loader.cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'cbr_raw'")
        tables = loader.cursor.fetchall()
        print(f"✓ Таблицы в cbr_raw: {', '.join(t[0] for t in tables)}")
        
        loader.disconnect()
    else:
        print("✗ Ошибка подключения к БД")

def test_daily_extractor():
    """Тест получения ежедневных данных"""
    try:
        from extractors.daily_extractor import DailyExtractor
        from config import API_CONFIG
        
        print("\n" + "=" * 50)
        print("ТЕСТ ЕЖЕДНЕВНОГО ЭКСТРАКТОРА")
        print("=" * 50)
        
        extractor = DailyExtractor(API_CONFIG['soap_wsdl_url'])
        
        if extractor.connect():
            print("✓ Подключение к SOAP API успешно")
            
            # Тестируем на вчерашнюю дату
            test_date = date.today() - timedelta(days=1)
            print(f"Запрашиваем данные на {test_date}...")
            
            rates = extractor.get_currencies_on_date(test_date)
            
            if rates:
                print(f"✓ Получено {len(rates)} записей")
                print("\nПримеры валют:")
                for i, rate in enumerate(rates[:5]):
                    print(f"  {i+1}. {rate['currency_code']}: {rate['exchange_rate']}")
            else:
                print("✗ Нет данных")
        else:
            print("✗ Ошибка подключения к SOAP API")
    except Exception as e:
        print(f"✗ Ошибка в daily_extractor: {e}")
        import traceback
        traceback.print_exc()

def test_monthly_extractor():
    """Тест получения ежемесячных данных"""
    try:
        from extractors.monthly_extractor import MonthlyExtractor
        from config import API_CONFIG
        
        print("\n" + "=" * 50)
        print("ТЕСТ ЕЖЕМЕСЯЧНОГО ЭКСТРАКТОРА")
        print("=" * 50)
        
        extractor = MonthlyExtractor(API_CONFIG['rest_base_url'])
        
        rates = extractor.get_monthly_rates(
            publication_id=API_CONFIG['monthly_publication_id'],
            dataset_id=API_CONFIG['monthly_dataset_id'],
            start_year=2024,
            end_year=2024
        )
        
        if rates:
            print(f"✓ Получено {len(rates)} записей")
            print("\nПримеры валют:")
            for i, rate in enumerate(rates[:5]):
                print(f"  {i+1}. {rate.currency_code}: {rate.exchange_rate}")
        else:
            print("✗ Нет данных")
    except Exception as e:
        print(f"✗ Ошибка в monthly_extractor: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_connection()
    test_daily_extractor()
    test_monthly_extractor()
