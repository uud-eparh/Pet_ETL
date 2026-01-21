"""
Главный скрипт ETL-пайплайна для сбора курсов валют ЦБ РФ

Архитектура:
1. Extract: daily_extractor + monthly_extractor
2. Transform: Очистка и валидация данных
3. Load: db_loader с транзакционной безопасностью
"""

import argparse
from ast import arguments
from random import sample
import sys
import os
from datetime import date, datetime, timedelta
import logging
from pathlib import Path
from urllib import request
from venv import logger

def setup_logging():
    """Настройка логирования ДО импорта модулей"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    log_format = '%(asctime)s - %(name)20s - %(levelname)8s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Очищаем все существующие обработчики
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Настраиваем корневой логгер
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                log_dir / f"etl_{date.today().strftime('%Y%m%d')}.log",
                encoding='utf-8',
                mode='w'  # 'w' - перезапись, 'a' - дополнение
            )
        ],
        force=True  # ВАЖНО! Перезаписывает существующие настройки
    )
    
    # Уменьшаем логи от внешних библиотек
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('zeep').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

# Настраиваем логирование
logger = setup_logging()

# Добавляем корень проекта в путь Python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Импортируем наши модули
from src.extractors.daily_extractor import DailyExtractor
from src.extractors.monthly_extractor import MonthlyExtractor
from src.extractors.historical_extractor import HistoricalExtractor
from src.loaders.db_loader import DatabaseLoader
from config import DB_CONFIG, API_CONFIG

def parse_arguments():
    """
    Парсинг аргументов командной строки
    """
    parser = argparse.ArgumentParser(
        description='ETL-пайплайн для курсов валют ЦБ РФ',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python main.py                         # Загрузка текущих данных
  python main.py --mode historical --start 2024-01-01 --end 2024-01-31
  python main.py --mode daily-only       # Только ежедневные данные
  python main.py --mode monthly-only     # Только ежемесячные данные
  python main.py --historical-last-days 30  # Последние 30 дней
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['full', 'daily-only', 'monthly-only', 'historical'],
        default='full',
        help='Режим работы пайплайна (по умолчанию: full)'
    )
    
    parser.add_argument(
        '--start-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help='Начальная дата для исторической загрузки (формат: YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--end-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help='Конечная дата для исторической загрузки (формат: YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--historical-last-days',
        type=int,
        help='Загрузить данные за последние N дней'
    )
    
    parser.add_argument(
        '--skip-weekends',
        action='store_true',
        default=True,
        help='Пропускать выходные дни (по умолчанию: True)'
    )
    
    parser.add_argument(
        '--target-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help='Конкретная дата для загрузки ежедневных данных'
    )
    
    return parser.parse_args()


class ETLPipline:
    """
    Главный класс ETL-пайплайна
    
    Координирует работу всех компонентов:
    1. Извлечение данных из источников
    2. Преобразование и валидация
    3. Загрузка в базу данных
    """
    def __init__(self):
        """Инициализация ETL-пайплайна"""
        self.logger = logging.getLogger(__name__)

        # Инициализируем компоненты
        self.daily_extractor = DailyExtractor(API_CONFIG['soap_wsdl_url'])
        self.monthly_extractor = MonthlyExtractor(API_CONFIG['rest_base_url'])
        self.db_loader = DatabaseLoader(DB_CONFIG)

        self.logger.info("=" * 60)
        self.logger.info("ИНИЦИАЛИЗАЦИЯ ETL-ПАЙПЛАЙНА")
        self.logger.info("=" * 60)

    def run(self, args):
        """
        Запуск полного ETL-процесса
        
        Args:
            load_daily: Загружать ли ежедневные данные
            load_monthly: Загружать ли ежемесячные данные
            target_date: Конкретная дата для загрузки (по умолчанию сегодня/вчера)
        """
        try:
            self.logger.info("🚀 ЗАПУСК ETL-ПАЙПЛАЙНА")
            self.logger.info(f"Режим: {args.mode}")

            # ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ
            if not self._connect_to_database():
                return False
            
            # Выбираем режим работы
            if args.mode == 'historical':
                success = self._run_historical_mode(args)
            elif args.mode == 'daily-only':
                success = self._run_daily_only_mode(args)
            elif args.mode == 'monthly-only':
                success = self._run_monthly_only_mode(args)
            else:  # full
                success = self._run_full_mode(args)

            # Генерация отчета
            if success:
                self._generate_report()
            
            return success
        
        except Exception as e:
            self.logger.error(f"Критическая ошибка в ETL-пайплайне: {e}", exc_info=True)
            return False
        
        finally: # Закрываем соединение с БД
            if hasattr(self, 'db_loader') and self.db_loader.connection:
                self.db_loader.disconnect()


    def _connect_to_database(self):
        """Подключение к базе данных"""
        self.logger.info("1. ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ")

        if not self.db_loader.connect():
            self.logger.error("✗ Не удалось подключиться к БД")
            return False

        # Создаем таблицу если не существуе
        if not self.db_loader.create_table_if_not_exists():
            self.logger.error("✗ Не удалось создать или проверить таблицу")
            return False
        
        self.logger.info("✓ Подключение к БД успешно")
        return True
    
    def _extract_daily_data(self, target_date=None):
        """
        Извлечение ежедневных данных
        
        Args:
            target_date: Дата для получения данных (по умолчанию вчера)
        
        Returns:
            Список словарей с ежедневными курсами или None
        """
        self.logger.info("2. ИЗВЛЕЧЕНИЕ ЕЖЕДНЕВНЫХ ДАННЫХ (SOAP API)")

        # Определяем дату (обычно берем вчерашнюю, т.к. сегодняшние данные могут быть не готовы)
        if target_date is None:
            target_date = date.today() - timedelta(days=1)

        self.logger.info(f"  Дата запроса: {target_date}")

        # Подключаемся к SOAP API
        if not self.daily_extractor.connect():
            self.logger.error("✗ Не удалось подключиться к SOAP API")
            return None
        
        # Получаем данные
        daily_data = self.daily_extractor.get_currencies_on_date(target_date)

        if not daily_data:
            self.logger.error(f"✗ Не удалось получить ежедневные данные на {target_date}")

            # Пробуем предыдущий рабочий день
            prev_date = target_date - timedelta(days=1)
            self.logger.info(self.logger.info(f"  Пробуем предыдущую дату: {prev_date}"))
            daily_data = self.daily_extractor.get_currencies_on_date(prev_date)

        if daily_data:
            self.logger.info(f"✓ Получено {len(daily_data)} ежедневных записей")

            # Логируем примеры данных
            sample_size = min(3, len(daily_data))
            self.logger.info(f"  Примеры валют ({sample_size} из {len(daily_data)}):")
            for i, rate in enumerate(daily_data[:sample_size]):
                self.logger.info(f"    - {rate['currency_code']}: {rate['exchange_rate']}")

            return daily_data
        
        else:
            self.logger.error("✗ Не удалось получить ежедневные данные")
            return None
        
    def _extract_monthly_data(self):
        """
        Извлечение ежемесячных данных
        
        Returns:
            Список объектов MonthlyRate или None
        """
        self.logger.info("3. ИЗВЛЕЧЕНИЕ ЕЖЕМЕСЯЧНЫХ ДАННЫХ (REST API)")

        # Получаем текущий год
        current_year = date.today().year

        self.logger.info(f"  Год запроса: {current_year}")
        self.logger.info(f"  Publication ID: {API_CONFIG['monthly_publication_id']}")
        self.logger.info(f"  Dataset ID: {API_CONFIG['monthly_dataset_id']}")

        # Получаем данные
        monthly_rates = self.monthly_extractor.get_monthly_rates(
            publication_id=API_CONFIG['monthly_publication_id'],
            dataset_id=API_CONFIG['monthly_dataset_id'],
            start_year=current_year - 1, # Прошлый год + текущий
            end_year=current_year
        )

        if not monthly_rates:
            self.logger.error("✗ Не удалось получить ежемесячные данные")
            return None
        
        self.logger.info(f"✓ Получено {len(monthly_rates)} ежемесячных записей")

        monthly_data = []
        for rate in monthly_rates:
            monthly_data.append({
                'currency_code': rate.currency_code,
                'currency_name': rate.currency_name,
                'exchange_rate': rate.exchange_rate,
                'rate_date': rate.rate_date,
                'rate_type': rate.rate_type,
                'nominal': rate.nominal                
            })

        # Логируем статистику
        unique_currencies = set(r.currency_code for r in monthly_rates)
        unique_dates = set(r.rate_date for r in monthly_rates)

        self.logger.info(f"  Уникальных валют: {len(unique_currencies)}")
        self.logger.info(f"  Уникальных дат: {len(unique_dates)}")

        return monthly_data
    
    def _load_to_database(self, daily_data, monthly_data):
        """
        Загрузка данных в базу данных
        
        Args:
            daily_data: Ежедневные данные
            monthly_data: Ежемесячные данные
            
        Returns:
            bool: True если загрузка успешна
        """
        self.logger.info("4. ЗАГРУЗКА ДАННЫХ В БАЗУ")

        total_loaded = 0

        try:
            # Загружаем ежедневные данные
            if daily_data:
                self.logger.info("  Загрузка ежедневных данных...")
                daily_count = self.db_loader.insert_exchange_rates(daily_data)
                total_loaded += daily_count
                self.logger.info(f"  ✓ Ежедневных: {daily_count} записей")
            else:
                self.logger.info("  ⚠ Ежедневных данных нет, пропускаем")

            # Загружаем ежемесячные данные
            if monthly_data:
                self.logger.info("  Загрузка ежемесячных данных...")
                monthly_count = self.db_loader.insert_exchange_rates(monthly_data)
                total_loaded += monthly_count
                self.logger.info(f"  ✓ Ежемесячных: {monthly_count} записей")
            else:
                self.logger.info("  ⚠ Ежемесячных данных нет, пропускаем")

            # Финализируем транзакцию
            self.db_loader.connection.commit()

            self.logger.info(f"✓ Всего загружено: {total_loaded} записей")
            return True
        
        except Exception as e:
            self.logger.error(f"✗ Ошибка при загрузке в БД: {e}", exc_info=True)
             # Если ошибка. Откатываемся. Не записываем в базу данных
            self.db_loader.connection.rollback()
            return False
        
    def _generate_report(self):
        """Генерация отчета о выполненной загрузке"""
        self.logger.info("5. ФОРМИРОВАНИЕ ОТЧЕТА")

        try:
            # Получаем статистику из БД
            stats = self.db_loader.get_record_count()

            self.logger.info("=" * 60)
            self.logger.info("ОТЧЕТ О ЗАГРУЗКЕ ДАННЫХ")
            self.logger.info("=" * 60)

            total_records = 0

            for rate_type, data in stats.items():
                self.logger.info(f"{rate_type.upper()}:")
                self.logger.info(f"  Количество записей: {data['count']}")
                self.logger.info(f"  Диапазон дат: {data['date_range']}")
                self.logger.info(f"  Уникальных валют: {data['currencies']}")
                total_records += data['count']

            self.logger.info("-" * 40)
            self.logger.info(f"ВСЕГО ЗАПИСЕЙ В БАЗЕ: {total_records}")
            self.logger.info("=" * 40)

            # Сохраняем отчет в файл
            self._save_report_to_file(stats,total_records)

        except Exception as e:
            self.logger.warning(f"Не удалось сформировать отчет: {e}")

    def _save_report_to_file(self, stats, total_records):
        """Сохранение отчета в файл"""
        report_dir = Path("reports")
        report_dir.mkdir(exist_ok=True)

        report_file = report_dir / f"etl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("=" * 50 + "\n")
            f.write("ОТЧЕТ ETL-ПАЙПЛАЙНА\n")
            f.write(f"Время генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 50 + "\n\n")

            f.write("СТАТИСТИКА БАЗЫ ДАННЫХ:\n")
            f.write("-" * 30 + "\n")

            for rate_type, data in stats.items():
                f.write(f"{rate_type.upper()}:\n")
                f.write(f"  Записей: {data['count']}\n")
                f.write(f"  Дата: {data['date_range']}\n")
                f.write(f"  Валют: {data['currencies']}\n\n")

            f.write(f"ВСЕГО: {total_records} записей\n")
            f.write("=" * 50 + "\n")

        self.logger.info(self.logger.info(f"✓ Отчет сохранен: {report_file}"))


    def _run_historical_mode(self, args):
        """
        Режим исторической загрузки
        """
        self.logger.info("📜 РЕЖИМ: ИСТОРИЧЕСКАЯ ЗАГРУЗКА")

        # Определяем период загрузки
        start_date, end_date = self._determine_historical_period(args)

        if not start_date or not end_date:
            self.logger.error("Не указан период для исторической загрузки")
            return False
        
        self.logger.info(f"Период загрузки: {start_date} - {end_date}")

        # Загружаем исторические данные
        historical_data = self._load_historical_data(start_date, end_date, args.skip_weekends)

        if not historical_data:
            self.logger.info("Не удалось загрузить исторические данные")
            return False
        
        # Загружаем в БД
        return self._load_to_database(historical_data, monthly_data=None)
    
    def _determine_historical_period(self, args):
        """
        Определение периода для исторической загрузки
        """

        # Вариант 1: Последние N дней
        if args.historical_last_days:
            end_date = date.today() - timedelta(days=1) # Вчера
            start_date = end_date - timedelta(days=args.historical_last_days - 1)
            return start_date, end_date
        
        # Вариант 2: Конкретный диапазон
        if args.start_date and args.end_date:
            return args.start_date, args.end_date
        
        # Вариант 3: По умолчанию - последние 7 дней
        self.logger.warning("Период не указан, используем последние 7 дней")
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=6)
        return start_date, end_date
    

    def _load_historical_data(self, start_date, end_date, skip_weekends):
        """
        Загрузка исторических данных за период
        """
        self.logger.info(f"Загрузка исторических данных с {start_date} по {end_date}")

        # Создаем исторический экстрактор
        historical_extractor = HistoricalExtractor(API_CONFIG['soap_wsdl_url'])

        # Получаем данные
        historical_data = historical_extractor.get_historical_data_flattened(
            start_date=start_date,
            end_date=end_date,
            skip_weekends=skip_weekends
        )

        if historical_data:
            self.logger.info(f"✓ Получено {len(historical_data)} исторических записей")

            # Логируем статистику
            unique_dates = set(r['rate_date'] for r in historical_data)
            unique_currencies = set(r['currency_code'] for r in historical_data)
            
            self.logger.info(f"  Уникальных дат: {len(unique_dates)}")
            self.logger.info(f"  Уникальных валют: {len(unique_currencies)}")
            self.logger.info(f"  Диапазон дат: {min(unique_dates)} - {max(unique_dates)}")
        
        return historical_data

    def _run_daily_only_mode(self, args):
        """
        Режим только ежедневных данных
        """
        self.logger.info("🌞 РЕЖИМ: ТОЛЬКО ЕЖЕДНЕВНЫЕ ДАННЫЕ")

        daily_data = self._extract_daily_data(args.target_date)
        if not daily_data:
            return False
        
        return self._load_to_database(daily_data, monthly_data=None)
    
    def _run_monthly_only_mode(self, args):
        """
        Режим только ежемесячных данных
        """
        self.logger.info("📅 РЕЖИМ: ТОЛЬКО ЕЖЕМЕСЯЧНЫЕ ДАННЫЕ")

        monthly_data = self._extract_monthly_data()
        if not monthly_data:
            return False
        
        return self._load_to_database(daily_data=None, monthly_data=monthly_data)

    def _run_full_mode(self, args):
        """
        Полный режим (по умолчанию)
        """
        self.logger.info("⚡ РЕЖИМ: ПОЛНЫЙ (ЕЖЕДНЕВНЫЕ + ЕЖЕМЕСЯЧНЫЕ)")

        daily_data = self._extract_daily_data(args.target_date)
        monthly_data = self._extract_monthly_data()

        # Если нет данных вообще
        if not daily_data and not monthly_data:
            self.logger.error("Не удалось получить никакие данные")
            return False
        
        return self._load_to_database(daily_data, monthly_data)
            

def main():
    """
    Главная функция запуска ETL-пайплайна.
    C поддержкой аргументов командной строки
    """

    # Настройка логирования
    logger = setup_logging()

    # Парсинг аргументов
    args = parse_arguments()

    print("=" * 60)
    print("ETL-ПАЙПЛАЙН ДЛЯ КУРСОВ ВАЛЮТ ЦБ РФ")
    print("=" * 60)
    print(f"Дата запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Режим работы: {args.mode}")

    if args.mode == 'historical':
        if args.historical_last_days:
            print(f"Историческая загрузка: последние {args.historical_last_days} дней")
        elif args.start_date and args.end_date:
            print(f"Историческая загрузка: {args.start_date} - {args.end_date}")

    print()

    try:
        # Создаем и запускае пайплайн
        pipeline = ETLPipline()
        success = pipeline.run(args)

        if success:
            print("\n" + "=" * 60)
            print("✅ ETL-ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН!")
            print("=" * 60)
            return 0
        else:
            print("\n" + "=" * 60)
            print("❌ ETL-ПАЙПЛАЙН ЗАВЕРШИЛСЯ С ОШИБКАМИ")
            print("=" * 60)
            return 1
        
    except KeyboardInterrupt:
        print("\n\n⏹ Пайплайн прерван пользователем")
        return 130
    except Exception as e:
        logger.error(f"Необработанная ошибка: {e}", exc_info=True)
        print(f"\n❌ Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    # Запуск пайплайна
    exit_code = main()
    sys.exit(exit_code)
