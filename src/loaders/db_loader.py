"""
Модуль для загрузки данных о курсах валют в PostgreSQL

Основные функции:
1. Подключение к базе данных
2. Создание таблиц (если не существуют)
3. Вставка данных с обработкой дубликатов
4. Логирование процесса загрузки
"""

from locale import currency
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from typing import List, Dict, Any, Optional
import logging
from datetime import date, datetime

# Настройка логирования
logger = logging.getLogger(__name__)

class DatabaseLoader:
    """
    Класс для работы с базой данных PostgreSQL.
    
    Отвечает за:
    1. Установление соединения с БД
    2. Создание схемы данных
    3. Загрузку данных из разных источников
    4. Обработку ошибок и транзакций
    """

    def __init__(self, db_config: Dict[str, Any]) -> None:
        """
        Инициализация загрузчика БД.
        Args:
            db_config: Конфигурация подключения к БД
                       Должна содержать: host, port, database, user, password
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None

        logger.info(f"Инициализирован DatabaseLoader для {db_config['host']}:{db_config['port']}")

    def connect(self) -> bool:
        """
        Установление соединения с базой данных PostgreSQL.
        Returns:
            bool: True если подключение успешно, False в случае ошибки
        """
        try:
            logger.info(f"Подключаемся к БД {self.db_config['database']} "
                       f"на {self.db_config['host']}:{self.db_config['port']}")
            
            # Устанавливаем соединение с БД
            self.connection = psycopg2.connect(**self.db_config)

            # Автоматически фиксируем изменения после каждого запроса
            self.connection.autocommit = False

            # Создаем курсор для выполнения SQL-запросов
            self.cursor = self.connection.cursor()

            # Тестируем подключение простым запросом
            self.cursor.execute("SELECT 1")
            test_result = self.cursor.fetchone()

            if test_result and test_result[0] == 1:
                logger.info("✓ Подключение к БД успешно установлено")
                return True
            else:
                logger.error("✗ Тестовый запрос не вернул ожидаемый результат")
                return False
            
        except psycopg2.OperationalError as e:
            logger.error(f"Ошибка подключения к БД (сеть/доступ): {e}")
            logger.error("Проверьте:")
            logger.error("  1. Запущен ли Docker-контейнер?")
            logger.error("  2. Правильный ли порт? (должен быть 6432)")
            logger.error("  3. Доступен ли localhost:6432?")
            return False
        except psycopg2.InterfaceError as e:
            logger.error(f"Ошибка параметров подключения: {e}")
            logger.error("Проверьте config.py: host, port, database, user, password")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при подключении: {e}")
            return False

    def disconnect(self):
        """
        Корректное отключение от базы данных.
        
        Важно всегда закрывать соединение для освобождения ресурсов.
        """
        if self.cursor:
            try:
                self.cursor.close()
                logger.debug("Курсор закрыт")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии курсора: {e}")

        if self.connection:
            try:
                self.connection.close()
                logger.debug("Соединение с БД закрыто")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии соединения: {e}")

        self.cursor = None
        self.connection = None

    def create_table_if_not_exists(self):
        """
        Создание таблицы для хранения курсов валют, если она не существует.
        
        Особенности таблицы:
        1. UNIQUE constraint для UPSERT операций
        2. Индексы для ускорения поиска
        3. CHECK constraint для валидации rate_type
        """
        create_table_sql = """
        -- Основная таблица для курсов валют
        CREATE TABLE IF NOT EXISTS exchange_rates (
            id SERIAL PRIMARY KEY,
            
            -- Данные о валюте
            currency_code VARCHAR(3) NOT NULL,
            currency_name VARCHAR(100) NOT NULL,
            exchange_rate DECIMAL(12,6) NOT NULL,
            
            -- Временные метки
            rate_date DATE NOT NULL,
            rate_type VARCHAR(10) NOT NULL,
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Дополнительные поля
            nominal INTEGER DEFAULT 1,
            
            -- Ограничения
            CONSTRAINT valid_rate_type CHECK (rate_type IN ('daily', 'monthly')),
            CONSTRAINT valid_currency_code CHECK (currency_code ~ '^[A-Z]{3}$'),
            CONSTRAINT positive_exchange_rate CHECK (exchange_rate > 0),
            
            -- Уникальный ключ для UPSERT операций
            -- Одна валюта, одна дата, один тип курса = уникальная запись
            UNIQUE(currency_code, rate_date, rate_type)
        );
        
        -- Комментарии к таблице и полям
        COMMENT ON TABLE exchange_rates IS 'Хранение курсов валют ЦБ РФ (ежедневные и ежемесячные)';
        COMMENT ON COLUMN exchange_rates.rate_type IS 'Тип курса: daily - ежедневный, monthly - среднемесячный';
        COMMENT ON COLUMN exchange_rates.nominal IS 'Номинал валюты (например, 100 для японских йен)';
        """
        
        # SQL для создания индексов (отдельно, чтобы не было ошибки если таблица уже есть)
        create_indexes_sql = """
        -- Индекс для быстрого поиска по дате
        CREATE INDEX IF NOT EXISTS idx_exchange_rates_date 
        ON exchange_rates(rate_date);
        
        -- Индекс для поиска по валюте
        CREATE INDEX IF NOT EXISTS idx_exchange_rates_currency 
        ON exchange_rates(currency_code);
        
        -- Составной индекс для часто используемых запросов
        CREATE INDEX IF NOT EXISTS idx_exchange_rates_currency_date 
        ON exchange_rates(currency_code, rate_date DESC);
        
        -- Индекс для фильтрации по типу курса
        CREATE INDEX IF NOT EXISTS idx_exchange_rates_type 
        ON exchange_rates(rate_type);
        """
        
        try:
            logger.info("Создаем таблицу exchange_rates (если не существует)...")
            
            # Создаем таблицу
            self.cursor.execute(create_table_sql)
            
            # Создаем индексы
            self.cursor.execute(create_indexes_sql)
            
            # Фиксируем изменения
            self.connection.commit()
            
            logger.info("✓ Таблица exchange_rates создана/проверена")
            logger.info("  - UNIQUE constraint: (currency_code, rate_date, rate_type)")
            logger.info("  - CHECK constraint: rate_type IN ('daily', 'monthly')")
            logger.info("  - Созданы индексы для оптимизации запросов")
            
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Ошибка при создании таблицы: {e}")
            self.connection.rollback()
            return False
        
    def _prepare_batch_data(self, rates_data: list[dict]) -> list[tuple]:
        """
        Подготовка данных для пакетной вставки в БД.
        
        Преобразует список словарей в список кортежей в правильном порядке
        для SQL запроса.
        
        Args:
            rates_data: Список словарей с данными о курсах валют
            
        Returns:
            Список кортежей в формате для execute_batch
        """
        prepared_records = []

        for rate in rates_data:
            try:
                # Извлекаем данные с проверкой наличия ключей
                currency_code = rate.get('currency_code', '').strip()
                currency_name = rate.get('currency_name', '').strip()

                # Проверяем обязательные поля
                if not currency_code or not currency_name:
                    logger.warning(f"Пропускаем запись с пустым кодом или названием валюты: {rate}")
                    continue

                # Преобразуем обменный курс
                try:
                    exchange_rate = float(rate.get('exchange_rate', 0))
                    if exchange_rate <= 0:
                        logger.warning(f"Некорректный курс {exchange_rate} для {currency_code}")
                        continue
                except (ValueError, TypeError):
                    logger.warning(f"Не удалось преобразовать курс для {currency_code}: {rate.get('exchange_rate')}")
                    continue

                # Преобразуем дату
                rate_date = rate.get('rate_date')
                if isinstance(rate_date, str):
                    # Если дата пришла как строка
                    from datetime import datetime
                    try:
                        rate_date = datetime.strptime(rate_date, '%Y-%m-%d').date()
                    except ValueError:
                        logger.warning(f"Неверный формат даты: {rate_date}")
                        continue

                elif not isinstance(rate_date, date):
                    logger.warning(f"Некорректный тип даты: {type(rate_date)}")
                    continue

                # Определяем тип курса (daily/monthly)
                rate_type = rate.get('rate_type', 'daily').lower()
                if rate_type not in ['daily', 'monthly']:
                    logger.warning(f"Некорректный rate_type: {rate_type}. Используем 'daily'")
                    rate_type = 'daily'

                # Получаем номинал
                nominal = int(rate.get('nominal', 1))
                if nominal <= 0:
                    logger.warning(f"Некорректный номинал {nominal} для {currency_code}")
                    continue

                # Создаем кортеж в правильном порядке для SQL
                record_tuple = (
                    currency_code,
                    currency_name,
                    exchange_rate,
                    rate_date,
                    rate_type,
                    nominal
                )

                prepared_records.append(record_tuple)

            except Exception as e:
                logger.warning(f"Ошибка при подготовке записи {rate}: {e}")
                continue

        logger.info(f"Подготовлено {len(prepared_records)} записей для вставки в БД")

        # Логируем пример данных для отладки
        if prepared_records and logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Пример подготовленных данных (первые 3):")
            for i, record in enumerate(prepared_records[:3]):
                logger.debug(f"  {i+1}. {record}")

        return prepared_records
    
    def insert_exchange_rates(self, rates_data: List[Dict]) -> int:
        """
        Вставка курсов валют в базу данных с использованием UPSERT.
        
        Args:
            rates_data: Список словарей с данными о курсах валют
            
        Returns:
            int: Количество успешно обработанных записей
        """
        if not rates_data:
            logger.warning("Пустой список данных для вставки")
            return 0
        
        logger.info(f"Начинаем вставку {len(rates_data)} записей...")

        # Подготавливаем данные
        prepared_data = self._prepare_batch_data(rates_data)

        if not prepared_data:
            logger.warning("Нет валидных данных для вставки после подготовки")
            return 0
        
        # ИСПРАВЛЕНИЕ: Добавляем схему cbr_raw к таблице
        insert_sql = """
        INSERT INTO cbr_raw.exchange_rates 
            (currency_code, currency_name, exchange_rate, rate_date, rate_type, nominal)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (currency_code, rate_date, rate_type) 
        DO UPDATE SET
            exchange_rate = EXCLUDED.exchange_rate,
            currency_name = EXCLUDED.currency_name,
            nominal = EXCLUDED.nominal,
            load_timestamp = CURRENT_TIMESTAMP
        RETURNING id;
        """

        inserted_count = 0

        try:
            # ВАЖНО: Начинаем транзакцию явно
            self.cursor.execute("BEGIN;")

            # Используем execute_batch для эффективной пакетной вставки
            execute_batch(
                self.cursor,
                insert_sql,
                prepared_data,
                page_size=100
            )

            # Получаем количество обработанных строк
            affected_rows = self.cursor.rowcount

            # Фиксируем транзакцию
            self.connection.commit()

            inserted_count = affected_rows
            logger.info(f"✓ UPSERT завершен: обработано {inserted_count} записей")

            # Дополнительная статистика
            self._log_insertion_statistics(prepared_data)

            return inserted_count
        
        except Exception as e:
            logger.error(f"Неожиданная ошибка при вставке: {e}", exc_info=True)
            self.connection.rollback()
            return 0
        
    def _log_insertion_statistics(self, prepared_data: List[tuple]):
        """
        Логирование статистики по вставленным данным
        
        Args:
            prepared_data: Подготовленные данные для вставки
        """
        if not prepared_data:
            return
        
        # Анализируем данные
        currencies = set(record[0] for record in prepared_data) # currency_code
        dates = set(record[3] for record in prepared_data) # rate_date
        rate_types = set(record[4] for record in prepared_data) # rate_type

        logger.info("Статистика загрузки:")
        logger.info(f"  Уникальных валют: {len(currencies)}")
        logger.info(f"  Уникальных дат: {len(dates)}")
        logger.info(f"  Типы курсов: {', '.join(rate_types)}")

        # Пример первых 3 валют
        if currencies:
            sample_currencies = list(currencies)[:3]
            logger.info(f"  Пример валют: {', '.join(sample_currencies)}")

        if dates:
            min_date = min(dates)
            max_date = max(dates)
            logger.info(f"  Диапазон дат: {min_date} - {max_date}")

    def _insert_one_by_one(self, prepared_data: List[tuple], insert_sql: str) -> int:
        """
        Резервный метод: вставка данных по одной записи.
        Используется при ошибках пакетной вставки.
        
        Args:
            prepared_data: Подготовленные данные
            insert_sql: SQL запрос для вставки
            
        Returns:
            Количество успешно вставленных записей        
        """
        logger.info("Пробуем вставить данные по одной записи...")

        success_count = 0
        error_count = 0

        for i, record in enumerate(prepared_data):
            try:
                self.cursor.execute("BEGIN;")
                self.cursor.execute(insert_sql, record)
                self.connection.commit()
                success_count += 1

                if (i + 1) % 10 == 0: # Показываем каждые 10 записей
                    logger.debug(f"  Прогресс: {i + 1}/{len(prepared_data)}")

            except Exception as e:
                self.connection.rollback()
                error_count += 1
                logger.warning(f"  Ошибка при вставке записи {i + 1}: {e}")
                continue

        logger.info(f"Поштучная вставка завершена: {success_count} успешно, {error_count} с ошибками")

        return success_count
    
    def get_existing_dates(self, rate_type: str = None) -> List[date]:
        """
        Получение списка дат, которые уже есть в БД
        
        Args:
            rate_type: Фильтр по типу курса ('daily', 'monthly') или None для всех
            
        Returns:
            Список объектов date
        """

        try:
            if rate_type:
                query = "SELECT DISTINCT rate_date FROM exchange_rates WHERE rate_type = %s ORDER BY rate_date"
                self.cursor.execute(query, (rate_type,))

            else:
                query = "SELECT DISTINCT rate_date FROM exchange_rates ORDER BY rate_date"
            
            dates = [row[0] for row in self.cursor.fetchall()]
            logger.info(f"Найдено {len(dates)} уникальных дат в БД "
                       f"{f'типа {rate_type}' if rate_type else ''}")
            return dates
        
        except Exception as e:
            logger.error(f"Ошибка при получении дат из БД: {e}")
            return []
        
    def get_record_count(self) -> Dict[str, int]:
        """
        Получение статистики по количеству записей
        
        Returns:
            Словарь с количеством записей по типам
        """
        try:
            query = """
            SELECT 
                rate_type,
                COUNT(*) as count,
                MIN(rate_date) as min_date,
                MAX(rate_date) as max_date
            FROM exchange_rates 
            GROUP BY rate_type
            """

            self.cursor.execute(query)
            results = self.cursor.fetchall()

            stats = {}
            for row in results:
                rate_type, count, min_date, max_date = row
                stats[rate_type] = {
                    'count': count,
                    'date_range': f"{min_date} - {max_date}",
                    'currencies': self._get_currency_count(rate_type)
                }
                logger.info(f"  {rate_type}: {count} записей ({min_date} - {max_date})")
            
            return stats
        
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            return {}
        
    def _get_currency_count(self, rate_type: str) -> int:
        """Получение количества уникальных валют по типу"""
        try:
            query = "SELECT COUNT(DISTINCT currency_code) FROM exchange_rates WHERE rate_type = %s"
            self.cursor.execute(query, (rate_type,))
            return self.cursor.fetchone()[0]
        except:
            return 0




def test_db_loader():
    """Тестирование модуля загрузки в БД"""
    from config import DB_CONFIG
    
    print("=" * 60)
    print("ТЕСТ МОДУЛЯ ЗАГРУЗКИ В БАЗУ ДАННЫХ")
    print("=" * 60)
    
    # Создаем тестовые данные
    test_data = [
        {
            'currency_code': 'USD',
            'currency_name': 'Доллар США',
            'exchange_rate': 91.1234,
            'rate_date': date.today(),
            'rate_type': 'daily',
            'nominal': 1
        },
        {
            'currency_code': 'EUR',
            'currency_name': 'Евро',
            'exchange_rate': 98.5678,
            'rate_date': date.today(),
            'rate_type': 'daily',
            'nominal': 1
        },
        {
            'currency_code': 'USD',
            'currency_name': 'Доллар США',
            'exchange_rate': 90.9999,
            'rate_date': date(2024, 1, 15),
            'rate_type': 'monthly',
            'nominal': 1
        }
    ]
    
    # Тестируем загрузчик
    loader = DatabaseLoader(DB_CONFIG)
    
    print("\n1. Подключение к БД...")
    if loader.connect():
        print("✓ Подключение к БД успешно")
        
        print("\n2. Создание таблицы...")
        if loader.create_table_if_not_exists():
            print("✓ Таблица создана/проверена")
            
            print("\n3. Тестовая вставка данных...")
            inserted = loader.insert_exchange_rates(test_data)
            print(f"✓ Вставлено тестовых записей: {inserted}")
            
            print("\n4. Проверка существующих данных...")
            dates = loader.get_existing_dates()
            print(f"✓ Даты в БД: {len(dates)} записей")
            
            print("\n5. Статистика БД...")
            stats = loader.get_record_count()
            for rate_type, data in stats.items():
                print(f"   {rate_type}: {data['count']} записей")
            
            print("\n6. Отключение от БД...")
            loader.disconnect()
            print("✓ Отключение успешно")
            
            print("\n" + "=" * 60)
            print("ТЕСТ ЗАВЕРШЕН УСПЕШНО!")
            print("=" * 60)
            
        else:
            print("✗ Не удалось создать/проверить таблицу")
    else:
        print("✗ Не удалось подключиться к БД")


if __name__ == "__main__":
    test_db_loader()
    


            