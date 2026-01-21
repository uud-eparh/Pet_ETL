"""
Модуль для извлечения ежемесячных курсов валют через REST API ЦБ РФ

Основные функции:
1. Подключение к REST API ЦБ РФ
2. Получение данных за указанный период
3. Преобразование сложного JSON в простые структуры
4. Извлечение кодов валют из русских названий
"""

from ast import Not
from datetime import datetime, date
from locale import currency
from multiprocessing import process
from urllib import response
from venv import logger
import requests
from typing import List, Dict, Optional
import logging
from dataclasses import dataclass

# Настраиваем логирование для этого модуля
logger = logging.getLogger(__name__)

@dataclass
class MonthlyRate:
    """
    Класс для хранения данных о ежемесячном курсе валюты.
    
    Используется dataclass для автоматического создания:
    - конструктора __init__
    - методов сравнения
    - красивого вывода через __repr__
    
    Атрибуты:
        currency_code: трехбуквенный код валюты (USD, EUR)
        currency_name: полное название ("Доллар США")
        exchange_rate: числовое значение курса
        rate_date: дата, на которую актуален курс
        rate_type: всегда 'monthly' для этого модуля
        nominal: номинал валюты (обычно 1)
        element_id: внутренний ID из API ЦБ
        period: текстовое представление периода ("Январь 2024")
    """
    currency_code: str
    currency_name: str
    exchange_rate: float
    rate_date: date
    rate_type: str = 'monthly' # Значение по умолчанию
    nominal: int = 1
    element_id: int = 0
    period: str = ''

class MonthlyExtractor:   
    """
    Главный класс для работы с REST API ежемесячных курсов валют.
    
    Отвечает за:
    1. Настройку HTTP-сессии
    2. Выполнение запросов к API
    3. Обработку ошибок сети
    4. Парсинг JSON-ответов
    """
    def __init__(self, base_url: str) -> None:
        """
        Инициализация экстрактора.
        Args:
            base_url: базовый URL API ЦБ РФ 
                     (например, 'http://www.cbr.ru/dataservice/')
        """
        self.base_url = base_url
        # Создаем сессию для повторного использования подключения
        self.session = requests.Session()
        # Добавляем заголовки для идентификации нашего запроса
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0',  # Имя нашего приложения
            'Accept': 'application/json',      # Ждем JSON в ответ
            'Accept-Encoding': 'gzip, deflate' # Поддержка сжатия
        })
        logger.info(f"Инициализирован MonthlyExtractor для {base_url}")

    def get_monthly_rates(self,
                          publication_id: int,
                          dataset_id: int,
                          start_year: int,
                          end_year: int) -> Optional[List[MonthlyRate]]:
        """
        Получение ежемесячных курсов валют за указанный период.
        
        Процесс:
        1. Формирование параметров запроса
        2. Отправка GET-запроса к API
        3. Проверка ответа
        4. Парсинг JSON
        
        Args:
            publication_id: ID публикации (33 для курсов валют)
            dataset_id: ID набора данных (128 для средних курсов)
            start_year: начальный год периода
            end_year: конечный год периода
            
        Returns:
            Список объектов MonthlyRate или None при ошибке
            
        Raises:
            RequestException: при проблемах с сетью или API
        """
        try:
            # 1. Формируем параметры запроса
            params = {
                    'y1':start_year,
                    'y2':end_year,
                    'publicationid':publication_id,
                    'datasetid':dataset_id,
                    }
            
            logger.info(f"Запрос ежемесячных данных с параметрами: {params}")

            # 2. Отправляем запрос
            response = self.session.get(
                f"{self.base_url}/data",
                params=params,
                timeout=30
            )

            # 3. Проверяем статус ответа (выбрасывает исключение если не 200)
            response.raise_for_status()

            #  4. Парсим JSON
            data = response.json()

            logger.info(f"Получен ответ от API, размер данных: {len(str(data))} байт")

            # 5. Обрабатываем данные
            return self._parse_monthly_response(data)

        except requests.exceptions.RequestException as e:
            # Обрабатываем сетевые ошибки и ошибки HTTP
            logger.error(f"Ошибка запроса к API: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Статус ответа {e.response.status_code}")
                logger.error(f"Тело ответа: {e.response.text[:200]}...")
            return None # pyright: ignore[reportUnusedExpression]

        except Exception as e:
            # Обрабатываем любые другие ошибки
            logger.error(f"Неожиданная ошибка при получении данных: {e}", exc_info=True)
            return None
        
    def _parse_monthly_response(self, data: Dict) -> List[MonthlyRate]:
        """
        Внутренний метод для парсинга JSON-ответа от API.
        
        Структура ответа API:
        {
          "RawData": [ ... ],      # Основные данные
          "headerData": [ ... ],   # Справочник валют
          "units": [ ... ],        # Единицы измерения
          "DTRange": [ ... ],      # Диапазон дат
          "SType": [ ... ]         # Тип данных
        }
        
        Args:
            data: словарь с данными от API
            
        Returns:
            Список нормализованных объектов MonthlyRate
        """
        monthly_rates = []

        try:
            # --- ШАГ 1: Создаем справочник валют ---
            currency_map = {}

            # headerData содержит информацию о валютах
            for header in data.get('headerData', []):
                element_id = header['id']
                currency_name = header['elname']
                currency_map[element_id] = {
                    'name': currency_name,
                    'code': self._extract_currency_code(currency_name)
                }

            logger.info(f"Создан справочник из {len(currency_map)} валют")

            # --- ШАГ 2: Обрабатываем основные данные ---
            raw_data = data.get('RawData', [])
            logger.info(f"Найдено {len(raw_data)} строк в RawData")

            # --- ШАГ 3: Обрабатываем каждую запись ---
            processed_count = 0
            skipped_count = 0

            for item in raw_data:
                # Получаем ID элемента (валюты
                element_id = item['element_id']

                # Проверяем, есть ли эта валюта в справочнике
                if element_id not in currency_map:
                    logger.warning(f"Неизвестный element_id: {element_id}, пропускаем")
                    skipped_count += 1
                    continue

                # --- ШАГ 4: Парсим дату ---
                try:
                    dt_str = item.get('date', '')
                    # Дата может быть в формате: "2024-02-01T00:00:00"
                    if 'T' in dt_str:
                        # Берем часть до 'T' и парсим как дату
                        date_part = dt_str.split('T')[0]
                        rate_date = datetime.fromisoformat(date_part).date()
                    else:
                        # Пробуем парсить как обычную дату
                        rate_date = datetime.strptime(dt_str, '%Y-%m-%d').date()

                except ValueError as e:
                    logger.warning(f"Не удалось распарсить дату '{dt_str}': {e}") # pyright: ignore[reportPossiblyUnboundVariable]
                    skipped_count += 1
                    continue

                except Exception as e:
                    logger.warning(f"Неожиданная ошибка при парсинге даты '{dt_str}': {e}") # pyright: ignore[reportPossiblyUnboundVariable]
                    skipped_count += 1
                    continue

                # --- ШАГ 5: Создаем объект MonthlyRate ---
                currency_info = currency_map[element_id]

                monthly_rate = MonthlyRate(
                    currency_code=currency_info['code'],
                    currency_name=currency_info['name'],
                    exchange_rate=float(item['obs_val']), # Конвертируем в float
                    rate_date=rate_date,
                    element_id=element_id,
                    period=item.get('dt', ''), # Текстовое представление ("Январь 2024")
                    nominal=1
                )

                monthly_rates.append(monthly_rate)
                processed_count += 1
            
            # --- ШАГ 6: Логируем результаты ---
            logger.info(f"Обработка завершена. "
                       f"Успешно: {processed_count}, "
                       f"Пропущено: {skipped_count}, "
                       f"Всего: {len(monthly_rates)}")
            
            return monthly_rates

        except KeyError as e:
            logger.error(f"Отсутствует обязательное поле в данных: {e}")
            logger.debug(f"Структура данных: {data.keys()}")
            return []
        except Exception as e:
            logger.error(f"Ошибка парсинга данных: {e}", exc_info=True)
            return []
        
    def _extract_currency_code(self, currency_name: str) -> str:
            """
            Извлекает трехбуквенный код валюты из русского названия.
            
            Примеры:
            "Доллара США к рублю" → "USD"
            "Евро к рублю" → "EUR"
            "Юаня к рублю" → "CNY"
            
            Args:
                currency_name: полное название валюты на русском
                
            Returns:
                трехбуквенный код валюты или первые 3 буквы, если код не найден
            """
            # Словарь соответствия русских названий кодам валют
            mapping = {
                'Доллара США': 'USD',
                'Евро': 'EUR',
                'Юаня': 'CNY',
                'Фунта стерлингов': 'GBP',
                'Йены': 'JPY',
                'Франка': 'CHF',
                'Казахстанского тенге': 'KZT',
                'Белорусского рубля': 'BYN',
                'Гривны': 'UAH',
                'Лари': 'GEL'
                }
            # Ищем соответствие в словаре
            for rus_name, code in mapping.items():
                if rus_name in currency_name:
                    logger.debug(f"Найдено соответствие: '{rus_name}' → '{code}'")
                    return code
            
            # Если не нашли в словаре, пробуем извлечь код по-другому
            logger.warning(f"Неизвестная валюта: '{currency_name}'")

            import re
            english_letters = re.findall(r'[A-Za-z]{3}', currency_name)
            if english_letters:
                return english_letters[0].upper()
            
            # Вариант 2: Берем первые 3 буквы и конвертируем в латиницу (грубо)
            # Это временное решение, в реальном проекте нужен полный справочник
            first_three = currency_name[:3].upper()

            # Грубая транслитерация кириллицы
            translit_map = {
                'ДОЛ': 'DOL', 'ЕВР': 'EUR', 'ЮАН': 'UAN',
                'ФУН': 'FUN', 'ЙЕН': 'YEN', 'ФРА': 'FRA'
            }
            
            return translit_map.get(first_three, first_three)
    

def test_monthly_extractor():
    """
    Тестирование модуля извлечения ежемесячных данных.
    
    Эта функция:
    1. Импортирует конфигурацию
    2. Создает экстрактор
    3. Получает данные за тестовый период
    4. Выводит результаты
    5. Сохраняет образец данных в файл
    """
    # Импортируем здесь, чтобы избежать циклических импортов
    from config import API_CONFIG
    
    print("=" * 60)
    print("ТЕСТ МОДУЛЯ ЕЖЕМЕСЯЧНЫХ ДАННЫХ")
    print("=" * 60)
    
    # Создаем экстрактор
    extractor = MonthlyExtractor(API_CONFIG['rest_base_url'])
    
    print(f"\n1. Запрашиваем данные за 2024 год...")
    print(f"   Публикация ID: {API_CONFIG['monthly_publication_id']}")
    print(f"   Набор данных ID: {API_CONFIG['monthly_dataset_id']}")
    
    # Получаем данные
    rates = extractor.get_monthly_rates(
        publication_id=API_CONFIG['monthly_publication_id'],
        dataset_id=API_CONFIG['monthly_dataset_id'],
        start_year=2024,
        end_year=2024
    )
    
    if rates:
        print(f"\n2. ✓ УСПЕХ! Получено {len(rates)} записей")
        
        # Показываем первые 5 записей
        print("\n3. Первые 5 записей:")
        print("-" * 60)
        for i, rate in enumerate(rates[:5], 1):
            print(f"{i}. {rate.currency_code} ({rate.currency_name}):")
            print(f"   Курс: {rate.exchange_rate}")
            print(f"   Дата: {rate.rate_date}")
            print(f"   Период: {rate.period}")
            print()
        
        # Сохраняем образец данных в файл
        print("4. Сохраняем образец данных...")
        try:
            import json
            from datetime import date
            
            # Функция для сериализации дат
            def date_serializer(obj):
                """Конвертирует date в строку для JSON"""
                if isinstance(obj, date):
                    return obj.isoformat()
                raise TypeError(f"Тип {type(obj)} не сериализуем")
            
            # Создаем директорию data, если её нет
            import os
            os.makedirs('data', exist_ok=True)
            
            # Конвертируем объекты MonthlyRate в словари
            rates_dicts = [rate.__dict__ for rate in rates[:10]]  # Первые 10
            
            # Сохраняем в файл
            with open('data/monthly_test.json', 'w', encoding='utf-8') as f:
                json.dump(rates_dicts, f, 
                         ensure_ascii=False,  # Сохраняем кириллицу как есть
                         indent=2,           # Красивое форматирование
                         default=date_serializer)  # Обработка дат
            
            print(f"   ✓ Файл сохранен: data/monthly_test.json")
            print(f"   ✓ Размер файла: {os.path.getsize('data/monthly_test.json')} байт")
            
        except Exception as e:
            print(f"   ✗ Ошибка сохранения файла: {e}")
        
        # Статистика
        print("\n5. СТАТИСТИКА:")
        print("-" * 40)
        unique_currencies = set(r.currency_code for r in rates)
        unique_dates = set(r.rate_date for r in rates)
        
        print(f"   Уникальных валют: {len(unique_currencies)}")
        print(f"   Уникальных дат: {len(unique_dates)}")
        print(f"   Диапазон дат: от {min(unique_dates)} до {max(unique_dates)}")
        
    else:
        print("\n✗ НЕ УДАЛОСЬ получить данные")
        print("Проверьте:")
        print("  1. Доступность API: http://www.cbr.ru/dataservice/data")
        print("  2. Параметры publicationId и datasetId")
        print("  3. Сетевое подключение")
    
    print("\n" + "=" * 60)
    print("ТЕСТ ЗАВЕРШЕН")
    print("=" * 60)


if __name__ == "__main__":
    # Запускаем тест при прямом выполнении файла
    test_monthly_extractor()
