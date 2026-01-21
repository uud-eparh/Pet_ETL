"""
Модуль для загрузки исторических данных о курсах валют
"""
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional
import logging
from src.extractors.daily_extractor import DailyExtractor

logger = logging.getLogger(__name__)


class HistoricalExtractor:
    """
    Класс для загрузки исторических данных за период
    
    Использует DailyExtractor для получения данных за каждый день периода
    """

    def __init__(self, wsdl_url: str):
        """
        Инициализация исторического экстрактора
        
        Args:
            wsdl_url: URL WSDL сервиса
        """
        self.wsdl_url = wsdl_url
        self.daily_extractor = DailyExtractor(wsdl_url)

        logger.info("Инициализирован HistoricalExtractor")

    def get_historical_data(
        self,
        start_date: date,
        end_date: date,
        skip_weekends: bool = True,
        max_days_per_request: int = 30
    ) -> Dict[date, List[Dict]]:
        """
        Получение исторических данных за период
        
        Args:
            start_date: Начальная дата периода
            end_date: Конечная дата периода
            skip_weekends: Пропускать ли выходные (суббота, воскресенье)
            max_days_per_request: Максимальное количество дней за один запрос
            
        Returns:
            Словарь {дата: список данных о валютах}
        """
        logger.info(f"Запрос исторических данных с {start_date} по {end_date}")

        # Проверяем валидность дат
        if start_date > end_date:
            logger.error(f"Некорректный диапазон дат: {start_date} > {end_date}")
            return {}
        
        days_count = (end_date - start_date).days + 1
        logger.info(f"Всего дней в периоде: {days_count}")

        # Подключаемся к SOAP API
        if not self.daily_extractor.connect():
            logger.error("Не удалось подключиться к SOAP API")
            return {}
        
        historical_data = {}
        successful_days = 0
        failed_days = 0
        skipped_days = 0

        # Генерируем список дат для запроса
        current_date = start_date
        day_counter = 0

        while current_date <= end_date:
            day_counter += 1

            # Пропускаем выходные если нужно
            if skip_weekends and current_date.weekday() >= 5: # 5=суббота, 6=воскресенье
                logger.debug(f"Пропускаем выходной: {current_date}")
                skipped_days += 1
                current_date += timedelta(days=1)
                continue

            # Логируем прогресс
            if day_counter % 10 == 0 or day_counter == 1 or current_date == end_date:
                logger.info(f"День {day_counter}/{days_count}: {current_date}")

            try:
                # Получаем данные за день
                daily_data = self.daily_extractor.get_currencies_on_date(current_date)

                if daily_data:
                    historical_data[current_date] = daily_data
                    successful_days += 1

                    # Логируем первую успешную загрузку
                    if successful_days == 1:
                        logger.info(f"✓ Первые данные получены: {len(daily_data)} валют")
                else:
                    logger.warning(f"Нет данных за {current_date}")
                    failed_days += 1

            except KeyboardInterrupt:
                    logger.warning("Загрузка прервана пользователем")
                    break
            
            except Exception as e:
                logger.error(f"Ошибка при получении данных за {current_date}: {e}")
                failed_days += 1

                # Небольшая задержка чтобы не нагружать API
            if day_counter % 5 == 0:
                import time
                time.sleep(0.5)

            current_date += timedelta(days=1)

            # Итоговая статистика
        logger.info("=" * 50)
        logger.info("ИТОГ ИСТОРИЧЕСКОЙ ЗАГРУЗКИ:")
        logger.info(f"  Успешно: {successful_days} дней")
        logger.info(f"  Ошибок: {failed_days} дней")
        logger.info(f"  Пропущено (выходные): {skipped_days} дней")
        logger.info(f"  Всего записей: {sum(len(data) for data in historical_data.values())}")

        if historical_data:
            # Анализируем полученные данные
            self._analyze_historical_data(historical_data)

        return historical_data
    
    def _analyze_historical_data(self, historical_data: Dict[date, List[Dict]]):
        """
        Анализ полученных исторических данных
        
        Args:
            historical_data: Словарь с историческими данными
        """
        if not historical_data:
            return
        
        dates = list(historical_data.keys())
        dates.sort()

        total_records = sum(len(data) for data in historical_data.values())
        unique_currencies = set()

        for data_key, data in historical_data.items():
            for record in data:
                unique_currencies.add(record['currency_code'])

        logger.info("АНАЛИЗ ИСТОРИЧЕСКИХ ДАННЫХ:")
        logger.info(f"  Диапазон дат: {dates[0]} - {dates[-1]}")
        logger.info(f"  Уникальных дат: {len(dates)}")
        logger.info(f"  Уникальных валют: {len(unique_currencies)}")
        logger.info(f"  Среднее валют в день: {total_records / len(dates):.1f}")

        # Примеры валют
        sample_currencies = list(unique_currencies)[:5]
        logger.info(f"  Пример валют: {', '.join(sample_currencies)}")

    def get_historical_data_flattened(
            self,
            start_date: date,
            end_date: date,
            **kwargs
    ) -> List[Dict]:
        """
        Получение исторических данных в плоском формате
        
        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            **kwargs: Дополнительные параметры для get_historical_data
            
        Returns:
            Единый список всех записей за период
        """
        historical_dict = self.get_historical_data(start_date, end_date, **kwargs)

        # Преобразуем словарь в список
        flattened_data = []
        for data_key, data_list in historical_dict.items():
            for record in data_list:
                # Убеждаемся что в записи есть дата
                record['rate_date'] = data_key
                flattened_data.append(record)

        logger.info(f"Плоский список создан: {len(flattened_data)} записей")
        return flattened_data



def test_historical_extractor():
    """Тестирование модуля исторических данных"""
    from config import API_CONFIG
    
    print("=" * 60)
    print("ТЕСТ ИСТОРИЧЕСКОЙ ЗАГРУЗКИ ДАННЫХ")
    print("=" * 60)
    
    extractor = HistoricalExtractor(API_CONFIG['soap_wsdl_url'])
    
    # Тестовый период: последние 3 дня
    end_date = date.today() - timedelta(days=1)  # Вчера
    start_date = end_date - timedelta(days=2)    # 3 дня назад
    
    print(f"\nТестовый период: {start_date} - {end_date}")
    print("Загрузка данных...")
    
    historical_data = extractor.get_historical_data_flattened(
        start_date=start_date,
        end_date=end_date,
        skip_weekends=True
    )
    
    if historical_data:
        print(f"\n✓ УСПЕХ! Получено {len(historical_data)} записей")
        
        # Группируем по дате для отображения
        from collections import defaultdict
        grouped = defaultdict(list)
        for record in historical_data:
            grouped[record['rate_date']].append(record)
        
        print("\nДанные по дням:")
        for date_key in sorted(grouped.keys()):
            records = grouped[date_key]
            print(f"  {date_key}: {len(records)} валют")
            
            # Показываем первые 3 валюты за день
            for i, record in enumerate(records[:3]):
                print(f"    {i+1}. {record['currency_code']}: {record['exchange_rate']}")
        
        # Сохраняем образец
        import json
        import os
        os.makedirs('data', exist_ok=True)
        
        with open('data/historical_test.json', 'w', encoding='utf-8') as f:
            json.dump(historical_data[:20], f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n✓ Образец сохранен: data/historical_test.json")
        
    else:
        print("\n✗ Не удалось получить исторические данные")
    
    print("\n" + "=" * 60)
    print("ТЕСТ ЗАВЕРШЕН")
    print("=" * 60)


if __name__ == "__main__":
    test_historical_extractor()
