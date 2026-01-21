"""
Модуль для извлечения ежедневных курсов валют через SOAP API ЦБ РФ
"""
from datetime import datetime, date, timedelta
from locale import currency
import zeep
from zeep.transports import Transport
import requests
from typing import List, Dict, Optional
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DailyExtractor:
    """Класс для работы с SOAP API ежедневных курсов валют"""

    def __init__(self, wsdl_url: str):  # Конструктор
        """
        Инициализация SOAP-клиента
        
        Args:
            wsdl_url: URL WSDL сервиса
        """
        self.wsdl_url = wsdl_url # URL WSDL-описания
        self.client = None # Будущий SOAP-клиент
        self.transport = Transport(timeout=30) # Настройки сети

    def connect(self) -> bool:
        """
        Подключение к SOAP-сервису
        
        Returns:
            bool: True если подключение успешно
        """
        try:
            logger.info(f"Подключаемся к SOAP-сервису: {self.wsdl_url}")
            self.client = zeep.Client(wsdl=self.wsdl_url, transport=self.transport)
            # Cпособ получить список операций
            service = self.client.wsdl.services['DailyInfo']
            port = service.ports['DailyInfoSoap']
            operations = port.binding._operations

            # Проверяем доступные методы
            methods = list(operations.keys())
            logger.info(f"Доступные методы: {methods}")
            
            if 'GetCursOnDate' in methods:
                logger.info("✓ Метод GetCursOnDate доступен")
                return True
            elif 'GetCursOnDateXML' in methods:
                logger.info("✓ Метод GetCursOnDateXML доступен (используем XML версию)")
                return True        
            else:
                logger.error(f"✗ Нужные методы не найдены! Доступны: {methods}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка подключения к SOAP-сервису: {e}")
            return False

    def get_currencies_on_date(self, target_date: date) -> Optional[List[Dict]]:
        """
        Получение курсов валют на определенную дату
        
        Args:
            target_date: Дата для получения курсов
            
        Returns:
            List[Dict]: Список словарей с данными о валютах или None при ошибке
        """
        if not self.client:
            logger.error("Клиент не инициализирован. Вызовите connect() сначала.")
            return None
        
        try:
            logger.info(f"Запрашиваем курсы на дату: {target_date}")

            # Вызов SOAP-метода GetCursOnDate
            # Zeep автоматически конвертирует Python datetime в формат SOAP
            response = self.client.service.GetCursOnDate(target_date)

            # SOAP-сервис возвращает сложный объект, нужно его распарсить
            return self._parse_soap_response(response, target_date)
        
        except Exception as e:
            logger.error(f"Ошибка при получении курсов на {target_date}: {e}")
            return None
    
    def _parse_soap_response(self, response, target_date: date) -> List[Dict]:
        """
        Парсинг ответа SOAP-сервиса
        Args:
            response: Ответ от SOAP-сервиса
            target_date: Дата запроса
        Returns:
            List[Dict]: Нормализованные данные о валютах
        """
        currencies_data = []

        try:
            # Добираемся до данных: response._value_1._value_1
            if hasattr(response, '_value_1'):
                valute_data = response._value_1

                if hasattr(valute_data, '_value_1'):
                    valute_array = valute_data._value_1

                    if isinstance(valute_array, list):
                        for item in valute_array:
                            if 'ValuteCursOnDate' in item:
                                valute = item['ValuteCursOnDate']

                                # Конвертируем Decimal в float
                                vcurs = float(getattr(valute, 'Vcurs', 0))
                                vnom = int(getattr(valute, 'Vnom', 1))
                                vunit_rate = float(getattr(valute, 'VunitRate', 0))

                                currency_data = {
                                    'currency_code': getattr(valute,'VchCode', '').strip(),
                                    'currency_name': getattr(valute,'Vname', '').strip(),
                                    'exchange_rate': vcurs,  # Курс для Vnom единиц
                                    'rate_date': target_date,
                                    'rate_type': 'daily',
                                    'nominal': vnom,  # Номинал (1, 10, 100, 1000)
                                    'unit_rate': vunit_rate,  # Курс за 1 единицу
                                    'num_code': getattr(valute,'Vcode', ''),
                                    'char_code': getattr(valute,'VchCode', '').strip(),
                                    'source': 'cbr_soap_api'
                                }
                                currencies_data.append(currency_data)
            if currencies_data:
                logger.info(f"Успешно получено {len(currencies_data)} курсов валют на {target_date}")
            else:
                logger.warning(f"Нет данных о валютах на {target_date}")

            return currencies_data
    
        except Exception as e:
            logger.error(f"Ошибка парсинга SOAP-ответа: {e}")
            import traceback
            traceback.print_exc()
            return []



    def get_available_currencies(self, seld: bool = False) -> Optional[List[Dict]]:
        """
        Получение справочника валют
        """
        if not self.client:
            logger.error("Клиент не инициализирован")
            return None
        
        try:
            logger.info(f"Запрашиваем справочник валют (seld={seld})")
            response = self.client.service.EnumValutes(seld)
            
            currencies_list  = []
            
            # Добираемся до данных: response._value_1._value_
            if hasattr(response, '_value_1'):
                valute_data = response._value_1

                if hasattr(valute_data, '_value_1'):
                    valute_array = valute_data._value_1

                    if isinstance(valute_array, list):
                        for item in valute_array:
                            if 'EnumValutes' in item:
                                valute = item['EnumValutes']
                                currency_info = {
                                'vname': getattr(valute, 'Vname', '').strip(),
                                'vnom': int(getattr(valute, 'Vnom', 1)),
                                'vcode': getattr(valute, 'Vcode', '').strip(),
                                'vengname': getattr(valute, 'VEngname', '').strip(),
                                'vcommoncode': getattr(valute, 'VcommonCode', '').strip(),
                                'vnumcode': getattr(valute,'VnumCode', ''),
                                'vcharcode': (getattr(valute, 'VcharCode') or '').strip()                                    
                                }
                                currencies_list.append(currency_info)
            
            logger.info(f"Получено {len(currencies_list)} валют в справочнике")
            
            if currencies_list:
                logger.info(f"Примеры валют:")
                for curr in currencies_list[:3]:
                    logger.info(f"  - {curr['vname']} ({curr['vcharcode']})")

            return currencies_list
            
        except Exception as e:
            logger.error(f"Ошибка получения справочника валют: {e}")
            import traceback
            traceback.print_exc()  # Подробный traceback
            return None
        
    def get_currencies_for_period(
        self, 
        start_date: date, 
        end_date: date
    ) -> Dict[date, List[Dict]]:
        """
        Получение курсов валют за период
        
        Args:
            start_date: Начальная дата периода
            end_date: Конечная дата периода
            
        Returns:
            Словарь {дата: список данных о валютах}
        """
        if not self.client:
            if not self.connect():
                return {}
        
        period_data = {}
        current_date = start_date
        
        while current_date <= end_date:
            logger.info(f"Запрашиваем данные на {current_date}")
            
            try:
                daily_data = self.get_currencies_on_date(current_date)
                if daily_data:
                    period_data[current_date] = daily_data
            except Exception as e:
                logger.error(f"Ошибка для даты {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        return period_data
        
def test_daily_extractor():
    """Тестирование модуля извлечения ежедневных данных"""
    from config import API_CONFIG
    
    extractor = DailyExtractor(API_CONFIG['soap_wsdl_url'])
    
    if extractor.connect():
        print("✓ Подключение к SOAP-сервису успешно")
        
        # Тестируем получение справочника
        currencies = extractor.get_available_currencies(seld=False)
        if currencies:
            print(f"✓ Получен справочник из {len(currencies)} валют")
            for curr in currencies[:3]:  # Покажем первые 3
                print(f"  - {curr['vname']} ({curr['vcharcode']})")
        
        # Тестируем получение курсов на сегодня
        today = date(2024, 12, 30) #date.today()
        daily_rates = extractor.get_currencies_on_date(today)
        
        if daily_rates:
            print(f"✓ Получены курсы на {today}")
            print(f"  Количество валют: {len(daily_rates)}")
            for rate in daily_rates[:3]:  # Покажем первые 3
                print(f"  - {rate['currency_code']}: {rate['exchange_rate']}")
            
            # Сохраняем для проверки
            import json
            with open('data/daily_test.json', 'w', encoding='utf-8') as f:
                json.dump(daily_rates[:5], f, ensure_ascii=False, indent=2, default=str)
            print("✓ Тестовые данные сохранены в data/daily_test.json")
        else:
            print("✗ Не удалось получить курсы на сегодня")
            # Пробуем вчерашнюю дату
            from datetime import timedelta
            yesterday = today - timedelta(days=1)
            print(f"Пробуем получить курсы на {yesterday}...")
            daily_rates = extractor.get_currencies_on_date(yesterday)
            if daily_rates:
                print(f"✓ Получены курсы на {yesterday}")
    else:
        print("✗ Не удалось подключиться к SOAP-сервису")




if __name__ == "__main__":
    test_daily_extractor()
