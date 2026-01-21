"""
Конфигурация исторической загрузки данных
"""

# Предопределенные периоды для загрузки
HISTORICAL_PERIODS = {
    'last_week': {
        'name': 'Последняя неделя',
        'days': 7
    },
    'last_month': {
        'name': 'Последний месяц',
        'days': 30
    },
    'last_quarter': {
        'name': 'Последний квартал',
        'days': 90
    },
    'last_year': {
        'name': 'Последний год',
        'days': 365
    },
    '2024': {
        'name': 'Весь 2024 год',
        'start_date': '2024-01-01',
        'end_date': '2024-12-31'
    },
    '2023': {
        'name': 'Весь 2023 год',
        'start_date': '2023-01-01',
        'end_date': '2023-12-31'
    }
}

# Важные даты для загрузки (кризисы, события)
SPECIAL_DATES = {
    'covid_start': '2020-03-01',      # Начало пандемии
    'sanctions_2022': '2022-02-24',   # Начало спецоперации
    'max_rate_usd': '2022-03-10',     # Максимальный курс USD (130+)
}


def get_period_config(period_name: str):
    """
    Получение конфигурации периода по имени
    
    Args:
        period_name: Имя периода из HISTORICAL_PERIODS
        
    Returns:
        Словарь с параметрами периода или None
    """
    return HISTORICAL_PERIODS.get(period_name)


def get_available_periods():
    """
    Получение списка доступных периодов
    
    Returns:
        Список имен доступных периодов
    """
    return list(HISTORICAL_PERIODS.keys())