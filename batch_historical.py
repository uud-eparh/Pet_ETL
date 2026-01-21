"""
Пакетная загрузка исторических данных
"""
import subprocess
from datetime import date, timedelta

def load_year(year):
    """Загрузка данных за весь год"""
    start_date = date(year, 1, 1)
    end_date = date(year, 12, 31)
    
    cmd = [
        'python', 'main.py',
        '--mode', 'historical',
        '--start-date', start_date.isoformat(),
        '--end-date', end_date.isoformat(),
        '--skip-weekends'
    ]
    
    print(f"\nЗагрузка {year} года: {start_date} - {end_date}")
    result = subprocess.run(cmd)
    return result.returncode == 0

if __name__ == "__main__":
    # Загружаем данные за последние 3 года
    current_year = date.today().year
    for year in range(current_year - 2, current_year):
        if not load_year(year):
            print(f"Ошибка при загрузке {year} года")
            break