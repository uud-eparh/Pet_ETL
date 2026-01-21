import logging.config
import sys
from pathlib import Path
from datetime import date

def setup_logging():
    """Продвинутая настройка логирования"""
    
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    log_config = {
        'version': 1,
        'disable_existing_loggers': False,  # ВАЖНО! Не отключаем существующие
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)20s - %(levelname)8s - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'formatter': 'standard',
                'stream': sys.stdout
            },
            'file': {
                'class': 'logging.FileHandler',
                'level': 'INFO',
                'formatter': 'standard',
                'filename': log_dir / f"etl_{date.today().strftime('%Y%m%d')}.log",
                'encoding': 'utf-8',
                'mode': 'a'  # 'a' - дополнение файла
            }
        },
        'loggers': {
            '': {  # Корневой логгер
                'handlers': ['console', 'file'],
                'level': 'INFO',
                'propagate': True
            },
            'src': {
                'handlers': ['console', 'file'],
                'level': 'INFO',
                'propagate': False
            }
        }
    }
    
    logging.config.dictConfig(log_config)
    
    # Уменьшаем шум от библиотек
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('zeep').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)