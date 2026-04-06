# -*- coding: utf-8 -*-
"""
Конфигурация подключений к базам данных 1С
Читает настройки из .env файла
"""

import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    print("Ошибка: библиотека python-dotenv не установлена")
    print("Установите её командой: pip install python-dotenv")
    sys.exit(1)

# Загружаем переменные окружения из .env файла
# Ищем .env файл в корне проекта (на уровень выше tools/)
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    # Если не найден, пытаемся загрузить из текущей директории
    load_dotenv()


def _get_env_var(key: str) -> str:
    """Получает переменную окружения, если не заполнено - выводит ошибку и завершает программу"""
    value = os.getenv(key)
    if not value or value.strip() == '':
        print(f"Ошибка: переменная окружения '{key}' не заполнена в .env файле")
        print(f"Проверьте файл .env и заполните обязательные переменные:")
        print(f"  - SOURCE_CONNECTION_STRING (строка подключения к базе-источнику)")
        print(f"  - TARGET_CONNECTION_STRING (строка подключения к базе-приемнику)")
        print(f"\nПримеры строк подключения:")
        print(f"  Для серверной базы: Srvr=\"сервер\";Ref=\"имя_базы\";Usr=\"пользователь\";Pwd=\"пароль\";")
        print(f"  Для файловой базы: File=\"C:\\путь\\к\\базе\";Usr=\"пользователь\";Pwd=\"пароль\";")
        sys.exit(1)
    return value


# Настройки подключения к базам данных 1С (читаем из .env)
SOURCE_CONNECTION_STRING = _get_env_var('SOURCE_CONNECTION_STRING')
TARGET_CONNECTION_STRING = _get_env_var('TARGET_CONNECTION_STRING')

# Словарь конфигураций (совместимость со старым кодом)
DATABASE_CONFIGS = {
    'source': {
        'connection_string': SOURCE_CONNECTION_STRING,
        'name': 'Source',
        'description': 'База данных источник'
    },
    'target': {
        'connection_string': TARGET_CONNECTION_STRING,
        'name': 'Target',
        'description': 'База данных приемник'
    }
}
