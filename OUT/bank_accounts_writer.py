# -*- coding: utf-8 -*-
"""
Модуль выгрузки банковскиесчета из обработанной БД в 1С приемник.
Данные в БД уже в формате приемника, запись происходит с сохранением UUID.
"""

import os
import sys
import json

# Добавляем родительскую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db
from tools.base_writer import prepare_catalog_item, finalize_catalog_item, setup_exchange_mode
from tools.logger import verbose_print  # noqa: E402

fix_encoding()


def _is_currency_non_rub(currency_field):
    """
    Проверяет, является ли валюта не рублем.
    
    Args:
        currency_field: Значение поля ВалютаДенежныхСредств (может быть JSON строкой или пустым)
        
    Returns:
        True если валюта не рубль, False если рубль или не указана
    """
    if not currency_field:
        return False
    
    try:
        # Пытаемся распарсить JSON
        if isinstance(currency_field, str) and currency_field.strip().startswith('{'):
            currency_data = json.loads(currency_field)
            currency_presentation = currency_data.get('presentation', '').strip()
        else:
            currency_presentation = str(currency_field).strip()
        
        if not currency_presentation:
            return False
        
        # Нормализуем название валюты для сравнения
        currency_lower = currency_presentation.lower()
        
        # Проверяем, является ли это рублем
        is_rub = any(rub_variant in currency_lower for rub_variant in [
            'рубль', 'rub', 'rur', 'российский рубль', 'руб.', 'руб ', 'rubles',
            'российский руб', 'russian ruble', 'ruble'
        ])
        
        return not is_rub
    except (json.JSONDecodeError, TypeError, AttributeError):
        # Если ошибка парсинга, считаем что это не валюта (не рубль)
        return False


def _write_item(com_object, item_data, processed_db=None):
    """
    Записывает элемент в 1С с сохранением UUID.
    Устанавливает поле Валютный на основе валюты счета.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item_data: Словарь с данными элемента
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        
    Returns:
        True если успешно, False если ошибка
    """
    # Подготавливаем элемент (создаем/находим и заполняем поля, но не записываем)
    item = prepare_catalog_item(
        com_object,
        item_data,
        "БанковскиеСчета",
        "Справочник.БанковскиеСчета",
        ["Код", "Наименование", "ПометкаУдаления", "Комментарий"],
        processed_db=processed_db
    )
    
    if not item:
        return False
    
    # Устанавливаем поле Валютный на основе валюты счета
    try:
        currency_field = item_data.get("ВалютаДенежныхСредств", "")
        is_currency = _is_currency_non_rub(currency_field)
        
        # Устанавливаем поле Валютный
        item.Валютный = is_currency
        
        if is_currency:
            # Получаем представление валюты для логирования
            currency_presentation = ""
            try:
                if isinstance(currency_field, str) and currency_field.strip().startswith('{'):
                    currency_data = json.loads(currency_field)
                    currency_presentation = currency_data.get('presentation', '')
                else:
                    currency_presentation = str(currency_field)
            except:
                pass
            
            verbose_print(f"    → Установлено Валютный = Истина (валюта: {currency_presentation})")
        else:
            verbose_print(f"    → Установлено Валютный = Ложь (валюта: рубль или не указана)")
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при установке поля Валютный: {e}")
        # Не прерываем выполнение, продолжаем запись
    
    # Завершаем запись элемента
    return finalize_catalog_item(
        com_object=com_object,
        item=item,
        item_data=item_data,
        catalog_name="БанковскиеСчета",
        type_name="Справочник.БанковскиеСчета",
        processed_db=processed_db
    )


def write_bank_accounts_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает банковскиесчета из обработанной БД в 1С приемник.
    Запись происходит в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.
    
    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) - обязательный параметр
        process_func: Не используется (данные уже обработаны)
    
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА БАНКОВСКИЕСЧЕТА ИЗ ОБРАБОТАННОЙ БД В 1С ПРИЕМНИК")
    verbose_print("=" * 80)
    
    if com_object is None:
        print("Ошибка: com_object обязателен")
        return False
    
    # Шаг 1: Подключение к БД
    verbose_print("\n[1/3] Подключение к обработанной базе данных SQLite...")
    db_connection = connect_to_sqlite(sqlite_db_file)
    
    if not db_connection:
        print("Ошибка: Не удалось подключиться к базе данных SQLite")
        return False
    
    # Шаг 2: Чтение элементов из БД
    verbose_print("\n[2/3] Чтение банковскиесчета из обработанной БД...")
    items = get_from_db(db_connection, "bank_accounts")
    db_connection.close()
    
    if not items:
        verbose_print("БанковскиеСчета не найдены в базе данных")
        return False
    
    verbose_print(f"Прочитано банковскиесчета: {len(items)}")
    
    # Шаг 3: Подключение к 1С и запись
    setup_exchange_mode(com_object)
    
    # Записываем элементы
    verbose_print(f"\nНачинаем запись {len(items)} банковскиесчета...")
    written_count = 0
    error_count = 0
    
    for i, item in enumerate(items, 1):
        if i % 10 == 0 or i == 1:
            verbose_print(f"\n[{i}/{len(items)}]")
        
        if _write_item(com_object, item, sqlite_db_file):
            written_count += 1
        else:
            error_count += 1
    
    verbose_print("\n" + "=" * 80)
    verbose_print("ИТОГИ ЗАПИСИ:")
    verbose_print(f"  Успешно записано: {written_count}")
    verbose_print(f"  Ошибок: {error_count}")
    verbose_print(f"  Всего обработано: {len(items)}")
    verbose_print("=" * 80)
    
    return error_count == 0


