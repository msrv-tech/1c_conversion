# -*- coding: utf-8 -*-
"""
Модуль выгрузки банки из обработанной БД в 1С приемник.
Данные в БД уже в формате приемника, запись происходит с сохранением UUID.
"""

import os
import sys

# Добавляем родительскую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, get_predefined_element_json
from tools.base_writer import write_catalog_item, setup_exchange_mode
from tools.logger import verbose_print  # noqa: E402

fix_encoding()


def _write_item(com_object, item_data, processed_db=None):
    """
    Записывает элемент в 1С с сохранением UUID.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item_data: Словарь с данными элемента
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        
    Returns:
        True если успешно, False если ошибка
    """
    import json
    
    # Заполняем поле Страна предопределенным элементом "Россия", если оно не заполнено
    country_field = item_data.get("Страна", "")
    need_fill_country = False
    
    if not country_field:
        need_fill_country = True
    else:
        # Проверяем, является ли это пустым JSON
        try:
            if isinstance(country_field, str) and country_field.strip().startswith('{'):
                country_json = json.loads(country_field)
                if not country_json.get("uuid") or country_json.get("uuid") == "00000000-0000-0000-0000-000000000000":
                    need_fill_country = True
        except (json.JSONDecodeError, AttributeError):
            # Если не JSON или ошибка парсинга, считаем пустым
            need_fill_country = True
    
    if need_fill_country:
        # Получаем предопределенный элемент "Россия" через функцию с кешированием
        country_json_str = get_predefined_element_json(
            com_object,
            "Справочники.СтраныМира.Россия",
            "Справочник.СтраныМира"
        )
        if country_json_str:
            item_data["Страна"] = country_json_str
            try:
                country_data = json.loads(country_json_str)
                verbose_print(f"    ✓ Заполнено поле Страна: {country_data.get('presentation', '')}")
            except Exception:
                pass
    
    return write_catalog_item(
        com_object,
        item_data,
        "Банки",
        "Справочник.Банки",
        ["Код", "Наименование", "ПометкаУдаления", "КоррСчет", "Город", "Адрес", "Телефоны", "Комментарий"],
        processed_db=processed_db
    )


def write_banks_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает банки из обработанной БД в 1С приемник.
    Запись происходит в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.
    
    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) - обязательный параметр
        process_func: Не используется (данные уже обработаны)
    
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА БАНКИ ИЗ ОБРАБОТАННОЙ БД В 1С ПРИЕМНИК")
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
    verbose_print("\n[2/3] Чтение банки из обработанной БД...")
    items = get_from_db(db_connection, "banks")
    db_connection.close()
    
    if not items:
        verbose_print("Банки не найдены в базе данных")
        return False
    
    verbose_print(f"Прочитано банки: {len(items)}")
    
    # Шаг 3: Запись в 1С
    verbose_print("\n[3/3] Запись в 1С приемник...")
    setup_exchange_mode(com_object)
    
    # Записываем элементы
    verbose_print(f"\nНачинаем запись {len(items)} банки...")
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

