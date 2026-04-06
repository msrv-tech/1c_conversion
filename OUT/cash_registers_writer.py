# -*- coding: utf-8 -*-
"""
Модуль выгрузки кассы из обработанной БД в 1С приемник.
Данные в БД уже в формате приемника, запись происходит с сохранением UUID.
"""

import os
import sys

# Добавляем родительскую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db
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
    return write_catalog_item(
        com_object,
        item_data,
        "Кассы",
        "Справочник.Кассы",
        ["Код", "Наименование", "ПометкаУдаления", "Комментарий"],
        processed_db=processed_db
    )


def write_cash_registers_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает кассы из обработанной БД в 1С приемник.
    Запись происходит в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.
    
    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) - обязательный параметр
        process_func: Не используется (данные уже обработаны)
    
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА КАССЫ ИЗ ОБРАБОТАННОЙ БД В 1С ПРИЕМНИК")
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
    verbose_print("\n[2/3] Чтение кассы из обработанной БД...")
    items = get_from_db(db_connection, "cash_registers")
    db_connection.close()
    
    if not items:
        verbose_print("Кассы не найдены в базе данных")
        return False
    
    verbose_print(f"Прочитано кассы: {len(items)}")
    
    # Шаг 3: Подключение к 1С и запись
    setup_exchange_mode(com_object)
    
    # Записываем элементы
    verbose_print(f"\nНачинаем запись {len(items)} кассы...")
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

