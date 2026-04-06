# -*- coding: utf-8 -*-
"""
Модуль выгрузки справочника «Проекты» из обработанной БД в 1С приемник.
Данные в БД уже в формате приемника, запись происходит с сохранением UUID.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db
from tools.base_writer import write_catalog_item, setup_exchange_mode
from tools.logger import verbose_print

fix_encoding()

STANDARD_FIELDS = [
    "Код",
    "Наименование",
    "ПометкаУдаления",
    "Комментарий",
    "Описание",
    "ДатаНачала",
    "ДатаОкончания",
    "customПолноеНаименование",
    "customЯвляетсяЭтапомПроекта",
]


def _write_item(com_object, item_data, processed_db=None):
    """
    Записывает элемент справочника Проекты в 1С с сохранением UUID.

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
        "Проекты",
        "Справочник.Проекты",
        standard_fields=STANDARD_FIELDS,
        processed_db=processed_db,
    )


def write_projects_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает проекты из обработанной БД в 1С приемник.
    Запись в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.

    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) — обязательный параметр
        process_func: Не используется (данные уже обработаны)

    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА СПРАВОЧНИКА «ПРОЕКТЫ» ИЗ ОБРАБОТАННОЙ БД В 1С ПРИЕМНИК")
    verbose_print("=" * 80)

    if com_object is None:
        print("Ошибка: com_object обязателен")
        return False

    verbose_print("\n[1/3] Подключение к обработанной базе данных SQLite...")
    db_connection = connect_to_sqlite(sqlite_db_file)

    if not db_connection:
        print("Ошибка: Не удалось подключиться к базе данных SQLite")
        return False

    verbose_print("\n[2/3] Чтение проектов из обработанной БД...")
    items = get_from_db(db_connection, "projects")
    db_connection.close()

    if not items:
        verbose_print("Проекты не найдены в базе данных")
        return False

    verbose_print(f"Прочитано проектов: {len(items)}")

    setup_exchange_mode(com_object)

    verbose_print(f"\nНачинаем запись {len(items)} проектов...")
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
