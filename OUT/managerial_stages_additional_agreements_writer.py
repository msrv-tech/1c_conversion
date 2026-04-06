# -*- coding: utf-8 -*-
"""
Модуль выгрузки управленческихэтаповработдополнительныхсоглашений из обработанной БД в 1С приемник.
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
        "custom_УправленческиеЭтапыРаботДополнительныхСоглашений",
        "Справочник.custom_УправленческиеЭтапыРаботДополнительныхСоглашений",
        ["Код", "Наименование", "ПометкаУдаления", "Комментарий"],
        processed_db=processed_db
    )


def write_managerial_stages_additional_agreements_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает управленческиеэтапыработдополнительныхсоглашений из обработанной БД в 1С приемник.
    Запись происходит в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.
    
    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) - обязательный параметр
        process_func: Опциональная функция для дополнительной обработки данных перед записью
        
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА СПРАВОЧНИКА «УПРАВЛЕНЧЕСКИЕ ЭТАПЫ РАБОТ ДОПОЛНИТЕЛЬНЫХ СОГЛАШЕНИЙ» В 1С")
    verbose_print("=" * 80)

    if com_object is None:
        verbose_print("Ошибка: com_object обязателен")
        return False

    verbose_print(f"\n[1/3] Подключение к базе данных: {sqlite_db_file}")
    connection = connect_to_sqlite(sqlite_db_file)
    if not connection:
        verbose_print("Не удалось подключиться к SQLite.")
        return False

    try:
        verbose_print("\n[2/3] Настройка режима обмена данными...")
        setup_exchange_mode(com_object)

        verbose_print("\n[3/3] Чтение и запись элементов...")
        items = get_from_db(connection, "managerial_stages_additional_agreements")
        
        if not items:
            verbose_print("Нет данных для выгрузки.")
            return False

        verbose_print(f"Найдено элементов: {len(items)}")

        if process_func:
            items = process_func(items)

        success_count = 0
        error_count = 0

        for i, item in enumerate(items, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(items)}")

            if _write_item(com_object, item, processed_db=sqlite_db_file):
                success_count += 1
            else:
                error_count += 1

        verbose_print(f"\nЭкспортировано справочник 'managerial_stages_additional_agreements' в приемник - {success_count} записей (БД: {sqlite_db_file})")
        
        if error_count > 0:
            verbose_print(f"Предупреждение: {error_count} записей не удалось экспортировать.")

        return error_count == 0

    finally:
        connection.close()

