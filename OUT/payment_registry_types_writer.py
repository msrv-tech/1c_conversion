# -*- coding: utf-8 -*-
"""
Модуль выгрузки custom_видыреестраплатежейcustom из обработанной БД в 1С приемник.
Данные в БД уже в формате приемника, запись происходит с сохранением UUID.
"""

import os
import sys
import sqlite3
import json

# Добавляем родительскую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field, get_reference_by_uuid
from tools.base_writer import write_catalog_item, setup_exchange_mode, create_reference_by_uuid
from tools.onec_connector import safe_getattr, call_if_callable
from tools.logger import verbose_print  # noqa: E402

fix_encoding()


def _write_conditions_tabular_section(com_object, item_obj, item_uuid, processed_db):
    """
    Заполняет табличную часть "Условия" для элемента справочника custom_ВидыРеестраПлатежейCUSTOM.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item_obj: Объект элемента справочника
        item_uuid: UUID элемента
        processed_db: Путь к обработанной БД
    """
    verbose_print(f"    → Заполнение табличной части Условия для элемента {item_uuid[:8]}...")
    
    try:
        # Читаем данные из БД
        conn = sqlite3.connect(processed_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT * FROM payment_registry_types_conditions 
                WHERE parent_uuid = ?
                ORDER BY НомерСтроки
            ''', (item_uuid,))
            
            rows = cursor.fetchall()
        except sqlite3.OperationalError:
            # Таблица может отсутствовать
            rows = []
        finally:
            conn.close()
        
        if not rows:
            verbose_print(f"    → Нет данных для табличной части Условия")
            return
        
        # Получаем табличную часть Условия
        tabular_section = safe_getattr(item_obj, "Условия", None)
        if not tabular_section:
            verbose_print(f"    ⚠ Табличная часть Условия не найдена")
            return
        
        tabular_section = call_if_callable(tabular_section)
        if not tabular_section:
            verbose_print(f"    ⚠ Не удалось получить табличную часть Условия")
            return
        
        # Очищаем существующие строки табличной части
        try:
            tabular_section.Очистить()
        except Exception:
            pass
        
        # Записываем строки
        rows_written = 0
        for row in rows:
            try:
                new_row = tabular_section.Добавить()
                row_dict = dict(row)
                
                # Устанавливаем Контрагент
                if 'Контрагент' in row_dict and row_dict['Контрагент']:
                    try:
                        ref_data = parse_reference_field(row_dict['Контрагент'])
                        if ref_data and ref_data.get('uuid') and ref_data.get('uuid') != "00000000-0000-0000-0000-000000000000":
                            ref_type = ref_data.get('type', 'Справочник.Контрагенты')
                            ref_presentation = ref_data.get('presentation', '')
                            
                            # Используем create_reference_by_uuid (найдет по UUID или создаст при необходимости)
                            ref_obj = create_reference_by_uuid(
                                com_object,
                                ref_data['uuid'],
                                ref_type,
                                ref_presentation,
                                None,
                                processed_db
                            )
                            
                            if ref_obj:
                                new_row.Контрагент = ref_obj
                            # Убираем избыточное логирование для каждой строки
                    except Exception as e:
                        verbose_print(f"    ⚠ Ошибка установки Контрагент: {e}")
                
                # Устанавливаем СтатьяОборотов
                if 'СтатьяОборотов' in row_dict and row_dict['СтатьяОборотов']:
                    try:
                        ref_data = parse_reference_field(row_dict['СтатьяОборотов'])
                        if ref_data and ref_data.get('uuid') and ref_data.get('uuid') != "00000000-0000-0000-0000-000000000000":
                            # Маппинг типа: СтатьиОборотовПоБюджетам -> СтатьиДоходовИРасходов
                            source_type = ref_data.get('type', '')
                            if source_type == 'Справочник.СтатьиОборотовПоБюджетам':
                                target_type = 'Справочник.СтатьиДоходовИРасходов'
                            else:
                                target_type = source_type
                            
                            # Используем create_reference_by_uuid для правильного маппинга
                            ref_obj = create_reference_by_uuid(
                                com_object,
                                ref_data['uuid'],
                                target_type,
                                ref_data.get('presentation', ''),
                                None,
                                processed_db
                            )
                            if ref_obj:
                                new_row.СтатьяОборотов = ref_obj
                            # Убираем избыточное логирование для каждой строки
                    except Exception as e:
                        verbose_print(f"    ⚠ Ошибка установки СтатьяОборотов: {e}")
                
                rows_written += 1
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при добавлении строки в табличную часть: {e}")
        
        verbose_print(f"    ✓ Записано строк в табличную часть Условия: {rows_written}")
        
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при заполнении табличной части Условия: {e}")
        import traceback
        traceback.print_exc()


def _write_item(com_object, item_data, processed_db=None):
    """
    Записывает элемент в 1С с сохранением UUID.
    Заполняет табличную часть "Условия" до записи элемента.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item_data: Словарь с данными элемента
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        
    Returns:
        True если успешно, False если ошибка
    """
    from tools.base_writer import prepare_catalog_item, finalize_catalog_item
    
    uuid_value = item_data.get('uuid', '')
    
    # Подготавливаем элемент (создаем/находим и заполняем поля, но не записываем)
    item = prepare_catalog_item(
        com_object,
        item_data,
        "custom_ВидыРеестраПлатежейCUSTOM",
        "Справочник.custom_ВидыРеестраПлатежейCUSTOM",
        ["Код", "Наименование", "ПометкаУдаления", "Комментарий"],
        processed_db=processed_db
    )
    
    if not item:
        return False
    
    # Заполняем табличную часть Условия ДО записи элемента
    if processed_db and uuid_value:
        _write_conditions_tabular_section(com_object, item, uuid_value, processed_db)
    
    # Завершаем запись элемента (записываем с табличной частью)
    return finalize_catalog_item(
        com_object=com_object,
        item=item,
        item_data=item_data,
        catalog_name="custom_ВидыРеестраПлатежейCUSTOM",
        type_name="Справочник.custom_ВидыРеестраПлатежейCUSTOM",
        processed_db=processed_db
    )


def write_payment_registry_types_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает custom_видыреестраплатежейcustom из обработанной БД в 1С приемник.
    Запись происходит в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.
    
    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) - обязательный параметр
        process_func: Не используется (данные уже обработаны)
    
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА CUSTOM_ВИДЫРЕЕСТРАПЛАТЕЖЕЙCUSTOM ИЗ ОБРАБОТАННОЙ БД В 1С ПРИЕМНИК")
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
    verbose_print("\n[2/3] Чтение custom_видыреестраплатежейcustom из обработанной БД...")
    items = get_from_db(db_connection, "payment_registry_types")
    db_connection.close()
    
    if not items:
        verbose_print("custom_ВидыРеестраПлатежейCUSTOM не найдены в базе данных")
        return False
    
    verbose_print(f"Прочитано custom_видыреестраплатежейcustom: {len(items)}")
    
    # Шаг 3: Подключение к 1С и запись
    setup_exchange_mode(com_object)
    
    # Записываем элементы
    verbose_print(f"\nНачинаем запись {len(items)} custom_видыреестраплатежейcustom...")
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

