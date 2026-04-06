# -*- coding: utf-8 -*-
"""
Модуль выгрузки номенклатурных групп из обработанной БД в 1С приемник.
Данные в БД уже в формате приемника, запись происходит с сохранением UUID.
"""

import json
import os
import sqlite3
import sys
from typing import Dict, List

# Добавляем родительскую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field, get_reference_by_uuid
from tools.base_writer import write_catalog_item, setup_exchange_mode
from tools.onec_connector import safe_getattr, call_if_callable
from tools.reference_objects import get_reference_objects_db_path
from tools.logger import verbose_print

fix_encoding()


def _write_nomenclature(com_object, nomenclature_data: Dict, processed_db=None) -> bool:
    """
    Записывает элемент номенклатуры в справочник Номенклатура.
    
    Args:
        com_object: COM-объект подключения к 1С
        nomenclature_data: Словарь с данными номенклатуры (должен содержать uuid, Код, Наименование)
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        
    Returns:
        True если успешно, False если ошибка
    """
    return write_catalog_item(
        com_object,
        nomenclature_data,
        "Номенклатура",
        "Справочник.Номенклатура",
        ['Код', 'Наименование', 'ПометкаУдаления', 'Комментарий'],
        processed_db=processed_db
    )


def _write_nomenclature_group(com_object, group_data, db_connection=None, processed_db=None):
    """
    Записывает номенклатурную группу в 1С с сохранением UUID и табличных частей.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        group_data: Словарь с данными номенклатурной группы
        db_connection: Подключение к БД для чтения табличных частей
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        
    Returns:
        True если успешно, False если ошибка
    """
    from tools.onec_connector import safe_getattr
    
    item_name = group_data.get('Наименование', 'Без наименования')
    uuid_value = group_data.get('uuid', '')
    
    # Используем write_catalog_item для записи основных полей и ссылочных полей
    # Он автоматически обработает все ссылочные поля в JSON формате
    standard_fields = ['Код', 'Наименование', 'ПометкаУдаления', 'Комментарий']
    success = write_catalog_item(
        com_object,
        group_data,
        "НоменклатурныеГруппы",
        "Справочник.НоменклатурныеГруппы",
        standard_fields,
        processed_db=processed_db
    )
    
    if not success:
        return False
    
    # После записи через write_catalog_item нужно получить элемент для записи табличных частей
    item = None
    if uuid_value and uuid_value != "00000000-0000-0000-0000-000000000000":
        from tools.onec_connector import find_object_by_uuid
        ref = find_object_by_uuid(com_object, uuid_value, "Справочник.НоменклатурныеГруппы")
        if ref:
            item = ref.ПолучитьОбъект()
    
    if not item:
        verbose_print(f"  ⚠ НоменклатурныеГруппы '{item_name}': не удалось получить элемент для записи табличных частей")
        return True  # Основной элемент уже записан
    
    try:
        
        # Записываем связанную номенклатуру в справочник Номенклатура
        nomenclature_written = 0
        if db_connection:
            try:
                cursor = db_connection.cursor()
                cursor.execute("""
                    SELECT * FROM nomenclature_groups_nomenclature
                    WHERE parent_uuid = ?
                    ORDER BY НомерСтроки
                """, (uuid_value,))
                
                # Получаем имена колонок
                column_names = [desc[0] for desc in cursor.description] if cursor.description else []
                
                nomenclature_items = []
                for row in cursor.fetchall():
                    item_dict = {}
                    for i, col_name in enumerate(column_names):
                        item_dict[col_name] = row[i] if i < len(row) else None
                    nomenclature_items.append(item_dict)
                
                # Записываем каждую номенклатуру в справочник Номенклатура
                for nom_item in nomenclature_items:
                    # Формируем данные для записи в справочник Номенклатура
                    nom_data = {}
                    
                    # Извлекаем UUID номенклатуры
                    nom_uuid = None
                    # Сначала пробуем из поля Номенклатура_UUID
                    if 'Номенклатура_UUID' in nom_item and nom_item['Номенклатура_UUID']:
                        nom_uuid = nom_item['Номенклатура_UUID']
                    # Если нет, парсим JSON из поля Номенклатура
                    elif 'Номенклатура' in nom_item and nom_item['Номенклатура']:
                        # Парсим JSON, если это JSON строка
                        nom_ref = parse_reference_field(nom_item['Номенклатура'])
                        if nom_ref:
                            nom_uuid = nom_ref.get('uuid', '')
                    
                    if not nom_uuid or nom_uuid == "00000000-0000-0000-0000-000000000000":
                        continue
                    
                    # Формируем данные номенклатуры
                    nom_data['uuid'] = nom_uuid
                    if 'Код' in nom_item and nom_item['Код']:
                        nom_data['Код'] = nom_item['Код']
                    if 'Наименование' in nom_item and nom_item['Наименование']:
                        nom_data['Наименование'] = nom_item['Наименование']
                    
                    # Записываем номенклатуру в справочник
                    if _write_nomenclature(com_object, nom_data, processed_db):
                        nomenclature_written += 1
                        
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при чтении/записи связанной номенклатуры: {e}")
                import traceback
                traceback.print_exc()
        
        # Элемент уже записан через write_catalog_item, но если были изменения в табличных частях, нужно записать еще раз
        if nomenclature_written > 0:
            try:
                item.Записать()
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при повторной записи после табличных частей: {e}")
        
        nomenclature_info = f", номенклатура: {nomenclature_written} элементов" if nomenclature_written > 0 else ""
        verbose_print(f"  ✓ НоменклатурныеГруппы '{item_name}' записан{nomenclature_info}")
        
        # Сохраняем информацию о записанном объекте в БД (filled=True - полная запись через основной обработчик)
        if uuid_value and uuid_value != "00000000-0000-0000-0000-000000000000":
            try:
                import sqlite3
                from tools.reference_objects import get_reference_objects_db_path, save_reference_object
                refs_db_path = get_reference_objects_db_path()
                conn = sqlite3.connect(refs_db_path)
                
                # Получаем полные данные из processed_db, если доступны
                source_data = None
                if processed_db:
                    try:
                        source_conn = sqlite3.connect(processed_db)
                        source_cursor = source_conn.cursor()
                        
                        # Ищем в таблице nomenclature_groups
                        source_cursor.execute("SELECT * FROM nomenclature_groups WHERE uuid = ?", (uuid_value,))
                        row = source_cursor.fetchone()
                        if row:
                            column_names = [desc[0] for desc in source_cursor.description]
                            source_data = {}
                            for i, col_name in enumerate(column_names):
                                source_data[col_name] = row[i]
                        
                        source_conn.close()
                    except Exception:
                        pass
                
                # Если source_data не получено, используем group_data
                if not source_data:
                    source_data = group_data
                
                save_reference_object(
                    conn,
                    uuid_value,
                    "Справочник.НоменклатурныеГруппы",
                    item_name,
                    source_data,
                    filled=True,  # Полная запись через основной обработчик
                    parent_type="catalog",
                    parent_name="НоменклатурныеГруппы",
                    parent_uuid="",
                    field_name=""
                )
                conn.close()
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при сохранении информации о записанном объекте: {e}")
        
        return True
    except Exception as e:
        verbose_print(f"  ✗ Ошибка при записи НоменклатурныеГруппы '{item_name}': {e}")
        import traceback
        traceback.print_exc()
        return False
    except Exception as e:
        verbose_print(f"  ✗ Ошибка при обработке НоменклатурныеГруппы '{item_name}': {e}")
        import traceback
        traceback.print_exc()
        return False




def write_nomenclature_groups_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает номенклатурные группы из обработанной БД в 1С приемник.
    Запись происходит в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.
    
    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) - обязательный параметр
        process_func: Не используется (данные уже обработаны)
    
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА НОМЕНКЛАТУРНЫХ ГРУПП ИЗ ОБРАБОТАННОЙ БД В 1С ПРИЕМНИК")
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
    
    # Шаг 2: Чтение номенклатурных групп из БД
    verbose_print("\n[2/3] Чтение номенклатурных групп из обработанной БД...")
    groups = get_from_db(db_connection, "nomenclature_groups")
    # Не закрываем соединение, оно нужно для чтения табличных частей
    
    if not groups:
        verbose_print("Номенклатурные группы не найдены в базе данных")
        return False
    
    verbose_print(f"Прочитано номенклатурных групп: {len(groups)}")
    
    # Шаг 3: Подключение к 1С и запись
    # Устанавливаем режим обмена данными
    setup_exchange_mode(com_object)
    
    # Определяем путь к единой БД для ссылочных объектов
    refs_db_path = get_reference_objects_db_path()
    verbose_print(f"\nБД для ссылочных объектов: {refs_db_path}")
    
    # Записываем номенклатурные группы
    verbose_print(f"\nНачинаем запись {len(groups)} номенклатурных групп...")
    written_count = 0
    error_count = 0
    
    for i, group in enumerate(groups, 1):
        if i % 10 == 0 or i == 1:
            verbose_print(f"\n[{i}/{len(groups)}]")
        
        if _write_nomenclature_group(com_object, group, db_connection, sqlite_db_file):
            written_count += 1
        else:
            error_count += 1
    
    # Закрываем соединение после записи всех групп
    db_connection.close()
    
    verbose_print(f"\n{'='*80}")
    verbose_print(f"ИТОГИ ЗАПИСИ:")
    verbose_print(f"  Успешно записано: {written_count}")
    verbose_print(f"  Ошибок: {error_count}")
    verbose_print(f"  Всего обработано: {len(groups)}")
    verbose_print(f"{'='*80}")
    
    return written_count > 0

