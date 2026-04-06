# -*- coding: utf-8 -*-
"""
Утилита для дозаполнения созданных объектов ссылочных типов.

Читает созданные объекты из БД и заполняет их полными данными из исходной БД.
"""

import os
import sqlite3
import sys
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding
from tools.onec_connector import connect_to_1c, find_object_by_uuid, safe_getattr, call_if_callable
from tools.reference_objects import get_reference_objects, mark_reference_filled, get_reference_objects_db_path
from tools.base_writer import write_catalog_item
from tools.base_processor import MappingProcessor

fix_encoding()


def get_table_name_by_type(ref_type: str, connection: sqlite3.Connection) -> Optional[str]:
    """
    Определяет имя таблицы по типу ссылки.
    
    Args:
        ref_type: Тип ссылки (например, "Справочник.Контрагенты")
        connection: Подключение к SQLite
    
    Returns:
        Имя таблицы или None
    """
    if not ref_type.startswith("Справочник."):
        return None
    
    catalog_name = ref_type.replace("Справочник.", "")
    
    # Маппинг типов на имена таблиц
    type_to_table = {
        "ВидыНоменклатуры": "nomenclature_types",
        "Контрагенты": "contractors",
        "НоменклатурныеГруппы": "nomenclature_groups",
        "Номенклатура": "nomenclature",
        # Добавить другие по необходимости
    }
    
    # Сначала пробуем маппинг
    if catalog_name in type_to_table:
        table_name = type_to_table[catalog_name]
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if cursor.fetchone():
            return table_name
    
    # Если не нашли, пробуем поиск по частичному совпадению
    cursor = connection.cursor()
    search_pattern = catalog_name.lower().replace("ы", "").replace("и", "").replace("е", "")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?", (f"%{search_pattern}%",))
    tables = [row[0] for row in cursor.fetchall()]
    
    if tables:
        return tables[0]
    
    return None


def fill_created_references(
    source_db: str,
    target_db_path: str,
    mapping_db_path: str = "CONF/type_mapping.db",
    reference_objects_db: Optional[str] = None
) -> bool:
    """
    Дозаполняет созданные объекты полными данными из исходной БД.
    
    Args:
        source_db: Путь к исходной БД (для получения полных данных)
        target_db_path: Путь к базе 1С приемника
        mapping_db_path: Путь к БД маппинга
        reference_objects_db: Путь к БД ссылочных объектов (если None, используется единая БД)
    
    Returns:
        True если успешно, False если ошибка
    """
    print("=" * 80)
    print("ДОЗАПОЛНЕНИЕ СОЗДАННЫХ ОБЪЕКТОВ")
    print("=" * 80)
    
    # Определяем путь к БД ссылочных объектов
    if reference_objects_db is None:
        reference_objects_db = get_reference_objects_db_path()
    
    print(f"БД ссылочных объектов: {reference_objects_db}")
    
    # Подключаемся к БД
    conn = sqlite3.connect(reference_objects_db)
    created_refs = get_reference_objects(conn, filled=0)
    conn.close()
    
    if not created_refs:
        print("Нет созданных объектов для дозаполнения")
        return True
    
    print(f"\nНайдено созданных объектов для дозаполнения: {len(created_refs)}")
    
    # Группируем по типам
    by_type = {}
    for ref in created_refs:
        ref_type = ref['ref_type']
        if ref_type not in by_type:
            by_type[ref_type] = []
        by_type[ref_type].append(ref)
    
    # Подключаемся к 1С
    com_object = connect_to_1c(target_db_path)
    if not com_object:
        print("Ошибка: Не удалось подключиться к базе данных 1С (приемник)")
        return False
    
    # Устанавливаем режим обмена данными
    try:
        exchange_data = safe_getattr(com_object, "ОбменДанными", None)
        if exchange_data:
            exchange_data = call_if_callable(exchange_data)
            if exchange_data:
                exchange_data.Загрузка = True
    except Exception:
        pass
    
    # Подключаемся к исходной БД
    source_conn = sqlite3.connect(source_db)
    
    filled_count = 0
    error_count = 0
    
    for ref_type, refs in by_type.items():
        print(f"\nОбработка типа: {ref_type} ({len(refs)} объектов)")
        
        # Определяем имя таблицы
        table_name = get_table_name_by_type(ref_type, source_conn)
        if not table_name:
            print(f"  ⚠ Таблица для типа {ref_type} не найдена в исходной БД")
            continue
        
        # Определяем имя справочника для записи
        if ref_type.startswith("Справочник."):
            catalog_name = ref_type.replace("Справочник.", "")
        else:
            continue
        
        # Создаем процессор для маппинга (если нужен)
        processor = None
        try:
            processor = MappingProcessor(mapping_db_path, catalog_name, "catalog")
        except Exception:
            pass
        
        for ref in refs:
            ref_uuid = ref['ref_uuid']
            ref_presentation = ref.get('ref_presentation', '')
            
            # Получаем данные из исходной БД
            source_cursor = source_conn.cursor()
            source_cursor.execute(f"SELECT * FROM {table_name} WHERE uuid = ?", (ref_uuid,))
            row = source_cursor.fetchone()
            
            if not row:
                print(f"  ⚠ Объект {ref_type} '{ref_presentation}' не найден в исходной БД")
                continue
            
            # Формируем словарь данных
            column_names = [desc[0] for desc in source_cursor.description]
            item_data = {}
            for i, col_name in enumerate(column_names):
                item_data[col_name] = row[i]
            
            # Обрабатываем через маппинг, если есть процессор
            if processor:
                try:
                    item_data = processor.process_item(item_data)
                except Exception as e:
                    print(f"  ⚠ Ошибка при обработке через маппинг: {e}")
            
            # Находим объект в приемнике
            ref_obj = find_object_by_uuid(com_object, ref_uuid, ref_type)
            if not ref_obj:
                print(f"  ⚠ Объект {ref_type} '{ref_presentation}' не найден в приемнике")
                continue
            
            # Получаем объект для редактирования
            try:
                item = ref_obj.ПолучитьОбъект()
                
                # Заполняем поля (кроме стандартных, которые уже заполнены)
                standard_fields = ['Код', 'Наименование', 'Комментарий']
                fields_filled = 0
                
                for field_name, field_value in item_data.items():
                    if field_name in ('uuid', 'Ссылка') or field_name in standard_fields:
                        continue
                    if field_name.endswith('_UUID') or field_name.endswith('_Представление') or field_name.endswith('_Тип'):
                        continue
                    
                    try:
                        # Парсим ссылочные поля
                        from tools.writer_utils import parse_reference_field, get_reference_by_uuid
                        ref_data = parse_reference_field(field_value)
                        if ref_data:
                            ref_uuid_val = ref_data.get('uuid', '')
                            ref_type_val = ref_data.get('type', '')
                            if ref_uuid_val and ref_type_val:
                                ref_obj_val = get_reference_by_uuid(com_object, ref_uuid_val, ref_type_val)
                                if ref_obj_val:
                                    setattr(item, field_name, ref_obj_val)
                                    fields_filled += 1
                        else:
                            # Обычное поле
                            if field_value is not None and field_value != '':
                                setattr(item, field_name, field_value)
                                fields_filled += 1
                    except Exception:
                        pass
                
                # Записываем
                item.Записать()
                print(f"  ✓ Заполнен {ref_type} '{ref_presentation}' ({fields_filled} полей)")
                
                # Помечаем как заполненный
                conn = sqlite3.connect(reference_objects_db)
                mark_reference_filled(conn, ref_uuid, ref_type)
                conn.close()
                
                filled_count += 1
            except Exception as e:
                print(f"  ✗ Ошибка при заполнении {ref_type} '{ref_presentation}': {e}")
                error_count += 1
    
    source_conn.close()
    
    print("\n" + "=" * 80)
    print("ИТОГИ ДОЗАПОЛНЕНИЯ:")
    print(f"  Заполнено: {filled_count}")
    print(f"  Ошибок: {error_count}")
    print("=" * 80)
    
    return filled_count > 0


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Дозаполнение созданных объектов ссылочных типов"
    )
    parser.add_argument(
        "--reference-objects-db",
        type=str,
        help="Путь к БД ссылочных объектов (если не указан, используется BD/reference_objects.db)",
    )
    parser.add_argument(
        "--source-db",
        type=str,
        required=True,
        help="Путь к исходной БД (для получения полных данных)",
    )
    parser.add_argument(
        "--target-1c",
        type=str,
        required=True,
        help="Путь к базе данных 1С (приемник)",
    )
    parser.add_argument(
        "--mapping-db",
        type=str,
        default="CONF/type_mapping.db",
        help="Путь к БД маппинга",
    )
    
    args = parser.parse_args()
    success = fill_created_references(
        args.source_db,
        args.target_1c,
        args.mapping_db,
        args.reference_objects_db
    )
    raise SystemExit(0 if success else 1)



