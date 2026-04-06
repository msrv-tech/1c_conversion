# -*- coding: utf-8 -*-
"""
Модуль для ручного маппинга полей, типов и значений перечислений между конфигурациями 1С.

Содержит функции для добавления и обновления маппингов вручную.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Optional

# Добавляем путь к корню проекта для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding

fix_encoding()


def ensure_manual_column(conn: sqlite3.Connection) -> None:
    """Убеждается, что колонка is_manual существует в таблице field_mapping"""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(field_mapping)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'is_manual' not in columns:
        cursor.execute("ALTER TABLE field_mapping ADD COLUMN is_manual INTEGER DEFAULT 0")
        conn.commit()


def ensure_search_method_column(conn: sqlite3.Connection) -> None:
    """Добавляет колонку search_method, если её нет."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(field_mapping)")
    columns = [col[1] for col in cursor.fetchall()]
    if "search_method" not in columns:
        cursor.execute("ALTER TABLE field_mapping ADD COLUMN search_method TEXT")
        conn.commit()


def ensure_enum_value_mapping_table(mapping_db_path: str) -> None:
    """Убеждается, что таблица enumeration_value_mapping существует"""
    conn = sqlite3.connect(mapping_db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS enumeration_value_mapping (
            source_enum_type TEXT NOT NULL,
            source_value TEXT NOT NULL,
            target_enum_type TEXT NOT NULL,
            target_value TEXT NOT NULL,
            is_manual INTEGER DEFAULT 0,
            PRIMARY KEY (source_enum_type, source_value)
        )
    """)
    
    conn.commit()
    conn.close()


def export_mapping_to_json_compact(mapping_db: str, json_output: Optional[str] = None) -> bool:
    """
    Экспортирует маппинг в компактный JSON файл для ручного отслеживания.
    Формат: массив объектов, каждый содержит имя объекта и массив строк вида "field -> target (m)".
    Добавляет информацию о целевом объекте и типах только если они отличаются от источника.
    
    Args:
        mapping_db: Путь к базе маппинга
        json_output: Путь к JSON файлу (если None, используется mapping_db с расширением .json)
    """
    if not Path(mapping_db).exists():
        print(f"Ошибка: база маппинга не найдена: {mapping_db}")
        return False
    
    if json_output is None:
        json_output = str(Path(mapping_db).with_suffix('.json'))
    
    try:
        conn = sqlite3.connect(mapping_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Сначала получаем маппинг объектов из object_mapping для использования, если target_object_name не заполнен
        cursor.execute("""
            SELECT object_type, source_name, target_name
            FROM object_mapping
            WHERE source_name != target_name
        """)
        object_mapping_dict = {}
        for row in cursor.fetchall():
            obj_key = f"{row['object_type']}.{row['source_name']}"
            object_mapping_dict[obj_key] = row['target_name']
        
        # Экспортируем только matched и manual маппинги, включая информацию о целевых объектах и типах
        cursor.execute("""
            SELECT object_type, object_name, field_name, target_field_name, 
                   is_manual, field_kind, section_name, target_section_name, status,
                   target_object_name, source_type, target_type
            FROM field_mapping 
            WHERE status = 'matched' OR is_manual = 1
            ORDER BY object_type, object_name, field_kind, section_name, field_name
        """)
        
        # Группируем по объектам
        mapping_data = {}
        for row in cursor.fetchall():
            obj_key = f"{row['object_type']}.{row['object_name']}"
            if obj_key not in mapping_data:
                # Определяем target_object_name при создании объекта
                # Приоритет: object_mapping > field_mapping > object_name
                target_obj_name = row['object_name']  # По умолчанию = object_name
                if obj_key in object_mapping_dict:
                    target_obj_name = object_mapping_dict[obj_key]
                elif 'target_object_name' in row.keys() and row['target_object_name']:
                    target_obj_name = row['target_object_name']
                
                mapping_data[obj_key] = {
                    'mappings': [],
                    'tabular_sections': {},
                    'target_object_name': target_obj_name,
                    'object_type': row['object_type'],
                    'object_name': row['object_name']
                }
            
            # Обновляем target_object_name, если нашли лучшее значение
            # Приоритет: object_mapping > field_mapping > текущее значение
            current_target = mapping_data[obj_key]['target_object_name']
            if current_target == mapping_data[obj_key]['object_name']:
                # Текущее значение = object_name, пытаемся найти лучшее
                if obj_key in object_mapping_dict:
                    mapping_data[obj_key]['target_object_name'] = object_mapping_dict[obj_key]
                elif 'target_object_name' in row.keys() and row['target_object_name']:
                    mapping_data[obj_key]['target_object_name'] = row['target_object_name']
            
            field_name = row['field_name']
            target_name = row['target_field_name'] or 'N/A'
            is_manual = bool(row['is_manual'])
            field_kind = row['field_kind']
            section_name = row['section_name'] or ''
            target_section_name = row['target_section_name'] or ''
            source_type = row['source_type'] if 'source_type' in row.keys() and row['source_type'] else ''
            target_type = row['target_type'] if 'target_type' in row.keys() and row['target_type'] else ''
            
            # Формируем строку маппинга с информацией о типах, если они отличаются
            type_info = ''
            if source_type and target_type and source_type != target_type:
                # Проверяем, является ли это мультитипами (есть запятые в типах или очень длинный список)
                source_has_comma = ',' in source_type
                target_has_comma = ',' in target_type
                # Считаем количество типов (примерно по запятым + 1)
                source_type_count = source_type.count(',') + 1 if source_has_comma else 1
                target_type_count = target_type.count(',') + 1 if target_has_comma else 1
                
                # Если есть запятые или больше 2 типов, считаем это мультитипами
                if source_has_comma or target_has_comma or source_type_count > 2 or target_type_count > 2:
                    type_info = " [Мультитипы]"
                else:
                    type_info = f" [{source_type} -> {target_type}]"
            
            # Для табличных частей группируем по секциям
            if field_kind in ('tabular_attribute', 'tabular_requisite'):
                # Пропускаем несопоставленные табличные части
                if row['status'] != 'matched' and not is_manual:
                    continue
                
                # Создаем ключ для секции
                section_key = section_name or 'unnamed'
                if section_key not in mapping_data[obj_key]['tabular_sections']:
                    mapping_data[obj_key]['tabular_sections'][section_key] = {
                        'section': section_name,
                        'target_section': target_section_name,
                        'mappings': []
                    }
                
                # Формат: "field -> target (m)" для ручных или "field -> target" для автоматических
                # Добавляем информацию о типах, если они отличаются
                if is_manual:
                    mapping_str = f"{field_name} -> {target_name} (m){type_info}"
                else:
                    mapping_str = f"{field_name} -> {target_name}{type_info}"
                
                mapping_data[obj_key]['tabular_sections'][section_key]['mappings'].append(mapping_str)
            else:
                # Обычные поля добавляем в основной массив
                if is_manual:
                    mapping_str = f"{field_name} -> {target_name} (m){type_info}"
                else:
                    mapping_str = f"{field_name} -> {target_name}{type_info}"
                
                mapping_data[obj_key]['mappings'].append(mapping_str)
        
        conn.close()
        
        # Преобразуем в список объектов с маппингами
        result = []
        for obj_key, data in sorted(mapping_data.items()):
            obj_type = data['object_type']
            obj_name = data['object_name']
            target_obj_name = data['target_object_name'] or obj_name
            
            obj_result = {
                "object": obj_key
            }
            
            # Добавляем target_object сразу после object, только если имена отличаются
            if target_obj_name != obj_name:
                target_obj_key = f"{obj_type}.{target_obj_name}"
                obj_result["target_object"] = target_obj_key
            
            # Добавляем mappings после target_object
            obj_result["mappings"] = sorted(data['mappings'])
            
            # Добавляем табличные части, если они есть
            if data['tabular_sections']:
                tabular_list = []
                for section_key in sorted(data['tabular_sections'].keys()):
                    section_data = data['tabular_sections'][section_key]
                    section_item = {
                        "section": section_data['section'],
                        "mappings": sorted(section_data['mappings'])
                    }
                    # Добавляем target_section только если имена отличаются
                    if section_data['target_section'] and section_data['target_section'] != section_data['section']:
                        section_item["target_section"] = section_data['target_section']
                    tabular_list.append(section_item)
                obj_result["tabular_sections"] = tabular_list
            
            result.append(obj_result)
        
        # Сохраняем в JSON
        with open(json_output, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        file_size = Path(json_output).stat().st_size / 1024  # KB
        print(f"✅ Компактный маппинг экспортирован: {json_output} ({file_size:.1f} KB)")
        return True
        
    except Exception as e:
        print(f"Ошибка при экспорте компактного маппинга: {e}")
        import traceback
        traceback.print_exc()
        return False


def add_manual_field_mapping(
    mapping_db: str,
    object_type: str,
    object_name: str,
    field_name: str,
    target_field_name: str,
    source_type: Optional[str] = None,
    target_type: Optional[str] = None,
    target_object_name: Optional[str] = None,
    search_method: Optional[str] = None,
    field_kind: Optional[str] = None,
    section_name: Optional[str] = None,
    target_section_name: Optional[str] = None,
) -> bool:
    """
    Добавляет ручной маппинг поля.
    
    Args:
        mapping_db: Путь к базе маппинга
        object_type: Тип объекта ('catalog', 'document')
        object_name: Имя объекта источника
        field_name: Имя поля источника
        target_field_name: Имя поля приемника
        source_type: Тип поля источника (опционально)
        target_type: Тип поля приемника (опционально)
        target_object_name: Имя объекта приемника (опционально)
        search_method: Способ поиска (опционально)
        field_kind: Вид поля ('requisite', 'tabular_attribute', 'tabular_requisite') (опционально)
        section_name: Имя табличной части источника (опционально)
        target_section_name: Имя табличной части приемника (опционально)
    
    Returns:
        True если маппинг добавлен успешно
    """
    if not Path(mapping_db).exists():
        print(f"Ошибка: база маппинга не найдена: {mapping_db}")
        return False
    
    conn = sqlite3.connect(mapping_db)
    ensure_manual_column(conn)
    ensure_search_method_column(conn)
    cursor = conn.cursor()
    
    # Определяем field_kind и section_name
    # Если не указаны, пытаемся найти в существующей записи
    actual_field_kind = field_kind
    actual_section_name = section_name or ""
    actual_target_section_name = target_section_name or ""
    
    # Получаем существующую запись или создаем новую
    if field_kind and section_name:
        # Ищем по полному ключу (включая field_kind и section_name)
        cursor.execute("""
            SELECT field_kind, section_name, source_type, target_type, target_section_name
            FROM field_mapping
            WHERE object_type = ? AND object_name = ? AND field_kind = ? AND section_name = ? AND field_name = ?
            LIMIT 1
        """, (object_type, object_name, field_kind, section_name or "", field_name))
    else:
        # Ищем только по object_type, object_name, field_name (старый способ)
        cursor.execute("""
            SELECT field_kind, section_name, source_type, target_type, target_section_name
            FROM field_mapping
            WHERE object_type = ? AND object_name = ? AND field_name = ?
            LIMIT 1
        """, (object_type, object_name, field_name))
    
    existing = cursor.fetchone()
    
    if existing:
        existing_field_kind, existing_section_name, existing_source_type, existing_target_type, existing_target_section = existing
        # Используем существующие значения, если не указаны новые
        if not actual_field_kind:
            actual_field_kind = existing_field_kind or 'requisite'
        if not actual_section_name:
            actual_section_name = existing_section_name or ""
        if not actual_target_section_name and existing_target_section:
            actual_target_section_name = existing_target_section or ""
        
        # Обновляем существующую запись
        if field_kind and section_name:
            # Обновляем по полному ключу
            cursor.execute("""
                UPDATE field_mapping
                SET target_field_name = ?,
                    target_type = ?,
                    source_type = ?,
                    target_object_name = COALESCE(?, target_object_name),
                    target_section_name = COALESCE(?, target_section_name),
                    status = 'matched',
                    is_manual = 1,
                    search_method = COALESCE(?, search_method)
                WHERE object_type = ? AND object_name = ? AND field_kind = ? AND section_name = ? AND field_name = ?
            """, (
                target_field_name,
                target_type or existing_target_type,
                source_type or existing_source_type,
                target_object_name,
                actual_target_section_name,
                search_method,
                object_type,
                object_name,
                actual_field_kind,
                actual_section_name,
                field_name
            ))
        else:
            # Обновляем по старому ключу (для обратной совместимости)
            cursor.execute("""
                UPDATE field_mapping
                SET target_field_name = ?,
                    target_type = ?,
                    source_type = ?,
                    target_object_name = COALESCE(?, target_object_name),
                    target_section_name = COALESCE(?, target_section_name),
                    status = 'matched',
                    is_manual = 1,
                    search_method = COALESCE(?, search_method)
                WHERE object_type = ? AND object_name = ? AND field_name = ?
            """, (
                target_field_name,
                target_type or existing_target_type,
                source_type or existing_source_type,
                target_object_name,
                actual_target_section_name,
                search_method,
                object_type,
                object_name,
                field_name
            ))
    else:
        # Создаем новую запись
        if not actual_field_kind:
            actual_field_kind = 'requisite'
        
        cursor.execute("""
            INSERT INTO field_mapping (
                object_type, object_name, field_kind, section_name,
                field_name, source_type, target_object_name,
                target_field_kind, target_section_name, target_field_name, target_type,
                status, is_manual, search_method
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            object_type,
            object_name,
            actual_field_kind,
            actual_section_name,  # Используем пустую строку вместо None для PRIMARY KEY
            field_name,
            source_type,
            target_object_name or object_name,
            actual_field_kind,
            actual_target_section_name,
            target_field_name,
            target_type,
            'matched',
            1,  # is_manual = 1
            search_method
        ))
    
    conn.commit()
    conn.close()
    
    print(f"✅ Ручной маппинг поля добавлен:")
    print(f"   {object_type}.{object_name}.{field_name} → {target_field_name}")
    if source_type:
        print(f"   Тип источника: {source_type}")
    if target_type:
        print(f"   Тип приемника: {target_type}")
    if search_method:
        print(f"   Способ поиска: {search_method}")
    
    # Обновляем JSON (компактный формат)
    export_mapping_to_json_compact(mapping_db)
    
    return True


def add_manual_type_mapping(
    mapping_db: str,
    source_type: str,
    target_type: str,
    status: str = 'mapped',
) -> bool:
    """
    Добавляет или обновляет ручной маппинг типа объекта (справочника, документа и т.д.).
    
    Универсальная функция для маппинга типов объектов между конфигурациями.
    Например: Справочник.УправленческиеДоговоры → Справочник.custom_УправленческиеДоговоры
    
    Args:
        mapping_db: Путь к базе маппинга
        source_type: Тип объекта источника (например, "Справочник.УправленческиеДоговоры")
        target_type: Тип объекта приемника (например, "Справочник.custom_УправленческиеДоговоры")
        status: Статус маппинга ('mapped', 'missing_target', 'unmatched') (по умолчанию 'mapped')
    
    Returns:
        True если маппинг добавлен/обновлен успешно
    """
    if not Path(mapping_db).exists():
        print(f"Ошибка: база маппинга не найдена: {mapping_db}")
        return False
    
    conn = sqlite3.connect(mapping_db)
    cursor = conn.cursor()
    
    # Проверяем, существует ли запись
    cursor.execute("SELECT source_type, target_type, status FROM type_mapping WHERE source_type = ?", (source_type,))
    existing = cursor.fetchone()
    
    if existing:
        # Обновляем существующую запись
        cursor.execute("""
            UPDATE type_mapping 
            SET target_type = ?, status = ?
            WHERE source_type = ?
        """, (target_type, status, source_type))
        
        print(f"✅ Маппинг типа обновлен:")
        print(f"   {source_type} → {target_type}")
        print(f"   Статус: {status}")
    else:
        # Создаем новую запись
        cursor.execute("""
            INSERT INTO type_mapping (source_type, target_type, status)
            VALUES (?, ?, ?)
        """, (source_type, target_type, status))
        
        print(f"✅ Маппинг типа добавлен:")
        print(f"   {source_type} → {target_type}")
        print(f"   Статус: {status}")
    
    conn.commit()
    conn.close()
    
    return True


def add_manual_enum_value_mapping(
    mapping_db: str,
    source_enum_type: str,
    source_value: str,
    target_enum_type: str,
    target_value: str,
) -> bool:
    """
    Добавляет ручной маппинг значения перечисления.
    
    Args:
        mapping_db: Путь к базе маппинга
        source_enum_type: Тип перечисления источника (например, "Перечисление.ЮрФизЛицо")
        source_value: Значение перечисления источника (например, "ЮрЛицо")
        target_enum_type: Тип перечисления приемника (например, "Перечисление.ЮридическоеФизическоеЛицо")
        target_value: Значение перечисления приемника (например, "ЮридическоеЛицо")
    
    Returns:
        True если маппинг добавлен успешно
    """
    ensure_enum_value_mapping_table(mapping_db)
    
    conn = sqlite3.connect(mapping_db)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO enumeration_value_mapping 
            (source_enum_type, source_value, target_enum_type, target_value, is_manual)
            VALUES (?, ?, ?, ?, ?)
        """, (source_enum_type, source_value, target_enum_type, target_value, 1))
        
        conn.commit()
        print(f"✅ Ручной маппинг значения перечисления добавлен:")
        print(f"   {source_enum_type}.{source_value} → {target_enum_type}.{target_value}")
        
        # Обновляем JSON (компактный формат)
        export_mapping_to_json_compact(mapping_db)
        
        return True
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False
    finally:
        conn.close()


def main() -> None:
    """Главная функция CLI для ручного маппинга"""
    parser = argparse.ArgumentParser(
        description="Ручной маппинг полей, типов и значений перечислений между конфигурациями 1С",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

1. Добавление ручного маппинга поля:
   python tools/manual_mapping.py add-field \\
     --object-type catalog --object-name УправленческиеДоговоры \\
     --field-name КРамочномуДоговору --target-field-name КРамочномуДоговору \\
     --source-type Справочник.УправленческиеДоговоры \\
     --target-type Справочник.custom_УправленческиеДоговоры

2. Добавление ручного маппинга типа объекта:
   python tools/manual_mapping.py add-type \\
     --source-type Справочник.УправленческиеДоговоры \\
     --target-type Справочник.custom_УправленческиеДоговоры

3. Добавление ручного маппинга значения перечисления:
   python tools/manual_mapping.py add-enum-value \\
     --source-enum-type Перечисление.ЮрФизЛицо --source-value ЮрЛицо \\
     --target-enum-type Перечисление.ЮридическоеФизическоеЛицо --target-value ЮридическоеЛицо
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Команда')
    
    # Команда add-field - добавление ручного маппинга поля
    add_field_parser = subparsers.add_parser('add-field', help='Добавить ручной маппинг поля')
    add_field_parser.add_argument(
        "--mapping-db",
        default=str(Path("CONF") / "type_mapping.db"),
        help="Путь к базе маппинга"
    )
    add_field_parser.add_argument(
        "--object-type",
        required=True,
        choices=['catalog', 'document'],
        help="Тип объекта"
    )
    add_field_parser.add_argument(
        "--object-name",
        required=True,
        help="Имя объекта источника"
    )
    add_field_parser.add_argument(
        "--field-name",
        required=True,
        help="Имя поля источника"
    )
    add_field_parser.add_argument(
        "--target-field-name",
        required=True,
        help="Имя поля приемника"
    )
    add_field_parser.add_argument(
        "--source-type",
        help="Тип поля источника"
    )
    add_field_parser.add_argument(
        "--target-type",
        help="Тип поля приемника"
    )
    add_field_parser.add_argument(
        "--target-object-name",
        help="Имя объекта приемника (по умолчанию такое же как источник)"
    )
    add_field_parser.add_argument(
        "--field-kind",
        choices=['requisite', 'attribute', 'tabular_attribute', 'tabular_requisite'],
        help="Тип поля (requisite, attribute, tabular_attribute, tabular_requisite)"
    )
    add_field_parser.add_argument(
        "--section-name",
        help="Имя табличной части (для tabular_attribute/tabular_requisite)"
    )
    add_field_parser.add_argument(
        "--target-section-name",
        help="Имя табличной части в приемнике (для tabular_attribute/tabular_requisite)"
    )
    add_field_parser.add_argument(
        "--search-method",
        help="Способ поиска (например, string_to_reference_by_name)"
    )
    
    # Команда add-type - добавление ручного маппинга типа объекта
    add_type_parser = subparsers.add_parser('add-type', help='Добавить ручной маппинг типа объекта')
    add_type_parser.add_argument(
        "--mapping-db",
        default=str(Path("CONF") / "type_mapping.db"),
        help="Путь к базе маппинга"
    )
    add_type_parser.add_argument(
        "--source-type",
        required=True,
        help="Тип объекта источника (например, Справочник.УправленческиеДоговоры)"
    )
    add_type_parser.add_argument(
        "--target-type",
        required=True,
        help="Тип объекта приемника (например, Справочник.custom_УправленческиеДоговоры)"
    )
    add_type_parser.add_argument(
        "--status",
        default='mapped',
        choices=['mapped', 'missing_target', 'unmatched'],
        help="Статус маппинга (по умолчанию: mapped)"
    )
    
    # Команда add-enum-value - добавление ручного маппинга значения перечисления
    add_enum_parser = subparsers.add_parser('add-enum-value', help='Добавить ручной маппинг значения перечисления')
    add_enum_parser.add_argument(
        "--mapping-db",
        default=str(Path("CONF") / "type_mapping.db"),
        help="Путь к базе маппинга"
    )
    add_enum_parser.add_argument(
        "--source-enum-type",
        required=True,
        help="Тип перечисления источника (например, Перечисление.ЮрФизЛицо)"
    )
    add_enum_parser.add_argument(
        "--source-value",
        required=True,
        help="Значение перечисления источника (например, ЮрЛицо)"
    )
    add_enum_parser.add_argument(
        "--target-enum-type",
        required=True,
        help="Тип перечисления приемника (например, Перечисление.ЮридическоеФизическоеЛицо)"
    )
    add_enum_parser.add_argument(
        "--target-value",
        required=True,
        help="Значение перечисления приемника (например, ЮридическоеЛицо)"
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Получаем путь к базе маппинга из args
    mapping_db = getattr(args, 'mapping_db', str(Path("CONF") / "type_mapping.db"))
    
    if args.command == 'add-field':
        # Добавление ручного маппинга поля
        success = add_manual_field_mapping(
            mapping_db,
            args.object_type,
            args.object_name,
            args.field_name,
            args.target_field_name,
            getattr(args, 'source_type', None),
            getattr(args, 'target_type', None),
            getattr(args, 'target_object_name', None),
            getattr(args, 'search_method', None),
            getattr(args, 'field_kind', None),
            getattr(args, 'section_name', None),
            getattr(args, 'target_section_name', None)
        )
        sys.exit(0 if success else 1)
    elif args.command == 'add-type':
        # Добавление ручного маппинга типа объекта
        success = add_manual_type_mapping(
            mapping_db,
            args.source_type,
            args.target_type,
            args.status
        )
        sys.exit(0 if success else 1)
    elif args.command == 'add-enum-value':
        # Добавление ручного маппинга значения перечисления
        success = add_manual_enum_value_mapping(
            mapping_db,
            args.source_enum_type,
            args.source_value,
            args.target_enum_type,
            args.target_value
        )
        sys.exit(0 if success else 1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()

