from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple, Any

# Добавляем путь к корню проекта для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding
from tools.manual_mapping import (
    add_manual_field_mapping,
    add_manual_enum_value_mapping,
    export_mapping_to_json_compact,
    ensure_manual_column,
    ensure_search_method_column,
    ensure_enum_value_mapping_table,
)

fix_encoding()


@dataclass
class FieldInfo:
    name: str
    type: str
    kind: str  # 'requisite', 'tabular_attribute', 'tabular_requisite'
    section: Optional[str]


@dataclass
class ObjectInfo:
    name: str
    synonym: str
    full_name: str
    fields: List[FieldInfo]


def normalize(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value.lower() if value else None


def split_types(type_str: Optional[str]) -> List[str]:
    if not type_str:
        return []
    return [part.strip() for part in type_str.split(",") if part.strip()]


def load_objects(db_path: str, table: str) -> Dict[str, ObjectInfo]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row

    objects: Dict[str, ObjectInfo] = {}

    try:
        rows = connection.execute(
            f"SELECT name, synonym, full_name, requisites_json, tabular_sections_json FROM {table}"
        ).fetchall()
    finally:
        connection.close()

    for row in rows:
        requisites = []
        try:
            requisites = json.loads(row["requisites_json"] or "[]")
        except (TypeError, json.JSONDecodeError):
            requisites = []

        sections = []
        try:
            sections = json.loads(row["tabular_sections_json"] or "[]")
        except (TypeError, json.JSONDecodeError):
            sections = []

        fields: List[FieldInfo] = []

        for item in requisites:
            fields.append(
                FieldInfo(
                    name=str(item.get("name", "")),
                    type=str(item.get("type", "")),
                    kind="requisite",
                    section=None,
                )
            )

        for section in sections:
            section_name = str(section.get("name", ""))
            for item in section.get("attributes", []):
                fields.append(
                    FieldInfo(
                        name=str(item.get("name", "")),
                        type=str(item.get("type", "")),
                        kind="tabular_attribute",
                        section=section_name,
                    )
                )
            for item in section.get("requisites", []):
                fields.append(
                    FieldInfo(
                        name=str(item.get("name", "")),
                        type=str(item.get("type", "")),
                        kind="tabular_requisite",
                        section=section_name,
                    )
                )

        objects[row["name"]] = ObjectInfo(
            name=row["name"],
            synonym=row["synonym"],
            full_name=row["full_name"],
            fields=fields,
        )

    return objects


def load_enumerations(db_path: str) -> Dict[str, ObjectInfo]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row

    try:
        rows = connection.execute(
            "SELECT name, synonym, full_name FROM metadata_enumerations"
        ).fetchall()
    finally:
        connection.close()

    enumerations: Dict[str, ObjectInfo] = {}
    for row in rows:
        enumerations[row["name"]] = ObjectInfo(
            name=row["name"],
            synonym=row["synonym"],
            full_name=row["full_name"],
            fields=[],
        )
    return enumerations


def load_enumeration_values(db_path: str) -> Dict[str, List[Dict[str, str]]]:
    """
    Загружает значения перечислений из базы метаданных.
    
    Returns:
        Словарь: enum_name -> список словарей с ключами 'name' и 'presentation'
    """
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    
    result: Dict[str, List[Dict[str, str]]] = {}
    
    try:
        rows = connection.execute(
            "SELECT name, values_json FROM metadata_enumerations WHERE values_json IS NOT NULL"
        ).fetchall()
        
        for row in rows:
            enum_name = row["name"]
            values_json = row["values_json"]
            
            if values_json:
                try:
                    values_data = json.loads(values_json)
                    values = []
                    for val in values_data:
                        val_name = val.get("name", "")
                        val_presentation = val.get("presentation", "")
                        if val_name:
                            values.append({
                                "name": val_name,
                                "presentation": val_presentation
                            })
                    result[enum_name] = values
                except (TypeError, json.JSONDecodeError):
                    pass
    finally:
        connection.close()
    
    return result


def build_index(objects: Dict[str, ObjectInfo]) -> Dict[Optional[str], List[ObjectInfo]]:
    """
    Строит индекс объектов по нормализованным именам.
    Использует только name и full_name (без synonym) для избежания неправильных сопоставлений.
    """
    index: Dict[Optional[str], List[ObjectInfo]] = {}
    for obj in objects.values():
        # Используем только name и full_name, без synonym
        for attr in (obj.name, obj.full_name):
            key = normalize(attr)
            if not key:
                continue
            index.setdefault(key, []).append(obj)
    return index


def gather_types(objects: Iterable[ObjectInfo]) -> Set[str]:
    result: Set[str] = set()
    for obj in objects:
        for field in obj.fields:
            result.update(split_types(field.type))
    return result


def register_type_mapping(
    storage: Dict[str, Dict[str, Set[str]]],
    source_type: str,
    target_types: Sequence[str],
) -> None:
    entry = storage.setdefault(source_type, {"targets": set(), "flags": set()})
    targets = entry["targets"]
    flags = entry["flags"]
    if target_types:
        targets.update(target_types)
        if source_type in target_types:
            flags.add("exact")
        else:
            flags.add("mapped")
    else:
        flags.add("missing")


def create_output_db(path: str) -> Tuple[sqlite3.Connection, List, List, List]:
    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    
    # Сохраняем ручные маппинги объектов перед удалением таблиц
    # Сохраняем не только маппинги с match_key = 'manual', но и все маппинги
    # со статусом 'matched' и непустым target_name (чтобы не перезаписывать ручные маппинги)
    manual_object_mappings = []
    try:
        cursor.execute("""
            SELECT * FROM object_mapping 
            WHERE match_key = 'manual' 
               OR (status = 'matched' AND target_name IS NOT NULL AND target_name != '')
        """)
        manual_object_mappings = cursor.fetchall()
    except sqlite3.OperationalError:
        # Таблица не существует
        pass
    
    # Сохраняем ручные маппинги полей перед удалением таблиц
    manual_field_mappings = []
    try:
        cursor.execute("SELECT * FROM field_mapping WHERE is_manual = 1")
        manual_field_mappings = cursor.fetchall()
    except sqlite3.OperationalError:
        # Таблица не существует или колонка is_manual отсутствует
        try:
            # Пробуем выбрать все и проверить структуру
            cursor.execute("SELECT * FROM field_mapping LIMIT 1")
            # Если таблица существует, проверяем есть ли колонка is_manual
            cursor.execute("PRAGMA table_info(field_mapping)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'is_manual' in columns:
                cursor.execute("SELECT * FROM field_mapping WHERE is_manual = 1")
                manual_field_mappings = cursor.fetchall()
        except sqlite3.OperationalError:
            pass
    
    # Сохраняем ручные маппинги значений перечислений перед удалением таблиц
    manual_enum_value_mappings = []
    try:
        cursor.execute("SELECT * FROM enumeration_value_mapping WHERE is_manual = 1")
        manual_enum_value_mappings = cursor.fetchall()
    except sqlite3.OperationalError:
        # Таблица может не существовать
        pass
    
    # Удаляем существующие таблицы, если они есть
    cursor.execute("DROP TABLE IF EXISTS object_mapping")
    cursor.execute("DROP TABLE IF EXISTS field_mapping")
    cursor.execute("DROP TABLE IF EXISTS type_mapping")
    cursor.execute("DROP TABLE IF EXISTS target_unmatched_objects")
    cursor.execute("DROP TABLE IF EXISTS enumeration_value_mapping")
    
    cursor.execute(
        """
        CREATE TABLE object_mapping (
            object_type TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_synonym TEXT,
            source_full_name TEXT,
            target_name TEXT,
            target_synonym TEXT,
            target_full_name TEXT,
            match_key TEXT,
            status TEXT NOT NULL,
            PRIMARY KEY (object_type, source_name)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE field_mapping (
            object_type TEXT NOT NULL,
            object_name TEXT NOT NULL,
            field_kind TEXT NOT NULL,
            section_name TEXT,
            field_name TEXT NOT NULL,
            source_type TEXT,
            target_object_name TEXT,
            target_field_kind TEXT,
            target_section_name TEXT,
            target_field_name TEXT,
            target_type TEXT,
            status TEXT NOT NULL,
            is_manual INTEGER DEFAULT 0,
            search_method TEXT,
            PRIMARY KEY (object_type, object_name, field_kind, section_name, field_name)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE type_mapping (
            source_type TEXT PRIMARY KEY,
            target_type TEXT,
            status TEXT NOT NULL,
            notes TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE target_unmatched_objects (
            object_type TEXT NOT NULL,
            target_name TEXT NOT NULL,
            target_synonym TEXT,
            target_full_name TEXT,
            PRIMARY KEY (object_type, target_name)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE enumeration_value_mapping (
            source_enum_type TEXT NOT NULL,
            source_value TEXT NOT NULL,
            target_enum_type TEXT NOT NULL,
            target_value TEXT NOT NULL,
            is_manual INTEGER DEFAULT 0,
            PRIMARY KEY (source_enum_type, source_value)
        )
        """
    )
    connection.commit()
    return connection, manual_field_mappings, manual_enum_value_mappings, manual_object_mappings


def map_objects(
    object_type: str,
    source_objects: Dict[str, ObjectInfo],
    target_objects: Dict[str, ObjectInfo],
    object_rows: List[Tuple],
    field_rows: List[Tuple],
    type_map: Dict[str, Dict[str, Set[str]]],
    source_types: Set[str],
    target_types: Set[str],
    unmatched_targets: Set[Tuple[str, str, str]],
) -> None:
    target_index = build_index(target_objects)
    used_targets: Set[str] = set()

    def find_target(source: ObjectInfo) -> Tuple[Optional[ObjectInfo], Optional[str]]:
        # Сопоставляем только по полному равенству имени (name) или полного имени (full_name)
        # Синонимы не используем, чтобы избежать неправильных сопоставлений
        for key_name in ("name", "full_name"):
            key = normalize(getattr(source, key_name))
            if not key:
                continue
            candidates = target_index.get(key, [])
            for candidate in candidates:
                if candidate.name in used_targets:
                    continue
                used_targets.add(candidate.name)
                return candidate, key_name
        return None, None

    for source in source_objects.values():
        target, match_key = find_target(source)
        status = "matched" if target else "missing_target"
        object_rows.append(
            (
                object_type,
                source.name,
                source.synonym,
                source.full_name,
                target.name if target else None,
                target.synonym if target else None,
                target.full_name if target else None,
                match_key,
                status,
            )
        )

        for field in source.fields:
            for part in split_types(field.type):
                source_types.add(part)

        if target:
            for field in target.fields:
                for part in split_types(field.type):
                    target_types.add(part)
        else:
            # no target, register all source field types as missing
            for field in source.fields:
                src_parts = split_types(field.type)
                if not src_parts:
                    continue
                for part in src_parts:
                    register_type_mapping(type_map, part, [])

        if not target:
            continue

        req_index: Dict[str, FieldInfo] = {}
        tab_index: Dict[Tuple[str, str], FieldInfo] = {}
        for field in target.fields:
            if field.kind == "requisite":
                key = normalize(field.name)
                if key and key not in req_index:
                    req_index[key] = field
            else:
                key = (normalize(field.section), normalize(field.name))
                if key[0] and key[1] and key not in tab_index:
                    tab_index[key] = field

        matched_req_keys: Set[str] = set()
        matched_tab_keys: Set[Tuple[str, str]] = set()

        for field in source.fields:
            src_parts = split_types(field.type)
            key_req = normalize(field.name)
            key_tab = (normalize(field.section), normalize(field.name))

            if field.kind == "requisite":
                target_field = req_index.get(key_req)
                if target_field:
                    matched_req_keys.add(key_req)  # type: ignore[arg-type]
                    tgt_parts = split_types(target_field.type)
                    # Маппинг только по точному совпадению типов
                    src_parts_set = set(src_parts)
                    tgt_parts_set = set(tgt_parts)
                    exact_matches = src_parts_set & tgt_parts_set
                    for part in exact_matches:
                        register_type_mapping(type_map, part, [part])
                    field_rows.append(
                        (
                            object_type,
                            source.name,
                            field.kind,
                            "",  # Используем пустую строку вместо None для PRIMARY KEY
                            field.name,
                            field.type,
                            target.name,
                            target_field.kind,
                            None,
                            target_field.name,
                            target_field.type,
                            "matched",
                        )
                    )
                    continue
                else:
                    for part in src_parts:
                        register_type_mapping(type_map, part, [])
                    field_rows.append(
                        (
                            object_type,
                            source.name,
                            field.kind,
                            "",  # Используем пустую строку вместо None для PRIMARY KEY
                            field.name,
                            field.type,
                            None,
                            None,
                            None,
                            None,
                            None,
                            "missing_target",
                        )
                    )
            else:
                target_field = tab_index.get(key_tab)
                if target_field:
                    matched_tab_keys.add(key_tab)
                    tgt_parts = split_types(target_field.type)
                    # Маппинг только по точному совпадению типов
                    src_parts_set = set(src_parts)
                    tgt_parts_set = set(tgt_parts)
                    exact_matches = src_parts_set & tgt_parts_set
                    for part in exact_matches:
                        register_type_mapping(type_map, part, [part])
                    field_rows.append(
                        (
                            object_type,
                            source.name,
                            field.kind,
                            field.section or "",  # Используем пустую строку вместо None для PRIMARY KEY
                            field.name,
                            field.type,
                            target.name,
                            target_field.kind,
                            target_field.section or None,  # Для target_section_name можно оставить None
                            target_field.name,
                            target_field.type,
                            "matched",
                        )
                    )
                    continue
                else:
                    for part in src_parts:
                        register_type_mapping(type_map, part, [])
                    field_rows.append(
                        (
                            object_type,
                            source.name,
                            field.kind,
                            field.section or "",  # Используем пустую строку вместо None для PRIMARY KEY
                            field.name,
                            field.type,
                            None,
                            None,
                            None,
                            None,
                            None,
                            "missing_target",
                        )
                    )

    for target in target_objects.values():
        if target.name not in used_targets:
            unmatched_targets.add((object_type, target.name, target.synonym, target.full_name))
            for field in target.fields:
                for part in split_types(field.type):
                    target_types.add(part)


def build_type_rows(
    source_types: Set[str],
    target_types: Set[str],
    type_map: Dict[str, Dict[str, Set[str]]],
) -> List[Tuple[str, Optional[str], str, Optional[str]]]:
    rows: List[Tuple[str, Optional[str], str, Optional[str]]] = []
    for source_type in sorted(source_types):
        entry = type_map.get(source_type)
        if not entry:
            rows.append((source_type, None, "missing_target", None))
            continue
        targets = entry["targets"]
        flags = entry["flags"]
        if source_type in targets:
            status = "exact"
            target_type = source_type
        elif targets:
            status = "mapped"
            target_type = sorted(targets)[0]
        else:
            status = "missing_target"
            target_type = None
        notes = None
        if len(targets) > 1:
            notes = ", ".join(sorted(targets))
        rows.append((source_type, target_type, status, notes))

    for target_type in sorted(target_types - source_types):
        rows.append((target_type, target_type, "target_only", None))

    return rows


# Функции ensure_manual_column, ensure_search_method_column, ensure_enum_value_mapping_table
# и export_mapping_to_json_compact перенесены в tools/manual_mapping.py


def export_mapping_to_json(mapping_db: str, json_output: Optional[str] = None) -> bool:
    """
    Экспортирует маппинг из SQLite БД в JSON файл.
    
    Args:
        mapping_db: Путь к базе маппинга
        json_output: Путь к JSON файлу (если None, используется mapping_db с расширением .json)
    
    Returns:
        True если экспорт успешен
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
        
        mapping_data = {
            "field_mapping": [],
            "type_mapping": [],
            "enumeration_value_mapping": []
        }
        
        # Экспорт field_mapping
        cursor.execute("""
            SELECT * FROM field_mapping 
            ORDER BY object_type, object_name, field_kind, section_name, field_name
        """)
        for row in cursor.fetchall():
            mapping_data["field_mapping"].append({
                "object_type": row["object_type"],
                "object_name": row["object_name"],
                "field_kind": row["field_kind"],
                "section_name": row["section_name"] if "section_name" in row.keys() and row["section_name"] else "",
                "field_name": row["field_name"],
                "source_type": row["source_type"] if "source_type" in row.keys() else None,
                "target_object_name": row["target_object_name"] if "target_object_name" in row.keys() else None,
                "target_field_kind": row["target_field_kind"] if "target_field_kind" in row.keys() else None,
                "target_section_name": row["target_section_name"] if "target_section_name" in row.keys() and row["target_section_name"] else "",
                "target_field_name": row["target_field_name"] if "target_field_name" in row.keys() else None,
                "target_type": row["target_type"] if "target_type" in row.keys() else None,
                "status": row["status"],
                "is_manual": bool(row["is_manual"] if "is_manual" in row.keys() else 0),
                "search_method": row["search_method"] if "search_method" in row.keys() else None
            })
        
        # Экспорт type_mapping
        cursor.execute("SELECT * FROM type_mapping ORDER BY source_type")
        for row in cursor.fetchall():
            mapping_data["type_mapping"].append({
                "source_type": row["source_type"],
                "target_type": row["target_type"] if "target_type" in row.keys() else None,
                "status": row["status"] if "status" in row.keys() else None
            })
        
        # Экспорт enumeration_value_mapping
        cursor.execute("""
            SELECT * FROM enumeration_value_mapping 
            ORDER BY source_enum_type, source_value
        """)
        for row in cursor.fetchall():
            mapping_data["enumeration_value_mapping"].append({
                "source_enum_type": row["source_enum_type"],
                "source_value": row["source_value"],
                "target_enum_type": row["target_enum_type"] if "target_enum_type" in row.keys() else None,
                "target_value": row["target_value"] if "target_value" in row.keys() else None,
                "is_manual": bool(row["is_manual"] if "is_manual" in row.keys() else 0)
            })
        
        conn.close()
        
        # Сохраняем в JSON
        with open(json_output, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Маппинг экспортирован в JSON: {json_output}")
        return True
        
    except Exception as e:
        print(f"Ошибка при экспорте маппинга в JSON: {e}")
        import traceback
        traceback.print_exc()
        return False


# Функции add_manual_field_mapping и add_manual_enum_value_mapping
# перенесены в tools/manual_mapping.py


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Работа с маппингом типов и полей между двумя конфигурациями 1С",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

1. Построение автоматического маппинга:
   python tools/auto_mapping.py build --source CONF/upp_metadata.db --target CONF/uh_metadata.db

Примечание: Для ручного маппинга полей, типов и значений перечислений используйте tools/manual_mapping.py
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Команда')
    
    # Команда build - построение автоматического маппинга
    build_parser = subparsers.add_parser('build', help='Построить автоматический маппинг')
    build_parser.add_argument("--source", required=True, help="Путь к метаданным источника")
    build_parser.add_argument("--target", required=True, help="Путь к метаданным приемника")
    build_parser.add_argument(
        "--output",
        default=str(Path("CONF") / "type_mapping.db"),
        help="Путь к результирующей базе со сопоставлениями",
    )
    
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
    
    # Команда export-compact - экспорт компактного JSON
    export_compact_parser = subparsers.add_parser('export-compact', help='Экспортировать компактный JSON маппинг для ручного отслеживания')
    export_compact_parser.add_argument(
        "--mapping-db",
        default=str(Path("CONF") / "type_mapping.db"),
        help="Путь к базе маппинга"
    )
    export_compact_parser.add_argument(
        "--output",
        help="Путь к выходному JSON файлу (по умолчанию: mapping_db.compact.json)"
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
    
    if args.command == 'build':
        # Построение автоматического маппинга
        _build_mapping(args.source, args.target, args.output)
    elif args.command == 'add-field':
        # Добавление ручного маппинга поля
        success = add_manual_field_mapping(
            args.mapping_db,
            args.object_type,
            args.object_name,
            args.field_name,
            args.target_field_name,
            args.source_type,
            args.target_type,
            args.target_object_name,
            None,  # search_method
            getattr(args, 'field_kind', None),
            getattr(args, 'section_name', None),
            getattr(args, 'target_section_name', None)
        )
        sys.exit(0 if success else 1)
    elif args.command == 'add-enum-value':
        # Добавление ручного маппинга значения перечисления
        success = add_manual_enum_value_mapping(
            args.mapping_db,
            args.source_enum_type,
            args.source_value,
            args.target_enum_type,
            args.target_value
        )
        sys.exit(0 if success else 1)
    elif args.command == 'export-compact':
        # Экспорт компактного JSON
        success = export_mapping_to_json_compact(
            args.mapping_db,
            args.output
        )
        sys.exit(0 if success else 1)
    else:
        parser.print_help()
        sys.exit(1)


def _build_mapping(source: str, target: str, output: str) -> None:
    """Внутренняя функция построения автоматического маппинга"""

    source_catalogs = load_objects(source, "metadata_catalogs")
    source_documents = load_objects(source, "metadata_documents")
    source_enumerations = load_enumerations(source)

    target_catalogs = load_objects(target, "metadata_catalogs")
    target_documents = load_objects(target, "metadata_documents")
    target_enumerations = load_enumerations(target)

    source_types: Set[str] = set()
    target_types: Set[str] = set()

    type_map: Dict[str, Dict[str, Set[str]]] = {}
    object_rows: List[Tuple] = []
    field_rows: List[Tuple] = []
    unmatched_targets: Set[Tuple[str, str, str]] = set()

    map_objects(
        "catalog",
        source_catalogs,
        target_catalogs,
        object_rows,
        field_rows,
        type_map,
        source_types,
        target_types,
        unmatched_targets,
    )
    map_objects(
        "document",
        source_documents,
        target_documents,
        object_rows,
        field_rows,
        type_map,
        source_types,
        target_types,
        unmatched_targets,
    )

    # Enumerations: only object-level mapping
    target_enum_index = build_index(target_enumerations)
    used_enum_targets: Set[str] = set()
    for enum in source_enumerations.values():
        target_enum, match_key = None, None
        for key_name in ("name", "full_name", "synonym"):
            key = normalize(getattr(enum, key_name))
            if not key:
                continue
            candidates = target_enum_index.get(key, [])
            for candidate in candidates:
                if candidate.name in used_enum_targets:
                    continue
                used_enum_targets.add(candidate.name)
                target_enum = candidate
                match_key = key_name
                break
            if target_enum:
                break
        status = "matched" if target_enum else "missing_target"
        object_rows.append(
            (
                "enumeration",
                enum.name,
                enum.synonym,
                enum.full_name,
                target_enum.name if target_enum else None,
                target_enum.synonym if target_enum else None,
                target_enum.full_name if target_enum else None,
                match_key,
                status,
            )
        )
    for enum in target_enumerations.values():
        if enum.name not in used_enum_targets:
            unmatched_targets.add(("enumeration", enum.name, enum.synonym, enum.full_name))

    # Collect remaining target types from unmatched objects
    for obj in target_catalogs.values():
        for field in obj.fields:
            for part in split_types(field.type):
                target_types.add(part)
    for obj in target_documents.values():
        for field in obj.fields:
            for part in split_types(field.type):
                target_types.add(part)

    # Читаем ручные маппинги объектов ДО создания новой БД, чтобы создать маппинги полей для них
    manual_object_mappings_for_fields = []
    try:
        temp_conn = sqlite3.connect(output)
        temp_cursor = temp_conn.cursor()
        try:
            temp_cursor.execute("SELECT * FROM object_mapping WHERE match_key = 'manual'")
            manual_object_mappings_for_fields = temp_cursor.fetchall()
        except sqlite3.OperationalError:
            pass
        temp_conn.close()
    except Exception:
        pass
    
    # Создаем маппинги полей для ручных маппингов объектов
    if manual_object_mappings_for_fields:
        for row in manual_object_mappings_for_fields:
            object_type = row[0]
            source_name = row[1]
            target_name = row[4]
            
            if target_name and object_type in ('catalog', 'document'):
                source_objects = source_catalogs if object_type == 'catalog' else source_documents
                target_objects = target_catalogs if object_type == 'catalog' else target_documents
                
                source_obj = source_objects.get(source_name)
                target_obj = target_objects.get(target_name)
                
                if source_obj and target_obj:
                    # Создаем индексы полей приемника
                    req_index: Dict[str, FieldInfo] = {}
                    tab_index: Dict[Tuple[str, str], FieldInfo] = {}
                    for field in target_obj.fields:
                        if field.kind == "requisite":
                            key = normalize(field.name)
                            if key and key not in req_index:
                                req_index[key] = field
                        else:
                            key = (normalize(field.section), normalize(field.name))
                            if key[0] and key[1] and key not in tab_index:
                                tab_index[key] = field
                    
                    # Создаем маппинги полей
                    for field in source_obj.fields:
                        src_parts = split_types(field.type)
                        key_req = normalize(field.name)
                        key_tab = (normalize(field.section), normalize(field.name))
                        
                        if field.kind == "requisite":
                            target_field = req_index.get(key_req)
                            if target_field:
                                tgt_parts = split_types(target_field.type)
                                src_parts_set = set(src_parts)
                                tgt_parts_set = set(tgt_parts)
                                exact_matches = src_parts_set & tgt_parts_set
                                for part in exact_matches:
                                    register_type_mapping(type_map, part, [part])
                                # Добавляем типы в source_types и target_types
                                for part in src_parts:
                                    source_types.add(part)
                                for part in tgt_parts:
                                    target_types.add(part)
                                field_rows.append((
                                    object_type,
                                    source_name,
                                    field.kind,
                                    "",
                                    field.name,
                                    field.type,
                                    target_name,
                                    target_field.kind,
                                    None,
                                    target_field.name,
                                    target_field.type,
                                    "matched",
                                ))
                        else:
                            target_field = tab_index.get(key_tab)
                            if target_field:
                                tgt_parts = split_types(target_field.type)
                                src_parts_set = set(src_parts)
                                tgt_parts_set = set(tgt_parts)
                                exact_matches = src_parts_set & tgt_parts_set
                                for part in exact_matches:
                                    register_type_mapping(type_map, part, [part])
                                # Добавляем типы в source_types и target_types
                                for part in src_parts:
                                    source_types.add(part)
                                for part in tgt_parts:
                                    target_types.add(part)
                                field_rows.append((
                                    object_type,
                                    source_name,
                                    field.kind,
                                    field.section or "",
                                    field.name,
                                    field.type,
                                    target_name,
                                    target_field.kind,
                                    target_field.section or None,
                                    target_field.name,
                                    target_field.type,
                                    "matched",
                                ))

    type_rows = build_type_rows(source_types, target_types, type_map)

    # Маппинг значений перечислений
    source_enum_values = load_enumeration_values(source)
    target_enum_values = load_enumeration_values(target)
    enum_value_rows: List[Tuple[str, str, str, str, int]] = []
    
    # Сопоставляем значения перечислений для смапленных перечислений
    for enum_row in object_rows:
        if enum_row[0] != "enumeration" or enum_row[8] != "matched":
            continue
        
        source_enum_name = enum_row[1]
        target_enum_name = enum_row[4]
        
        if not source_enum_name or not target_enum_name:
            continue
        
        source_values = source_enum_values.get(source_enum_name, [])
        target_values = target_enum_values.get(target_enum_name, [])
        
        if not source_values or not target_values:
            continue
        
        source_enum_type = f"Перечисление.{source_enum_name}"
        target_enum_type = f"Перечисление.{target_enum_name}"
        
        # Создаем индекс значений приемника по нормализованному имени
        target_index: Dict[str, Dict[str, str]] = {}
        for target_val in target_values:
            target_val_name = target_val.get("name", "")
            if target_val_name:
                normalized = normalize(target_val_name)
                if normalized and normalized not in target_index:
                    target_index[normalized] = target_val
        
        # Сопоставляем значения источника с приемником
        used_target_values: Set[str] = set()
        
        for source_val in source_values:
            source_val_name = source_val.get("name", "")
            if not source_val_name:
                continue
            
            # Ищем точное совпадение по нормализованному имени
            normalized_source = normalize(source_val_name)
            target_val = target_index.get(normalized_source)
            
            if target_val and normalized_source not in used_target_values:
                target_val_name = target_val.get("name", "")
                used_target_values.add(normalized_source)
                enum_value_rows.append((
                    source_enum_type,
                    source_val_name,
                    target_enum_type,
                    target_val_name,
                    0  # is_manual = 0 (автоматический маппинг)
                ))

    conn, manual_field_mappings, manual_enum_value_mappings, manual_object_mappings = create_output_db(output)
    cursor = conn.cursor()
    
    # Собираем ключи ручных маппингов объектов, чтобы исключить их из автоматических
    manual_object_keys = set()
    if manual_object_mappings:
        for row in manual_object_mappings:
            # row: (object_type, source_name, source_synonym, source_full_name, target_name, target_synonym, target_full_name, match_key, status)
            if len(row) >= 2:
                key = (row[0], row[1])  # (object_type, source_name) - PRIMARY KEY
                manual_object_keys.add(key)
    
    # Исключаем ручные маппинги объектов из автоматических
    filtered_object_rows = []
    for row in object_rows:
        # row: (object_type, source_name, source_synonym, source_full_name, target_name, target_synonym, target_full_name, match_key, status)
        key = (row[0], row[1])  # (object_type, source_name)
        if key not in manual_object_keys:
            filtered_object_rows.append(row)
    
    # Вставляем автоматические маппинги (только для объектов, которых нет в ручных)
    cursor.executemany(
        "INSERT INTO object_mapping VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", filtered_object_rows
    )
    
    # Восстанавливаем ручные маппинги объектов и существующие matched маппинги
    # Используем INSERT OR REPLACE, чтобы гарантировать их сохранение
    if manual_object_mappings:
        for row in manual_object_mappings:
            # row: (object_type, source_name, source_synonym, source_full_name, target_name, target_synonym, target_full_name, match_key, status)
            # Убеждаемся, что match_key установлен для сохранения в будущем
            row_list = list(row)
            if len(row_list) >= 8 and (row_list[7] is None or row_list[7] == ''):
                # Если match_key не установлен, но это matched маппинг, устанавливаем его
                if len(row_list) >= 9 and row_list[8] == 'matched' and len(row_list) >= 5 and row_list[4]:
                    row_list[7] = 'manual'  # Устанавливаем match_key = 'manual' для сохранения
            cursor.execute("""
                INSERT OR REPLACE INTO object_mapping 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(row_list))
    
    # Собираем ключи ручных маппингов, чтобы исключить их из автоматических
    # PRIMARY KEY: (object_type, object_name, field_kind, section_name, field_name)
    manual_keys = set()
    if manual_field_mappings:
        for row in manual_field_mappings:
            row_list = list(row)
            # Определяем ключ из первых 5 элементов
            if len(row_list) >= 5:
                key = (
                    row_list[0] if len(row_list) > 0 else None,  # object_type
                    row_list[1] if len(row_list) > 1 else None,  # object_name
                    row_list[2] if len(row_list) > 2 else None,  # field_kind
                    (row_list[3] if len(row_list) > 3 and row_list[3] is not None else ""),  # section_name
                    row_list[4] if len(row_list) > 4 else None,  # field_name
                )
                manual_keys.add(key)
    
    # Добавляем is_manual=0 для автоматических маппингов
    field_rows_with_manual = [row + (0, None) for row in field_rows]  # Добавляем is_manual=0 и search_method=None
    
    # Удаляем дубликаты по PRIMARY KEY и исключаем ручные маппинги
    seen_keys = set()
    unique_field_rows = []
    for row in field_rows_with_manual:
        # row: (object_type, object_name, field_kind, section_name, field_name, ...)
        key = (row[0], row[1], row[2], row[3] if row[3] is not None else "", row[4])
        # Пропускаем, если это ручной маппинг
        if key in manual_keys:
            continue
        if key not in seen_keys:
            seen_keys.add(key)
            unique_field_rows.append(row)
        else:
            # Дубликат найден - пропускаем (используем первую запись)
            pass
    
    if len(field_rows_with_manual) > len(unique_field_rows):
        skipped_manual = len([r for r in field_rows_with_manual if (r[0], r[1], r[2], r[3] if r[3] is not None else "", r[4]) in manual_keys])
        skipped_duplicates = len(field_rows_with_manual) - len(unique_field_rows) - skipped_manual
        if skipped_manual > 0:
            print(f"Пропущено {skipped_manual} полей, которые имеют ручные маппинги")
        if skipped_duplicates > 0:
            print(f"Предупреждение: удалено {skipped_duplicates} дубликатов полей перед вставкой")
    
    cursor.executemany(
        "INSERT INTO field_mapping VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", unique_field_rows
    )
    cursor.executemany(
        "INSERT INTO type_mapping VALUES (?, ?, ?, ?)", type_rows
    )
    cursor.executemany(
        "INSERT INTO target_unmatched_objects VALUES (?, ?, ?, ?)", list(unmatched_targets)
    )
    
    # Добавляем маппинг значений перечислений
    if enum_value_rows:
        cursor.executemany(
            "INSERT INTO enumeration_value_mapping VALUES (?, ?, ?, ?, ?)", enum_value_rows
        )
    
    # Восстанавливаем ручные маппинги полей
    if manual_field_mappings:
        for row in manual_field_mappings:
            # row - это кортеж из SELECT *
            # Таблица имеет 14 колонок: object_type, object_name, field_kind, section_name, field_name,
            # source_type, target_object_name, target_field_kind, target_section_name, target_field_name,
            # target_type, status, is_manual, search_method
            row_list = list(row)
            
            # Обрабатываем разные варианты структуры
            if len(row_list) == 12:
                # Старая структура без is_manual и search_method
                row_list.append(1)  # is_manual = 1
                row_list.append(None)  # search_method = NULL
            elif len(row_list) == 13:
                # Структура с is_manual, но без search_method
                # Проверяем, что последний элемент - это is_manual
                if isinstance(row_list[-1], int):
                    row_list[-1] = 1  # Устанавливаем is_manual = 1
                    row_list.append(None)  # Добавляем search_method = NULL
                else:
                    # Последний элемент не is_manual, добавляем оба
                    row_list.append(1)  # is_manual = 1
                    row_list.append(None)  # search_method = NULL
            elif len(row_list) == 14:
                # Полная структура с is_manual и search_method
                row_list[12] = 1  # Устанавливаем is_manual = 1 (индекс 12)
                # search_method уже есть (индекс 13), оставляем как есть
            
            # Заменяем None на "" для section_name (4-й элемент, индекс 3)
            if len(row_list) > 3 and row_list[3] is None:
                row_list[3] = ""
            
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO field_mapping 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, tuple(row_list))
            except sqlite3.OperationalError as e:
                # Если структура не совпадает, пропускаем
                print(f"Предупреждение: не удалось восстановить ручной маппинг полей: {e}")
            except Exception as e:
                print(f"Предупреждение: ошибка при восстановлении ручного маппинга полей: {e}")
    
    # Восстанавливаем ручные маппинги значений перечислений
    if manual_enum_value_mappings:
        for row in manual_enum_value_mappings:
            # row - это кортеж из SELECT *
            # Нужно убедиться, что is_manual = 1
            if len(row) == 4:
                # Старая структура без is_manual, добавляем её
                row = row + (1,)
            elif len(row) == 5:
                # Новая структура с is_manual, заменяем последнее значение на 1
                row = row[:-1] + (1,)
            
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO enumeration_value_mapping 
                    VALUES (?, ?, ?, ?, ?)
                """, row)
            except sqlite3.OperationalError as e:
                # Если структура не совпадает, пропускаем
                print(f"Предупреждение: не удалось восстановить ручной маппинг значений перечислений: {e}")
    
    conn.commit()
    conn.close()

    print(f"Объектов сопоставлено: {sum(1 for row in object_rows if row[-1] == 'matched')}")
    print(f"Полей сопоставлено: {sum(1 for row in field_rows if row[-1] == 'matched')}")
    print(f"Значений перечислений сопоставлено: {len(enum_value_rows)}")
    print(f"Всего типов в источнике: {len(source_types)}")
    print(f"Результат записан в: {output}")
    
    # Экспортируем маппинг в компактный JSON
    json_output = str(Path(output).with_suffix('.json'))
    export_mapping_to_json_compact(output, json_output)


if __name__ == "__main__":
    main()

