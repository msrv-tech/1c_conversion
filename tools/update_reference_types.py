"""
Утилита, которая сопоставляет значения перечислений в JSON с ярлыками из метаданных.

Пример использования:

    python -m tools.update_reference_types ^
        --metadata-db BD/upp_metadata.db ^
        --target-db BD/nomenclature_groups.db ^
        --table nomenclature_groups
"""

from __future__ import annotations

import argparse
import json

from tools.encoding_fix import fix_encoding

fix_encoding()
import sqlite3
from typing import Dict, Iterable, Optional, Tuple


MetadataMap = Dict[str, str]
EnumerationIndex = Dict[str, Dict[str, str]]


def _normalize_key(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return value.strip()


def build_enumeration_index(metadata_db: str) -> EnumerationIndex:
    """
    Формирует индекс перечислений по синонимам значений.
    """
    connection = sqlite3.connect(metadata_db)
    connection.row_factory = sqlite3.Row

    try:
        index: EnumerationIndex = {}
        rows = connection.execute(
            "SELECT name, full_name, values_json FROM metadata_enumerations"
        ).fetchall()
        for row in rows:
            full_name = _normalize_key(row["full_name"])
            if not full_name:
                name_value = _normalize_key(row["name"])
                if not name_value:
                    continue
                full_name = f"Перечисление.{name_value}"

            try:
                values_list = json.loads(row["values_json"] or "[]")
            except (TypeError, json.JSONDecodeError):
                continue

            value_map: Dict[str, str] = {}
            for value_entry in values_list:
                if not isinstance(value_entry, dict):
                    continue
                value_name = _normalize_key(value_entry.get("name"))
                synonym_value = _normalize_key(value_entry.get("synonym"))
                if not value_name:
                    continue
                descriptor = f"{full_name}.{value_name}"
                for candidate in (synonym_value, value_name):
                    key = _normalized_lookup_key(candidate)
                    if key and key not in value_map:
                        value_map[key] = descriptor

            if value_map:
                index[full_name] = value_map

        return index
    finally:
        connection.close()


def _normalized_lookup_key(value: Optional[str]) -> Optional[str]:
    normalized = _normalize_key(value)
    return normalized.lower() if normalized else None


def update_reference_types(
    target_db: str,
    table_name: str,
    enumeration_index: EnumerationIndex,
    metadata_map: Optional[MetadataMap] = None,
    key_column: str = "uuid",
) -> int:
    """
    Обновляет JSON-колонки в таблице target_db, заменяя значение поля "type".
    """
    connection = sqlite3.connect(target_db)
    connection.row_factory = sqlite3.Row

    try:
        cursor = connection.cursor()
        columns = [
            row["name"]
            for row in cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
        ]
        if key_column not in columns:
            raise RuntimeError(f"В таблице {table_name} отсутствует ключевой столбец {key_column}")

        select_sql = f"SELECT {', '.join(columns)} FROM {table_name}"
        rows = cursor.execute(select_sql).fetchall()

        updated_rows = 0
        for row in rows:
            updates = {}
            for column in columns:
                value = row[column]
                if not isinstance(value, str):
                    continue
                text = value.strip()
                if not (text.startswith("{") and text.endswith("}")):
                    continue
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    continue

                type_value = _normalize_key(payload.get("type"))
                if not type_value or type_value.lower() == "null":
                    continue

                enum_descriptor = None
                enum_map = enumeration_index.get(type_value) if type_value else None
                if enum_map:
                    candidates = [
                        payload.get("presentation"),
                        payload.get("synonym"),
                        payload.get("name"),
                        payload.get("value"),
                    ]
                    for candidate in candidates:
                        lookup_key = _normalized_lookup_key(candidate)
                        if lookup_key and lookup_key in enum_map:
                            enum_descriptor = enum_map[lookup_key]
                            break

                if enum_descriptor and enum_descriptor != value:
                    updates[column] = enum_descriptor

            if not updates:
                continue

            key_value = row[key_column]
            set_clause = ", ".join(f"{col} = ?" for col in updates.keys())
            update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {key_column} = ?"
            cursor.execute(update_sql, [*updates.values(), key_value])
            updated_rows += 1

        connection.commit()
        return updated_rows
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Замена синонимов типов ссылок на полные имена по метаданным."
    )
    parser.add_argument(
        "--metadata-db",
        required=True,
        help="Путь к базе метаданных (например, BD/upp_metadata.db).",
    )
    parser.add_argument(
        "--target-db",
        required=True,
        help="Путь к целевой базе, где нужно обновить JSON (например, BD/nomenclature_groups.db).",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Имя таблицы в целевой базе, которую нужно обработать.",
    )
    parser.add_argument(
        "--key-column",
        default="uuid",
        help="Имя ключевого столбца для идентификации строк (по умолчанию uuid).",
    )

    args = parser.parse_args()
    enumeration_index = build_enumeration_index(args.metadata_db)
    updated = update_reference_types(
        target_db=args.target_db,
        table_name=args.table,
        enumeration_index=enumeration_index,
        key_column=args.key_column,
    )
    print(f"Обновлено строк: {updated}")


if __name__ == "__main__":
    main()

