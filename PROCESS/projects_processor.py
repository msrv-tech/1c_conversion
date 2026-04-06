# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «Проекты».

Читает проекты из исходной БД, приводит ссылочные поля к формату приемника
(JSON с uuid, presentation, type, для Родитель — is_group) и сохраняет в обработанную БД.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows
from tools.processor_utils import read_from_db
from tools.logger import verbose_print

fix_encoding()

TABLE_NAME = "projects"


def _ensure_parent_json_with_is_group(items: List[Dict]) -> None:
    """Добавляет в JSON поля Родитель признак is_group из колонки Родитель_ЭтоГруппа."""
    for item in items:
        parent_json = item.get("Родитель")
        is_group_key = "Родитель_ЭтоГруппа"
        if is_group_key not in item:
            continue
        is_group_value = item.pop(is_group_key, None)
        if is_group_value is None:
            continue
        if isinstance(is_group_value, bool):
            pass
        elif isinstance(is_group_value, (int, str)):
            is_group_value = str(is_group_value).lower() in ("1", "true", "истина", "да")
        else:
            is_group_value = False
        if isinstance(parent_json, str) and parent_json.strip().startswith("{"):
            try:
                data = json.loads(parent_json)
                data["is_group"] = is_group_value
                item["Родитель"] = json.dumps(data, ensure_ascii=False)
            except (json.JSONDecodeError, TypeError):
                pass


def process_projects(source_db_path: str, processed_db_path: str) -> bool:
    """
    Обрабатывает проекты из исходной БД и сохраняет в обработанную БД.

    Args:
        source_db_path: Путь к исходной базе данных SQLite
        processed_db_path: Путь к выходной базе данных SQLite

    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА СПРАВОЧНИКА «ПРОЕКТЫ»")
    verbose_print("=" * 80)

    verbose_print(f"\n[1/3] Чтение проектов из исходной БД: {source_db_path}")
    items = read_from_db(source_db_path, TABLE_NAME)
    if not items:
        verbose_print("Проекты не найдены в исходной БД")
        return False
    verbose_print(f"Прочитано проектов: {len(items)}")

    verbose_print("\n[2/3] Преобразование ссылочных полей (Родитель — is_group)...")
    _ensure_parent_json_with_is_group(items)

    # Маппинг приёмника: в обработанной БД дублируем Описание в НаименованиеПолное
    for item in items:
        if "Описание" in item and "НаименованиеПолное" not in item:
            item["НаименованиеПолное"] = item["Описание"]

    verbose_print("\n[3/3] Сохранение в обработанную БД...")
    if not ensure_database_exists(processed_db_path):
        verbose_print(f"Не удалось подготовить базу данных: {processed_db_path}")
        return False

    connection = connect_to_sqlite(processed_db_path)
    if not connection:
        verbose_print(f"Не удалось подключиться к базе данных: {processed_db_path}")
        return False

    try:
        saved = upsert_rows(
            connection,
            TABLE_NAME,
            items,
            {
                "uuid": "TEXT PRIMARY KEY",
                "Ссылка": "TEXT",
                "Код": "TEXT",
                "Наименование": "TEXT",
                "ПометкаУдаления": "INTEGER",
            },
        )
        if saved:
            verbose_print(f"Сохранено проектов в БД: {len(items)}")
            verbose_print(f"База данных: {processed_db_path}")
            verbose_print(f"Таблица: {TABLE_NAME}")
        connection.commit()
        return bool(saved)
    except Exception as error:
        print(f"Ошибка при сохранении проектов: {error}")
        import traceback
        traceback.print_exc()
        connection.rollback()
        return False
    finally:
        connection.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Обработка справочника Проекты")
    parser.add_argument("--source-db", required=True, help="Путь к исходной БД")
    parser.add_argument("--processed-db", required=True, help="Путь к обработанной БД")

    args = parser.parse_args()
    success = process_projects(args.source_db, args.processed_db)
    sys.exit(0 if success else 1)
