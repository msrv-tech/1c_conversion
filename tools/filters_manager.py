# -*- coding: utf-8 -*-
"""
Утилиты для работы с базой данных фильтров загрузки.

Формат базы данных:
    таблица load_filters (
        catalog TEXT NOT NULL,
        uuid TEXT NOT NULL,
        PRIMARY KEY (catalog, uuid)
    )
В эту таблицу различные модули могут записывать UUID элементов, которые нужно
принудительно загрузить из 1С.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Iterable, List

FILTERS_TABLE = "load_filters"


def _ensure_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {FILTERS_TABLE} (
            catalog TEXT NOT NULL,
            uuid TEXT NOT NULL,
            PRIMARY KEY (catalog, uuid)
        )
        """
    )
    connection.commit()


def get_catalog_uuids(filters_db_path: str | None, catalog_name: str) -> List[str]:
    """
    Возвращает список UUID, заданных для указанного справочника.

    Args:
        filters_db_path: путь к базе данных фильтров (может быть None).
        catalog_name: имя справочника (совпадает с параметром --catalog).
    """
    if not filters_db_path:
        return []

    db_path = os.path.abspath(filters_db_path)
    if not os.path.exists(db_path):
        return []

    connection = sqlite3.connect(db_path)
    try:
        _ensure_schema(connection)
        cursor = connection.execute(
            f"SELECT uuid FROM {FILTERS_TABLE} WHERE catalog = ?", (catalog_name,)
        )
        uuids = [row[0] for row in cursor.fetchall() if row and row[0]]
        return uuids
    finally:
        connection.close()


def add_catalog_uuids(
    filters_db_path: str,
    catalog_name: str,
    uuids: Iterable[str],
) -> int:
    """
    Добавляет в базу данных фильтров набор UUID для указанного справочника.

    Returns:
        Количество добавленных записей (без дубликатов).
    """
    if not uuids:
        return 0

    db_path = os.path.abspath(filters_db_path)
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    connection = sqlite3.connect(db_path)
    inserted = 0
    try:
        _ensure_schema(connection)
        cursor = connection.cursor()
        for uuid_value in uuids:
            uuid_value = (uuid_value or "").strip()
            if not uuid_value:
                continue
            try:
                cursor.execute(
                    f"INSERT OR IGNORE INTO {FILTERS_TABLE} (catalog, uuid) VALUES (?, ?)",
                    (catalog_name, uuid_value),
                )
                if cursor.rowcount:
                    inserted += 1
            except sqlite3.DatabaseError:
                continue
        connection.commit()
    finally:
        connection.close()
    return inserted
