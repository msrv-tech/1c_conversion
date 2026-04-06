# -*- coding: utf-8 -*-
"""
Загрузка справочника «Проекты» из 1С в SQLite.

Запрос выбирает элементы Справочник.Проекты с полями по заданному списку
и метаданными для ссылок (uuid, представление, тип).
"""

import os
import sys
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import (  # noqa: E402
    connect_to_sqlite,
    ensure_database_exists,
    process_reference_fields,
)
from tools.filters_manager import get_catalog_uuids  # noqa: E402
from tools.logger import verbose_print  # noqa: E402
from tools.onec_connector import (  # noqa: E402
    execute_query,
    upsert_rows,
)

TABLE_NAME = "projects"

TEST_LIMIT = 50
DEFAULT_MODE = "test"

MAIN_QUERY_TEMPLATE = """ВЫБРАТЬ
{limit_clause}    Проекты.Ссылка КАК Ссылка,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Проекты.Ссылка)) КАК uuid,
    Проекты.Представление КАК Представление,
    Проекты.ВерсияДанных КАК ВерсияДанных,
    Проекты.ПометкаУдаления КАК ПометкаУдаления,
    Проекты.Предопределенный КАК Предопределенный,
    Проекты.Родитель КАК Родитель,
    ПРЕДСТАВЛЕНИЕ(Проекты.Родитель) КАК Родитель_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Проекты.Родитель)) КАК Родитель_UUID,
    ВЫБОР
        КОГДА Проекты.Родитель = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Проекты.Родитель)
    КОНЕЦ КАК Родитель_Тип,
    ВЫБОР
        КОГДА Проекты.Родитель = НЕОПРЕДЕЛЕНО
            ТОГДА ЛОЖЬ
        ИНАЧЕ Проекты.Родитель.ЭтоГруппа
    КОНЕЦ КАК Родитель_ЭтоГруппа,
    Проекты.ЭтоГруппа КАК ЭтоГруппа,
    Проекты.Код КАК Код,
    Проекты.Наименование КАК Наименование,
    Проекты.ДатаНачала КАК ДатаНачала,
    Проекты.ДатаОкончания КАК ДатаОкончания,
    Проекты.Описание КАК Описание,
    Проекты.customПолноеНаименование КАК customПолноеНаименование,
    Проекты.customЯвляетсяЭтапомПроекта КАК customЯвляетсяЭтапомПроекта,
    Проекты.customСвязанныйСЭтапомПроект КАК customСвязанныйСЭтапомПроект,
    ПРЕДСТАВЛЕНИЕ(Проекты.customСвязанныйСЭтапомПроект) КАК customСвязанныйСЭтапомПроект_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Проекты.customСвязанныйСЭтапомПроект)) КАК customСвязанныйСЭтапомПроект_UUID,
    ВЫБОР
        КОГДА Проекты.customСвязанныйСЭтапомПроект = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Проекты.customСвязанныйСЭтапомПроект)
    КОНЕЦ КАК customСвязанныйСЭтапомПроект_Тип
ИЗ
    Справочник.Проекты КАК Проекты
"""

MAIN_QUERY_COLUMNS: List[str] = [
    "Ссылка",
    "uuid",
    "Представление",
    "ВерсияДанных",
    "ПометкаУдаления",
    "Предопределенный",
    "Родитель",
    "Родитель_Представление",
    "Родитель_UUID",
    "Родитель_Тип",
    "Родитель_ЭтоГруппа",
    "ЭтоГруппа",
    "Код",
    "Наименование",
    "ДатаНачала",
    "ДатаОкончания",
    "Описание",
    "customПолноеНаименование",
    "customЯвляетсяЭтапомПроекта",
    "customСвязанныйСЭтапомПроект",
    "customСвязанныйСЭтапомПроект_Представление",
    "customСвязанныйСЭтапомПроект_UUID",
    "customСвязанныйСЭтапомПроект_Тип",
]

REFERENCE_COLUMNS: List[str] = [
    "Родитель",
    "customСвязанныйСЭтапомПроект",
]


def build_main_query_text(mode: str) -> str:
    limit_clause = ""
    if mode == "test":
        limit_clause = f"    ПЕРВЫЕ {TEST_LIMIT}\n"
    return MAIN_QUERY_TEMPLATE.format(limit_clause=limit_clause)


def _log(message: str) -> None:
    verbose_print(message)


def load_projects(
    sqlite_db_file: str,
    com_object,
    mode: str = DEFAULT_MODE,
    process_func=None,
    filters_db: str | None = None,
) -> bool:
    _log("=" * 80)
    _log("ЗАГРУЗКА СПРАВОЧНИКА «ПРОЕКТЫ»")
    _log("=" * 80)

    if com_object is None:
        _log("Ошибка: com_object обязателен")
        return False

    mode = mode or DEFAULT_MODE
    if mode not in ("test", "full"):
        _log(f"Неизвестный режим '{mode}'. Доступны: test, full.")
        return False

    limit_descr = (
        f"первые {TEST_LIMIT} записей" if mode == "test" else "полная выборка"
    )
    _log(f"\n[2/4] Чтение элементов (режим: {mode.upper()} — {limit_descr})...")

    try:
        query_text = build_main_query_text(mode)
        rows = execute_query(
            com_object,
            query_text,
            MAIN_QUERY_COLUMNS,
        )
    except Exception as error:
        _log(f"Ошибка выполнения запроса: {error}")
        return False

    filter_uuids = get_catalog_uuids(filters_db, TABLE_NAME)
    if filter_uuids:
        uuid_set = {value.strip().lower() for value in filter_uuids if value}
        filtered_rows = []
        for row in rows:
            row_uuid = str(row.get("uuid", "") or "").strip().lower()
            if row_uuid and row_uuid in uuid_set:
                filtered_rows.append(row)
        rows = filtered_rows
        _log(f"Применено ограничение по UUID: {len(rows)} записей из {len(filter_uuids)}")

    if not rows:
        _log("Не удалось получить записи справочника.")
        return False

    _log(f"Получено записей: {len(rows)}")

    if process_func:
        _log("\n[3/4] Обработка данных...")
        rows = process_func(rows)

    _log("\n[4/4] Сохранение в SQLite...")
    if not ensure_database_exists(sqlite_db_file):
        _log("Не удалось подготовить базу данных SQLite.")
        return False

    connection = connect_to_sqlite(sqlite_db_file)
    if not connection:
        _log("Не удалось подключиться к SQLite.")
        return False

    try:
        rows = process_reference_fields(rows, REFERENCE_COLUMNS)

        saved = upsert_rows(
            connection,
            TABLE_NAME,
            rows,
            {
                "uuid": "TEXT PRIMARY KEY",
                "Ссылка": "TEXT",
                "Код": "TEXT",
                "Наименование": "TEXT",
                "Родитель_ЭтоГруппа": "INTEGER",
            },
        )
        _log(f"Сохранено строк: {saved}")
    finally:
        connection.close()

    return True
