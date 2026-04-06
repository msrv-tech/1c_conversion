# -*- coding: utf-8 -*-
"""
Загрузка справочника «customВидРеестраПлатежей» из 1С в SQLite.

Запрос сформирован утилитой `tools/generate_1c_query.py`.
"""

import json
import os
import sys
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite, ensure_database_exists  # noqa: E402
from tools.filters_manager import get_catalog_uuids  # noqa: E402
from tools.logger import verbose_print  # noqa: E402
from tools.onec_connector import (  # noqa: E402
 connect_to_1c,
    execute_query,
    save_tabular_sections,
    upsert_rows,
)

TABLE_NAME = "payment_registry_types"

TEST_LIMIT = 50
DEFAULT_MODE = "test"

MAIN_QUERY_TEMPLATE = """ВЫБРАТЬ
{limit_clause}    Каталог.Ссылка КАК Ссылка,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Ссылка)) КАК uuid,
    Каталог.Код КАК Код,
    Каталог.Наименование КАК Наименование,
    Каталог.ПометкаУдаления КАК ПометкаУдаления,
    Каталог.Представление КАК Представление
ИЗ
    Справочник.customВидРеестраПлатежей КАК Каталог
"""

MAIN_QUERY_COLUMNS: List[str] = [
    "Ссылка",
    "uuid",
    "Код",
    "Наименование",
    "ПометкаУдаления",
    "Представление",
]

REFERENCE_COLUMNS: List[str] = []

TABULAR_QUERIES: List[dict] = [
    {
        "name": "Условия",
        "table": "payment_registry_types_conditions",
        "query": """ВЫБРАТЬ
    ТЧ.Ссылка КАК parent_link,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ТЧ.Ссылка)) КАК parent_uuid,
    ТЧ.НомерСтроки КАК НомерСтроки,
    ТЧ.Контрагент КАК Контрагент,
    ПРЕДСТАВЛЕНИЕ(ТЧ.Контрагент) КАК Контрагент_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ТЧ.Контрагент)) КАК Контрагент_UUID,
    ВЫБОР
        КОГДА ТЧ.Контрагент = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(ТЧ.Контрагент)
    КОНЕЦ КАК Контрагент_Тип,
    ТЧ.СтатьяОборотов КАК СтатьяОборотов,
    ПРЕДСТАВЛЕНИЕ(ТЧ.СтатьяОборотов) КАК СтатьяОборотов_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ТЧ.СтатьяОборотов)) КАК СтатьяОборотов_UUID,
    ВЫБОР
        КОГДА ТЧ.СтатьяОборотов = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(ТЧ.СтатьяОборотов)
    КОНЕЦ КАК СтатьяОборотов_Тип
ИЗ
    Справочник.customВидРеестраПлатежей.Условия КАК ТЧ
ГДЕ
    ТЧ.Ссылка В (&Ссылки)
УПОРЯДОЧИТЬ ПО
    ТЧ.Ссылка,
    ТЧ.НомерСтроки""",
        "columns": [
            "parent_link",
            "parent_uuid",
            "НомерСтроки",
            "Контрагент",
            "Контрагент_Представление",
            "Контрагент_UUID",
            "Контрагент_Тип",
            "СтатьяОборотов",
            "СтатьяОборотов_Представление",
            "СтатьяОборотов_UUID",
            "СтатьяОборотов_Тип",
        ],
        "base_columns": {
            "parent_uuid": "TEXT",
            "parent_link": "TEXT",
            "НомерСтроки": "INTEGER",
        },
        "reference_columns": [
            "Контрагент",
            "СтатьяОборотов",
        ],
    },
]


def build_main_query_text(mode: str) -> str:
    limit_clause = ""
    if mode == "test":
        limit_clause = f"    ПЕРВЫЕ {TEST_LIMIT}\n"
    return MAIN_QUERY_TEMPLATE.format(limit_clause=limit_clause)


def _log(message: str) -> None:
    verbose_print(message)


def load_payment_registry_types(
    sqlite_db_file: str,
    com_object,
    mode: str = DEFAULT_MODE,
    process_func=None,
    filters_db: str | None = None,
) -> bool:
    _log("=" * 80)
    _log("ЗАГРУЗКА СПРАВОЧНИКА «CUSTOMВИДРЕЕСТРАПЛАТЕЖЕЙ»")
    _log("=" * 80)

    if com_object is None:
        _log("Ошибка: com_object обязателен")
        return False
    
    mode = mode or DEFAULT_MODE
    if mode not in ("test", "full"):
        _log(f"Неизвестный режим '{mode}'. Доступны: test, full.")
        return False

    limit_descr = f"первые {TEST_LIMIT} записей" if mode == "test" else "полная выборка"
    _log(f"\n[2/4] Чтение элементов (режим: {mode.upper()} — {limit_descr})...")

    try:
        query_text = build_main_query_text(mode)
        rows, references = execute_query(
            com_object,
            query_text,
            MAIN_QUERY_COLUMNS,
            reference_attr="Ссылка",
            uuid_column="uuid",
        )
    except Exception as error:
        _log(f"Ошибка выполнения запроса: {error}")
        return False

    filter_uuids = get_catalog_uuids(filters_db, TABLE_NAME)
    if filter_uuids:
        uuid_set = {value.strip().lower() for value in filter_uuids if value}
        filtered_rows = []
        filtered_refs = []
        for row, ref in zip(rows, references):
            row_uuid = str(row.get("uuid", "") or "").strip().lower()
            if row_uuid and row_uuid in uuid_set:
                filtered_rows.append(row)
                filtered_refs.append(ref)
        rows, references = filtered_rows, filtered_refs
        _log(f"Применено ограничение по UUID: {len(rows)} записей из {len(filter_uuids)}")

    if not rows:
        _log("Предупреждение: записи справочника «customВидРеестраПлатежей» не найдены.")

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
        saved = upsert_rows(
            connection,
            TABLE_NAME,
            rows,
            {
                "uuid": "TEXT PRIMARY KEY",
                "Ссылка": "TEXT",
                "Код": "TEXT",
                "Наименование": "TEXT",
            },
        )
        _log(f"Сохранено строк: {saved}")

        tabular_saved = save_tabular_sections(
            com_object,
            connection,
            TABULAR_QUERIES,
            references,
        )
        for section_name, count in tabular_saved.items():
            _log(f"Сохранено строк табличной части '{section_name}': {count}")
    finally:
        connection.close()

    return True

