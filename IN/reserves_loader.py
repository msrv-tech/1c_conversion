# -*- coding: utf-8 -*-
"""
Загрузка справочника «Резервы» из 1С в SQLite.

Запрос сформирован утилитой `tools/generate_1c_query.py`.
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
 connect_to_1c,
    execute_query,
    save_tabular_sections,
    upsert_rows,
)

TABLE_NAME = "reserves"

TEST_LIMIT = 50
DEFAULT_MODE = "test"

MAIN_QUERY_TEMPLATE = """ВЫБРАТЬ
{limit_clause}    Каталог.Ссылка КАК Ссылка,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Ссылка)) КАК uuid,
    Каталог.Код КАК Код,
    Каталог.Наименование КАК Наименование
ИЗ
    Справочник.Резервы КАК Каталог
"""

MAIN_QUERY_COLUMNS: List[str] = [
    "Ссылка",
    "uuid",
    "Код",
    "Наименование",
    "ПометкаУдаления",
]

TABULAR_QUERIES: List[dict] = [
    {
        "name": "БазовыеВидыРасчета",
        "table": "reserves_base_calculation_types",
        "query": """ВЫБРАТЬ
    ТЧ.Ссылка КАК parent_link,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ТЧ.Ссылка)) КАК parent_uuid,
    ТЧ.НомерСтроки КАК НомерСтроки,
    ТЧ.ВидРасчета КАК ВидРасчета,
    ПРЕДСТАВЛЕНИЕ(ТЧ.ВидРасчета) КАК ВидРасчета_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ТЧ.ВидРасчета)) КАК ВидРасчета_UUID,
    ВЫБОР
        КОГДА ТЧ.ВидРасчета = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(ТЧ.ВидРасчета)
    КОНЕЦ КАК ВидРасчета_Тип
ИЗ
    Справочник.Резервы.БазовыеВидыРасчета КАК ТЧ
ГДЕ
    ТЧ.Ссылка В (&Ссылки)
УПОРЯДОЧИТЬ ПО
    ТЧ.Ссылка,
    ТЧ.НомерСтроки""",
        "columns": [
            "parent_link",
            "parent_uuid",
            "НомерСтроки",
            "ВидРасчета",
            "ВидРасчета_Представление",
            "ВидРасчета_UUID",
            "ВидРасчета_Тип",
        ],
        "base_columns": {
            "parent_uuid": "TEXT",
            "parent_link": "TEXT",
            "НомерСтроки": "INTEGER",
        },
        "reference_columns": ["ВидРасчета"],
    },
]

REFERENCE_COLUMNS: List[str] = []


def build_main_query_text(mode: str) -> str:
    limit_clause = ""
    if mode == "test":
        limit_clause = f"    ПЕРВЫЕ {TEST_LIMIT}\n"
    return MAIN_QUERY_TEMPLATE.format(limit_clause=limit_clause)


def _log(message: str) -> None:
    verbose_print(message)


def load_reserves(
    sqlite_db_file: str,
    com_object,
    mode: str = DEFAULT_MODE,
    process_func=None,
    filters_db: str | None = None,
) -> bool:
    _log("=" * 80)
    _log("ЗАГРУЗКА СПРАВОЧНИКА «РЕЗЕРВЫ»")
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
        _log("Предупреждение: записи справочника «Резервы» не найдены.")

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
        # Обрабатываем поля-ссылки и перечисления
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

