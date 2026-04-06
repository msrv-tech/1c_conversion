# -*- coding: utf-8 -*-
"""
Загрузка справочника «Прочие доходы и расходы» из 1С в SQLite.

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
    upsert_rows,
)

TABLE_NAME = "other_income_and_expenses"

TEST_LIMIT = 50
DEFAULT_MODE = "test"

MAIN_QUERY_TEMPLATE = """ВЫБРАТЬ
{limit_clause}    Каталог.Ссылка КАК Ссылка,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Ссылка)) КАК uuid,
    Каталог.Код КАК Код,
    Каталог.Наименование КАК Наименование,
    Каталог.ПометкаУдаления КАК ПометкаУдаления,
    Каталог.ЭтоГруппа КАК ЭтоГруппа,
    Каталог.Родитель КАК Родитель,
    ПРЕДСТАВЛЕНИЕ(Каталог.Родитель) КАК Родитель_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Родитель)) КАК Родитель_UUID,
    ВЫБОР
        КОГДА Каталог.Родитель = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.Родитель)
    КОНЕЦ КАК Родитель_Тип,
    ВЫБОР
        КОГДА Каталог.Родитель = НЕОПРЕДЕЛЕНО
            ТОГДА ЛОЖЬ
        ИНАЧЕ Каталог.Родитель.ЭтоГруппа
    КОНЕЦ КАК Родитель_ЭтоГруппа,
    Каталог.ВидПрочихДоходовИРасходов КАК ВидПрочихДоходовИРасходов,
    ПРЕДСТАВЛЕНИЕ(Каталог.ВидПрочихДоходовИРасходов) КАК ВидПрочихДоходовИРасходов_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.ВидПрочихДоходовИРасходов)) КАК ВидПрочихДоходовИРасходов_UUID,
    ВЫБОР
        КОГДА Каталог.ВидПрочихДоходовИРасходов = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.ВидПрочихДоходовИРасходов)
    КОНЕЦ КАК ВидПрочихДоходовИРасходов_Тип,
    Каталог.ПринятиеКналоговомуУчету КАК ПринятиеКналоговомуУчету,
    Каталог.customСтатьяБДР КАК customСтатьяБДР,
    ПРЕДСТАВЛЕНИЕ(Каталог.customСтатьяБДР) КАК customСтатьяБДР_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.customСтатьяБДР)) КАК customСтатьяБДР_UUID,
    ВЫБОР
        КОГДА Каталог.customСтатьяБДР = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.customСтатьяБДР)
    КОНЕЦ КАК customСтатьяБДР_Тип,
    Каталог.customСтатьяБДР2020 КАК customСтатьяБДР2020,
    ПРЕДСТАВЛЕНИЕ(Каталог.customСтатьяБДР2020) КАК customСтатьяБДР2020_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.customСтатьяБДР2020)) КАК customСтатьяБДР2020_UUID,
    ВЫБОР
        КОГДА Каталог.customСтатьяБДР2020 = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.customСтатьяБДР2020)
    КОНЕЦ КАК customСтатьяБДР2020_Тип
ИЗ
    Справочник.ПрочиеДоходыИРасходы КАК Каталог
"""

MAIN_QUERY_COLUMNS: List[str] = [
    "Ссылка",
    "uuid",
    "Код",
    "Наименование",
    "ПометкаУдаления",
    "ЭтоГруппа",
    "Родитель",
    "Родитель_Представление",
    "Родитель_UUID",
    "Родитель_Тип",
    "Родитель_ЭтоГруппа",  # Временная колонка для добавления в JSON
    "ВидПрочихДоходовИРасходов",
    "ВидПрочихДоходовИРасходов_Представление",
    "ВидПрочихДоходовИРасходов_UUID",
    "ВидПрочихДоходовИРасходов_Тип",
    "ПринятиеКналоговомуУчету",
    "customСтатьяБДР",
    "customСтатьяБДР_Представление",
    "customСтатьяБДР_UUID",
    "customСтатьяБДР_Тип",
    "customСтатьяБДР2020",
    "customСтатьяБДР2020_Представление",
    "customСтатьяБДР2020_UUID",
    "customСтатьяБДР2020_Тип",
]

REFERENCE_COLUMNS: List[str] = [
    "Родитель",
    "ВидПрочихДоходовИРасходов",
    "customСтатьяБДР",
    "customСтатьяБДР2020",
]


def build_main_query_text(mode: str) -> str:
    limit_clause = ""
    if mode == "test":
        limit_clause = f"    ПЕРВЫЕ {TEST_LIMIT}\n"
    return MAIN_QUERY_TEMPLATE.format(limit_clause=limit_clause)


def _log(message: str) -> None:
    verbose_print(message)


def load_other_income_and_expenses(
    sqlite_db_file: str,
    com_object,
    mode: str = DEFAULT_MODE,
    process_func=None,
    filters_db: str | None = None,
) -> bool:
    _log("=" * 80)
    _log("ЗАГРУЗКА СПРАВОЧНИКА «ПРОЧИЕ ДОХОДЫ И РАСХОДЫ»")
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
                "Родитель_ЭтоГруппа": "INTEGER",  # Сохраняем для использования в процессоре
            },
        )
        _log(f"Сохранено строк: {saved}")
    finally:
        connection.close()

    return True

