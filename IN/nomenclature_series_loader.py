# -*- coding: utf-8 -*-
"""
Загрузка справочника «Серии номенклатуры» из 1С в SQLite.
Владелец серии — только Номенклатура.
Загружает поля серии и все поля владельца (номенклатуры).
"""

import os
import sys
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import (
    connect_to_sqlite,
    ensure_database_exists,
    process_reference_fields,
)
from tools.filters_manager import get_catalog_uuids
from tools.logger import verbose_print
from tools.onec_connector import (
    connect_to_1c,
    execute_query,
    upsert_rows,
)

TABLE_NAME = "nomenclature_series"

TEST_LIMIT = 50
DEFAULT_MODE = "test"

# Запрос: серии с владельцем Номенклатура, поля номенклатуры через Каталог.Владелец
MAIN_QUERY_TEMPLATE = """ВЫБРАТЬ
{limit_clause}    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Ссылка)) КАК uuid,
    Каталог.Ссылка КАК Ссылка,
    Каталог.СерийныйНомер КАК СерийныйНомер,
    ПРЕДСТАВЛЕНИЕ(Каталог.Ссылка) КАК Серия_Представление,
    Каталог.ПометкаУдаления КАК ПометкаУдаления,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Владелец)) КАК Владелец_UUID,
    Каталог.Владелец.Код КАК Код,
    Каталог.Владелец.Наименование КАК Наименование,
    Каталог.Владелец.ЭтоГруппа КАК ЭтоГруппа,
    Каталог.Владелец.Родитель КАК Родитель,
    ПРЕДСТАВЛЕНИЕ(Каталог.Владелец.Родитель) КАК Родитель_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Владелец.Родитель)) КАК Родитель_UUID,
    ВЫБОР
        КОГДА Каталог.Владелец.Родитель = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.Владелец.Родитель)
    КОНЕЦ КАК Родитель_Тип,
    Каталог.Владелец.Артикул КАК Артикул,
    Каталог.Владелец.ВидНоменклатуры КАК ВидНоменклатуры,
    ПРЕДСТАВЛЕНИЕ(Каталог.Владелец.ВидНоменклатуры) КАК ВидНоменклатуры_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Владелец.ВидНоменклатуры)) КАК ВидНоменклатуры_UUID,
    ВЫБОР
        КОГДА Каталог.Владелец.ВидНоменклатуры = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.Владелец.ВидНоменклатуры)
    КОНЕЦ КАК ВидНоменклатуры_Тип,
    Каталог.Владелец.БазоваяЕдиницаИзмерения КАК БазоваяЕдиницаИзмерения,
    ПРЕДСТАВЛЕНИЕ(Каталог.Владелец.БазоваяЕдиницаИзмерения) КАК БазоваяЕдиницаИзмерения_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Владелец.БазоваяЕдиницаИзмерения)) КАК БазоваяЕдиницаИзмерения_UUID,
    ВЫБОР
        КОГДА Каталог.Владелец.БазоваяЕдиницаИзмерения = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.Владелец.БазоваяЕдиницаИзмерения)
    КОНЕЦ КАК БазоваяЕдиницаИзмерения_Тип,
    Каталог.Владелец.СтавкаНДС КАК СтавкаНДС,
    ПРЕДСТАВЛЕНИЕ(Каталог.Владелец.СтавкаНДС) КАК СтавкаНДС_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Владелец.СтавкаНДС)) КАК СтавкаНДС_UUID,
    ВЫБОР
        КОГДА Каталог.Владелец.СтавкаНДС = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.Владелец.СтавкаНДС)
    КОНЕЦ КАК СтавкаНДС_Тип,
    Каталог.Владелец.Комментарий КАК Комментарий
ИЗ
    Справочник.СерииНоменклатуры КАК Каталог
"""

MAIN_QUERY_COLUMNS: List[str] = [
    "uuid",
    "Ссылка",
    "СерийныйНомер",
    "Серия_Представление",
    "ПометкаУдаления",
    "Владелец_UUID",
    "Код",
    "Наименование",
    "ЭтоГруппа",
    "Родитель",
    "Родитель_Представление",
    "Родитель_UUID",
    "Родитель_Тип",
    "Артикул",
    "ВидНоменклатуры",
    "ВидНоменклатуры_Представление",
    "ВидНоменклатуры_UUID",
    "ВидНоменклатуры_Тип",
    "БазоваяЕдиницаИзмерения",
    "БазоваяЕдиницаИзмерения_Представление",
    "БазоваяЕдиницаИзмерения_UUID",
    "БазоваяЕдиницаИзмерения_Тип",
    "СтавкаНДС",
    "СтавкаНДС_Представление",
    "СтавкаНДС_UUID",
    "СтавкаНДС_Тип",
    "Комментарий",
]

REFERENCE_COLUMNS: List[str] = [
    "Родитель",
    "ВидНоменклатуры",
    "БазоваяЕдиницаИзмерения",
    "СтавкаНДС",
]


def build_main_query_text(mode: str) -> str:
    limit_clause = ""
    if mode == "test":
        limit_clause = f"    ПЕРВЫЕ {TEST_LIMIT}\n"
    return MAIN_QUERY_TEMPLATE.format(limit_clause=limit_clause)


def _log(message: str) -> None:
    verbose_print(message)


def load_nomenclature_series(
    sqlite_db_file: str,
    com_object,
    mode: str = DEFAULT_MODE,
    process_func=None,
    filters_db: str | None = None,
) -> bool:
    _log("=" * 80)
    _log("ЗАГРУЗКА СПРАВОЧНИКА «СЕРИИ НОМЕНКЛАТУРЫ»")
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
        result = execute_query(
            com_object,
            query_text,
            MAIN_QUERY_COLUMNS,
            reference_columns=REFERENCE_COLUMNS,
        )
        if isinstance(result, tuple):
            rows, _ = result
        else:
            rows = result
    except Exception as error:
        _log(f"Ошибка выполнения запроса: {error}")
        import traceback
        traceback.print_exc()
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
                "СерийныйНомер": "TEXT",
                "Серия_Представление": "TEXT",
                "Владелец_UUID": "TEXT",  # для copy_ppe_for_nom_children
            },
        )
        _log(f"Сохранено строк: {saved}")
    finally:
        connection.close()

    return True
