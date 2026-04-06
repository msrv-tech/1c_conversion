# -*- coding: utf-8 -*-
"""
Загрузка справочника «Нематериальные активы» из 1С в SQLite.

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

TABLE_NAME = "intangible_assets"

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
    Каталог.НаименованиеПолное КАК НаименованиеПолное,
    Каталог.ВидНМА КАК ВидНМА,
    ПРЕДСТАВЛЕНИЕ(Каталог.ВидНМА) КАК ВидНМА_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.ВидНМА)) КАК ВидНМА_UUID,
    ВЫБОР
        КОГДА Каталог.ВидНМА = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.ВидНМА)
    КОНЕЦ КАК ВидНМА_Тип,
    Каталог.АмортизационнаяГруппа КАК АмортизационнаяГруппа,
    ПРЕДСТАВЛЕНИЕ(Каталог.АмортизационнаяГруппа) КАК АмортизационнаяГруппа_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.АмортизационнаяГруппа)) КАК АмортизационнаяГруппа_UUID,
    ВЫБОР
        КОГДА Каталог.АмортизационнаяГруппа = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.АмортизационнаяГруппа)
    КОНЕЦ КАК АмортизационнаяГруппа_Тип,
    Каталог.ПрочиеСведения КАК ПрочиеСведения,
    Каталог.ВидОбъектаУчета КАК ВидОбъектаУчета,
    ПРЕДСТАВЛЕНИЕ(Каталог.ВидОбъектаУчета) КАК ВидОбъектаУчета_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.ВидОбъектаУчета)) КАК ВидОбъектаУчета_UUID,
    ВЫБОР
        КОГДА Каталог.ВидОбъектаУчета = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.ВидОбъектаУчета)
    КОНЕЦ КАК ВидОбъектаУчета_Тип,
    Каталог.ВключенВРеестрРоссийскогоПО КАК ВключенВРеестрРоссийскогоПО,
    Каталог.НомерРеестрРоссийскогоПО КАК НомерРеестрРоссийскогоПО,
    Каталог.ДатаРеестрРоссийскогоПО КАК ДатаРеестрРоссийскогоПО
ИЗ
    Справочник.НематериальныеАктивы КАК Каталог
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
    "НаименованиеПолное",
    "ВидНМА",
    "ВидНМА_Представление",
    "ВидНМА_UUID",
    "ВидНМА_Тип",
    "АмортизационнаяГруппа",
    "АмортизационнаяГруппа_Представление",
    "АмортизационнаяГруппа_UUID",
    "АмортизационнаяГруппа_Тип",
    "ПрочиеСведения",
    "ВидОбъектаУчета",
    "ВидОбъектаУчета_Представление",
    "ВидОбъектаУчета_UUID",
    "ВидОбъектаУчета_Тип",
    "ВключенВРеестрРоссийскогоПО",
    "НомерРеестрРоссийскогоПО",
    "ДатаРеестрРоссийскогоПО",
]

REFERENCE_COLUMNS: List[str] = [
    "Родитель",
    "ВидНМА",
    "АмортизационнаяГруппа",
    "ВидОбъектаУчета",
]


def build_main_query_text(mode: str) -> str:
    limit_clause = ""
    if mode == "test":
        limit_clause = f"    ПЕРВЫЕ {TEST_LIMIT}\n"
    return MAIN_QUERY_TEMPLATE.format(limit_clause=limit_clause)


def _log(message: str) -> None:
    verbose_print(message)


def load_intangible_assets(
    sqlite_db_file: str,
    com_object,
    mode: str = DEFAULT_MODE,
    process_func=None,
    filters_db: str | None = None,
) -> bool:
    _log("=" * 80)
    _log("ЗАГРУЗКА СПРАВОЧНИКА «НЕМАТЕРИАЛЬНЫЕ АКТИВЫ»")
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
        rows = [
            row
            for row in rows
            if str(row.get("uuid", "") or "").strip().lower() in uuid_set
        ]
        _log(f"Применено ограничение по UUID: {len(rows)} записей из {len(filter_uuids)}")

    if not rows:
        _log("Предупреждение: записи справочника «Нематериальные активы» не найдены.")

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
                "НаименованиеПолное": "TEXT",
            },
        )
        _log(f"Сохранено строк: {saved}")
    finally:
        connection.close()

    return True

