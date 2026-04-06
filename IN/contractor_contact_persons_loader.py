# -*- coding: utf-8 -*-
"""
Загрузка справочника «Контактные лица контрагентов» из 1С в SQLite.

Запрос сформирован с помощью `tools/generate_1c_query.py`.
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
    save_tabular_sections,
    build_reference_array,
    ensure_table_schema,
)

TABLE_NAME = "contractor_contact_persons"

TEST_LIMIT = 50
DEFAULT_MODE = "test"

MAIN_QUERY_TEMPLATE = """ВЫБРАТЬ
{limit_clause}    Каталог.Ссылка КАК Ссылка,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Ссылка)) КАК uuid,
    Каталог.Код КАК Код,
    Каталог.Наименование КАК Наименование,
    Каталог.ПометкаУдаления КАК ПометкаУдаления,
    Каталог.Владелец КАК Владелец,
    ПРЕДСТАВЛЕНИЕ(Каталог.Владелец) КАК Владелец_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Владелец)) КАК Владелец_UUID,
    ВЫБОР
        КОГДА Каталог.Владелец = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.Владелец)
    КОНЕЦ КАК Владелец_Тип,
    Каталог.Должность КАК Должность,
    Каталог.Комментарий КАК Комментарий,
    Каталог.КонтактноеЛицо КАК КонтактноеЛицо,
    ПРЕДСТАВЛЕНИЕ(Каталог.КонтактноеЛицо) КАК КонтактноеЛицо_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.КонтактноеЛицо)) КАК КонтактноеЛицо_UUID,
    ВЫБОР
        КОГДА Каталог.КонтактноеЛицо = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.КонтактноеЛицо)
    КОНЕЦ КАК КонтактноеЛицо_Тип,
    Каталог.РольКонтактногоЛица КАК РольКонтактногоЛица,
    ПРЕДСТАВЛЕНИЕ(Каталог.РольКонтактногоЛица) КАК РольКонтактногоЛица_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.РольКонтактногоЛица)) КАК РольКонтактногоЛица_UUID,
    ВЫБОР
        КОГДА Каталог.РольКонтактногоЛица = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.РольКонтактногоЛица)
    КОНЕЦ КАК РольКонтактногоЛица_Тип,
    Каталог.customКонтактФизлицоCUSTOM КАК customКонтактФизлицоCUSTOM,
    ПРЕДСТАВЛЕНИЕ(Каталог.customКонтактФизлицоCUSTOM) КАК customКонтактФизлицоCUSTOM_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.customКонтактФизлицоCUSTOM)) КАК customКонтактФизлицоCUSTOM_UUID,
    ВЫБОР
        КОГДА Каталог.customКонтактФизлицоCUSTOM = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.customКонтактФизлицоCUSTOM)
    КОНЕЦ КАК customКонтактФизлицоCUSTOM_Тип
ИЗ
    Справочник.КонтактныеЛицаКонтрагентов КАК Каталог
"""

MAIN_QUERY_COLUMNS: List[str] = [
    "Ссылка",
    "uuid",
    "Код",
    "Наименование",
    "ПометкаУдаления",
    "Владелец",
    "Владелец_Представление",
    "Владелец_UUID",
    "Владелец_Тип",
    "Должность",
    "Комментарий",
    "КонтактноеЛицо",
    "КонтактноеЛицо_Представление",
    "КонтактноеЛицо_UUID",
    "КонтактноеЛицо_Тип",
    "РольКонтактногоЛица",
    "РольКонтактногоЛица_Представление",
    "РольКонтактногоЛица_UUID",
    "РольКонтактногоЛица_Тип",
    "customКонтактФизлицоCUSTOM",
    "customКонтактФизлицоCUSTOM_Представление",
    "customКонтактФизлицоCUSTOM_UUID",
    "customКонтактФизлицоCUSTOM_Тип",
]

REFERENCE_COLUMNS: List[str] = [
    "Владелец",
    "КонтактноеЛицо",
    "РольКонтактногоЛица",
    "customКонтактФизлицоCUSTOM",
]

# В источнике (UPP) у КонтактныеЛицаКонтрагентов нет табличной части КонтактнаяИнформация
# Контактная информация хранится в регистре сведений КонтактнаяИнформация
# Загружаем из регистра сведений, как для контрагентов
TABULAR_QUERIES: List[dict] = []

# Запрос для загрузки контактной информации из регистра сведений
CONTACT_INFO_QUERY = """ВЫБРАТЬ
    Регистр.Объект КАК Объект,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Регистр.Объект)) КАК parent_uuid,
    Регистр.Тип КАК Тип,
    ПРЕДСТАВЛЕНИЕ(Регистр.Тип) КАК Тип_Представление,
    ВЫБОР
        КОГДА Регистр.Тип = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Регистр.Тип)
    КОНЕЦ КАК Тип_Тип,
    Регистр.Вид КАК Вид,
    ПРЕДСТАВЛЕНИЕ(Регистр.Вид) КАК Вид_Представление,
    ВЫБОР
        КОГДА Регистр.Вид = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Регистр.Вид)
    КОНЕЦ КАК Вид_Тип,
    Регистр.Представление КАК Представление,
    Регистр.Значение КАК Значение
ИЗ
    РегистрСведений.КонтактнаяИнформация КАК Регистр
ГДЕ
    Регистр.Объект В (&Ссылки)
УПОРЯДОЧИТЬ ПО
    Регистр.Объект,
    Регистр.Тип,
    Регистр.Вид"""

CONTACT_INFO_COLUMNS = [
    "parent_uuid",
    "Тип",
    "Тип_Представление",
    "Тип_Тип",
    "Вид",
    "Вид_Представление",
    "Вид_Тип",
    "Представление",
    "Значение",
]


def build_main_query_text(mode: str) -> str:
    limit_clause = ""
    if mode == "test":
        limit_clause = f"    ПЕРВЫЕ {TEST_LIMIT}\n"
    return MAIN_QUERY_TEMPLATE.format(limit_clause=limit_clause)


def _log(message: str) -> None:
    verbose_print(message)


def load_contractor_contact_persons(
    sqlite_db_file: str,
    com_object,
    mode: str = DEFAULT_MODE,
    process_func=None,
    filters_db: str | None = None,
) -> bool:
    _log("=" * 80)
    _log("ЗАГРУЗКА СПРАВОЧНИКА «КОНТАКТНЫЕ ЛИЦА КОНТРАГЕНТОВ»")
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
            },
        )
        _log(f"Сохранено строк: {saved}")

        # Загружаем контактную информацию из регистра сведений
        _log("\n[5/5] Загрузка контактной информации из регистра сведений...")
        contact_info_saved = _load_contact_info_from_register(
            com_object,
            connection,
            references,
        )
        if contact_info_saved > 0:
            _log(f"Сохранено строк контактной информации: {contact_info_saved}")
    finally:
        connection.close()

    return True


def _load_contact_info_from_register(
    com_object,
    connection,
    references,
) -> int:
    """
    Загружает контактную информацию из регистра сведений КонтактнаяИнформация
    и сохраняет как табличную часть contractor_contact_persons_contact_info.
    
    Args:
        com_object: COM-объект подключения к 1С
        connection: Подключение к SQLite
        references: Список ссылок на контактные лица контрагентов
        
    Returns:
        Количество сохраненных записей
    """
    from tools.onec_connector import (
        build_reference_array,
        execute_query,
        ensure_table_schema,
        upsert_rows,
    )
    from tools.db_manager import process_reference_fields
    import json
    
    if not references:
        return 0
    
    ref_array = build_reference_array(com_object, references)
    if not ref_array:
        return 0
    
    try:
        # Выполняем запрос к регистру сведений
        rows = execute_query(
            com_object,
            CONTACT_INFO_QUERY,
            CONTACT_INFO_COLUMNS,
            params={"Ссылки": ref_array},
        )
        
        if not rows:
            return 0
        
        # Для Тип и Вид сохраняем как строки (могут быть перечислениями)
        for row in rows:
            if "Тип_Представление" in row:
                row["Тип"] = row.pop("Тип_Представление", "")
                row.pop("Тип_Тип", None)
            if "Вид_Представление" in row:
                row["Вид"] = row.pop("Вид_Представление", "")
                row.pop("Вид_Тип", None)
        
        # Добавляем НомерСтроки для каждой записи контактного лица
        # Группируем по parent_uuid и нумеруем
        grouped_rows = {}
        for row in rows:
            parent_uuid = row.get("parent_uuid", "")
            if parent_uuid not in grouped_rows:
                grouped_rows[parent_uuid] = []
            grouped_rows[parent_uuid].append(row)
        
        # Нумеруем строки
        numbered_rows = []
        for parent_uuid, group_rows in grouped_rows.items():
            for i, row in enumerate(group_rows, 1):
                row["НомерСтроки"] = i
                numbered_rows.append(row)
        
        # Определяем базовые колонки
        base_columns = {
            "parent_uuid": "TEXT",
            "НомерСтроки": "INTEGER",
        }
        
        # Создаем таблицу
        table_name = "contractor_contact_persons_contact_info"
        ensure_table_schema(connection, table_name, CONTACT_INFO_COLUMNS + ["НомерСтроки"], base_columns)
        
        # Очищаем существующие записи для загружаемых контактных лиц
        if numbered_rows:
            cursor = connection.cursor()
            parent_uuids = set(row.get("parent_uuid", "") for row in numbered_rows if row.get("parent_uuid"))
            if parent_uuids:
                cursor.execute(
                    "CREATE TEMP TABLE IF NOT EXISTS __tmp_parent_uuids(uuid TEXT)"
                )
                cursor.execute("DELETE FROM __tmp_parent_uuids")
                cursor.executemany(
                    "INSERT INTO __tmp_parent_uuids(uuid) VALUES (?)",
                    ((uuid,) for uuid in parent_uuids),
                )
                cursor.execute(
                    f'''
                    DELETE FROM "{table_name}"
                    WHERE parent_uuid IN (SELECT uuid FROM __tmp_parent_uuids)
                    '''
                )
                cursor.execute("DELETE FROM __tmp_parent_uuids")
                connection.commit()
        
        # Сохраняем данные
        saved = upsert_rows(connection, table_name, numbered_rows, base_columns)
        return saved
        
    except Exception as error:
        from tools.logger import verbose_print
        verbose_print(f"Ошибка при загрузке контактной информации: {error}")
        import traceback
        traceback.print_exc()
        return 0

