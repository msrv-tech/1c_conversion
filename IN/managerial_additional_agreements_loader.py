# -*- coding: utf-8 -*-
"""
Загрузка справочника «УправленческиеДополнительныеСоглашения» из 1С в SQLite.

Запрос сформирован утилитой `tools/generate_1c_query.py`.
Справочник имеет владельца.
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
from tools.generate_1c_query import (  # noqa: E402
    _collect_tabular_sections,
    _build_tabular_query,
)

TABLE_NAME = "managerial_additional_agreements"
CATALOG_PATH = "Справочник.УправленческиеДополнительныеСоглашения"

TEST_LIMIT = 50
DEFAULT_MODE = "test"

MAIN_QUERY_TEMPLATE = """ВЫБРАТЬ
{limit_clause}    Каталог.Ссылка КАК Ссылка,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Ссылка)) КАК uuid,
    Каталог.Код КАК Код,
    Каталог.Наименование КАК Наименование,
    Каталог.Владелец КАК Владелец,
    ПРЕДСТАВЛЕНИЕ(Каталог.Владелец) КАК Владелец_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Владелец)) КАК Владелец_UUID,
    ВЫБОР
        КОГДА Каталог.Владелец = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.Владелец)
    КОНЕЦ КАК Владелец_Тип,
    Каталог.Заказчик КАК Заказчик,
    ПРЕДСТАВЛЕНИЕ(Каталог.Заказчик) КАК Заказчик_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Заказчик)) КАК Заказчик_UUID,
    ВЫБОР
        КОГДА Каталог.Заказчик = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.Заказчик)
    КОНЕЦ КАК Заказчик_Тип,
    Каталог.Исполнитель КАК Исполнитель,
    ПРЕДСТАВЛЕНИЕ(Каталог.Исполнитель) КАК Исполнитель_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Исполнитель)) КАК Исполнитель_UUID,
    ВЫБОР
        КОГДА Каталог.Исполнитель = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.Исполнитель)
    КОНЕЦ КАК Исполнитель_Тип,
    Каталог.Дата КАК Дата,
    Каталог.ДатаПодписания КАК ДатаПодписания,
    Каталог.НаименованиеПолное КАК НаименованиеПолное,
    Каталог.Шифр КАК Шифр,
    Каталог.НачалоДействия КАК НачалоДействия,
    Каталог.КонецДействия КАК КонецДействия,
    Каталог.РазрешеноНачалоРабот КАК РазрешеноНачалоРабот,
    Каталог.ВременнаяЗагрузка КАК ВременнаяЗагрузка,
    Каталог.ВидДополнительногоСоглашения КАК ВидДополнительногоСоглашения,
    ПРЕДСТАВЛЕНИЕ(Каталог.ВидДополнительногоСоглашения) КАК ВидДополнительногоСоглашения_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.ВидДополнительногоСоглашения)) КАК ВидДополнительногоСоглашения_UUID,
    ВЫБОР
        КОГДА Каталог.ВидДополнительногоСоглашения = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.ВидДополнительногоСоглашения)
    КОНЕЦ КАК ВидДополнительногоСоглашения_Тип,
    Каталог.Статус КАК Статус,
    ПРЕДСТАВЛЕНИЕ(Каталог.Статус) КАК Статус_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.Статус)) КАК Статус_UUID,
    ВЫБОР
        КОГДА Каталог.Статус = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.Статус)
    КОНЕЦ КАК Статус_Тип,
    Каталог.НомерПодписания КАК НомерПодписания,
    Каталог.ДатаЗагрузкиНаФилиал КАК ДатаЗагрузкиНаФилиал,
    Каталог.ПроцентАванса КАК ПроцентАванса,
    Каталог.Сумма КАК Сумма,
    Каталог.КонтрагентДляВзаиморасчетов КАК КонтрагентДляВзаиморасчетов,
    ПРЕДСТАВЛЕНИЕ(Каталог.КонтрагентДляВзаиморасчетов) КАК КонтрагентДляВзаиморасчетов_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Каталог.КонтрагентДляВзаиморасчетов)) КАК КонтрагентДляВзаиморасчетов_UUID,
    ВЫБОР
        КОГДА Каталог.КонтрагентДляВзаиморасчетов = НЕОПРЕДЕЛЕНО
            ТОГДА ""
        ИНАЧЕ ТИПЗНАЧЕНИЯ(Каталог.КонтрагентДляВзаиморасчетов)
    КОНЕЦ КАК КонтрагентДляВзаиморасчетов_Тип
ИЗ
    Справочник.УправленческиеДополнительныеСоглашения КАК Каталог
"""

MAIN_QUERY_COLUMNS: List[str] = [
    "Ссылка",
    "uuid",
    "Код",
    "Наименование",
    "Владелец",
    "Владелец_Представление",
    "Владелец_UUID",
    "Владелец_Тип",
    "Заказчик",
    "Заказчик_Представление",
    "Заказчик_UUID",
    "Заказчик_Тип",
    "Исполнитель",
    "Исполнитель_Представление",
    "Исполнитель_UUID",
    "Исполнитель_Тип",
    "Дата",
    "ДатаПодписания",
    "НаименованиеПолное",
    "Шифр",
    "НачалоДействия",
    "КонецДействия",
    "РазрешеноНачалоРабот",
    "ВременнаяЗагрузка",
    "ВидДополнительногоСоглашения",
    "ВидДополнительногоСоглашения_Представление",
    "ВидДополнительногоСоглашения_UUID",
    "ВидДополнительногоСоглашения_Тип",
    "Статус",
    "Статус_Представление",
    "Статус_UUID",
    "Статус_Тип",
    "НомерПодписания",
    "ДатаЗагрузкиНаФилиал",
    "ПроцентАванса",
    "Сумма",
    "КонтрагентДляВзаиморасчетов",
    "КонтрагентДляВзаиморасчетов_Представление",
    "КонтрагентДляВзаиморасчетов_UUID",
    "КонтрагентДляВзаиморасчетов_Тип",
]

REFERENCE_COLUMNS: List[str] = [
    "Владелец",
    "Заказчик",
    "Исполнитель",
    "ВидДополнительногоСоглашения",
    "Статус",
    "КонтрагентДляВзаиморасчетов",
]


def build_main_query_text(mode: str) -> str:
    limit_clause = ""
    if mode == "test":
        limit_clause = f"    ПЕРВЫЕ {TEST_LIMIT}\n"
    return MAIN_QUERY_TEMPLATE.format(limit_clause=limit_clause)


def _log(message: str) -> None:
    verbose_print(message)


def load_managerial_additional_agreements(
    sqlite_db_file: str,
    com_object,
    mode: str = DEFAULT_MODE,
    process_func=None,
    filters_db: str | None = None,
) -> bool:
    _log("=" * 80)
    _log("ЗАГРУЗКА СПРАВОЧНИКА «УПРАВЛЕНЧЕСКИЕ ДОПОЛНИТЕЛЬНЫЕ СОГЛАШЕНИЯ»")
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
        _log(f"Выполняем запрос к справочнику {CATALOG_PATH}...")
        raw_rows, raw_references = execute_query(
            com_object,
            query_text,
            MAIN_QUERY_COLUMNS,
            reference_attr="Ссылка",
            uuid_column="uuid",
        )
        rows = raw_rows
        if not rows:
            _log("Предупреждение: справочник пуст или не найден в источнике.")
            return False
    except Exception as error:
        _log(f"Ошибка выполнения запроса: {error}")
        import traceback
        _log(f"Детали ошибки:\n{traceback.format_exc()}")
        return False

    # Сохраняем ссылки для табличных частей до обработки
    # Используем raw_references, полученные из execute_query
    # save_tabular_sections ожидает список словарей вида {"uuid": "...", "reference": COM_объект}
    if 'raw_references' in locals() and raw_references:
        # Сохраняем как список словарей для save_tabular_sections
        references = raw_references
    else:
        references = []

    filter_uuids = get_catalog_uuids(filters_db, TABLE_NAME)
    if filter_uuids:
        uuid_set = {value.strip().lower() for value in filter_uuids if value}
        filtered_rows = []
        filtered_references = []
        
        # Фильтруем строки и соответствующие ссылки
        for i, row in enumerate(rows):
            row_uuid = str(row.get("uuid", "") or "").strip().lower()
            if row_uuid and row_uuid in uuid_set:
                filtered_rows.append(row)
                # Сохраняем соответствующую ссылку, если она есть
                # references - это список словарей вида {"uuid": "...", "reference": COM_объект}
                if i < len(references) and references[i] and references[i].get("reference"):
                    filtered_references.append(references[i])
        
        rows = filtered_rows
        references = filtered_references
        _log(f"Применено ограничение по UUID: {len(rows)} записей из {len(filter_uuids)}")

    if not rows:
        _log("Не удалось получить записи справочника.")
        return False

    _log(f"Получено записей: {len(rows)}")
    _log(f"Получено ссылок для табличных частей: {len(references)}")
    if references:
        # Проверяем, что ссылки действительно есть
        valid_refs = [r for r in references if r and r.get("reference")]
        _log(f"  Валидных ссылок (с COM-объектом): {len(valid_refs)}")

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

        # Загружаем табличные части
        try:
            # Получаем метаданные справочника для определения табличных частей
            from tools.onec_connector import find_catalog_metadata
            catalog_metadata, metadata_name = find_catalog_metadata(com_object, [CATALOG_PATH.split('.')[-1]])
            if not catalog_metadata:
                _log(f"Предупреждение: метаданные для справочника {CATALOG_PATH} не найдены")
            else:
                tabular_metadata = _collect_tabular_sections(catalog_metadata)
            
            if 'tabular_metadata' in locals() and tabular_metadata:
                _log(f"\nНайдено табличных частей: {len(tabular_metadata)}")
                tabular_queries = []
                
                for section_info in tabular_metadata:
                    section_name = section_info.get("name")
                    if section_name == "ГрафикПлатежей":
                        section_requisites = section_info.get("requisites", [])
                        # Строим запрос для табличной части
                        tabular_query, tabular_columns = _build_tabular_query(
                            com_object,
                            CATALOG_PATH,
                            section_name,
                            section_requisites
                        )
                        if tabular_query:
                            # Определяем ссылочные колонки для табличной части
                            reference_columns = []
                            for col in tabular_columns:
                                if not col.endswith("_Представление") and not col.endswith("_UUID") and not col.endswith("_Тип"):
                                    if col not in ["parent_link", "parent_uuid", "НомерСтроки"]:
                                        if f"{col}_UUID" in tabular_columns or f"{col}_Тип" in tabular_columns:
                                            reference_columns.append(col)
                            
                            tabular_queries.append({
                                "name": section_name,
                                "table": "managerial_additional_agreements_payment_schedule",
                                "query": tabular_query,
                                "columns": tabular_columns,
                                "base_columns": {
                                    "parent_uuid": "TEXT",
                                    "parent_link": "TEXT",
                                    "НомерСтроки": "INTEGER",
                                },
                                "reference_columns": reference_columns,
                            })
                            _log(f"  → Добавлен запрос для табличной части '{section_name}'")
                
                if tabular_queries:
                    tabular_saved = save_tabular_sections(
                        com_object,
                        connection,
                        tabular_queries,
                        references,
                    )
                    for section_name, count in tabular_saved.items():
                        _log(f"Сохранено строк табличной части '{section_name}': {count}")
        except Exception as tab_error:
            _log(f"Предупреждение: не удалось загрузить табличные части: {tab_error}")
            import traceback
            _log(f"Детали ошибки:\n{traceback.format_exc()}")
    finally:
        connection.close()

    return True
