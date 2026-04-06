# -*- coding: utf-8 -*-
"""
Загрузка остатков по счёту 006 (Бланки строгой отчётности) из 1С в SQLite.
Субконто: Субконто1 — Бланки строгой отчётности, Субконто2 — Склады.
"""

import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import (
    connect_to_sqlite,
    ensure_database_exists,
    process_reference_fields,
)
from tools.logger import verbose_print
from tools.onec_connector import (
    connect_to_1c,
    execute_batch_query,
    upsert_rows,
)

TABLE_NAME = "account_006_balances"

ACCOUNT_CODE = "006"

QUERY_TEXT = """
ВЫБРАТЬ
    ХозрасчетныйОстатки.Организация КАК Организация,
    ХозрасчетныйОстатки.Счет КАК СчетУчета,
    ХозрасчетныйОстатки.Субконто1 КАК БланкиСтрогойОтчетности,
    ХозрасчетныйОстатки.Субконто2 КАК Склад,
    СУММА(ХозрасчетныйОстатки.КоличествоОстаток) КАК КоличествоОстаток,
    СУММА(ХозрасчетныйОстатки.СуммаОстаток) КАК СуммаОстаток
ПОМЕСТИТЬ ВТ_Остатки
ИЗ
    РегистрБухгалтерии.Хозрасчетный.Остатки(ДАТАВРЕМЯ(2026, 1, 1), Счет В (&Счета), , ) КАК ХозрасчетныйОстатки
ГДЕ
    ХозрасчетныйОстатки.КоличествоОстаток <> 0
    ИЛИ ХозрасчетныйОстатки.СуммаОстаток <> 0
СГРУППИРОВАТЬ ПО
    ХозрасчетныйОстатки.Организация,
    ХозрасчетныйОстатки.Счет,
    ХозрасчетныйОстатки.Субконто1,
    ХозрасчетныйОстатки.Субконто2
;

////////////////////////////////////////////////////////////////////////////////
ВЫБРАТЬ
    ВТ_Остатки.Организация КАК Организация,
    ПРЕДСТАВЛЕНИЕ(ВТ_Остатки.Организация) КАК Организация_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ВТ_Остатки.Организация)) КАК Организация_UUID,
    ТИПЗНАЧЕНИЯ(ВТ_Остатки.Организация) КАК Организация_Тип,

    ВТ_Остатки.СчетУчета КАК СчетУчета,
    ПРЕДСТАВЛЕНИЕ(ВТ_Остатки.СчетУчета) КАК СчетУчета_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ВТ_Остатки.СчетУчета)) КАК СчетУчета_UUID,
    ТИПЗНАЧЕНИЯ(ВТ_Остатки.СчетУчета) КАК СчетУчета_Тип,

    ВТ_Остатки.БланкиСтрогойОтчетности КАК БланкиСтрогойОтчетности,
    ПРЕДСТАВЛЕНИЕ(ВТ_Остатки.БланкиСтрогойОтчетности) КАК БланкиСтрогойОтчетности_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ВТ_Остатки.БланкиСтрогойОтчетности)) КАК БланкиСтрогойОтчетности_UUID,
    ТИПЗНАЧЕНИЯ(ВТ_Остатки.БланкиСтрогойОтчетности) КАК БланкиСтрогойОтчетности_Тип,

    ВТ_Остатки.Склад КАК Склад,
    ПРЕДСТАВЛЕНИЕ(ВТ_Остатки.Склад) КАК Склад_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ВТ_Остатки.Склад)) КАК Склад_UUID,
    ТИПЗНАЧЕНИЯ(ВТ_Остатки.Склад) КАК Склад_Тип,

    ВТ_Остатки.КоличествоОстаток КАК КоличествоОстаток,
    ВТ_Остатки.СуммаОстаток КАК СуммаОстаток
ИЗ
    ВТ_Остатки КАК ВТ_Остатки
"""

COLUMNS = [
    "Организация", "Организация_Представление", "Организация_UUID", "Организация_Тип",
    "СчетУчета", "СчетУчета_Представление", "СчетУчета_UUID", "СчетУчета_Тип",
    "БланкиСтрогойОтчетности", "БланкиСтрогойОтчетности_Представление", "БланкиСтрогойОтчетности_UUID", "БланкиСтрогойОтчетности_Тип",
    "Склад", "Склад_Представление", "Склад_UUID", "Склад_Тип",
    "КоличествоОстаток", "СуммаОстаток",
]

REFERENCE_COLUMNS = [
    "Организация", "СчетУчета", "БланкиСтрогойОтчетности", "Склад",
]


def load_account_006_balances(sqlite_db_file: str, com_object, mode: str = "test") -> bool:
    verbose_print("=" * 80)
    verbose_print("ЗАГРУЗКА ОСТАТКОВ ПО СЧЁТУ 006 (БЛАНКИ СТРОГОЙ ОТЧЁТНОСТИ)")
    verbose_print("=" * 80)

    if com_object is None:
        verbose_print("Ошибка: com_object обязателен")
        return False

    account_ref = com_object.ПланыСчетов.Хозрасчетный.НайтиПоКоду(ACCOUNT_CODE)
    if account_ref.Пустая():
        verbose_print(f"Счёт с кодом «{ACCOUNT_CODE}» не найден в плане счетов источника.")
        return False

    accounts_list = com_object.NewObject("Массив")
    accounts_list.Add(account_ref)

    try:
        params = {"Счета": accounts_list}
        rows = execute_batch_query(com_object, QUERY_TEXT, COLUMNS, params=params)
    except Exception as error:
        verbose_print(f"Ошибка выполнения запроса: {error}")
        import traceback
        verbose_print(traceback.format_exc())
        return False

    if not rows:
        verbose_print("Не удалось получить записи остатков.")
        return True

    verbose_print(f"Получено записей: {len(rows)}")

    for row in rows:
        if "КоличествоОстаток" in row:
            val = row.pop("КоличествоОстаток")
            try:
                row["Quantity"] = float(val) if val is not None and val != "" else 0.0
            except (TypeError, ValueError):
                row["Quantity"] = 0.0
        if "СуммаОстаток" in row:
            val = row.pop("СуммаОстаток")
            try:
                row["Amount"] = float(val) if val is not None and val != "" else 0.0
            except (TypeError, ValueError):
                row["Amount"] = 0.0

    rows = process_reference_fields(rows, REFERENCE_COLUMNS)

    for row in rows:
        row["uuid"] = str(uuid.uuid4())

    if not ensure_database_exists(sqlite_db_file):
        return False

    connection = connect_to_sqlite(sqlite_db_file)
    if not connection:
        return False

    try:
        cursor = connection.cursor()
        cursor.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')
        connection.commit()

        saved = upsert_rows(
            connection,
            TABLE_NAME,
            rows,
            {
                "uuid": "TEXT PRIMARY KEY",
                "Quantity": "REAL",
                "Amount": "REAL",
            },
        )
        verbose_print(f"Сохранено строк: {saved}")
    finally:
        connection.close()

    return True


if __name__ == "__main__":
    target = os.getenv("TARGET_1C", "source")
    com = connect_to_1c(target)
    if com:
        load_account_006_balances("BD/account_006_balances.db", com)
