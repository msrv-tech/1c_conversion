# -*- coding: utf-8 -*-
"""
Загрузка остатков по счёту МЦЭ (МЦ на работниках) из 1С в SQLite.
Субконто в источнике: Работники организации (1), Номенклатура (2), Характеристика (3).
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

TABLE_NAME = "materials_workers_balances"

# Остатки по счёту МЦЭ: Субконто1=Работники организации, Субконто2=Номенклатура, Субконто3=Характеристика
QUERY_TEXT = """
ВЫБРАТЬ
    ХозрасчетныйОстатки.Организация КАК Организация,
    ХозрасчетныйОстатки.Счет КАК СчетУчета,
    ХозрасчетныйОстатки.Субконто1 КАК РаботникиОрганизации,
    ХозрасчетныйОстатки.Субконто2 КАК Номенклатура,
    ХозрасчетныйОстатки.Субконто3 КАК Характеристика,
    СУММА(ХозрасчетныйОстатки.КоличествоОстаток) КАК КоличествоОстаток,
    СУММА(ХозрасчетныйОстатки.СуммаОстаток) КАК СуммаОстаток
ПОМЕСТИТЬ ВТ_Остатки
ИЗ
    РегистрБухгалтерии.Хозрасчетный.Остатки(ДАТАВРЕМЯ(2026, 1, 1), Счет В (&Счета), , ) КАК ХозрасчетныйОстатки
ГДЕ
    ХозрасчетныйОстатки.КоличествоОстаток <> 0
СГРУППИРОВАТЬ ПО
    ХозрасчетныйОстатки.Организация,
    ХозрасчетныйОстатки.Счет,
    ХозрасчетныйОстатки.Субконто1,
    ХозрасчетныйОстатки.Субконто2,
    ХозрасчетныйОстатки.Субконто3
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

    ВТ_Остатки.РаботникиОрганизации КАК РаботникиОрганизации,
    ПРЕДСТАВЛЕНИЕ(ВТ_Остатки.РаботникиОрганизации) КАК РаботникиОрганизации_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ВТ_Остатки.РаботникиОрганизации)) КАК РаботникиОрганизации_UUID,
    ТИПЗНАЧЕНИЯ(ВТ_Остатки.РаботникиОрганизации) КАК РаботникиОрганизации_Тип,

    ВТ_Остатки.Номенклатура КАК Номенклатура,
    ПРЕДСТАВЛЕНИЕ(ВТ_Остатки.Номенклатура) КАК Номенклатура_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ВТ_Остатки.Номенклатура)) КАК Номенклатура_UUID,
    ТИПЗНАЧЕНИЯ(ВТ_Остатки.Номенклатура) КАК Номенклатура_Тип,
    ПРЕДСТАВЛЕНИЕ(ВТ_Остатки.Номенклатура.ВидНоменклатуры) КАК ВидНоменклатуры_Представление,

    ВТ_Остатки.Характеристика КАК Характеристика,
    ПРЕДСТАВЛЕНИЕ(ВТ_Остатки.Характеристика) КАК Характеристика_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ВТ_Остатки.Характеристика)) КАК Характеристика_UUID,
    ТИПЗНАЧЕНИЯ(ВТ_Остатки.Характеристика) КАК Характеристика_Тип,

    ВТ_Остатки.КоличествоОстаток КАК КоличествоОстаток,
    ВТ_Остатки.СуммаОстаток КАК СуммаОстаток
ИЗ
    ВТ_Остатки КАК ВТ_Остатки
"""

COLUMNS = [
    "Организация", "Организация_Представление", "Организация_UUID", "Организация_Тип",
    "СчетУчета", "СчетУчета_Представление", "СчетУчета_UUID", "СчетУчета_Тип",
    "РаботникиОрганизации", "РаботникиОрганизации_Представление", "РаботникиОрганизации_UUID", "РаботникиОрганизации_Тип",
    "Номенклатура", "Номенклатура_Представление", "Номенклатура_UUID", "Номенклатура_Тип",
    "ВидНоменклатуры_Представление",
    "Характеристика", "Характеристика_Представление", "Характеристика_UUID", "Характеристика_Тип",
    "КоличествоОстаток", "СуммаОстаток",
]

REFERENCE_COLUMNS = [
    "Организация", "СчетУчета", "РаботникиОрганизации", "Номенклатура", "Характеристика",
]

# В источнике счёт может быть с кодом МЦЭ или 013
ACCOUNT_CODES_TO_TRY = ["МЦЭ", "013"]


def load_materials_workers_balances(sqlite_db_file: str, com_object, mode: str = "test") -> bool:
    verbose_print("=" * 80)
    verbose_print("ЗАГРУЗКА ОСТАТКОВ ПО СЧЁТУ МЦЭ (МЦ НА РАБОТНИКАХ)")
    verbose_print("=" * 80)

    if com_object is None:
        verbose_print("Ошибка: com_object обязателен")
        return False

    account_ref = None
    for code in ACCOUNT_CODES_TO_TRY:
        account_ref = com_object.ПланыСчетов.Хозрасчетный.НайтиПоКоду(code)
        if not account_ref.Пустая():
            verbose_print(f"Используется счёт с кодом: {code}")
            break
    if account_ref is None or account_ref.Пустая():
        verbose_print(f"Счёт МЦЭ не найден (пробованы коды: {ACCOUNT_CODES_TO_TRY}).")
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

    # Целевой счёт приёмника по виду номенклатуры: Спецодежда и спецоснастка (10.10) -> МЦ.02, Хоз. инвентарь (10.09) -> МЦ.04, иначе МЦ.03
    for row in rows:
        vid_repr = str(row.get("ВидНоменклатуры_Представление") or "")
        if "10.10" in vid_repr or "Спецодежда и спецоснастка" in vid_repr or "спецоснастка на складе" in vid_repr:
            row["TargetAccountCode"] = "МЦ.02"
        elif "10.09" in vid_repr or "Хозяйственный инвентарь" in vid_repr or "инвентарь и принадлежности" in vid_repr:
            row["TargetAccountCode"] = "МЦ.04"
        else:
            row["TargetAccountCode"] = "МЦ.03"
        row.pop("ВидНоменклатуры_Представление", None)

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
                "TargetAccountCode": "TEXT",
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
        load_materials_workers_balances("BD/materials_workers_balances.db", com)
