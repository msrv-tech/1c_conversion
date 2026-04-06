# -*- coding: utf-8 -*-
"""
Модуль обработки остатков МЦ.02, МЦ.04 (партии материалов в эксплуатации).
Переносит данные из сырой БД в обработанную.

Номенклатура_ДляЗаписи: новая номенклатура в УХ с форматом Наименование_Характеристика_Серия.
UUID: приоритет Серия → Характеристика → Номенклатура.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.writer_utils import parse_reference_field
from tools.onec_connector import upsert_rows
from tools.processor_utils import read_from_db
from tools.logger import verbose_print

SOURCE_TABLE = "parties_mc_balances"
PROCESSED_TABLE = "parties_mc_balances"


def _get_presentation(value) -> str:
    """Извлекает представление из JSON-ссылки или строки."""
    if not value:
        return ""
    info = parse_reference_field(value) if isinstance(value, str) else None
    if info:
        return (info.get("presentation") or "").strip()
    return str(value).strip() if value else ""


def _get_uuid(value) -> str:
    """Извлекает UUID из JSON-ссылки."""
    if not value:
        return ""
    info = parse_reference_field(value) if isinstance(value, str) else None
    if info:
        return (info.get("uuid") or "").strip()
    return ""


def _is_filled_uuid(uuid_val: str) -> bool:
    return bool(uuid_val and uuid_val != "00000000-0000-0000-0000-000000000000")


def _get_nomenclature_for_write(item: dict) -> str:
    """
    Формирует JSON для новой номенклатуры в УХ.
    Наименование: Наименование_Характеристика_Серия (пустые части пропускаются).
    UUID: приоритет Серия → Характеристика → Номенклатура.
    """
    nom_pr = _get_presentation(item.get("Номенклатура"))
    char_pr = _get_presentation(item.get("ХарактеристикаНоменклатуры"))
    ser_pr = _get_presentation(item.get("СерияНоменклатуры"))

    parts = [p for p in (nom_pr, char_pr, ser_pr) if p]
    presentation = "_".join(parts) if parts else nom_pr or "Без наименования"

    series_uuid = _get_uuid(item.get("СерияНоменклатуры"))
    char_uuid = _get_uuid(item.get("ХарактеристикаНоменклатуры"))
    nom_uuid = _get_uuid(item.get("Номенклатура"))

    if _is_filled_uuid(series_uuid):
        uuid_val = series_uuid
    elif _is_filled_uuid(char_uuid):
        uuid_val = char_uuid
    else:
        uuid_val = nom_uuid or ""

    return json.dumps({
        "uuid": uuid_val,
        "presentation": presentation,
        "type": "Справочник.Номенклатура",
    }, ensure_ascii=False)


def process_parties_mc_balances(source_db_path: str, processed_db_path: str) -> bool:
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ОСТАТКОВ МЦ.02, МЦ.04 (ПАРТИИ МАТЕРИАЛОВ В ЭКСПЛУАТАЦИИ)")
    verbose_print("=" * 80)

    items = read_from_db(source_db_path, SOURCE_TABLE)
    if not items:
        verbose_print("Нет данных для обработки.")
        return True

    verbose_print(f"Прочитано записей: {len(items)}")

    for item in items:
        item["Номенклатура_ДляЗаписи"] = _get_nomenclature_for_write(item)

    if not ensure_database_exists(processed_db_path):
        return False

    connection = connect_to_sqlite(processed_db_path)
    if not connection:
        return False

    try:
        cursor = connection.cursor()
        cursor.execute(f'DROP TABLE IF EXISTS "{PROCESSED_TABLE}"')
        connection.commit()

        saved = upsert_rows(
            connection,
            PROCESSED_TABLE,
            items,
            {
                "uuid": "TEXT PRIMARY KEY",
                "TargetAccountCode": "TEXT",
                "Quantity": "REAL",
                "Amount": "REAL",
                "WrittenOffAmount": "REAL",
                "ДокументПередачиНомер": "TEXT",
            },
        )
        verbose_print(f"Обработано и сохранено строк: {saved}")
    finally:
        connection.close()

    return True


if __name__ == "__main__":
    process_parties_mc_balances(
        "BD/parties_mc_balances.db",
        "BD/parties_mc_balances_processed.db",
    )
