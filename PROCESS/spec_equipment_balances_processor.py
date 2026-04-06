# -*- coding: utf-8 -*-
"""
Модуль обработки остатков по счёту 10.11 (спецодежда и спецоснастка в эксплуатации).
Переносит данные из сырой БД в обработанную.

Номенклатура_ДляЗаписи: если заполнена ХарактеристикаНоменклатуры — её UUID (в приёмнике
характеристики записаны как Номенклатура с наименованием = Номенклатура + Характеристика);
иначе — Номенклатура. Аналогично characteristics_balances.
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

SOURCE_TABLE = "spec_equipment_balances"
PROCESSED_TABLE = "spec_equipment_balances"


def _get_nomenclature_for_write(item: dict) -> str:
    """Возвращает JSON ссылки для записи: Характеристика (если заполнена) или Номенклатура. Тип всегда Справочник.Номенклатура."""
    char_json = item.get("ХарактеристикаНоменклатуры")
    char_info = parse_reference_field(char_json) if char_json else None
    char_uuid = (char_info.get("uuid") or "").strip() if char_info else ""
    char_filled = bool(char_uuid and char_uuid != "00000000-0000-0000-0000-000000000000")

    if char_filled:
        return json.dumps({
            "uuid": char_info["uuid"],
            "presentation": char_info.get("presentation", ""),
            "type": "Справочник.Номенклатура",
        }, ensure_ascii=False)
    nom_json = item.get("Номенклатура") or ""
    if isinstance(nom_json, str) and nom_json.strip():
        nom_info = parse_reference_field(nom_json)
        if nom_info:
            return json.dumps({
                "uuid": nom_info.get("uuid", ""),
                "presentation": nom_info.get("presentation", ""),
                "type": "Справочник.Номенклатура",
            }, ensure_ascii=False)
    return nom_json


def process_spec_equipment_balances(source_db_path: str, processed_db_path: str) -> bool:
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ОСТАТКОВ ПО СЧЁТУ 10.11 (СПЕЦОДЕЖДА И СПЕЦОСНАСТКА)")
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
                "Quantity": "REAL",
                "Amount": "REAL",
                "WrittenOffAmount": "REAL",
            },
        )
        verbose_print(f"Обработано и сохранено строк: {saved}")
    finally:
        connection.close()

    return True


if __name__ == "__main__":
    process_spec_equipment_balances(
        "BD/spec_equipment_balances.db",
        "BD/spec_equipment_balances_processed.db",
    )
