# -*- coding: utf-8 -*-
"""
Обработка остатков счёта 013.01 (Субконто1 = РБП): маппинг кода счёта для приёмника, ссылка РБП для записи.
"""

import json
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.writer_utils import parse_reference_field
from tools.onec_connector import upsert_rows
from tools.processor_utils import read_from_db
from tools.chart_of_accounts_mapper import load_mapping, extract_account_code, get_mapped_account_code
from tools.logger import verbose_print

SOURCE_TABLE = "account_01301_rbp_balances"
PROCESSED_TABLE = "account_01301_rbp_balances"


def _get_target_account_code(item: dict, coa_mapping: dict) -> str:
    acc_json = item.get("Счет")
    acc_info = parse_reference_field(acc_json) if acc_json else None
    source_code = extract_account_code(acc_info.get("presentation", "")) if acc_info else ""
    if not source_code:
        return ""
    mapped = get_mapped_account_code(source_code, coa_mapping)
    if mapped is not None:
        return mapped
    return source_code


def _get_rbp_ref_from_subconto1(item: dict) -> str | None:
    sub_json = item.get("Субконто1")
    if not sub_json:
        return None
    info = parse_reference_field(sub_json) if isinstance(sub_json, str) else sub_json
    if not info:
        return None
    ref_type = (info.get("type") or "").lower()
    if "расходыбудущихпериодов" in ref_type and info.get("uuid"):
        return json.dumps({
            "uuid": info["uuid"],
            "presentation": info.get("presentation", ""),
            "type": "Справочник.РасходыБудущихПериодов",
        }, ensure_ascii=False)
    return None


def process_account_01301_rbp_balances(source_db_path: str, processed_db_path: str) -> bool:
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ОСТАТКОВ СЧЁТА 013.01 (РБП В СУБКОНТО1)")
    verbose_print("=" * 80)

    items = read_from_db(source_db_path, SOURCE_TABLE)
    if not items:
        verbose_print("Нет данных для обработки.")
        return True

    coa_mapping, _ = load_mapping("CONF/chart_of_accounts_mapping.json")
    verbose_print(f"Прочитано записей: {len(items)}")

    for item in items:
        if "row_uuid" not in item:
            item["row_uuid"] = str(uuid.uuid4())
        item["TargetAccountCode"] = _get_target_account_code(item, coa_mapping)
        item["РБП_ДляЗаписи"] = _get_rbp_ref_from_subconto1(item)

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
                "row_uuid": "TEXT PRIMARY KEY",
                "TargetAccountCode": "TEXT",
                "РБП_ДляЗаписи": "TEXT",
                "СуммаДт": "REAL",
                "СуммаКт": "REAL",
            },
        )
        verbose_print(f"Обработано и сохранено строк: {saved}")
    finally:
        connection.close()

    return True


if __name__ == "__main__":
    process_account_01301_rbp_balances(
        "BD/account_01301_rbp_balances.db",
        "BD/account_01301_rbp_balances_processed.db",
    )
