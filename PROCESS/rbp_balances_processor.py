# -*- coding: utf-8 -*-
"""
Модуль обработки остатков РБП.
Применяет маппинг счетов: 97, 76.19→76.01.9, 76.09→76.09.1.
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

SOURCE_TABLE = "rbp_balances"
PROCESSED_TABLE = "rbp_balances"

def _get_balance_type(item: dict, coa_mapping: dict) -> str:
    """Возвращает тип остатка: '97', '76.01.9', '76.09.1'."""
    acc_json = item.get("Счет")
    acc_info = parse_reference_field(acc_json) if acc_json else None
    source_code = extract_account_code(acc_info.get("presentation", "")) if acc_info else ""
    if not source_code:
        return ""
    mapped = get_mapped_account_code(source_code, coa_mapping)
    return mapped or source_code


def _get_rbp_ref_for_write(item: dict) -> str | None:
    """Возвращает JSON ссылки РБП из Субконто1 или Субконто2 (если тип РБП)."""
    for col in ("Субконто1", "Субконто2"):
        sub_json = item.get(col)
        if not sub_json:
            continue
        info = parse_reference_field(sub_json) if isinstance(sub_json, str) else sub_json
        if not info:
            continue
        ref_type = (info.get("type") or "").lower()
        if "расходыбудущихпериодов" in ref_type and info.get("uuid"):
            return json.dumps({
                "uuid": info["uuid"],
                "presentation": info.get("presentation", ""),
                "type": "Справочник.РасходыБудущихПериодов",
            }, ensure_ascii=False)
    return None


def _get_contractor_ref(item: dict) -> str | None:
    """Контрагент из Субконто1, Субконто2 или Субконто3 (для 76.09 и 76.19)."""
    for col in ("Субконто1", "Субконто2", "Субконто3"):
        sub_json = item.get(col)
        if not sub_json:
            continue
        info = parse_reference_field(sub_json) if isinstance(sub_json, str) else sub_json
        if not info:
            continue
        ref_type = (info.get("type") or "").lower()
        if "контрагент" in ref_type and info.get("uuid"):
            return json.dumps({
                "uuid": info.get("uuid", ""),
                "presentation": info.get("presentation", ""),
                "type": "Справочник.Контрагенты",
            }, ensure_ascii=False)
    return None


def _get_contract_ref(item: dict) -> str | None:
    """Договор из Субконто1, Субконто2 или Субконто3 (для 76.09 и 76.19)."""
    for col in ("Субконто1", "Субконто2", "Субконто3"):
        sub_json = item.get(col)
        if not sub_json:
            continue
        info = parse_reference_field(sub_json) if isinstance(sub_json, str) else sub_json
        if not info:
            continue
        ref_type = (info.get("type") or "").lower()
        if "договор" in ref_type and info.get("uuid"):
            return json.dumps({
                "uuid": info.get("uuid", ""),
                "presentation": info.get("presentation", ""),
                "type": "Справочник.ДоговорыКонтрагентов",
            }, ensure_ascii=False)
    return None


def _get_workers_ref(item: dict) -> str | None:
    """Для 97.01, 97.71: Работники организации из Субконто2."""
    sub_json = item.get("Субконто2")
    if not sub_json:
        return None
    info = parse_reference_field(sub_json) if isinstance(sub_json, str) else sub_json
    if not info:
        return None
    ref_type = (info.get("type") or "").lower()
    if "работник" in ref_type or "физическоелицо" in ref_type or "сотрудник" in ref_type:
        return json.dumps({
            "uuid": info.get("uuid", ""),
            "presentation": info.get("presentation", ""),
            "type": info.get("type", "Справочник.ФизическиеЛица"),
        }, ensure_ascii=False)
    return None


def process_rbp_balances(source_db_path: str, processed_db_path: str) -> bool:
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ОСТАТКОВ РБП")
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
        balance_type = _get_balance_type(item, coa_mapping)
        item["balance_type"] = balance_type
        item["РБП_ДляЗаписи"] = _get_rbp_ref_for_write(item)
        item["Работники_ДляЗаписи"] = _get_workers_ref(item)
        item["Контрагент_ДляЗаписи"] = _get_contractor_ref(item)
        item["Договор_ДляЗаписи"] = _get_contract_ref(item)

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
                "balance_type": "TEXT",
                "РБП_ДляЗаписи": "TEXT",
                "Работники_ДляЗаписи": "TEXT",
                "Контрагент_ДляЗаписи": "TEXT",
                "Договор_ДляЗаписи": "TEXT",
                "СуммаДт": "REAL",
                "СуммаКт": "REAL",
                "СуммаНУ": "REAL",
                "СуммаПР": "REAL",
                "СуммаВР": "REAL",
            },
        )
        verbose_print(f"Обработано и сохранено строк: {saved}")
    finally:
        connection.close()

    return True


if __name__ == "__main__":
    process_rbp_balances(
        "BD/rbp_balances.db",
        "BD/rbp_balances_processed.db",
    )
