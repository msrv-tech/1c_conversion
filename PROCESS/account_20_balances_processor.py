# -*- coding: utf-8 -*-
"""
Модуль обработки остатков по счёту 20 (Основное производство).
Переносит данные из сырой БД в обработанную.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows
from tools.processor_utils import read_from_db
from tools.logger import verbose_print

SOURCE_TABLE = "account_20_balances"
PROCESSED_TABLE = "account_20_balances"


def process_account_20_balances(source_db_path: str, processed_db_path: str) -> bool:
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ОСТАТКОВ ПО СЧЁТУ 20 (ОСНОВНОЕ ПРОИЗВОДСТВО)")
    verbose_print("=" * 80)

    items = read_from_db(source_db_path, SOURCE_TABLE)
    if not items:
        verbose_print("Нет данных для обработки.")
        return True

    verbose_print(f"Прочитано записей: {len(items)}")

    # Маппинг субконто для приёмника: Субконто1 = Ном группы (источник Субконто2),
    # Субконто2 = Статьи затрат (источник Субконто3)
    for item in items:
        subc2 = item.get("Субконто2")
        subc3 = item.get("Субконто3")
        item["Субконто1"] = subc2  # Номенклатурные группы
        item["Субконто2"] = subc3  # Статьи затрат

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
                "AmountCredit": "REAL",
                "CurrencyAmount": "REAL",
                "AmountNU": "REAL",
                "AmountPR": "REAL",
                "AmountVR": "REAL",
            },
        )
        verbose_print(f"Обработано и сохранено строк: {saved}")
    finally:
        connection.close()

    return True


if __name__ == "__main__":
    process_account_20_balances(
        "BD/account_20_balances.db",
        "BD/account_20_balances_processed.db",
    )
