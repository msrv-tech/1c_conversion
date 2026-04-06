# -*- coding: utf-8 -*-
"""
Модуль обработки остатков по счёту 006 (Бланки строгой отчётности).
Переносит данные из сырой БД в обработанную без изменения субконто.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows
from tools.processor_utils import read_from_db
from tools.logger import verbose_print

SOURCE_TABLE = "account_006_balances"
PROCESSED_TABLE = "account_006_balances"


def process_account_006_balances(source_db_path: str, processed_db_path: str) -> bool:
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ОСТАТКОВ ПО СЧЁТУ 006 (БЛАНКИ СТРОГОЙ ОТЧЁТНОСТИ)")
    verbose_print("=" * 80)

    items = read_from_db(source_db_path, SOURCE_TABLE)
    if not items:
        verbose_print("Нет данных для обработки.")
        return True

    verbose_print(f"Прочитано записей: {len(items)}")

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
            },
        )
        verbose_print(f"Обработано и сохранено строк: {saved}")
    finally:
        connection.close()

    return True


if __name__ == "__main__":
    process_account_006_balances(
        "BD/account_006_balances.db",
        "BD/account_006_balances_processed.db",
    )
