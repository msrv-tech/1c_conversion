# -*- coding: utf-8 -*-
"""
Модуль обработки остатков по характеристикам.
Переносит данные из сырой БД в обработанную.
"""

import os
import sys
from typing import List, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows
from tools.processor_utils import read_from_db
from tools.logger import verbose_print

SOURCE_TABLE = "characteristics_balances"
PROCESSED_TABLE = "characteristics_balances"

def process_characteristics_balances(source_db_path: str, processed_db_path: str) -> bool:
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ОСТАТКОВ ПО ХАРАКТЕРИСТИКАМ")
    verbose_print("=" * 80)

    # 1. Читаем данные из исходной БД
    items = read_from_db(source_db_path, SOURCE_TABLE)
    if not items:
        verbose_print("Нет данных для обработки.")
        return True

    verbose_print(f"Прочитано записей: {len(items)}")

    # 2. Сохраняем в обработанную БД
    if not ensure_database_exists(processed_db_path):
        return False

    connection = connect_to_sqlite(processed_db_path)
    if not connection:
        return False

    try:
        # Очищаем таблицу перед записью
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
                "AmountNU": "REAL"
            },
        )
        verbose_print(f"Обработано и сохранено строк: {saved}")
    finally:
        connection.close()

    return True

if __name__ == "__main__":
    process_characteristics_balances("BD/characteristics_balances.db", "BD/characteristics_balances_processed.db")

