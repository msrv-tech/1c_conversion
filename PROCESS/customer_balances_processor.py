# -*- coding: utf-8 -*-
"""
Модуль обработки остатков взаиморасчетов с покупателями.
"""

import os
import sys
import json
from typing import Dict, List

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows
from tools.processor_utils import read_from_db
from tools.logger import verbose_print

SOURCE_TABLE = "customer_balances"
PROCESSED_TABLE = "customer_balances"

def process_customer_balances(source_db_path: str, processed_db_path: str) -> bool:
    """Обработка данных об остатках для записи в документ."""
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ОСТАТКОВ ВЗАИМОРАСЧЕТОВ С ПОКУПАТЕЛЯМИ")
    verbose_print("=" * 80)

    verbose_print(f"Чтение данных из: {source_db_path}")
    rows = read_from_db(source_db_path, SOURCE_TABLE)
    
    if not rows:
        verbose_print("Нет данных для обработки.")
        return True

    verbose_print(f"Прочитано строк: {len(rows)}")

    # Данные уже подготовлены загрузчиком.
    # Просто переносим их в обработанную БД.
    
    # Сохранение в обработанную БД
    if not ensure_database_exists(processed_db_path):
        return False

    connection = connect_to_sqlite(processed_db_path)
    if not connection:
        return False

    try:
        schema = {
            "row_uuid": "TEXT PRIMARY KEY",
            "Организация": "TEXT",
            "Контрагент": "TEXT",
            "Договор": "TEXT",
            "Счет": "TEXT",
            "Документ": "TEXT",
            "Документ_Номер": "TEXT",
            "Документ_Дата": "TEXT",
            "Документ_Сумма": "REAL",
            "Документ_Валюта": "TEXT",
            "Сделка": "TEXT",
            "Валюта": "TEXT",
            "СуммаКонечныйОстатокБУ": "REAL",
            "СуммаВзаиморасчетовОстаток": "REAL",
        }
        
        # Очистка старых данных
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {PROCESSED_TABLE}")
        connection.commit()
        
        saved = upsert_rows(connection, PROCESSED_TABLE, rows, schema)
        verbose_print(f"Обработано и сохранено: {saved} строк")
        return True
    except Exception as e:
        verbose_print(f"Ошибка при обработке данных: {e}")
        return False
    finally:
        connection.close()

if __name__ == "__main__":
    process_customer_balances(
        "BD/customer_balances.db",
        "BD/customer_balances_processed.db"
    )

