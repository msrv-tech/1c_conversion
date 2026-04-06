# -*- coding: utf-8 -*-
"""
Модуль обработки номенклатурных групп, полученных из остатков.
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

SOURCE_TABLE = "balances_nomenclature_groups"
PROCESSED_TABLE = "balances_nomenclature_groups"


def process_balances_nomenclature_groups(source_db_path: str, processed_db_path: str) -> bool:
    """Обработка данных о номенклатурных группах для записи в документ."""
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА НОМЕНКЛАТУРНЫХ ГРУПП ИЗ ОСТАТКОВ")
    verbose_print("=" * 80)

    verbose_print(f"Чтение данных из: {source_db_path}")
    rows = read_from_db(source_db_path, SOURCE_TABLE)
    
    if not rows:
        verbose_print("Нет данных для обработки.")
        return True

    verbose_print(f"Прочитано строк: {len(rows)}")

    # В данном случае данные уже подготовлены загрузчиком в формате JSON.
    # Просто переносим их в обработанную БД.
    
    processed_rows = []
    for row in rows:
        # Убеждаемся, что uuid на месте
        if "uuid" not in row and "НоменклатурнаяГруппа" in row:
            try:
                ref_data = json.loads(row["НоменклатурнаяГруппа"])
                row["uuid"] = ref_data.get("uuid")
            except:
                continue
        
        processed_rows.append(row)

    # Сохранение в обработанную БД
    if not ensure_database_exists(processed_db_path):
        return False

    connection = connect_to_sqlite(processed_db_path)
    if not connection:
        return False

    try:
        schema = {
            "uuid": "TEXT PRIMARY KEY",
            "НоменклатурнаяГруппа": "TEXT"
        }
        
        # Очистка старых данных
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {PROCESSED_TABLE}")
        connection.commit()
        
        saved = upsert_rows(connection, PROCESSED_TABLE, processed_rows, schema)
        verbose_print(f"Обработано и сохранено: {saved} строк")
        return True
    except Exception as e:
        verbose_print(f"Ошибка при обработке данных: {e}")
        return False
    finally:
        connection.close()


if __name__ == "__main__":
    # Для отладки
    process_balances_nomenclature_groups(
        "BD/balances_nomenclature_groups.db",
        "BD/balances_nomenclature_groups_processed.db"
    )
