# -*- coding: utf-8 -*-
"""
Модуль обработки остатков НМА.
Копирует данные из загруженной БД в обработанную, применяет маппинг перечислений
(СпособыПоступленияАктивов, СпособыНачисленияАмортизацииНМА, МетодыНачисленияАмортизации и др.)
"""

import os
import sys
import sqlite3
from typing import Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows
from tools.processor_utils import read_from_db
from tools.logger import verbose_print

MAPPING_DB = os.path.join(os.path.dirname(os.path.dirname(__file__)), "CONF", "type_mapping.db")

def _load_enum_value_mapping(mapping_db_path: str) -> Dict[str, Dict[str, str]]:
    """Загружает маппинг значений перечислений из enumeration_value_mapping."""
    result = {}
    if not os.path.exists(mapping_db_path):
        return result
    try:
        conn = sqlite3.connect(mapping_db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='enumeration_value_mapping'
        """)
        if not cursor.fetchone():
            conn.close()
            return result
        cursor.execute("""
            SELECT source_enum_type, source_value, target_enum_type, target_value
            FROM enumeration_value_mapping
        """)
        for row in cursor.fetchall():
            src_enum = row["source_enum_type"]
            src_val = row["source_value"]
            tgt_val = row["target_value"]
            if src_enum not in result:
                result[src_enum] = {}
            result[src_enum][src_val] = tgt_val
        conn.close()
    except Exception as e:
        verbose_print(f"  ⚠ Не удалось загрузить маппинг перечислений: {e}")
    return result

def process_intangible_assets_balances(source_db_path: str, processed_db_path: str) -> bool:
    """
    Переносит данные из исходной БД в обработанную.
    """
    table_name = "intangible_assets_balances"

    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ОСТАТКОВ НМА")
    verbose_print("=" * 80)

    verbose_print(f"Чтение данных из {source_db_path}...")
    items = read_from_db(source_db_path, table_name)
    if not items:
        verbose_print("Данные не найдены.")
        return False
    verbose_print(f"Прочитано записей: {len(items)}")

    enum_value_mapping = _load_enum_value_mapping(MAPPING_DB)
    enum_src_postup = "Перечисление.СпособыПоступленияАктивов"
    enum_src_amort = "Перечисление.СпособыНачисленияАмортизацииНМА"
    enum_src_method_nu = "Перечисление.МетодыНачисленияАмортизации"
    enum_src_order_nu = "Перечисление.ПорядокВключенияСтоимостиОСВСоставРасходовНУ"
    enum_src_order_usn = "Перечисление.ПорядокВключенияСтоимостиОСиНМАВСоставРасходовУСН"

    def _to_full_enum(enum_type: str, value: str) -> str:
        if not value:
            return ""
        if value.startswith("Перечисление."):
            return value
        return f"{enum_type}.{value}"

    def _map_enum(val: str, enum_type: str) -> Optional[str]:
        if not val or not isinstance(val, str):
            return None
        short_val = val.split(".")[-1] if "." in val else val
        mapped = None
        if enum_type in enum_value_mapping:
            mapped = enum_value_mapping[enum_type].get(short_val) or enum_value_mapping[enum_type].get(val)
        if not mapped:
            mapped = short_val
        return _to_full_enum(enum_type, mapped)

    for item in items:
        val = item.get("СпособПоступленияРегл")
        if val and isinstance(val, str):
            full_val = _map_enum(val, enum_src_postup)
            if full_val:
                item["СпособПоступленияРегл"] = full_val

        val = item.get("СпособНачисленияАмортизацииБУ")
        if val and isinstance(val, str):
            full_val = _map_enum(val, enum_src_amort)
            if full_val:
                item["СпособНачисленияАмортизацииБУ"] = full_val

        for enum_field, enum_type in [
            ("МетодНачисленияАмортизацииНУ", enum_src_method_nu),
            ("ПорядокВключенияСтоимостиВСоставРасходовНУ", enum_src_order_nu),
            ("ПорядокВключенияСтоимостиВСоставРасходовУСН", enum_src_order_usn),
        ]:
            val = item.get(enum_field)
            if val and isinstance(val, str):
                full_val = _map_enum(val, enum_type)
                if full_val:
                    item[enum_field] = full_val

        # ПорядокПогашенияСтоимостиБУ — маппинг из ПорядокВключенияСтоимостиВСоставРасходовНУ
        # УХ использует ПорядокПогашенияСтоимостиОС для НМА (общая логика)
        ORDER_IN_NU_TO_ORDER_PAYMENT = {
            "НачислениеАмортизации": "НачислениеАмортизации",
            "ВключениеВРасходыПриПринятииКУчету": "НачислениеАмортизации",
            "СтоимостьНеВключаетсяВРасходы": "СтоимостьНеПогашается",
        }
        val_order = item.get("ПорядокВключенияСтоимостиВСоставРасходовНУ")
        if val_order and isinstance(val_order, str):
            short_val = val_order.split(".")[-1] if "." in val_order else val_order
            mapped = ORDER_IN_NU_TO_ORDER_PAYMENT.get(short_val, "НачислениеАмортизации")
        else:
            mapped = "НачислениеАмортизации"
        item["ПорядокПогашенияСтоимостиБУ"] = f"Перечисление.ПорядокПогашенияСтоимостиОС.{mapped}"

    if not ensure_database_exists(processed_db_path):
        return False

    connection = connect_to_sqlite(processed_db_path)
    if not connection:
        return False

    try:
        column_types = {"uuid": "TEXT PRIMARY KEY"}
        numeric_columns = [
            "СпециальныйКоэффициентНУ", "СрокПолезногоИспользованияБУ", "СрокПолезногоИспользованияНУ", "СрокПолезногоИспользованияУСН",
            "ПервоначальнаяСтоимостьБУ", "ПервоначальнаяСтоимостьНУ", "ПервоначальнаяСтоимостьУСН",
            "ТекущаяСтоимостьНУ", "ТекущаяСтоимостьПР", "ТекущаяСтоимостьВР", "ТекущаяСтоимостьБУ",
            "НакопленнаяАмортизацияНУ", "НакопленнаяАмортизацияПР", "НакопленнаяАмортизацияВР", "НакопленнаяАмортизацияБУ",
            "СуммаНачисленнойАмортизацииУСН",
            "КоэффициентАмортизацииБУ"
        ]
        for col in numeric_columns:
            column_types[col] = "REAL"

        saved = upsert_rows(
            connection,
            table_name,
            items,
            column_types,
        )
        verbose_print(f"Сохранено в обработанную БД: {saved}")
        connection.commit()
        return True
    except Exception as e:
        verbose_print(f"Ошибка при обработке: {e}")
        import traceback
        verbose_print(traceback.format_exc())
        return False
    finally:
        connection.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Обработка остатков НМА")
    parser.add_argument("--source-db", default="BD/intangible_assets_balances.db", help="Путь к исходной БД")
    parser.add_argument("--processed-db", default="BD/intangible_assets_balances_processed.db", help="Путь к обработанной БД")

    args = parser.parse_args()
    process_intangible_assets_balances(args.source_db, args.processed_db)
