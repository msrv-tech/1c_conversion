# -*- coding: utf-8 -*-
"""
Модуль обработки дополнительных регистров ОС.
Копирует данные из загруженной БД в обработанную, применяет маппинг перечислений.
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

TABLES = [
    ("property_tax_rates", "property_tax_rates"),
    ("vehicles_registration", "vehicles_registration"),
    ("land_plots_registration", "land_plots_registration"),
]

# Числовые колонки для каждой таблицы
NUMERIC_COLUMNS = {
    "property_tax_rates": [
        "НалоговаяСтавка", "СниженнаяНалоговаяСтавка",
        "ДоляСтоимостиЧислитель", "ДоляСтоимостиЗнаменатель",
        "КадастроваяСтоимость", "НеоблагаемаяКадастроваяСтоимость",
        "ДоляВПравеОбщейСобственностиЧислитель", "ДоляВПравеОбщейСобственностиЗнаменатель",
        "ДоляПлощадиЗнаменатель", "ДоляПлощадиЧислитель",
    ],
    "vehicles_registration": [
        "НалоговаяБаза", "НалоговаяСтавка", "ЛьготнаяСтавка",
        "ПроцентУменьшения", "СуммаУменьшения", "ПовышающийКоэффициент",
        "ДоляВПравеОбщейСобственностиЧислитель", "ДоляВПравеОбщейСобственностиЗнаменатель",
    ],
    "land_plots_registration": [
        "КадастроваяСтоимость",
        "ДоляВПравеОбщейСобственностиЧислитель", "ДоляВПравеОбщейСобственностиЗнаменатель",
        "НалоговаяСтавка", "СниженнаяНалоговаяСтавка",
        "ПроцентУменьшенияСуммыНалога", "СуммаУменьшенияСуммыНалога", "НеОблагаемаяНалогомСумма",
        "ДоляНеОблагаемойНалогомПлощадиЧислитель", "ДоляНеОблагаемойНалогомПлощадиЗнаменатель",
    ],
}


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


def _map_enum(val: str, enum_type: str, enum_mapping: Dict[str, Dict[str, str]]) -> Optional[str]:
    """Применяет маппинг перечисления. Возвращает Перечисление.Имя.Значение."""
    if not val or not isinstance(val, str):
        return None
    short_val = val.split(".")[-1] if "." in val else val
    mapped = None
    if enum_type in enum_mapping:
        mapped = enum_mapping[enum_type].get(short_val) or enum_mapping[enum_type].get(val)
    if not mapped:
        mapped = short_val
    return f"{enum_type}.{mapped}" if enum_type.startswith("Перечисление.") else f"Перечисление.{enum_type}.{mapped}"


def process_fixed_assets_additional_registers(
    source_db_path: str,
    processed_db_path: str,
) -> bool:
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ДОПОЛНИТЕЛЬНЫХ РЕГИСТРОВ ОС")
    verbose_print("=" * 80)

    enum_mapping = _load_enum_value_mapping(MAPPING_DB)

    if not ensure_database_exists(processed_db_path):
        return False

    connection = connect_to_sqlite(processed_db_path)
    if not connection:
        return False

    total_saved = 0

    try:
        for table_name, _ in TABLES:
            verbose_print(f"Обработка таблицы {table_name}...")
            items = read_from_db(source_db_path, table_name)
            if not items:
                verbose_print(f"  Нет данных")
                continue

            # Маппинг перечислений (если есть в данных)
            enum_fields = {
                "property_tax_rates": [("ПорядокНалогообложения", "Перечисление.ПорядокНалогообложенияИмущества")],
                "vehicles_registration": [("НалоговаяЛьгота", "Перечисление.ВидыНалоговыхЛьготПоТранспортномуНалогу")],
                "land_plots_registration": [
                    ("НалоговаяЛьготаПоНалоговойБазе", "Перечисление.ВидыНалоговыхЛьготПоНалоговойБазеПоЗемельномуНалогу"),
                    ("ВидЗаписи", "Перечисление.ВидыЗаписейОРегистрации"),
                ],
            }
            for item in items:
                for field, enum_type in enum_fields.get(table_name, []):
                    val = item.get(field)
                    if val and isinstance(val, str):
                        mapped = _map_enum(val, enum_type, enum_mapping)
                        if mapped:
                            item[field] = mapped

            # Нормализация числовых полей
            for item in items:
                for col in NUMERIC_COLUMNS.get(table_name, []):
                    if col in item:
                        val = item[col]
                        try:
                            item[col] = float(val) if val is not None and val != "" else 0.0
                        except (TypeError, ValueError):
                            item[col] = 0.0

            column_types = {"uuid": "TEXT PRIMARY KEY"}
            for col in NUMERIC_COLUMNS.get(table_name, []):
                column_types[col] = "REAL"

            cursor = connection.cursor()
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            connection.commit()

            saved = upsert_rows(connection, table_name, items, column_types)
            total_saved += saved
            verbose_print(f"  Сохранено: {saved}")

        verbose_print(f"Всего сохранено: {total_saved}")
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
    parser = argparse.ArgumentParser(description="Обработка дополнительных регистров ОС")
    parser.add_argument(
        "--source-db",
        default="BD/fixed_assets_additional_registers.db",
        help="Путь к исходной БД",
    )
    parser.add_argument(
        "--processed-db",
        default="BD/fixed_assets_additional_registers_processed.db",
        help="Путь к обработанной БД",
    )
    args = parser.parse_args()
    process_fixed_assets_additional_registers(args.source_db, args.processed_db)
