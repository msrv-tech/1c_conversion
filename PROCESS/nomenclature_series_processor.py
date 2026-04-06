# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «Серии номенклатуры» с преобразованием в номенклатуру.

Читает серии из исходной БД, берёт данные из Владелец (номенклатура),
заменяет UUID на UUID серии и формирует Наименование = Наименование + "_" + Серия.

Расширяемость: опционально читает из таблиц остатков (*_balances) — при наличии
Номенклатура + Характеристика + Серия использует полное наименование.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows
from tools.processor_utils import read_from_db
from tools.base_processor import MappingProcessor
from tools.writer_utils import parse_reference_field
from tools.logger import verbose_print

fix_encoding()

# Таблицы остатков с возможной детализацией Номенклатура + Характеристика + Серия
BALANCE_TABLES = [
    "spec_equipment_balances",
    "characteristics_balances",
    "parties_mc_balances",
]

# Колонки для извлечения из остатков (могут отличаться по таблицам)
BALANCE_NOM_COL = "Номенклатура"
BALANCE_CHAR_COL = "ХарактеристикаНоменклатуры"
BALANCE_SERIES_COL = "СерияНоменклатуры"


def _get_presentation(value) -> str:
    """Извлекает представление из JSON-ссылки или строки."""
    if not value:
        return ""
    info = parse_reference_field(value) if isinstance(value, str) else None
    if info:
        return (info.get("presentation") or "").strip()
    return str(value).strip() if value else ""


def _read_balance_sources(db_dir: str) -> Dict[str, Dict]:
    """
    Опционально читает из таблиц остатков комбинации (series_uuid -> {nom, char, series}).
    Возвращает словарь: series_uuid -> {nom_presentation, char_presentation, series_presentation}.
    """
    result = {}
    if not db_dir or not os.path.isdir(db_dir):
        return result

    for table_name in BALANCE_TABLES:
        # Ищем БД с именем таблицы (spec_equipment_balances.db и т.д.)
        db_path = os.path.join(db_dir, f"{table_name}.db")
        if not os.path.exists(db_path):
            continue

        try:
            conn = connect_to_sqlite(db_path)
            if not conn:
                continue
            cursor = conn.cursor()
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            columns_info = cursor.fetchall()
            col_names = {c[1] for c in columns_info}

            if not (BALANCE_NOM_COL in col_names and BALANCE_SERIES_COL in col_names):
                conn.close()
                continue

            # Читаем уникальные комбинации
            cursor.execute(f'SELECT "{BALANCE_NOM_COL}", "{BALANCE_CHAR_COL}", "{BALANCE_SERIES_COL}" FROM "{table_name}"')
            rows = cursor.fetchall()
            conn.close()

            for row in rows:
                nom_val, char_val, series_val = row[0], row[1], row[2]
                series_info = parse_reference_field(series_val) if series_val else None
                series_uuid = (series_info.get("uuid") or "").strip().lower() if series_info else ""
                if not series_uuid or series_uuid == "00000000-0000-0000-0000-000000000000":
                    continue

                nom_pr = _get_presentation(nom_val)
                char_pr = _get_presentation(char_val)
                series_pr = _get_presentation(series_val)

                # Приоритет: запись с заполненной Характеристикой
                if series_uuid not in result or char_pr:
                    result[series_uuid] = {
                        "nom": nom_pr,
                        "char": char_pr,
                        "series": series_pr,
                    }
        except Exception as e:
            verbose_print(f"  ⚠ Ошибка чтения {db_path}: {e}")

    return result


class NomenclatureSeriesProcessor(MappingProcessor):
    """Процессор для преобразования серий номенклатуры в номенклатуру."""

    def __init__(self, source_db_path: str, mapping_db_path: str = "CONF/type_mapping.db"):
        super().__init__(mapping_db_path, "Номенклатура", "catalog")
        self.source_db_path = source_db_path

    def process_series(self, series: Dict, balance_overrides: Optional[Dict[str, Dict]] = None) -> Dict:
        """
        Преобразует серию номенклатуры в номенклатуру.
        """
        series_uuid = (series.get("uuid") or "").strip()
        owner_name = (series.get("Наименование") or "").strip()
        series_repr = (series.get("Серия_Представление") or series.get("СерийныйНомер") or "").strip()

        processed = self.process_item(series)
        if series_uuid:
            processed["uuid"] = series_uuid

        # Проверяем переопределение из остатков
        overrides = (balance_overrides or {}).get(series_uuid.lower())
        if overrides and overrides.get("char"):
            nom = overrides.get("nom", owner_name)
            char = overrides.get("char", "")
            ser = overrides.get("series", series_repr)
            parts = [p for p in (nom, char, ser) if p]
            processed["Наименование"] = "_".join(parts)
            processed["DoNotOverwriteName"] = False
        else:
            if owner_name and series_repr:
                processed["Наименование"] = f"{owner_name}_{series_repr}"
            elif series_repr:
                processed["Наименование"] = series_repr
            elif owner_name:
                processed["Наименование"] = owner_name
            # При догрузке без остатков: не перезаписывать наименование (комбинация Номенклатура+Характеристика+Серия только в остатках)
            processed["DoNotOverwriteName"] = True

        processed.pop("СерийныйНомер", None)
        processed.pop("Серия_Представление", None)
        return processed

    def process_series_list(
        self,
        series_list: List[Dict],
        balance_overrides: Optional[Dict[str, Dict]] = None,
    ) -> List[Dict]:
        verbose_print(f"\nОбработка {len(series_list)} серий номенклатуры...")
        processed = []
        for i, s in enumerate(series_list, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(series_list)}")
            p = self.process_series(s, balance_overrides)
            if p:
                processed.append(p)
        verbose_print(f"Обработано серий: {len(processed)}")
        return processed

    def process_and_save(
        self,
        series_list: List[Dict],
        output_db_path: str,
        table_name: str = "nomenclature",
    ) -> bool:
        balance_overrides = _read_balance_sources(os.path.dirname(self.source_db_path))
        if balance_overrides:
            verbose_print(f"  Найдено {len(balance_overrides)} серий в остатках (Номенклатура+Характеристика+Серия)")

        processed = self.process_series_list(series_list, balance_overrides)
        if not processed:
            verbose_print("Нет обработанных серий для сохранения")
            return False

        if not ensure_database_exists(output_db_path):
            verbose_print("Не удалось подготовить базу данных SQLite.")
            return False

        conn = connect_to_sqlite(output_db_path)
        if not conn:
            verbose_print("Не удалось подключиться к SQLite.")
            return False

        try:
            saved = upsert_rows(
                conn,
                table_name,
                processed,
                {
                    "uuid": "TEXT PRIMARY KEY",
                    "Ссылка": "TEXT",
                    "Код": "TEXT",
                    "Наименование": "TEXT",
                    "ПометкаУдаления": "INTEGER",
                    "ВидСтавкиНДС": "TEXT",
                    "DoNotOverwriteName": "INTEGER",
                },
            )
            verbose_print(f"Сохранено серий в БД: {saved}")
            return saved > 0
        except Exception as e:
            verbose_print(f"Ошибка при сохранении: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            conn.close()


def process_nomenclature_series(source_db_path: str, processed_db_path: str) -> bool:
    """
    Точка входа обработки серий номенклатуры.
    """
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА СЕРИЙ НОМЕНКЛАТУРЫ С ПРЕОБРАЗОВАНИЕМ В НОМЕНКЛАТУРУ")
    verbose_print("=" * 80)

    verbose_print(f"\n[1/3] Чтение серий из исходной БД: {source_db_path}")
    series_list = read_from_db(source_db_path, "nomenclature_series")

    if not series_list:
        verbose_print("Ошибка: не удалось прочитать серии из исходной БД")
        return False

    verbose_print(f"Прочитано серий: {len(series_list)}")

    verbose_print(f"\n[2/3] Инициализация процессора...")
    mapping_db_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "CONF",
        "type_mapping.db",
    )
    processor = NomenclatureSeriesProcessor(source_db_path, mapping_db_path)

    verbose_print(f"\n[3/3] Обработка серий с преобразованием в номенклатуру...")
    success = processor.process_and_save(series_list, processed_db_path, "nomenclature")

    if not success:
        verbose_print("Ошибка при обработке серий")
        return False

    verbose_print("Обработка завершена успешно!")
    verbose_print(f"Результат сохранен в: {processed_db_path}")
    return True


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Обработка серий номенклатуры с преобразованием в номенклатуру"
    )
    parser.add_argument("--source-db", required=True, help="Путь к исходной БД с сериями")
    parser.add_argument("--processed-db", required=True, help="Путь к выходной БД для обработанных данных")
    cli_args = parser.parse_args()

    success = process_nomenclature_series(cli_args.source_db, cli_args.processed_db)
    raise SystemExit(0 if success else 1)
