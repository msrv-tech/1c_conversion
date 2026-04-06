# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «Классификатор единиц измерения» с использованием маппинга типов и полей.

Читает единицы измерения из исходной БД, применяет маппинг из type_mapping.db
и сохраняет результат в новую БД в формате приемника (UH).
"""

from __future__ import annotations

import os
import sys
from typing import Dict, List

# Добавляем путь к корню проекта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows

from tools.base_processor import MappingProcessor
from tools.processor_utils import read_from_db, copy_tabular_sections
from tools.logger import verbose_print  # noqa: E402

fix_encoding()


class UnitsClassifierMappingProcessor(MappingProcessor):
    """Процессор для преобразования единиц измерения с использованием маппинга."""

    def __init__(self, mapping_db_path: str = "CONF/type_mapping.db"):
        """
        Инициализация процессора.
        
        Args:
            mapping_db_path: Путь к базе данных с маппингом
        """
        super().__init__(mapping_db_path, "КлассификаторЕдиницИзмерения", "catalog")

    def process_unit(self, unit: Dict) -> Dict:
        """
        Преобразует единицу измерения из формата источника в формат приемника.
        
        Args:
            unit: Словарь с данными единицы измерения из источника
            
        Returns:
            Словарь с данными единицы измерения для приемника
        """
        return self.process_item(unit)

    def process_units(self, units: List[Dict]) -> List[Dict]:
        """
        Преобразует список единиц измерения.
        
        Args:
            units: Список словарей с данными единиц измерения из источника
            
        Returns:
            Список словарей с данными единиц измерения для приемника
        """
        verbose_print(f"\nОбработка {len(units)} единиц измерения...")
        processed = []

        for i, unit in enumerate(units, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(units)}")

            processed_unit = self.process_unit(unit)
            if processed_unit:
                processed.append(processed_unit)

        verbose_print(f"Обработано единиц измерения: {len(processed)}")
        return processed

    def process_and_save_units(
        self, units: List[Dict], output_db_path: str, table_name: str = "units_classifier"
    ) -> bool:
        """
        Преобразует единицы измерения и сохраняет их в новую базу данных.
        
        Args:
            units: Список словарей с данными единиц измерения из источника
            output_db_path: Путь к выходной базе данных SQLite
            table_name: Имя таблицы для сохранения (по умолчанию "units_classifier")
            
        Returns:
            True если успешно, False если ошибка
        """
        # Обрабатываем единицы измерения
        processed = self.process_units(units)
        
        if not processed:
            verbose_print("Нет обработанных единиц измерения для сохранения")
            return False

        # Подготавливаем базу данных
        if not ensure_database_exists(output_db_path):
            verbose_print(f"Не удалось подготовить базу данных: {output_db_path}")
            return False

        connection = connect_to_sqlite(output_db_path)
        if not connection:
            verbose_print(f"Не удалось подключиться к базе данных: {output_db_path}")
            return False

        try:
            saved = upsert_rows(
                connection,
                table_name,
                processed,
                {"uuid": "TEXT PRIMARY KEY", "Ссылка": "TEXT", "Код": "TEXT", "Наименование": "TEXT", "ПометкаУдаления": "INTEGER"},
            )

            if saved:
                verbose_print(f"\nСохранено единиц измерения в БД: {len(processed)}")
                verbose_print(f"База данных: {output_db_path}")
                verbose_print(f"Таблица: {table_name}")

            connection.commit()
            return saved

        except Exception as error:
            print(f"Ошибка при сохранении единиц измерения: {error}")
            import traceback
            traceback.print_exc()
            connection.rollback()
            return False
        finally:
            connection.close()


def process_units_classifier(
    source_db_path: str,
    processed_db_path: str,
) -> bool:
    """
    Обрабатывает единицы измерения из исходной БД и сохраняет в новую БД.
    
    Args:
        source_db_path: Путь к исходной базе данных SQLite
        processed_db_path: Путь к выходной базе данных SQLite
        
    Returns:
        True если успешно, False если ошибка
    """
    mapping_db_path = "CONF/type_mapping.db"
    table_name = "units_classifier"
    
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ЕДИНИЦ ИЗМЕРЕНИЯ С МАППИНГОМ")
    verbose_print("=" * 80)

    # Читаем данные из исходной БД
    verbose_print(f"\n[1/5] Чтение единиц измерения из исходной БД: {source_db_path}")
    units = read_from_db(source_db_path, table_name)
    if not units:
        verbose_print("Единицы измерения не найдены в исходной БД")
        return False
    verbose_print(f"Прочитано единиц измерения: {len(units)}")

    # Инициализируем процессор
    verbose_print("\n[2/5] Инициализация процессора маппинга...")
    processor = UnitsClassifierMappingProcessor(mapping_db_path)

    # Обрабатываем данные
    verbose_print("\n[3/5] Обработка единиц измерения с использованием маппинга...")
    success = processor.process_and_save_units(units, processed_db_path, table_name)

    if success:
        verbose_print("\n[4/5] Копирование табличных частей...")
        copy_tabular_sections(source_db_path, processed_db_path, table_name)

        verbose_print("\n[5/5] Обработка завершена успешно!")
        verbose_print(f"Результат сохранен в: {processed_db_path}")

    return success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Обработка единиц измерения с маппингом")
    parser.add_argument("--source-db", required=True, help="Путь к исходной БД")
    parser.add_argument("--processed-db", required=True, help="Путь к обработанной БД")

    args = parser.parse_args()
    success = process_units_classifier(args.source_db, args.processed_db)
    sys.exit(0 if success else 1)

