# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «СпособыОтраженияЗарплатыВБухУчете» с использованием маппинга типов и полей.

Читает способыотражениязарплатывбухучете из исходной БД, применяет маппинг из type_mapping.db
и сохраняет результат в новую БД в формате приемника (UH).
"""

from __future__ import annotations

import json
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


class SalaryPostingMethodsMappingProcessor(MappingProcessor):
    """Процессор для преобразования способыотражениязарплатывбухучете с использованием маппинга."""

    def __init__(self, mapping_db_path: str = "CONF/type_mapping.db"):
        """
        Инициализация процессора.
        
        Args:
            mapping_db_path: Путь к базе данных с маппингом
        """
        super().__init__(mapping_db_path, "СпособыОтраженияЗарплатыВБухУчете", "catalog")

    def process_item_single(self, item: Dict) -> Dict:
        """
        Преобразует элемент из формата источника в формат приемника.
        
        Args:
            item: Словарь с данными элемента из источника
            
        Returns:
            Словарь с данными элемента для приемника
        """
        return self.process_item(item)

    def process_items(self, items: List[Dict]) -> List[Dict]:
        """
        Преобразует список элементов.
        
        Args:
            items: Список словарей с данными элементов из источника
            
        Returns:
            Список словарей с данными элементов для приемника
        """
        verbose_print(f"\nОбработка {len(items)} способыотражениязарплатывбухучете...")
        processed = []

        for i, item in enumerate(items, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(items)}")

            processed_item = self.process_item_single(item)
            if processed_item:
                processed.append(processed_item)

        verbose_print(f"Обработано способыотражениязарплатывбухучете: {len(processed)}")
        return processed

    def process_and_save_items(
        self, items: List[Dict], output_db_path: str, table_name: str = "salary_posting_methods"
    ) -> bool:
        """
        Преобразует элементы и сохраняет их в новую базу данных.
        
        Args:
            items: Список словарей с данными элементов из источника
            output_db_path: Путь к выходной базе данных SQLite
            table_name: Имя таблицы для сохранения (по умолчанию "salary_posting_methods")
            
        Returns:
            True если успешно, False если ошибка
        """
        # Обрабатываем элементы
        processed = self.process_items(items)
        
        if not processed:
            verbose_print("Нет обработанных способыотражениязарплатывбухучете для сохранения")
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
            # Преобразуем ссылки в JSON формат
            reference_fields = set()
            for item in processed:
                for field_name in item.keys():
                    if field_name.endswith("_UUID") or field_name.endswith("_Представление") or field_name.endswith("_Тип"):
                        base_field = field_name.rsplit("_", 1)[0]
                        reference_fields.add(base_field)

            for item in processed:
                for ref_field in reference_fields:
                    uuid_field = f"{ref_field}_UUID"
                    presentation_field = f"{ref_field}_Представление"
                    type_field = f"{ref_field}_Тип"

                    if uuid_field in item and item[uuid_field]:
                        ref_uuid = item[uuid_field]
                        ref_presentation = item.get(presentation_field, "")
                        ref_type = item.get(type_field, "")

                        ref_json = json.dumps({
                            "uuid": ref_uuid,
                            "presentation": ref_presentation,
                            "type": ref_type
                        }, ensure_ascii=False)

                        item[ref_field] = ref_json

                        item.pop(uuid_field, None)
                        item.pop(presentation_field, None)
                        item.pop(type_field, None)

            saved = upsert_rows(
                connection,
                table_name,
                processed,
                {"uuid": "TEXT PRIMARY KEY", "Ссылка": "TEXT", "Код": "TEXT", "Наименование": "TEXT", "ПометкаУдаления": "INTEGER"},
            )

            if saved:
                verbose_print(f"\nСохранено способыотражениязарплатывбухучете в БД: {len(processed)}")
                verbose_print(f"База данных: {output_db_path}")
                verbose_print(f"Таблица: {table_name}")

            connection.commit()
            return saved

        except Exception as error:
            print(f"Ошибка при сохранении способыотражениязарплатывбухучете: {error}")
            import traceback
            traceback.print_exc()
            connection.rollback()
            return False
        finally:
            connection.close()


def process_salary_posting_methods(
    source_db_path: str,
    processed_db_path: str,
) -> bool:
    """
    Обрабатывает способыотражениязарплатывбухучете из исходной БД и сохраняет в новую БД.
    
    Args:
        source_db_path: Путь к исходной базе данных SQLite
        processed_db_path: Путь к выходной базе данных SQLite
        
    Returns:
        True если успешно, False если ошибка
    """
    mapping_db_path = "CONF/type_mapping.db"
    table_name = "salary_posting_methods"
    
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА СПОСОБЫОТРАЖЕНИЯЗАРПЛАТЫВБУХУЧЕТЕ С МАППИНГОМ")
    verbose_print("=" * 80)

    # Читаем данные из исходной БД
    verbose_print(f"\n[1/5] Чтение способыотражениязарплатывбухучете из исходной БД: {source_db_path}")
    items = read_from_db(source_db_path, table_name)
    if not items:
        verbose_print("СпособыОтраженияЗарплатыВБухУчете не найдены в исходной БД")
        return False
    verbose_print(f"Прочитано способыотражениязарплатывбухучете: {len(items)}")

    # Инициализируем процессор
    verbose_print("\n[2/5] Инициализация процессора маппинга...")
    processor = SalaryPostingMethodsMappingProcessor(mapping_db_path)

    # Обрабатываем данные
    verbose_print("\n[3/5] Обработка способыотражениязарплатывбухучете с использованием маппинга...")
    success = processor.process_and_save_items(items, processed_db_path, table_name)

    if success:
        verbose_print("\n[4/5] Копирование табличных частей...")
        copy_tabular_sections(source_db_path, processed_db_path, table_name)

        verbose_print("\n[5/5] Обработка завершена успешно!")
        verbose_print(f"Результат сохранен в: {processed_db_path}")

    return success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Обработка способыотражениязарплатывбухучете с маппингом")
    parser.add_argument("--source-db", required=True, help="Путь к исходной БД")
    parser.add_argument("--processed-db", required=True, help="Путь к обработанной БД")

    args = parser.parse_args()
    success = process_salary_posting_methods(args.source_db, args.processed_db)
    sys.exit(0 if success else 1)
