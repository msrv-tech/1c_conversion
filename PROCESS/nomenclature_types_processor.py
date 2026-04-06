# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «Виды номенклатуры» с использованием маппинга типов и полей.

Читает виды номенклатуры из исходной БД, применяет маппинг из type_mapping.db
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


class NomenclatureTypesMappingProcessor(MappingProcessor):
    """Процессор для преобразования видов номенклатуры с использованием маппинга."""

    def __init__(self, mapping_db_path: str = "CONF/type_mapping.db"):
        """
        Инициализация процессора.
        
        Args:
            mapping_db_path: Путь к базе данных с маппингом
        """
        super().__init__(mapping_db_path, "ВидыНоменклатуры", "catalog")

    def process_nomenclature_type(self, nomenclature_type: Dict) -> Dict:
        """
        Преобразует вид номенклатуры из формата источника в формат приемника.
        
        Args:
            nomenclature_type: Словарь с данными вида номенклатуры из источника
            
        Returns:
            Словарь с данными вида номенклатуры для приемника
        """
        return self.process_item(nomenclature_type)

    def process_nomenclature_types(self, nomenclature_types: List[Dict]) -> List[Dict]:
        """
        Преобразует список видов номенклатуры.
        
        Args:
            nomenclature_types: Список словарей с данными видов номенклатуры из источника
            
        Returns:
            Список словарей с данными видов номенклатуры для приемника
        """
        verbose_print(f"\nОбработка {len(nomenclature_types)} видов номенклатуры...")
        processed = []

        for i, nomenclature_type in enumerate(nomenclature_types, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(nomenclature_types)}")

            processed_type = self.process_nomenclature_type(nomenclature_type)
            if processed_type:
                processed.append(processed_type)

        verbose_print(f"Обработано видов номенклатуры: {len(processed)}")
        return processed

    def process_and_save_nomenclature_types(
        self, nomenclature_types: List[Dict], output_db_path: str, table_name: str = "nomenclature_types"
    ) -> bool:
        """
        Преобразует виды номенклатуры и сохраняет их в новую базу данных.
        
        Args:
            nomenclature_types: Список словарей с данными видов номенклатуры из источника
            output_db_path: Путь к выходной базе данных SQLite
            table_name: Имя таблицы для сохранения (по умолчанию "nomenclature_types")
            
        Returns:
            True если успешно, False если ошибка
        """
        # Обрабатываем виды номенклатуры
        processed = self.process_nomenclature_types(nomenclature_types)
        
        if not processed:
            verbose_print("Нет обработанных видов номенклатуры для сохранения")
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
            for nomenclature_type in processed:
                for field_name in nomenclature_type.keys():
                    if field_name.endswith("_UUID") or field_name.endswith("_Представление") or field_name.endswith("_Тип"):
                        base_field = field_name.rsplit("_", 1)[0]
                        reference_fields.add(base_field)

            for nomenclature_type in processed:
                for ref_field in reference_fields:
                    uuid_field = f"{ref_field}_UUID"
                    presentation_field = f"{ref_field}_Представление"
                    type_field = f"{ref_field}_Тип"
                    
                    # Для поля "Родитель" проверяем наличие Родитель_ЭтоГруппа и добавляем в JSON
                    is_group_value = None
                    if ref_field == "Родитель":
                        is_group_key = f"{ref_field}_ЭтоГруппа"
                        if is_group_key in nomenclature_type:
                            is_group_value = nomenclature_type.pop(is_group_key, None)
                            # Преобразуем в булево значение
                            if isinstance(is_group_value, bool):
                                pass  # Уже булево
                            elif isinstance(is_group_value, (int, str)):
                                is_group_value = str(is_group_value).lower() in ('1', 'true', 'истина', 'да')
                            elif is_group_value is None:
                                is_group_value = False

                    if uuid_field in nomenclature_type and nomenclature_type[uuid_field]:
                        ref_uuid = nomenclature_type[uuid_field]
                        ref_presentation = nomenclature_type.get(presentation_field, "")
                        ref_type = nomenclature_type.get(type_field, "")
                        
                        json_data = {
                            "uuid": ref_uuid,
                            "presentation": ref_presentation,
                            "type": ref_type
                        }
                        # Добавляем is_group для поля "Родитель"
                        if is_group_value is not None:
                            json_data["is_group"] = is_group_value
                        
                        ref_json = json.dumps(json_data, ensure_ascii=False)

                        nomenclature_type[ref_field] = ref_json

                        nomenclature_type.pop(uuid_field, None)
                        nomenclature_type.pop(presentation_field, None)
                        nomenclature_type.pop(type_field, None)

            saved = upsert_rows(
                connection,
                table_name,
                processed,
                {"uuid": "TEXT PRIMARY KEY", "Ссылка": "TEXT", "Код": "TEXT", "Наименование": "TEXT", "ПометкаУдаления": "INTEGER"},
            )

            if saved:
                verbose_print(f"\nСохранено видов номенклатуры в БД: {len(processed)}")
                verbose_print(f"База данных: {output_db_path}")
                verbose_print(f"Таблица: {table_name}")

            connection.commit()
            return saved

        except Exception as error:
            print(f"Ошибка при сохранении видов номенклатуры: {error}")
            import traceback
            traceback.print_exc()
            connection.rollback()
            return False
        finally:
            connection.close()


def process_nomenclature_types(
    source_db_path: str,
    processed_db_path: str,
) -> bool:
    """
    Обрабатывает виды номенклатуры из исходной БД и сохраняет в новую БД.
    
    Args:
        source_db_path: Путь к исходной базе данных SQLite
        processed_db_path: Путь к выходной базе данных SQLite
        
    Returns:
        True если успешно, False если ошибка
    """
    mapping_db_path = "CONF/type_mapping.db"
    table_name = "nomenclature_types"
    
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ВИДОВ НОМЕНКЛАТУРЫ С МАППИНГОМ")
    verbose_print("=" * 80)

    # Читаем данные из исходной БД
    verbose_print(f"\n[1/5] Чтение видов номенклатуры из исходной БД: {source_db_path}")
    nomenclature_types = read_from_db(source_db_path, table_name)
    if not nomenclature_types:
        verbose_print("Виды номенклатуры не найдены в исходной БД")
        return False
    verbose_print(f"Прочитано видов номенклатуры: {len(nomenclature_types)}")

    # Инициализируем процессор
    verbose_print("\n[2/5] Инициализация процессора маппинга...")
    processor = NomenclatureTypesMappingProcessor(mapping_db_path)

    # Обрабатываем данные
    verbose_print("\n[3/5] Обработка видов номенклатуры с использованием маппинга...")
    success = processor.process_and_save_nomenclature_types(nomenclature_types, processed_db_path, table_name)

    if success:
        verbose_print("\n[4/5] Копирование табличных частей...")
        copy_tabular_sections(source_db_path, processed_db_path, table_name)

        verbose_print("\n[5/5] Обработка завершена успешно!")
        verbose_print(f"Результат сохранен в: {processed_db_path}")

    return success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Обработка видов номенклатуры с маппингом")
    parser.add_argument("--source-db", required=True, help="Путь к исходной БД")
    parser.add_argument("--processed-db", required=True, help="Путь к обработанной БД")

    args = parser.parse_args()
    success = process_nomenclature_types(args.source_db, args.processed_db)
    sys.exit(0 if success else 1)

