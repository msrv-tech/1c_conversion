# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «Номенклатурные группы» с использованием маппинга типов и полей.

Читает номенклатурные группы из исходной БД, применяет маппинг из type_mapping.db
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


class NomenclatureGroupMappingProcessor(MappingProcessor):
    """Процессор для преобразования номенклатурных групп с использованием маппинга."""

    def __init__(self, mapping_db_path: str = "CONF/type_mapping.db"):
        """
        Инициализация процессора.
        
        Args:
            mapping_db_path: Путь к базе данных с маппингом
        """
        super().__init__(mapping_db_path, "НоменклатурныеГруппы", "catalog")

    def process_nomenclature_group(self, nomenclature_group: Dict) -> Dict:
        """
        Преобразует номенклатурную группу из формата источника в формат приемника.
        
        Args:
            nomenclature_group: Словарь с данными номенклатурной группы из источника
            
        Returns:
            Словарь с данными номенклатурной группы для приемника
        """
        return self.process_item(nomenclature_group)

    def process_nomenclature_groups(self, nomenclature_groups: List[Dict]) -> List[Dict]:
        """
        Преобразует список номенклатурных групп.
        
        Args:
            nomenclature_groups: Список словарей с данными номенклатурных групп из источника
            
        Returns:
            Список словарей с данными номенклатурных групп для приемника
        """
        verbose_print(f"\nОбработка {len(nomenclature_groups)} номенклатурных групп...")
        processed = []

        for i, nomenclature_group in enumerate(nomenclature_groups, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(nomenclature_groups)}")

            processed_group = self.process_nomenclature_group(nomenclature_group)
            if processed_group:
                processed.append(processed_group)

        verbose_print(f"Обработано номенклатурных групп: {len(processed)}")
        return processed

    def process_and_save_nomenclature_groups(
        self, nomenclature_groups: List[Dict], output_db_path: str, table_name: str = "nomenclature_groups"
    ) -> bool:
        """
        Преобразует номенклатурные группы и сохраняет их в новую базу данных.
        
        Args:
            nomenclature_groups: Список словарей с данными номенклатурных групп из источника
            output_db_path: Путь к выходной базе данных SQLite
            table_name: Имя таблицы для сохранения (по умолчанию "nomenclature_groups")
            
        Returns:
            True если успешно, False если ошибка
        """
        # Обрабатываем номенклатурные группы
        processed = self.process_nomenclature_groups(nomenclature_groups)
        
        if not processed:
            verbose_print("Нет обработанных номенклатурных групп для сохранения")
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
            for group in processed:
                for field_name in group.keys():
                    if field_name.endswith("_UUID") or field_name.endswith("_Представление") or field_name.endswith("_Тип"):
                        base_field = field_name.rsplit("_", 1)[0]
                        reference_fields.add(base_field)

            for group in processed:
                for ref_field in reference_fields:
                    presentation_key = f"{ref_field}_Представление"
                    uuid_key = f"{ref_field}_UUID"
                    type_key = f"{ref_field}_Тип"

                    has_meta = (
                        presentation_key in group or
                        uuid_key in group or
                        type_key in group
                    )

                    if not has_meta:
                        continue

                    presentation = group.pop(presentation_key, "")
                    uuid_value = group.pop(uuid_key, "")
                    type_value = group.pop(type_key, "")
                    
                    # Для поля "Родитель" проверяем наличие Родитель_ЭтоГруппа и добавляем в JSON
                    is_group_value = None
                    if ref_field == "Родитель":
                        is_group_key = f"{ref_field}_ЭтоГруппа"
                        if is_group_key in group:
                            is_group_value = group.pop(is_group_key, None)
                            # Преобразуем в булево значение
                            if isinstance(is_group_value, bool):
                                pass  # Уже булево
                            elif isinstance(is_group_value, (int, str)):
                                is_group_value = str(is_group_value).lower() in ('1', 'true', 'истина', 'да')
                            elif is_group_value is None:
                                is_group_value = False

                    if presentation or uuid_value or type_value:
                        json_data = {
                            "presentation": presentation,
                            "uuid": uuid_value,
                            "type": type_value,
                        }
                        # Добавляем is_group для поля "Родитель"
                        if is_group_value is not None:
                            json_data["is_group"] = is_group_value
                        
                        group[ref_field] = json.dumps(
                            json_data,
                            ensure_ascii=False,
                        )
                    else:
                        group[ref_field] = ""

            # Определяем базовые колонки для таблицы
            base_columns = {
                "uuid": "TEXT PRIMARY KEY",
                "Ссылка": "TEXT",
                "Код": "TEXT",
                "Наименование": "TEXT",
                "ПометкаУдаления": "INTEGER",
                "Комментарий": "TEXT",
            }
            
            # Расширяем base_columns всеми смапленными полями
            base_columns = self.extend_base_columns_with_mapped_fields(base_columns)

            # Очищаем таблицу перед записью (перезаписываем, а не добавляем)
            cursor = connection.cursor()
            # Проверяем существование таблицы перед удалением
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            if cursor.fetchone():
                cursor.execute(f'DELETE FROM "{table_name}"')
                connection.commit()

            # Сохраняем в БД
            saved = upsert_rows(connection, table_name, processed, base_columns)
            connection.commit()

            verbose_print(f"\nСохранено номенклатурных групп в БД: {saved}")
            verbose_print(f"База данных: {output_db_path}")
            verbose_print(f"Таблица: {table_name}")

            return True

        except Exception as error:
            print(f"Ошибка при сохранении в БД: {error}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            connection.close()


def process_nomenclature_groups(source_db_path: str, processed_db_path: str) -> bool:
    """
    Точка входа обработки номенклатурных групп с использованием маппинга.
    
    Читает номенклатурные группы из исходной БД, применяет маппинг полей и типов,
    и сохраняет результат в новую БД.

    Args:
        source_db_path: Путь к исходной базе данных (результат этапа загрузки)
        processed_db_path: Путь к базе данных после обработки

    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА НОМЕНКЛАТУРНЫХ ГРУПП С МАППИНГОМ")
    verbose_print("=" * 80)
    
    mapping_db_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "CONF",
        "type_mapping.db"
    )
    
    verbose_print(f"\n[1/5] Чтение номенклатурных групп из исходной БД: {source_db_path}")
    nomenclature_groups = read_from_db(source_db_path, "nomenclature_groups")
    
    if not nomenclature_groups:
        print("Ошибка: не удалось прочитать номенклатурные группы из исходной БД")
        return False
    
    verbose_print(f"Прочитано номенклатурных групп: {len(nomenclature_groups)}")
    
    verbose_print(f"\n[2/5] Инициализация процессора маппинга...")
    processor = NomenclatureGroupMappingProcessor(mapping_db_path)
    
    verbose_print(f"\n[3/5] Обработка номенклатурных групп с использованием маппинга...")
    success = processor.process_and_save_nomenclature_groups(nomenclature_groups, processed_db_path, "nomenclature_groups")
    
    if not success:
        verbose_print(f"\n[3/5] Ошибка при обработке номенклатурных групп")
        return False
    
    verbose_print(f"\n[4/5] Копирование табличных частей...")
    tabular_success = copy_tabular_sections(source_db_path, processed_db_path, "nomenclature_groups")
    
    if success and tabular_success:
        verbose_print(f"\n[5/5] Обработка завершена успешно!")
        verbose_print(f"Результат сохранен в: {processed_db_path}")
    else:
        verbose_print(f"\n[5/5] Обработка завершена с ошибками")
    
    return success and tabular_success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Обработка базы данных номенклатурных групп с маппингом"
    )
    parser.add_argument(
        "--source-db",
        required=True,
        help="Путь к исходной базе данных (результат этапа загрузки)",
    )
    parser.add_argument(
        "--processed-db",
        required=True,
        help="Путь к базе данных после обработки",
    )

    cli_args = parser.parse_args()
    success = process_nomenclature_groups(cli_args.source_db, cli_args.processed_db)
    raise SystemExit(0 if success else 1)
