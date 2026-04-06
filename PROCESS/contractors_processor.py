# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «Контрагенты» с использованием маппинга типов и полей.

Читает контрагентов из исходной БД, применяет маппинг из type_mapping.db
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

fix_encoding()


class ContractorMappingProcessor(MappingProcessor):
    """Процессор для преобразования контрагентов с использованием маппинга."""

    def __init__(self, mapping_db_path: str = "CONF/type_mapping.db"):
        """
        Инициализация процессора.
        
        Args:
            mapping_db_path: Путь к базе данных с маппингом
        """
        super().__init__(mapping_db_path, "Контрагенты", "catalog")

    def process_contractor(self, contractor: Dict) -> Dict:
        """
        Преобразует контрагента из формата источника в формат приемника.
        
        Args:
            contractor: Словарь с данными контрагента из источника
            
        Returns:
            Словарь с данными контрагента для приемника
        """
        return self.process_item(contractor)

    def process_contractors(self, contractors: List[Dict]) -> List[Dict]:
        """
        Преобразует список контрагентов.
        
        Args:
            contractors: Список словарей с данными контрагентов из источника
            
        Returns:
            Список словарей с данными контрагентов для приемника
        """
        from tools.logger import verbose_print
        verbose_print(f"\nОбработка {len(contractors)} контрагентов...")
        processed = []

        for i, contractor in enumerate(contractors, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(contractors)}")

            processed_contractor = self.process_contractor(contractor)
            if processed_contractor:
                processed.append(processed_contractor)

        verbose_print(f"Обработано контрагентов: {len(processed)}")
        return processed

    def process_and_save_contractors(
        self, contractors: List[Dict], output_db_path: str, table_name: str = "contractors"
    ) -> bool:
        """
        Преобразует контрагентов и сохраняет их в новую базу данных.
        
        Args:
            contractors: Список словарей с данными контрагентов из источника
            output_db_path: Путь к выходной базе данных SQLite
            table_name: Имя таблицы для сохранения (по умолчанию "contractors")
            
        Returns:
            True если успешно, False если ошибка
        """
        # Обрабатываем контрагентов
        processed = self.process_contractors(contractors)
        
        if not processed:
            verbose_print("Нет обработанных контрагентов для сохранения")
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
            for contractor in processed:
                for field_name in contractor.keys():
                    if field_name.endswith("_UUID") or field_name.endswith("_Представление") or field_name.endswith("_Тип"):
                        base_field = field_name.rsplit("_", 1)[0]
                        reference_fields.add(base_field)

            for contractor in processed:
                for ref_field in reference_fields:
                    presentation_key = f"{ref_field}_Представление"
                    uuid_key = f"{ref_field}_UUID"
                    type_key = f"{ref_field}_Тип"

                    has_meta = (
                        presentation_key in contractor or
                        uuid_key in contractor or
                        type_key in contractor
                    )

                    if not has_meta:
                        continue

                    presentation = contractor.pop(presentation_key, "")
                    uuid_value = contractor.pop(uuid_key, "")
                    type_value = contractor.pop(type_key, "")
                    
                    # Для поля "Родитель" проверяем наличие Родитель_ЭтоГруппа и добавляем в JSON
                    is_group_value = None
                    if ref_field == "Родитель":
                        is_group_key = f"{ref_field}_ЭтоГруппа"
                        if is_group_key in contractor:
                            is_group_value = contractor.pop(is_group_key, None)
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
                        
                        contractor[ref_field] = json.dumps(
                            json_data,
                            ensure_ascii=False,
                        )
                    else:
                        contractor[ref_field] = ""

            # Определяем базовые колонки для таблицы
            base_columns = {
                "uuid": "TEXT PRIMARY KEY",
                "Ссылка": "TEXT",
                "Код": "TEXT",
                "Наименование": "TEXT",
                "ПометкаУдаления": "INTEGER",
                "ИНН": "TEXT",
                "КПП": "TEXT",
                "НаименованиеПолное": "TEXT",
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

            from tools.logger import verbose_print
            verbose_print(f"\nСохранено контрагентов в БД: {saved}")
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




def process_contractors(source_db_path: str, processed_db_path: str) -> bool:
    """
    Точка входа обработки контрагентов с использованием маппинга.
    
    Читает контрагентов из исходной БД, применяет маппинг полей и типов,
    и сохраняет результат в новую БД.

    Args:
        source_db_path: Путь к исходной базе данных (результат этапа загрузки)
        processed_db_path: Путь к базе данных после обработки

    Returns:
        True если успешно, False если ошибка
    """
    from tools.logger import verbose_print
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА КОНТРАГЕНТОВ С МАППИНГОМ")
    verbose_print("=" * 80)
    
    mapping_db_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "CONF",
        "type_mapping.db"
    )
    
    verbose_print(f"\n[1/5] Чтение контрагентов из исходной БД: {source_db_path}")
    contractors = read_from_db(source_db_path, "contractors")
    
    if not contractors:
        print("Ошибка: не удалось прочитать контрагентов из исходной БД")
        return False
    
    verbose_print(f"Прочитано контрагентов: {len(contractors)}")

    verbose_print(f"\n[2/5] Инициализация процессора маппинга...")
    processor = ContractorMappingProcessor(mapping_db_path)
    
    verbose_print(f"\n[3/5] Обработка контрагентов с использованием маппинга...")
    success = processor.process_and_save_contractors(contractors, processed_db_path, "contractors")
    
    if not success:
        verbose_print(f"\n[3/5] Ошибка при обработке контрагентов")
        return False
    
    verbose_print(f"\n[4/5] Копирование табличных частей...")
    tabular_success = copy_tabular_sections(source_db_path, processed_db_path, "contractors")
    
    if success and tabular_success:
        verbose_print(f"\n[5/5] Обработка завершена успешно!")
        verbose_print(f"Результат сохранен в: {processed_db_path}")
    else:
        verbose_print(f"\n[5/5] Обработка завершена с ошибками")
    
    return success and tabular_success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Обработка базы данных контрагентов с маппингом"
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
    success = process_contractors(cli_args.source_db, cli_args.processed_db)
    raise SystemExit(0 if success else 1)
