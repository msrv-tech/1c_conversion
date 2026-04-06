# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «Номенклатура» с использованием маппинга типов и полей.

Читает номенклатуру из исходной БД, применяет маппинг из type_mapping.db
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


class NomenclatureMappingProcessor(MappingProcessor):
    """Процессор для преобразования номенклатуры с использованием маппинга."""

    def __init__(self, mapping_db_path: str = "CONF/type_mapping.db"):
        """
        Инициализация процессора.
        
        Args:
            mapping_db_path: Путь к базе данных с маппингом
        """
        super().__init__(mapping_db_path, "Номенклатура", "catalog")

    def process_nomenclature(self, nomenclature: Dict) -> Dict:
        """
        Преобразует номенклатуру из формата источника в формат приемника.
        
        Args:
            nomenclature: Словарь с данными номенклатуры из источника
            
        Returns:
            Словарь с данными номенклатуры для приемника
        """
        # Сохраняем исходное значение СтавкаНДС до обработки базовым процессором
        original_stavka_nds = nomenclature.get("СтавкаНДС", "") if nomenclature else ""
        
        processed = self.process_item(nomenclature)
        
        # Преобразуем СтавкаНДС в ВидСтавкиНДС
        # Используем исходное значение, так как базовый процессор может его изменить
        if original_stavka_nds and isinstance(original_stavka_nds, str) and original_stavka_nds.startswith("Перечисление.СтавкиНДС."):
            # Извлекаем значение перечисления (например, "НДС20" из "Перечисление.СтавкиНДС.НДС20")
            enum_value = original_stavka_nds.replace("Перечисление.СтавкиНДС.", "")
            
            # Ищем маппинг значения в enum_value_mapping
            source_enum_type = "Перечисление.СтавкиНДС"
            if source_enum_type in self.enum_value_mapping:
                value_mapping = self.enum_value_mapping[source_enum_type]
                target_enum_value = value_mapping.get(enum_value)
                
                if target_enum_value:
                    # Получаем целевой тип перечисления из маппинга
                    target_enum_type = self.enum_type_mapping.get(source_enum_type, "Перечисление.ВидыСтавокНДС")
                    target_enum_name = target_enum_type.replace("Перечисление.", "")
                    # Формируем значение в формате "Перечисление.ВидыСтавокНДС.Значение"
                    vid_stavki_value = f"Перечисление.{target_enum_name}.{target_enum_value}"
                    processed["ВидСтавкиНДС"] = vid_stavki_value
                    # Восстанавливаем исходное значение СтавкаНДС (оно должно оставаться в исходном формате)
                    processed["СтавкаНДС"] = original_stavka_nds
                    verbose_print(f"  → Преобразовано СтавкаНДС '{original_stavka_nds}' → ВидСтавкиНДС '{vid_stavki_value}'")
                else:
                    verbose_print(f"  ⚠ Маппинг для значения '{enum_value}' не найден (СтавкаНДС: '{original_stavka_nds}')")
            else:
                verbose_print(f"  ⚠ Маппинг для перечисления '{source_enum_type}' не найден")
        
        return processed

    def process_nomenclatures(self, nomenclatures: List[Dict]) -> List[Dict]:
        """
        Преобразует список номенклатуры.
        
        Args:
            nomenclatures: Список словарей с данными номенклатуры из источника
            
        Returns:
            Список словарей с данными номенклатуры для приемника
        """
        verbose_print(f"\nОбработка {len(nomenclatures)} номенклатуры...")
        processed = []

        for i, nomenclature in enumerate(nomenclatures, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(nomenclatures)}")

            processed_nomenclature = self.process_nomenclature(nomenclature)
            if processed_nomenclature:
                processed.append(processed_nomenclature)

        verbose_print(f"Обработано номенклатуры: {len(processed)}")
        return processed

    def process_and_save_nomenclatures(
        self, nomenclatures: List[Dict], output_db_path: str, table_name: str = "nomenclature"
    ) -> bool:
        """
        Преобразует номенклатуру и сохраняет их в новую базу данных.
        
        Args:
            nomenclatures: Список словарей с данными номенклатуры из источника
            output_db_path: Путь к выходной базе данных SQLite
            table_name: Имя таблицы для сохранения (по умолчанию "nomenclature")
            
        Returns:
            True если успешно, False если ошибка
        """
        # Обрабатываем номенклатуру
        processed = self.process_nomenclatures(nomenclatures)
        
        if not processed:
            verbose_print("Нет обработанной номенклатуры для сохранения")
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
            for nomenclature in processed:
                for field_name in nomenclature.keys():
                    if field_name.endswith("_UUID") or field_name.endswith("_Представление") or field_name.endswith("_Тип"):
                        base_field = field_name.rsplit("_", 1)[0]
                        reference_fields.add(base_field)

            for nomenclature in processed:
                for ref_field in reference_fields:
                    uuid_field = f"{ref_field}_UUID"
                    presentation_field = f"{ref_field}_Представление"
                    type_field = f"{ref_field}_Тип"
                    
                    # Для поля "Родитель" проверяем наличие Родитель_ЭтоГруппа и добавляем в JSON
                    is_group_value = None
                    if ref_field == "Родитель":
                        is_group_key = f"{ref_field}_ЭтоГруппа"
                        if is_group_key in nomenclature:
                            is_group_value = nomenclature.pop(is_group_key, None)
                            # Преобразуем в булево значение
                            if isinstance(is_group_value, bool):
                                pass  # Уже булево
                            elif isinstance(is_group_value, (int, str)):
                                is_group_value = str(is_group_value).lower() in ('1', 'true', 'истина', 'да')
                            elif is_group_value is None:
                                is_group_value = False
                    
                    # Если есть UUID, создаем JSON для ссылочного поля
                    if uuid_field in nomenclature and nomenclature[uuid_field]:
                        ref_uuid = nomenclature[uuid_field]
                        ref_presentation = nomenclature.get(presentation_field, "")
                        ref_type = nomenclature.get(type_field, "")
                        
                        # Создаем JSON объект
                        json_data = {
                            "uuid": ref_uuid,
                            "presentation": ref_presentation,
                            "type": ref_type
                        }
                        # Добавляем is_group для поля "Родитель"
                        if is_group_value is not None:
                            json_data["is_group"] = is_group_value
                        
                        ref_json = json.dumps(json_data, ensure_ascii=False)
                        
                        # Устанавливаем JSON в основное поле
                        nomenclature[ref_field] = ref_json
                        
                        # Удаляем служебные поля (они уже в JSON)
                        nomenclature.pop(uuid_field, None)
                        nomenclature.pop(presentation_field, None)
                        nomenclature.pop(type_field, None)

            # Сохраняем обработанные данные
            # upsert_rows автоматически добавляет новые колонки, но указываем основные для схемы
            saved = upsert_rows(
                connection,
                table_name,
                processed,
                {
                    "uuid": "TEXT PRIMARY KEY",
                    "Ссылка": "TEXT",
                    "Код": "TEXT",
                    "Наименование": "TEXT",
                    "ПометкаУдаления": "INTEGER",
                    "ВидСтавкиНДС": "TEXT",
                },
            )

            verbose_print(f"\nСохранено номенклатуры в БД: {saved}")
            verbose_print(f"База данных: {output_db_path}")
            verbose_print(f"Таблица: {table_name}")

            return saved > 0

        except Exception as e:
            print(f"Ошибка при сохранении номенклатуры: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            connection.close()


def process_nomenclature(source_db_path: str, processed_db_path: str) -> bool:
    """
    Точка входа обработки номенклатуры с использованием маппинга.
    
    Читает номенклатуру из исходной БД, применяет маппинг полей и типов,
    и сохраняет результат в новую БД.

    Args:
        source_db_path: Путь к исходной базе данных (результат этапа загрузки)
        processed_db_path: Путь к базе данных после обработки

    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА НОМЕНКЛАТУРЫ С МАППИНГОМ")
    verbose_print("=" * 80)
    
    mapping_db_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "CONF",
        "type_mapping.db"
    )
    
    verbose_print(f"\n[1/5] Чтение номенклатуры из исходной БД: {source_db_path}")
    nomenclatures = read_from_db(source_db_path, "nomenclature")
    
    if not nomenclatures:
        print("Ошибка: не удалось прочитать номенклатуру из исходной БД")
        return False
    
    verbose_print(f"Прочитано номенклатуры: {len(nomenclatures)}")
    
    verbose_print(f"\n[2/5] Инициализация процессора маппинга...")
    processor = NomenclatureMappingProcessor(mapping_db_path)
    
    verbose_print(f"\n[3/5] Обработка номенклатуры с использованием маппинга...")
    success = processor.process_and_save_nomenclatures(nomenclatures, processed_db_path, "nomenclature")
    
    if not success:
        verbose_print(f"\n[3/5] Ошибка при обработке номенклатуры")
        return False
    
    verbose_print(f"\n[4/5] Копирование табличных частей...")
    tabular_success = copy_tabular_sections(source_db_path, processed_db_path, "nomenclature")
    
    if success and tabular_success:
        verbose_print(f"\n[5/5] Обработка завершена успешно!")
        verbose_print(f"Результат сохранен в: {processed_db_path}")
    else:
        verbose_print(f"\n[5/5] Обработка завершена с ошибками")
    
    return success and tabular_success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Обработка базы данных номенклатуры с маппингом"
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
    success = process_nomenclature(cli_args.source_db, cli_args.processed_db)
    raise SystemExit(0 if success else 1)

