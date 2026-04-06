# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «УправленческиеЭтапыРаботДополнительныхСоглашений» с использованием маппинга типов и полей.

Читает управленческиеэтапыработдополнительныхсоглашений из исходной БД, применяет маппинг из type_mapping.db
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


class ManagerialStagesAdditionalAgreementsMappingProcessor(MappingProcessor):
    """Процессор для преобразования управленческихэтаповработдополнительныхсоглашений с использованием маппинга."""

    def __init__(self, mapping_db_path: str = "CONF/type_mapping.db"):
        """
        Инициализация процессора.
        
        Args:
            mapping_db_path: Путь к базе данных с маппингом
        """
        super().__init__(mapping_db_path, "УправленческиеЭтапыРаботДополнительныхСоглашений", "catalog")

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
        verbose_print(f"\nОбработка {len(items)} управленческихэтаповработдополнительныхсоглашений...")
        processed = []

        for i, item in enumerate(items, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(items)}")

            processed_item = self.process_item_single(item)
            if processed_item:
                processed.append(processed_item)

        verbose_print(f"Обработано управленческихэтаповработдополнительныхсоглашений: {len(processed)}")
        return processed

    def process_and_save_items(
        self, items: List[Dict], output_db_path: str, table_name: str = "managerial_stages_additional_agreements"
    ) -> bool:
        """
        Преобразует элементы и сохраняет их в новую базу данных.
        
        Args:
            items: Список словарей с данными элементов из источника
            output_db_path: Путь к выходной базе данных SQLite
            table_name: Имя таблицы для сохранения (по умолчанию "managerial_stages_additional_agreements")
            
        Returns:
            True если успешно, False если ошибка
        """
        # Обрабатываем элементы
        processed = self.process_items(items)
        
        if not processed:
            verbose_print("Нет обработанных управленческихэтаповработдополнительныхсоглашений для сохранения")
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
            # Определяем базовые колонки
            base_columns = {
                "uuid": "TEXT PRIMARY KEY",
                "Ссылка": "TEXT",
                "Код": "TEXT",
                "Наименование": "TEXT",
                "ПометкаУдаления": "INTEGER",
            }

            # Расширяем колонки всеми смапленными полями
            extended_columns = self.extend_base_columns_with_mapped_fields(base_columns)

            # Удаляем служебные поля из обработанных элементов перед сохранением
            for item in processed:
                # Удаляем поля _Представление, _UUID, _Тип для всех ссылочных полей
                keys_to_remove = []
                for key in item.keys():
                    if key.endswith("_Представление") or key.endswith("_UUID") or key.endswith("_Тип"):
                        keys_to_remove.append(key)
                
                for key in keys_to_remove:
                    item.pop(key, None)

            saved = upsert_rows(
                connection,
                table_name,
                processed,
                extended_columns,
            )

            if saved:
                verbose_print(f"\nСохранено управленческихэтаповработдополнительныхсоглашений в БД: {len(processed)}")
                verbose_print(f"База данных: {output_db_path}")
                verbose_print(f"Таблица: {table_name}")

            connection.commit()
            return saved

        except Exception as error:
            print(f"Ошибка при сохранении управленческихэтаповработдополнительныхсоглашений: {error}")
            import traceback
            traceback.print_exc()
            connection.rollback()
            return False
        finally:
            connection.close()


def process_managerial_stages_additional_agreements(
    source_db_path: str,
    processed_db_path: str,
    table_name: str = "managerial_stages_additional_agreements",
    mapping_db_path: str = "CONF/type_mapping.db",
) -> bool:
    """
    Обрабатывает управленческиеэтапыработдополнительныхсоглашений из исходной БД и сохраняет в обработанную БД.
    
    Args:
        source_db_path: Путь к исходной базе данных SQLite
        processed_db_path: Путь к обработанной базе данных SQLite
        table_name: Имя таблицы (по умолчанию "managerial_stages_additional_agreements")
        mapping_db_path: Путь к базе данных с маппингом
        
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА СПРАВОЧНИКА «УПРАВЛЕНЧЕСКИЕ ЭТАПЫ РАБОТ ДОПОЛНИТЕЛЬНЫХ СОГЛАШЕНИЙ»")
    verbose_print("=" * 80)

    verbose_print(f"\n[1/5] Чтение данных из исходной БД: {source_db_path}")
    items = read_from_db(source_db_path, table_name)
    if not items:
        verbose_print("УправленческиеЭтапыРаботДополнительныхСоглашений не найдены в исходной БД")
        return False
    verbose_print(f"Прочитано управленческихэтаповработдополнительныхсоглашений: {len(items)}")

    verbose_print(f"\n[2/5] Инициализация процессора (маппинг: {mapping_db_path})...")
    processor = ManagerialStagesAdditionalAgreementsMappingProcessor(mapping_db_path)

    verbose_print("\n[3/5] Обработка данных...")
    success = processor.process_and_save_items(items, processed_db_path, table_name)

    if success:
        verbose_print("\n[4/5] Копирование табличных частей...")
        copy_tabular_sections(source_db_path, processed_db_path, table_name)

        verbose_print("\n[5/5] Обработка завершена успешно!")
        return True
    else:
        verbose_print("\n[5/5] Обработка завершена с ошибками!")
        return False

