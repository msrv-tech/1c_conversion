# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «Характеристики номенклатуры» с преобразованием в номенклатуру.

Читает характеристики номенклатуры из исходной БД, берет данные из Владелец (номенклатура),
заменяет UUID на UUID характеристики и формирует Наименование = НаименованиеВладельца + "_" + НаименованиеХарактеристики.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from typing import Dict, List

# Добавляем путь к корню проекта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows
from tools.processor_utils import read_from_db
from tools.base_processor import MappingProcessor
from tools.logger import verbose_print  # noqa: E402

fix_encoding()


class NomenclatureCharacteristicsProcessor(MappingProcessor):
    """Процессор для преобразования характеристик номенклатуры в номенклатуру."""

    def __init__(self, source_db_path: str, mapping_db_path: str = "CONF/type_mapping.db"):
        """
        Инициализация процессора.
        
        Args:
            source_db_path: Путь к исходной БД с характеристиками (все данные уже загружены)
            mapping_db_path: Путь к базе данных с маппингом
        """
        # Инициализируем базовый процессор для маппинга полей номенклатуры
        super().__init__(mapping_db_path, "Номенклатура", "catalog")
        self.source_db_path = source_db_path

    def process_characteristic(self, characteristic: Dict) -> Dict:
        """
        Преобразует характеристику номенклатуры в номенклатуру.
        
        Все данные из Владелец (номенклатуры) уже загружены в loader.
        Применяем маппинг полей через базовый процессор, затем формируем Наименование.
        
        Args:
            characteristic: Словарь с данными характеристики из источника (включая все поля номенклатуры)
            
        Returns:
            Словарь с данными номенклатуры для приемника
        """
        # Сохраняем UUID характеристики и НаименованиеХарактеристики до обработки
        characteristic_uuid = characteristic.get("uuid", "")
        characteristic_name = characteristic.get("НаименованиеХарактеристики", "")
        owner_name = characteristic.get("Наименование", "")  # Наименование номенклатуры (из Владелец)
        
        # Применяем базовый маппинг полей номенклатуры
        processed = self.process_item(characteristic)
        
        # Убеждаемся, что UUID остался из характеристики
        if characteristic_uuid:
            processed["uuid"] = characteristic_uuid
        
        # Формируем Наименование = НаименованиеВладельца + "_" + НаименованиеХарактеристики
        if owner_name and characteristic_name:
            processed["Наименование"] = f"{owner_name}_{characteristic_name}"
        elif characteristic_name:
            processed["Наименование"] = characteristic_name
        elif owner_name:
            processed["Наименование"] = owner_name
        
        # Удаляем служебное поле НаименованиеХарактеристики, оно больше не нужно
        processed.pop("НаименованиеХарактеристики", None)
        
        return processed

    def process_characteristics(self, characteristics: List[Dict]) -> List[Dict]:
        """
        Преобразует список характеристик номенклатуры.
        
        Все данные из Владелец (номенклатуры) уже загружены в loader,
        поэтому просто обрабатываем каждую характеристику.
        
        Args:
            characteristics: Список словарей с данными характеристик из источника
            
        Returns:
            Список словарей с данными номенклатуры для приемника
        """
        verbose_print(f"\nОбработка {len(characteristics)} характеристик номенклатуры...")
        
        processed = []
        skipped = 0
        
        for i, characteristic in enumerate(characteristics, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(characteristics)}")
            
            # Преобразуем характеристику в номенклатуру
            processed_nomenclature = self.process_characteristic(characteristic)
            if processed_nomenclature:
                processed.append(processed_nomenclature)
            else:
                skipped += 1
        
        if skipped > 0:
            verbose_print(f"  ⚠ Пропущено характеристик: {skipped}")
        
        verbose_print(f"Обработано характеристик: {len(processed)}")
        return processed

    def process_and_save_characteristics(
        self, characteristics: List[Dict], output_db_path: str, table_name: str = "nomenclature"
    ) -> bool:
        """
        Преобразует характеристики номенклатуры и сохраняет их в новую базу данных.
        
        Args:
            characteristics: Список словарей с данными характеристик из источника
            output_db_path: Путь к выходной базе данных SQLite
            table_name: Имя таблицы для сохранения (по умолчанию "nomenclature")
            
        Returns:
            True если успешно, False если ошибка
        """
        # Обрабатываем характеристики
        processed = self.process_characteristics(characteristics)
        
        if not processed:
            verbose_print("Нет обработанных характеристик для сохранения")
            return False
        
        # Сохраняем обработанные данные
        if not ensure_database_exists(output_db_path):
            verbose_print("Не удалось подготовить базу данных SQLite.")
            return False
        
        connection = connect_to_sqlite(output_db_path)
        if not connection:
            verbose_print("Не удалось подключиться к SQLite.")
            return False
        
        try:
            # Сохраняем обработанные данные
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
            
            verbose_print(f"Сохранено характеристик в БД: {saved}")
            verbose_print(f"База данных: {output_db_path}")
            verbose_print(f"Таблица: {table_name}")
            
            return saved > 0
            
        except Exception as e:
            print(f"Ошибка при сохранении характеристик: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            connection.close()


def process_nomenclature_characteristics(source_db_path: str, processed_db_path: str) -> bool:
    """
    Точка входа обработки характеристик номенклатуры.
    
    Читает характеристики из исходной БД, преобразует их в номенклатуру
    и сохраняет результат в новую БД.
    
    Args:
        source_db_path: Путь к исходной БД с характеристиками
        processed_db_path: Путь к выходной БД для обработанных данных
        
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ХАРАКТЕРИСТИК НОМЕНКЛАТУРЫ С ПРЕОБРАЗОВАНИЕМ В НОМЕНКЛАТУРУ")
    verbose_print("=" * 80)
    
    verbose_print(f"\n[1/3] Чтение характеристик из исходной БД: {source_db_path}")
    characteristics = read_from_db(source_db_path, "nomenclature_characteristics")
    
    if not characteristics:
        verbose_print("Ошибка: не удалось прочитать характеристики из исходной БД")
        return False
    
    verbose_print(f"Прочитано характеристик: {len(characteristics)}")
    
    verbose_print(f"\n[2/3] Инициализация процессора...")
    mapping_db_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "CONF",
        "type_mapping.db"
    )
    processor = NomenclatureCharacteristicsProcessor(source_db_path, mapping_db_path)
    
    verbose_print(f"\n[3/3] Обработка характеристик с преобразованием в номенклатуру...")
    success = processor.process_and_save_characteristics(
        characteristics, processed_db_path, "nomenclature"
    )
    
    if not success:
        verbose_print(f"\n[3/3] Ошибка при обработке характеристик")
        return False
    
    verbose_print(f"\n[4/4] Обработка завершена успешно!")
    verbose_print(f"Результат сохранен в: {processed_db_path}")
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Обработка характеристик номенклатуры с преобразованием в номенклатуру")
    parser.add_argument("--source-db", required=True, help="Путь к исходной БД с характеристиками")
    parser.add_argument("--processed-db", required=True, help="Путь к выходной БД для обработанных данных")
    
    cli_args = parser.parse_args()
    
    success = process_nomenclature_characteristics(cli_args.source_db, cli_args.processed_db)
    raise SystemExit(0 if success else 1)
