# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «ДоговорыКонтрагентов» с использованием маппинга типов и полей.

Читает договорыконтрагентов из исходной БД, применяет маппинг из type_mapping.db
и сохраняет результат в новую БД в формате приемника (UH).
"""

from __future__ import annotations

import json
import os
import sys
from typing import Dict, List, Optional

# Добавляем путь к корню проекта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows

from tools.base_processor import MappingProcessor
from tools.processor_utils import read_from_db, copy_tabular_sections
from tools.logger import verbose_print  # noqa: E402

fix_encoding()


class ContractorContractsMappingProcessor(MappingProcessor):
    """Процессор для преобразования договорыконтрагентов с использованием маппинга."""

    def __init__(self, mapping_db_path: str = "CONF/type_mapping.db"):
        """
        Инициализация процессора.
        
        Args:
            mapping_db_path: Путь к базе данных с маппингом
        """
        super().__init__(mapping_db_path, "ДоговорыКонтрагентов", "catalog")

    def process_item_single(self, item: Dict) -> Dict:
        """
        Преобразует элемент из формата источника в формат приемника.
        
        Args:
            item: Словарь с данными элемента из источника
            
        Returns:
            Словарь с данными элемента для приемника
        """
        result = self.process_item(item)
        
        # Обрабатываем поле УправленческийДоговор_ВидДоговора -> ВидСоглашения
        source_presentation_field = "УправленческийДоговор_ВидДоговора_Представление"
        source_field = "УправленческийДоговор_ВидДоговора"
        
        if source_presentation_field in item and source_field in self.field_mapping:
            # Получаем значение перечисления из источника (строковое представление)
            enum_value = item[source_presentation_field]
            
            if enum_value:
                # Получаем тип перечисления из маппинга полей (источник)
                mapping = self.field_mapping[source_field]
                source_type = mapping.get("source_type", "")
                target_field = mapping["target_field"]
                target_type = mapping.get("target_type", "")
                
                # Используем тип из маппинга, если он есть, иначе пробуем определить из поля _Тип
                if not source_type or source_type == "string":
                    # Если тип не определен или это строка, используем известный тип перечисления
                    source_type = "Перечисление.ВидыУправленческихДоговоров"
                
                # Ищем маппинг значений перечисления по строковому представлению
                target_enum_value = self._map_enum_value_by_name(source_type, enum_value)
                if target_enum_value:
                    # Формируем значение перечисления для приемника
                    if target_type and target_type.startswith("Перечисление."):
                        target_enum_name = target_type.replace("Перечисление.", "")
                        result[target_field] = f"Перечисление.{target_enum_name}.{target_enum_value}"
                    else:
                        # Если тип не определен, используем только значение
                        result[target_field] = target_enum_value
        
        # Заполняем поле ВидДоговораУХ на основе ВидДоговора
        self._fill_vid_dogovora_uh(result, item)
        
        # Копируем значение Дата в ДатаНачалаДействия
        self._copy_date_to_date_nachala_dejstvija(result, item)
        
        return result
    
    def _fill_vid_dogovora_uh(self, result: Dict, source_item: Dict) -> None:
        """
        Заполняет поле ВидДоговораУХ на основе значения ВидДоговора.
        
        Args:
            result: Словарь с обработанными данными элемента (будет изменен)
            source_item: Словарь с исходными данными элемента
        """
        # Проверяем, нужно ли заполнять ВидДоговораУХ
        vid_dogovora_uh_field = result.get("ВидДоговораУХ", "")
        if vid_dogovora_uh_field:
            # Проверяем, является ли это пустым JSON
            try:
                if isinstance(vid_dogovora_uh_field, str) and vid_dogovora_uh_field.strip().startswith('{'):
                    vid_uh_json = json.loads(vid_dogovora_uh_field)
                    if vid_uh_json.get("uuid") and vid_uh_json.get("uuid") != "00000000-0000-0000-0000-000000000000":
                        # Поле уже заполнено валидной ссылкой, не перезаписываем
                        return
            except (json.JSONDecodeError, AttributeError):
                pass
        
        # Извлекаем значение перечисления ВидДоговора
        vid_dogovora_field = result.get("ВидДоговора", "")
        if not vid_dogovora_field:
            # Пробуем получить из исходного элемента
            vid_dogovora_field = source_item.get("ВидДоговора", "")
            if not vid_dogovora_field:
                vid_dogovora_field = source_item.get("ВидДоговора_Представление", "")
        
        if not vid_dogovora_field:
            return
        
        # Извлекаем значение перечисления
        enum_value = None
        try:
            # Если это JSON-строка
            if isinstance(vid_dogovora_field, str) and vid_dogovora_field.strip().startswith('{'):
                enum_json = json.loads(vid_dogovora_field)
                enum_value = enum_json.get("presentation", "")
            # Если это строка вида "Перечисление.ВидыДоговоровКонтрагентов.СПокупателем"
            elif isinstance(vid_dogovora_field, str):
                if vid_dogovora_field.startswith("Перечисление."):
                    parts = vid_dogovora_field.split(".")
                    if len(parts) >= 3:
                        enum_value = parts[-1]  # Берем последнюю часть (значение перечисления)
                else:
                    enum_value = vid_dogovora_field
        except Exception:
            pass
        
        if not enum_value:
            return
        
        # Маппинг значений перечисления на наименования в справочнике ВидыДоговоровКонтрагентовУХ
        enum_to_catalog_mapping = {
            # Продажи и покупки
            "СПокупателем": "С покупателем",
            "СПоставщиком": "С поставщиком",
            "СКомитентом": "С комитентом",
            "СКомиссионером": "С комиссионером",
            "СКомитентомНаЗакупку": "С комитентом на закупку",
            "СКомиссионеромНаЗакупку": "С комиссионером на закупку",
            "СФакторинговойКомпанией": "С факторинговой компанией",
            "СТранспортнойКомпанией": "С транспортной компанией",
            "ВвозИзЕАЭС": "Ввоз из ЕАЭС",
            "Импорт": "Импорт",
            "СДавальцем": "С давальцем",
            "СПереработчиком": "С переработчиком",
            "СПоклажедателем": "С поклажедателем",
            "СХранителем": "С хранителем",
            "ЛизингПолученный": "Лизинг полученный",
            "РасчетноКассовоеОбслуживание": "Расчетно-кассовое обслуживание",
            # Привлечение средств
            "ЗаемПолученный": "Заем полученный",
            "Кредит": "Кредит",
            "Овердрафт": "Овердрафт",
            "УниверсальноеПривлечение": "Универсальное привлечение",
            # Производные финансовые инструменты
            "ВалютноПроцентныйСвоп": "Валютно-процентный своп",
            # Обеспечение
            "АккредитивВыданный": "Аккредитив выданный",
            "ГарантияВыданная": "Гарантия выданная",
            "ГарантияПолученная": "Гарантия полученная",
            "ЗалогиИПоручительстваВходящие": "Залоги и поручительства входящие",
            "ЗалогиИПоручительстваИсходящие": "Залоги и поручительства исходящие",
            "Страхование": "Страхование",
            # Прочие виды договоров
            "РасчетыСБрокером": "Расчеты с брокером",
            "Прочее": "Прочее",
            "ЦессияИсходящая": "Цессия исходящая",
            "ЦессияВходящая": "Цессия входящая",
            # Размещение средств
            "ЗаемВыданный": "Заем выданный",
            "Депозит": "Депозит",
            "МинимальныйНеснижаемыйОстаток": "Минимальный неснижаемый остаток",
            "УниверсальноеРазмещение": "Универсальное размещение",
        }
        
        # Преобразуем значение перечисления в наименование для поиска
        catalog_name = enum_to_catalog_mapping.get(enum_value, None)
        
        # Если не нашли точное совпадение, пробуем преобразовать автоматически
        # (добавляем пробелы перед заглавными буквами, кроме первой)
        if catalog_name is None and len(enum_value) > 1:
            import re
            # Добавляем пробелы перед заглавными буквами
            catalog_name = re.sub(r'(?<!^)(?=[А-ЯЁ])', ' ', enum_value)
        
        if catalog_name:
            # Формируем предопределенное значение для поля ВидДоговораУХ
            # Значение перечисления (enum_value) уже соответствует коду предопределенного элемента
            # Например: "СПоставщиком" -> "Справочник.ВидыДоговоровКонтрагентовУХ.СПоставщиком"
            # Используем enum_value напрямую, так как он уже в правильном формате
            predefined_code = enum_value
            
            # Формируем строку в формате предопределенного значения
            predefined_value = f"Справочник.ВидыДоговоровКонтрагентовУХ.{predefined_code}"
            result["ВидДоговораУХ"] = predefined_value
            verbose_print(f"    ✓ Заполнено поле ВидДоговораУХ: {predefined_value} (из перечисления '{enum_value}')")
    
    def _copy_date_to_date_nachala_dejstvija(self, result: Dict, source_item: Dict) -> None:
        """
        Копирует значение поля Дата в поле ДатаНачалаДействия.
        
        Args:
            result: Словарь с обработанными данными элемента (будет изменен)
            source_item: Словарь с исходными данными элемента
        """
        # Получаем значение поля Дата из результата (после маппинга)
        date_value = result.get("Дата")
        
        # Если в результате нет, пробуем получить из исходного элемента
        if not date_value:
            date_value = source_item.get("Дата")
        
        # Если значение найдено, копируем его в ДатаНачалаДействия
        if date_value:
            result["ДатаНачалаДействия"] = date_value
            verbose_print(f"    ✓ Скопировано поле Дата в ДатаНачалаДействия: {date_value}")
    
    def _map_enum_value_by_name(self, enum_type: str, enum_value: str) -> Optional[str]:
        """
        Преобразует значение перечисления по его строковому представлению (наименованию).
        
        Пробует найти маппинг как по точному совпадению, так и по нормализованному значению
        (убирая пробелы и приводя к одному регистру).
        
        Args:
            enum_type: Тип перечисления источника (например, "Перечисление.ВидыУправленческихДоговоров")
            enum_value: Строковое представление значения перечисления (например, "Доходный договор на ПИР" или "ДоходныйДоговорНаПИР")
            
        Returns:
            Преобразованное значение перечисления или None
        """
        if not enum_type or not enum_value:
            return None
        
        # Ищем маппинг значений перечисления
        if enum_type in self.enum_value_mapping:
            value_mapping = self.enum_value_mapping[enum_type]
            
            # Пробуем точное совпадение
            if enum_value in value_mapping:
                return value_mapping[enum_value]
            
            # Пробуем нормализованное значение (убираем пробелы, приводим к нижнему регистру)
            normalized_value = enum_value.replace(" ", "").replace("-", "").lower()
            for source_val, target_val in value_mapping.items():
                normalized_source = source_val.replace(" ", "").replace("-", "").lower()
                if normalized_value == normalized_source:
                    return target_val
        
        return None

    def process_items(self, items: List[Dict]) -> List[Dict]:
        """
        Преобразует список элементов.
        
        Args:
            items: Список словарей с данными элементов из источника
            
        Returns:
            Список словарей с данными элементов для приемника
        """
        verbose_print(f"\nОбработка {len(items)} договорыконтрагентов...")
        processed = []

        for i, item in enumerate(items, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(items)}")

            processed_item = self.process_item_single(item)
            if processed_item:
                processed.append(processed_item)

        verbose_print(f"Обработано договорыконтрагентов: {len(processed)}")
        return processed

    def process_and_save_items(
        self, items: List[Dict], output_db_path: str, table_name: str = "contractor_contracts"
    ) -> bool:
        """
        Преобразует элементы и сохраняет их в новую базу данных.
        
        Args:
            items: Список словарей с данными элементов из источника
            output_db_path: Путь к выходной базе данных SQLite
            table_name: Имя таблицы для сохранения (по умолчанию "contractor_contracts")
            
        Returns:
            True если успешно, False если ошибка
        """
        # Обрабатываем элементы
        processed = self.process_items(items)
        
        if not processed:
            verbose_print("Нет обработанных договорыконтрагентов для сохранения")
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
                    
                    # Для поля "Родитель" проверяем наличие Родитель_ЭтоГруппа и добавляем в JSON
                    is_group_value = None
                    if ref_field == "Родитель":
                        is_group_key = f"{ref_field}_ЭтоГруппа"
                        if is_group_key in item:
                            is_group_value = item.pop(is_group_key, None)
                            # Преобразуем в булево значение
                            if isinstance(is_group_value, bool):
                                pass  # Уже булево
                            elif isinstance(is_group_value, (int, str)):
                                is_group_value = str(is_group_value).lower() in ('1', 'true', 'истина', 'да')
                            elif is_group_value is None:
                                is_group_value = False

                    if uuid_field in item and item[uuid_field]:
                        ref_uuid = item[uuid_field]
                        ref_presentation = item.get(presentation_field, "")
                        ref_type = item.get(type_field, "")
                        
                        json_data = {
                            "uuid": ref_uuid,
                            "presentation": ref_presentation,
                            "type": ref_type
                        }
                        # Добавляем is_group для поля "Родитель"
                        if is_group_value is not None:
                            json_data["is_group"] = is_group_value
                        
                        ref_json = json.dumps(json_data, ensure_ascii=False)

                        item[ref_field] = ref_json

                        item.pop(uuid_field, None)
                        item.pop(presentation_field, None)
                        item.pop(type_field, None)

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
                    "ВидДоговораУХ": "TEXT",  # Добавляем поле ВидДоговораУХ
                },
            )

            if saved:
                verbose_print(f"\nСохранено договорыконтрагентов в БД: {len(processed)}")
                verbose_print(f"База данных: {output_db_path}")
                verbose_print(f"Таблица: {table_name}")

            connection.commit()
            return saved

        except Exception as error:
            print(f"Ошибка при сохранении договорыконтрагентов: {error}")
            import traceback
            traceback.print_exc()
            connection.rollback()
            return False
        finally:
            connection.close()


def process_contractor_contracts(
    source_db_path: str,
    processed_db_path: str,
) -> bool:
    """
    Обрабатывает договорыконтрагентов из исходной БД и сохраняет в новую БД.
    
    Args:
        source_db_path: Путь к исходной базе данных SQLite
        processed_db_path: Путь к выходной базе данных SQLite
        
    Returns:
        True если успешно, False если ошибка
    """
    mapping_db_path = "CONF/type_mapping.db"
    table_name = "contractor_contracts"
    
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ДОГОВОРЫКОНТРАГЕНТОВ С МАППИНГОМ")
    verbose_print("=" * 80)

    # Читаем данные из исходной БД
    verbose_print(f"\n[1/5] Чтение договорыконтрагентов из исходной БД: {source_db_path}")
    items = read_from_db(source_db_path, table_name)
    if not items:
        verbose_print("ДоговорыКонтрагентов не найдены в исходной БД")
        return False
    verbose_print(f"Прочитано договорыконтрагентов: {len(items)}")

    # Инициализируем процессор
    verbose_print("\n[2/5] Инициализация процессора маппинга...")
    processor = ContractorContractsMappingProcessor(mapping_db_path)

    # Обрабатываем данные
    verbose_print("\n[3/5] Обработка договорыконтрагентов с использованием маппинга...")
    success = processor.process_and_save_items(items, processed_db_path, table_name)

    if success:
        verbose_print("\n[4/5] Копирование табличных частей...")
        copy_tabular_sections(source_db_path, processed_db_path, table_name)

        verbose_print("\n[5/5] Обработка завершена успешно!")
        verbose_print(f"Результат сохранен в: {processed_db_path}")

    return success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Обработка договорыконтрагентов с маппингом")
    parser.add_argument("--source-db", required=True, help="Путь к исходной БД")
    parser.add_argument("--processed-db", required=True, help="Путь к обработанной БД")

    args = parser.parse_args()
    success = process_contractor_contracts(args.source_db, args.processed_db)
    sys.exit(0 if success else 1)
