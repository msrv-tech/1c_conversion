# -*- coding: utf-8 -*-
"""
Модуль обработки справочника «СпособыОтраженияРасходовПоАмортизации» с использованием маппинга типов и полей.

Читает способыотражениярасходовпоамортизации из исходной БД, применяет маппинг из type_mapping.db
и сохраняет результат в новую БД в формате приемника (UH).
"""

from __future__ import annotations

import json
import os
import sys
from typing import Dict, List, Any, Optional

# Добавляем путь к корню проекта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows

from tools.base_processor import MappingProcessor
from tools.processor_utils import read_from_db, copy_tabular_sections
from tools.chart_of_accounts_mapper import extract_account_code, get_mapped_account_code
from tools.logger import verbose_print  # noqa: E402

fix_encoding()


class AmortizationExpenseMethodsMappingProcessor(MappingProcessor):
    """Процессор для преобразования способыотражениярасходовпоамортизации с использованием маппинга."""

    def __init__(self, mapping_db_path: str = "CONF/type_mapping.db"):
        """
        Инициализация процессора.
        
        Args:
            mapping_db_path: Путь к базе данных с маппингом
        """
        super().__init__(mapping_db_path, "СпособыОтраженияРасходовПоАмортизации", "catalog")

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
        verbose_print(f"\nОбработка {len(items)} способыотражениярасходовпоамортизации...")
        processed = []

        for i, item in enumerate(items, 1):
            if i % 100 == 0:
                verbose_print(f"  Обработано: {i}/{len(items)}")

            processed_item = self.process_item_single(item)
            if processed_item:
                processed.append(processed_item)

        verbose_print(f"Обработано способыотражениярасходовпоамортизации: {len(processed)}")
        return processed

    def process_and_save_items(
        self, items: List[Dict], output_db_path: str, table_name: str = "amortization_expense_methods"
    ) -> bool:
        """
        Преобразует элементы и сохраняет их в новую базу данных.
        
        Args:
            items: Список словарей с данными элементов из источника
            output_db_path: Путь к выходной базе данных SQLite
            table_name: Имя таблицы для сохранения (по умолчанию "amortization_expense_methods")
            
        Returns:
            True если успешно, False если ошибка
        """
        # Обрабатываем элементы
        processed = self.process_items(items)
        
        if not processed:
            verbose_print("Нет обработанных способыотражениярасходовпоамортизации для сохранения")
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
                verbose_print(f"\nСохранено способыотражениярасходовпоамортизации в БД: {len(processed)}")
                verbose_print(f"База данных: {output_db_path}")
                verbose_print(f"Таблица: {table_name}")

            connection.commit()
            return saved

        except Exception as error:
            print(f"Ошибка при сохранении способыотражениярасходовпоамортизации: {error}")
            import traceback
            traceback.print_exc()
            connection.rollback()
            return False
        finally:
            connection.close()


def process_tabular_section_subconto(
    processed_db_path: str,
    source_db_path: str,
    chart_of_accounts_mapping: Dict[str, Optional[str]],
    chart_of_accounts_subconto: Dict[str, Dict[str, str]]
) -> bool:
    """
    Обрабатывает табличную часть amortization_expense_methods_details и заполняет Субконто1-3
    на основе СтатьяЗатрат и НоменклатурнаяГруппа из исходных данных, если они указаны в конфигурации субконто.
    
    Args:
        processed_db_path: Путь к обработанной базе данных
        source_db_path: Путь к исходной базе данных (для чтения исходных значений СтатьяЗатрат и НоменклатурнаяГруппа)
        chart_of_accounts_mapping: Маппинг плана счетов
        chart_of_accounts_subconto: Конфигурация субконто для счетов
        
    Returns:
        True если успешно, False если ошибка
    """
    table_name = "amortization_expense_methods_details"
    
    connection = connect_to_sqlite(processed_db_path)
    if not connection:
        verbose_print(f"Не удалось подключиться к базе данных: {processed_db_path}")
        return False
    
    try:
        cursor = connection.cursor()
        
        # Проверяем существование таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            verbose_print(f"Таблица {table_name} не найдена")
            return True
        
        # Получаем структуру таблицы
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        # Проверяем наличие необходимых колонок
        required_columns = ["СчетЗатрат", "СтатьяЗатрат", "НоменклатурнаяГруппа", "Субконто1", "Субконто2", "Субконто3"]
        missing_columns = [col for col in required_columns if col not in column_names]
        if missing_columns:
            verbose_print(f"Отсутствуют необходимые колонки: {', '.join(missing_columns)}")
            return False
        
        # Читаем все строки табличной части
        cursor.execute(f'SELECT * FROM "{table_name}"')
        rows = cursor.fetchall()
        
        if not rows:
            verbose_print(f"Таблица {table_name} пуста")
            return True
        
        verbose_print(f"Обработка {len(rows)} строк табличной части {table_name}...")
        
        updated_count = 0
        
        for row in rows:
            # Преобразуем строку в словарь
            row_dict = {col_name: row[i] for i, col_name in enumerate(column_names)}
            
            # Получаем СчетЗатрат
            account_field = row_dict.get("СчетЗатрат")
            if not account_field:
                continue
            
            # Парсим JSON счета, если он в формате JSON
            account_json = None
            if isinstance(account_field, str) and account_field.strip().startswith('{'):
                try:
                    account_json = json.loads(account_field)
                except (json.JSONDecodeError, ValueError):
                    pass
            
            # Извлекаем код счета из presentation
            if account_json:
                presentation = account_json.get("presentation", "").strip()
            else:
                # Пробуем получить из отдельных полей
                presentation_field = "СчетЗатрат_Представление"
                if presentation_field in column_names:
                    presentation_idx = column_names.index(presentation_field)
                    presentation = str(row[presentation_idx]) if row[presentation_idx] else ""
                else:
                    presentation = ""
            
            if not presentation:
                continue
            
            # Извлекаем код счета
            account_code = extract_account_code(presentation)
            if not account_code:
                continue
            
            # Применяем маппинг для получения кода приемника
            mapped_code = get_mapped_account_code(account_code, chart_of_accounts_mapping)
            if mapped_code:
                account_code = mapped_code
            
            # Проверяем конфигурацию субконто для этого счета
            subconto_config = chart_of_accounts_subconto.get(account_code)
            if not subconto_config:
                continue
            
            # Вспомогательная функция для получения данных ссылки
            def get_reference_data(field_name: str) -> Dict:
                """Получает данные ссылки из строки (JSON или отдельные поля)."""
                # Проверяем JSON формат
                field_value = row_dict.get(field_name)
                if field_value and isinstance(field_value, str) and field_value.strip().startswith('{'):
                    try:
                        return json.loads(field_value)
                    except (json.JSONDecodeError, ValueError):
                        pass
                
                # Пробуем собрать из отдельных полей
                uuid_field = f"{field_name}_UUID"
                presentation_field = f"{field_name}_Представление"
                type_field = f"{field_name}_Тип"
                
                ref_uuid = row_dict.get(uuid_field, "")
                ref_presentation = row_dict.get(presentation_field, "")
                ref_type = row_dict.get(type_field, "")
                
                if ref_uuid or ref_presentation or ref_type:
                    return {
                        "uuid": ref_uuid or "",
                        "presentation": ref_presentation or "",
                        "type": ref_type or ""
                    }
                
                return {}
            
            # Заполняем субконто согласно конфигурации
            updates = {}
            for subconto_key, source_field_name in subconto_config.items():
                # source_field_name может быть "СтатьяЗатрат" или "НоменклатурнаяГруппа"
                ref_data = get_reference_data(source_field_name)
                if ref_data and (ref_data.get("uuid") or ref_data.get("presentation")):
                    # subconto_key может быть "subconto1", "subconto2", "subconto3"
                    # Преобразуем в имя поля: "Субконто1", "Субконто2", "Субконто3"
                    if subconto_key.startswith("subconto"):
                        subconto_num = subconto_key.replace("subconto", "")
                        target_field = f"Субконто{subconto_num}"
                        # Сохраняем субконто как JSON строку
                        updates[target_field] = json.dumps(ref_data, ensure_ascii=False)
            
            # Обновляем строку в БД, если есть изменения
            if updates:
                # Формируем UPDATE запрос
                set_clauses = []
                values = []
                for field_name, field_value in updates.items():
                    set_clauses.append(f'"{field_name}" = ?')
                    values.append(field_value)
                
                # Добавляем WHERE условие
                values.append(row_dict["parent_uuid"])
                values.append(row_dict["НомерСтроки"])
                
                update_sql = f'''
                    UPDATE "{table_name}"
                    SET {", ".join(set_clauses)}
                    WHERE "parent_uuid" = ? AND "НомерСтроки" = ?
                '''
                
                cursor.execute(update_sql, values)
                updated_count += 1
        
        connection.commit()
        verbose_print(f"Обновлено строк с субконто: {updated_count}")
        
        return True
        
    except Exception as error:
        print(f"Ошибка при обработке субконто табличной части: {error}")
        import traceback
        traceback.print_exc()
        connection.rollback()
        return False
    finally:
        connection.close()


def process_amortization_expense_methods(
    source_db_path: str,
    processed_db_path: str,
) -> bool:
    """
    Обрабатывает способыотражениярасходовпоамортизации из исходной БД и сохраняет в новую БД.
    
    Args:
        source_db_path: Путь к исходной базе данных SQLite
        processed_db_path: Путь к выходной базе данных SQLite
        
    Returns:
        True если успешно, False если ошибка
    """
    mapping_db_path = "CONF/type_mapping.db"
    table_name = "amortization_expense_methods"
    
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА СПОСОБЫОТРАЖЕНИЯРАСХОДОВПОАМОРТИЗАЦИИ С МАППИНГОМ")
    verbose_print("=" * 80)

    # Читаем данные из исходной БД
    verbose_print(f"\n[1/5] Чтение способыотражениярасходовпоамортизации из исходной БД: {source_db_path}")
    items = read_from_db(source_db_path, table_name)
    if not items:
        verbose_print("СпособыОтраженияРасходовПоАмортизации не найдены в исходной БД")
        return False
    verbose_print(f"Прочитано способыотражениярасходовпоамортизации: {len(items)}")

    # Инициализируем процессор
    verbose_print("\n[2/5] Инициализация процессора маппинга...")
    processor = AmortizationExpenseMethodsMappingProcessor(mapping_db_path)

    # Обрабатываем данные
    verbose_print("\n[3/5] Обработка способыотражениярасходовпоамортизации с использованием маппинга...")
    success = processor.process_and_save_items(items, processed_db_path, table_name)

    if success:
        verbose_print("\n[4/6] Копирование табличных частей с применением маппинга плана счетов...")
        # Передаем маппинг плана счетов для обработки JSON полей в табличных частях
        chart_of_accounts_mapping = processor.chart_of_accounts_mapping if hasattr(processor, 'chart_of_accounts_mapping') else None
        copy_tabular_sections(source_db_path, processed_db_path, table_name, chart_of_accounts_mapping)

        verbose_print("\n[5/6] Обработка субконто в табличной части...")
        # Обрабатываем субконто в табличной части
        chart_of_accounts_subconto = processor.chart_of_accounts_subconto if hasattr(processor, 'chart_of_accounts_subconto') else {}
        if chart_of_accounts_mapping and chart_of_accounts_subconto:
            process_tabular_section_subconto(
                processed_db_path,
                source_db_path,
                chart_of_accounts_mapping,
                chart_of_accounts_subconto
            )

        verbose_print("\n[6/6] Обработка завершена успешно!")
        verbose_print(f"Результат сохранен в: {processed_db_path}")

    return success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Обработка способыотражениярасходовпоамортизации с маппингом")
    parser.add_argument("--source-db", required=True, help="Путь к исходной БД")
    parser.add_argument("--processed-db", required=True, help="Путь к обработанной БД")

    args = parser.parse_args()
    success = process_amortization_expense_methods(args.source_db, args.processed_db)
    sys.exit(0 if success else 1)
