# -*- coding: utf-8 -*-
"""
Утилиты для процессоров обработки данных.

Содержит общие функции для чтения данных из БД и копирования табличных частей.
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Dict, List

from tools.db_manager import connect_to_sqlite


def read_from_db(source_db_path: str, table_name: str) -> List[Dict]:
    """
    Читает данные из базы данных SQLite.
    
    Args:
        source_db_path: Путь к исходной базе данных
        table_name: Имя таблицы
        
    Returns:
        Список словарей с данными
    """
    if not os.path.exists(source_db_path):
        print(f"Ошибка: база данных не найдена: {source_db_path}")
        return []

    connection = connect_to_sqlite(source_db_path)
    if not connection:
        print(f"Ошибка: не удалось подключиться к базе данных: {source_db_path}")
        return []

    try:
        cursor = connection.cursor()
        
        # Получаем все колонки таблицы
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        if not columns_info:
            print(f"Ошибка: таблица '{table_name}' не найдена в базе данных")
            return []
        
        column_names = [col[1] for col in columns_info]
        
        # Читаем все данные
        cursor.execute(f'SELECT * FROM "{table_name}"')
        rows = cursor.fetchall()
        
        items = []
        for row in rows:
            item = {}
            # Сначала собираем все значения, чтобы определить, какие поля уже в JSON
            row_data = {col_name: row[i] for i, col_name in enumerate(column_names)}
            
            for i, col_name in enumerate(column_names):
                value = row[i]
                
                # Если это служебное поле (_Представление, _UUID, _Тип), проверяем,
                # есть ли основное поле с JSON данными
                if col_name.endswith("_Представление") or col_name.endswith("_UUID") or col_name.endswith("_Тип"):
                    base_field = col_name.rsplit("_", 1)[0]
                    # Если основное поле уже содержит JSON, пропускаем служебное поле
                    if base_field in row_data:
                        base_value = row_data[base_field]
                        if isinstance(base_value, str) and base_value.strip().startswith('{') and '"presentation"' in base_value:
                            # Данные уже в JSON - пропускаем служебное поле
                            continue
                    # Если основное поле не содержит JSON, но есть служебные поля,
                    # они будут обработаны позже (не пропускаем их здесь)
                    # Но если основное поле уже есть в item, значит оно уже обработано
                    if base_field in item:
                        continue
                
                # Парсим JSON для ссылочных полей (если данные уже в JSON формате)
                if isinstance(value, str) and value.startswith('{') and '"presentation"' in value:
                    try:
                        ref_data = json.loads(value)
                        # Сохраняем JSON строку в основном поле для дальнейшей обработки
                        item[col_name] = value
                        # НЕ создаем дополнительные поля _Представление, _UUID, _Тип,
                        # так как все данные уже в JSON
                        # Для поля "Родитель" извлекаем is_group и сохраняем как Родитель_ЭтоГруппа
                        if col_name == "Родитель" and "is_group" in ref_data:
                            item[f"{col_name}_ЭтоГруппа"] = ref_data.get("is_group", False)
                    except json.JSONDecodeError:
                        item[col_name] = value
                else:
                    # Если это не JSON и не служебное поле, сохраняем как есть
                    # Но только если это не служебное поле, которое мы уже пропустили
                    if not (col_name.endswith("_Представление") or col_name.endswith("_UUID") or col_name.endswith("_Тип")):
                        item[col_name] = value
                    elif col_name not in item:
                        # Это служебное поле, но основное поле не содержит JSON
                        # Сохраняем его для дальнейшей обработки
                        item[col_name] = value
            
            # После обработки всех колонок, проверяем наличие отдельной колонки Родитель_ЭтоГруппа
            # и используем её, если она есть (приоритет над значением из JSON)
            if "Родитель_ЭтоГруппа" in column_names:
                parent_is_group_idx = column_names.index("Родитель_ЭтоГруппа")
                parent_is_group_value = row[parent_is_group_idx]
                if parent_is_group_value is not None:
                    item["Родитель_ЭтоГруппа"] = parent_is_group_value
            
            items.append(item)
        
        return items
        
    except sqlite3.Error as error:
        print(f"Ошибка при чтении из базы данных: {error}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        connection.close()


def copy_tabular_sections(source_db_path: str, processed_db_path: str, main_table_name: str, chart_of_accounts_mapping: Optional[Dict[str, Optional[str]]] = None) -> bool:
    """
    Копирует табличные части из исходной БД в обработанную БД с применением маппинга плана счетов.
    
    Args:
        source_db_path: Путь к исходной базе данных
        processed_db_path: Путь к обработанной базе данных
        main_table_name: Имя основной таблицы (для исключения из списка табличных частей)
        chart_of_accounts_mapping: Маппинг плана счетов (опционально)
        
    Returns:
        True если успешно, False если ошибка
    """
    source_conn = connect_to_sqlite(source_db_path)
    if not source_conn:
        print(f"Ошибка: не удалось подключиться к исходной БД: {source_db_path}")
        return False
    
    processed_conn = connect_to_sqlite(processed_db_path)
    if not processed_conn:
        print(f"Ошибка: не удалось подключиться к обработанной БД: {processed_db_path}")
        source_conn.close()
        return False
    
    try:
        source_cursor = source_conn.cursor()
        processed_cursor = processed_conn.cursor()
        
        # Получаем список всех таблиц в исходной БД
        source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        all_tables = [row[0] for row in source_cursor.fetchall()]
        
        # Исключаем основную таблицу
        tabular_tables = [t for t in all_tables if t != main_table_name]
        
        from tools.logger import verbose_print
        if not tabular_tables:
            verbose_print("  Табличные части не найдены")
            source_conn.close()
            processed_conn.close()
            return True

        verbose_print(f"  Найдено табличных частей: {len(tabular_tables)}")
        
        for table_name in tabular_tables:
            try:
                # Сначала получаем список колонок из исходной таблицы
                source_cursor.execute(f'PRAGMA table_info("{table_name}")')
                columns_info = source_cursor.fetchall()
                if not columns_info:
                    continue
                
                column_names = [col[1] for col in columns_info]
            
                # Получаем SQL создания таблицы из исходной БД (сохраняет PRIMARY KEY и другие ограничения)
                source_cursor.execute("""
                    SELECT sql FROM sqlite_master 
                    WHERE type='table' AND name=?
                """, (table_name,))
                create_sql_row = source_cursor.fetchone()
                
                if create_sql_row and create_sql_row[0]:
                    # Используем оригинальный SQL для создания таблицы (сохраняет PRIMARY KEY)
                    create_table_sql = create_sql_row[0]
                    # Заменяем имя таблицы на обработанную БД (на случай, если в SQL есть имя)
                    create_table_sql = create_table_sql.replace(f'CREATE TABLE "{table_name}"', f'CREATE TABLE IF NOT EXISTS "{table_name}"')
                    create_table_sql = create_table_sql.replace(f'CREATE TABLE {table_name}', f'CREATE TABLE IF NOT EXISTS "{table_name}"')
                    
                    # Если в SQL нет PRIMARY KEY, добавляем его для табличных частей
                    # (используем parent_uuid + НомерСтроки как составной ключ)
                    if 'PRIMARY KEY' not in create_table_sql.upper() and 'parent_uuid' in column_names and 'НомерСтроки' in column_names:
                        # Добавляем PRIMARY KEY перед закрывающей скобкой
                        if create_table_sql.strip().endswith(')'):
                            create_table_sql = create_table_sql.rstrip(')').rstrip() + ',\n    PRIMARY KEY ("parent_uuid", "НомерСтроки")\n)'
                    
                    processed_cursor.execute(create_table_sql)
                else:
                    # Fallback: если не получилось получить SQL, создаем таблицу вручную
                    # Определяем типы колонок и PRIMARY KEY
                    column_defs = []
                    primary_key_cols = []
                    for col_info in columns_info:
                        col_name = col_info[1]
                        col_type = col_info[2] or "TEXT"
                        is_pk = col_info[5]  # pk flag
                        column_defs.append(f'"{col_name}" {col_type}')
                        if is_pk:
                            primary_key_cols.append(f'"{col_name}"')
                    
                    # Добавляем PRIMARY KEY если есть
                    if primary_key_cols:
                        column_defs.append(f'PRIMARY KEY ({", ".join(primary_key_cols)})')
                    elif 'parent_uuid' in column_names and 'НомерСтроки' in column_names:
                        # Для табличных частей добавляем составной PRIMARY KEY, если его нет
                        column_defs.append(f'PRIMARY KEY ("parent_uuid", "НомерСтроки")')
                    
                    create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(column_defs)})'
                    processed_cursor.execute(create_table_sql)
            
                # Проверяем структуру целевой таблицы в обработанной БД
                processed_cursor.execute(f'PRAGMA table_info("{table_name}")')
                processed_columns_info = processed_cursor.fetchall()
                processed_column_names = {col[1]: col[2] for col in processed_columns_info} if processed_columns_info else {}
                
                # Добавляем недостающие колонки в обработанную БД
                missing_cols = []
                for col_info in columns_info:
                    col_name = col_info[1]
                    col_type = col_info[2] or "TEXT"
                    if col_name not in processed_column_names:
                        try:
                            processed_cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_type}')
                            missing_cols.append(col_name)
                            from tools.logger import verbose_print
                            verbose_print(f"  {table_name}: добавлена колонка '{col_name}' ({col_type})")
                        except Exception as e:
                            from tools.logger import verbose_print
                            verbose_print(f"  {table_name}: ошибка добавления колонки '{col_name}': {e}")
                
                # Обновляем список колонок после добавления
                if missing_cols:
                    processed_cursor.execute(f'PRAGMA table_info("{table_name}")')
                    processed_columns_info = processed_cursor.fetchall()
                    processed_column_names = {col[1]: col[2] for col in processed_columns_info} if processed_columns_info else {}
                
                # Определяем колонки, которые есть в обеих таблицах
                common_columns = [col for col in column_names if col in processed_column_names]
                
                if not common_columns:
                    from tools.logger import verbose_print
                    verbose_print(f"  {table_name}: пропущена (нет общих колонок)")
                    continue
                
                # Очищаем таблицу перед копированием (перезаписываем, а не добавляем)
                processed_cursor.execute(f'DELETE FROM "{table_name}"')
                
                # Читаем данные из исходной таблицы только для общих колонок
                quoted_columns = ", ".join([f'"{col}"' for col in common_columns])
                source_cursor.execute(f'SELECT {quoted_columns} FROM "{table_name}"')
                rows = source_cursor.fetchall()
                
                # Применяем маппинг плана счетов к JSON полям, если маппинг указан
                if chart_of_accounts_mapping and rows:
                    from tools.chart_of_accounts_mapper import extract_account_code, get_mapped_account_code
                    
                    processed_rows = []
                    for row in rows:
                        # Преобразуем кортеж в список для изменения
                        row_list = list(row)
                        
                        # Обрабатываем каждую колонку
                        for col_idx, col_name in enumerate(common_columns):
                            value = row_list[col_idx]
                            
                            # Проверяем, является ли значение JSON строкой с планом счетов
                            if isinstance(value, str) and value.strip().startswith('{'):
                                try:
                                    json_data = json.loads(value)
                                    # Проверяем, есть ли в JSON поле type с планом счетов
                                    json_type = json_data.get('type', '')
                                    if json_type and (json_type.startswith('ПланСчетов.') or json_type.startswith('ChartOfAccountsRef.')):
                                        # Извлекаем код счета из presentation
                                        presentation = json_data.get('presentation', '')
                                        if presentation:
                                            source_code = extract_account_code(presentation)
                                            if source_code:
                                                # Применяем маппинг
                                                mapped_code = get_mapped_account_code(source_code, chart_of_accounts_mapping)
                                                if mapped_code and mapped_code != source_code:
                                                    # Обновляем presentation в JSON
                                                    new_presentation = presentation.replace(source_code, mapped_code, 1)
                                                    json_data['presentation'] = new_presentation
                                                    # Обновляем значение в строке
                                                    row_list[col_idx] = json.dumps(json_data, ensure_ascii=False)
                                except (json.JSONDecodeError, ValueError):
                                    pass
                        
                        processed_rows.append(tuple(row_list))
                    
                    rows = processed_rows
                
                # Вставляем данные (если есть)
                if rows:
                    # Удаляем дубликаты по parent_uuid + НомерСтроки, оставляя первую запись
                    # Это нужно, если в исходной БД есть дубликаты
                    if 'parent_uuid' in common_columns and 'НомерСтроки' in common_columns:
                        seen_keys = set()
                        unique_rows = []
                        parent_uuid_idx = common_columns.index('parent_uuid')
                        номер_строки_idx = common_columns.index('НомерСтроки')
                        
                        for row in rows:
                            key = (row[parent_uuid_idx], row[номер_строки_idx])
                            if key not in seen_keys:
                                seen_keys.add(key)
                                unique_rows.append(row)
                        
                        if len(unique_rows) < len(rows):
                            print(f"  {table_name}: удалено {len(rows) - len(unique_rows)} дубликатов (осталось {len(unique_rows)} уникальных записей)")
                        rows = unique_rows
                    
                    placeholders = ", ".join(["?"] * len(common_columns))
                    quoted_columns = ", ".join([f'"{col}"' for col in common_columns])
                    insert_sql = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders})'
                    
                    processed_cursor.executemany(insert_sql, rows)
                    processed_conn.commit()
                    from tools.logger import verbose_print
                    verbose_print(f"  {table_name}: скопировано {len(rows)} записей")
                else:
                    processed_conn.commit()
                    verbose_print(f"  {table_name}: создана (0 записей)")
            except Exception as table_error:
                # Ошибка при обработке конкретной таблицы не должна останавливать весь процесс
                print(f"  {table_name}: ошибка при копировании - {table_error}")
                import traceback
                traceback.print_exc()
                try:
                    processed_conn.rollback()
                except:
                    pass
                continue
        
        return True
        
    except Exception as error:
        print(f"Ошибка при копировании табличных частей: {error}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        source_conn.close()
        processed_conn.close()

