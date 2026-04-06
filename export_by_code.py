# -*- coding: utf-8 -*-
"""
Скрипт для экспорта выборочных элементов справочника в приемник с отбором по полю "Код".

ВАЖНО: Исходная SQLite БД НЕ изменяется - скрипт только читает из неё данные.
Все изменения выполняются только во временной БД, которая автоматически удаляется после экспорта.

Использование:
    python export_by_code.py \
        --catalog contractors \
        --sqlite-db BD/contractors_processed.db \
        --target-1c target \
        --codes "001" "002" "003"
    
    или из файла:
    python export_by_code.py \
        --catalog contractors \
        --sqlite-db BD/contractors_processed.db \
        --target-1c target \
        --codes-file codes.txt
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import tempfile
from typing import List, Optional

# Добавляем путь к корню проекта
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite
from tools.logger import verbose_print, set_verbose
from export_stage import load_from_db_to_1c
from tools.reference_objects import set_prod_mode

# Загружаем переменные окружения из .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

fix_encoding()

# В обработанной БД для этих каталогов данные лежат в таблице с суффиксом _processed
# (writer читает из неё; без маппинга фильтр искал бы несуществующую таблицу catalog_name)
# Для characteristics: процессор сохраняет в таблицу nomenclature (характеристики→номенклатура)
EXPORT_TABLE_OVERRIDE = {
    "customer_orders": "customer_orders_processed",
    "supplier_orders": "supplier_orders_processed",
    "nomenclature_characteristics": "nomenclature",
    "nomenclature_series": "nomenclature",
}

# Для заказов «код» при экспорте — это Номер документа заказа, не поле Код
EXPORT_CODE_COLUMN_OVERRIDE = {
    "customer_orders": "Номер",
    "supplier_orders": "Номер",
}

# Для таблиц, где uuid хранится внутри JSON-колонки (например, Номенклатура_ДляЗаписи)
EXPORT_UUID_JSON_COLUMN = {
    "parties_mc_balances": "Номенклатура_ДляЗаписи",
}


def get_codes_from_file(file_path: str) -> List[str]:
    """
    Читает коды из текстового файла (по одному коду на строку).
    
    Args:
        file_path: Путь к файлу с кодами
        
    Returns:
        Список кодов
    """
    codes = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                code = line.strip()
                if code and not code.startswith('#'):  # Пропускаем пустые строки и комментарии
                    codes.append(code)
    except Exception as e:
        print(f"Ошибка при чтении файла с кодами '{file_path}': {e}")
        sys.exit(1)
    
    return codes


def filter_items(
    source_db_path: str,
    table_name: str,
    codes: List[str] = None,
    uuids: List[str] = None,
    temp_db_path: str = "",
    code_column: str = "Код",
    uuid_json_column: Optional[str] = None,
) -> bool:
    """
    Фильтрует элементы из исходной БД по кодам или UUID и сохраняет во временную БД.
    
    Args:
        source_db_path: Путь к исходной БД (только чтение)
        table_name: Имя таблицы
        codes: Список кодов для фильтрации
        uuids: Список UUID для фильтрации
        temp_db_path: Путь к временной БД для сохранения отфильтрованных данных
        code_column: Имя колонки для отбора по кодам (по умолчанию "Код", для заказов — "Номер")
    """
    verbose_print(f"\n[Фильтрация данных]")
    verbose_print(f"Исходная БД: {source_db_path}")
    verbose_print(f"Таблица: {table_name}")
    
    if codes:
        verbose_print(f"Коды для фильтрации: {len(codes)} шт.")
    if uuids:
        verbose_print(f"UUID для фильтрации: {len(uuids)} шт.")
    
    if not codes and not uuids:
        print("Ошибка: Не указаны ни коды, ни UUID для фильтрации")
        return False
    
    # Подключаемся к исходной БД (только для чтения - БД не изменяется)
    source_conn = connect_to_sqlite(source_db_path)
    if not source_conn:
        print(f"Ошибка: Не удалось подключиться к исходной БД: {source_db_path}")
        return False
    
    try:
        cursor = source_conn.cursor()
        
        # Проверяем существование таблицы
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            print(f"Ошибка: Таблица '{table_name}' не найдена в БД")
            return False
        
        # Получаем структуру таблицы
        cursor.execute(f"PRAGMA table_info(\"{table_name}\")")
        columns_info = cursor.fetchall()
        if not columns_info:
            print(f"Ошибка: Не удалось получить структуру таблицы '{table_name}'")
            return False
        
        column_names = [col[1] for col in columns_info]
        
        # Формируем SQL запрос
        conditions = []
        params = []
        
        if codes:
            # Проверяем наличие поля для отбора по кодам (Код или Номер для заказов)
            if code_column not in column_names:
                print(f"Ошибка: Поле '{code_column}' не найдено в таблице '{table_name}'")
                return False
            placeholders = ','.join(['?' for _ in codes])
            conditions.append(f'TRIM("{code_column}") IN ({placeholders})')
            params.extend([code.strip() for code in codes])
            
        if uuids:
            if uuid_json_column:
                # UUID внутри JSON-колонки (например, Номенклатура_ДляЗаписи)
                if uuid_json_column not in column_names:
                    print(f"Ошибка: Колонка '{uuid_json_column}' не найдена в таблице '{table_name}'")
                    return False
                placeholders = ','.join(['?' for _ in uuids])
                conditions.append(f"LOWER(TRIM(json_extract(\"{uuid_json_column}\", '$.uuid'))) IN ({placeholders})")
                params.extend([u.strip().lower() for u in uuids])
            else:
                # Стандартная колонка uuid
                if "uuid" not in column_names:
                    print(f"Ошибка: Поле 'uuid' не найдено в таблице '{table_name}'")
                    return False
                placeholders = ','.join(['?' for _ in uuids])
                conditions.append(f'"uuid" IN ({placeholders})')
                params.extend([u.strip() for u in uuids])
            
        # Объединяем условия через OR
        where_clause = " OR ".join(conditions)
        query = f'SELECT * FROM "{table_name}" WHERE {where_clause}'
        
        verbose_print(f"\n[Фильтрация] Выполняем запрос с отбором...")
        
        import time
        start_time = time.time()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        query_time = time.time() - start_time
        
        verbose_print(f"[Фильтрация] Найдено записей: {len(rows)} (запрос выполнен за {query_time:.2f} сек)")
        
        if not rows:
            print("Предупреждение: Не найдено записей с указанными кодами/UUID")
            return False
        
        # Создаем временную БД и копируем структуру таблицы
        temp_conn = connect_to_sqlite(temp_db_path)
        if not temp_conn:
            print(f"Ошибка: Не удалось создать временную БД: {temp_db_path}")
            return False
        
        try:
            temp_cursor = temp_conn.cursor()
            
            # Создаем таблицу с такой же структурой
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            create_table_sql = cursor.fetchone()[0]
            temp_cursor.execute(create_table_sql)
            
            # Копируем индексы (если есть)
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=?", (table_name,))
            indexes = cursor.fetchall()
            for index_row in indexes:
                if index_row[0]:  # Пропускаем автоматические индексы
                    try:
                        temp_cursor.execute(index_row[0])
                    except:
                        pass  # Игнорируем ошибки при создании индексов
            
            # Вставляем отфильтрованные данные
            placeholders = ','.join(['?' for _ in column_names])
            quoted_columns = ",".join([f'"{col}"' for col in column_names])
            insert_query = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders})'
            
            temp_cursor.executemany(insert_query, rows)
            
            # Копируем табличные части
            uuid_idx = column_names.index('uuid') if 'uuid' in column_names else None
            filtered_uuids = set()
            if uuid_idx is not None:
                for row in rows:
                    if uuid_idx < len(row) and row[uuid_idx]:
                        filtered_uuids.add(row[uuid_idx])
            
            if filtered_uuids:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != ?", (table_name,))
                all_tables = [row[0] for row in cursor.fetchall()]
                
                tabular_tables = []
                for other_table in all_tables:
                    cursor.execute(f"PRAGMA table_info(\"{other_table}\")")
                    other_columns = [col[1] for col in cursor.fetchall()]
                    if 'parent_uuid' in other_columns:
                        tabular_tables.append(other_table)
                
                for tabular_table_name in tabular_tables:
                    verbose_print(f"  Копирование табличной части: {tabular_table_name}")
                    cursor.execute(f"PRAGMA table_info(\"{tabular_table_name}\")")
                    tabular_columns_info = cursor.fetchall()
                    tabular_column_names = [col[1] for col in tabular_columns_info]
                    
                    placeholders = ','.join(['?' for _ in filtered_uuids])
                    tabular_query = f'SELECT * FROM "{tabular_table_name}" WHERE "parent_uuid" IN ({placeholders})'
                    cursor.execute(tabular_query, list(filtered_uuids))
                    tabular_rows = cursor.fetchall()
                    
                    if tabular_rows:
                        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (tabular_table_name,))
                        create_tabular_sql_row = cursor.fetchone()
                        if create_tabular_sql_row and create_tabular_sql_row[0]:
                            create_tabular_sql = create_tabular_sql_row[0]
                            create_tabular_sql = create_tabular_sql.replace(
                                f'CREATE TABLE "{tabular_table_name}"',
                                f'CREATE TABLE IF NOT EXISTS "{tabular_table_name}"'
                            )
                            create_tabular_sql = create_tabular_sql.replace(
                                f'CREATE TABLE {tabular_table_name}',
                                f'CREATE TABLE IF NOT EXISTS "{tabular_table_name}"'
                            )
                            temp_cursor.execute(create_tabular_sql)
                            
                            tabular_placeholders = ','.join(['?' for _ in tabular_column_names])
                            quoted_columns = ",".join([f'"{col}"' for col in tabular_column_names])
                            insert_tabular_query = f'INSERT INTO "{tabular_table_name}" ({quoted_columns}) VALUES ({tabular_placeholders})'
                            temp_cursor.executemany(insert_tabular_query, tabular_rows)
                            verbose_print(f"    Скопировано строк: {len(tabular_rows)}")
            
            temp_conn.commit()
            verbose_print(f"\n✓ Данные сохранены во временную БД: {temp_db_path}")
            return True
        finally:
            temp_conn.close()
    finally:
        source_conn.close()


def export_catalog_by_codes(
    catalog_name: str,
    sqlite_db_path: str,
    target_db_path: str,
    codes: List[str] = None,
    uuids: List[str] = None,
    mode: str = "full"
) -> bool:
    """
    Экспортирует элементы справочника в приемник с фильтрацией по кодам или UUID.
    
    Args:
        catalog_name: Имя справочника (например, "contractors")
        sqlite_db_path: Путь к исходной БД SQLite
        target_db_path: Путь к приемнику 1С
        codes: Список кодов для фильтрации
        uuids: Список UUID для фильтрации
        mode: Режим экспорта ("test" или "full")
        
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print(f"ЭКСПОРТ СПРАВОЧНИКА '{catalog_name}' С ФИЛЬТРАЦИЕЙ")
    verbose_print("=" * 80)
    
    if not codes and not uuids:
        print("Ошибка: Не указаны ни коды, ни UUID для фильтрации")
        return False
    
    # Определяем имя таблицы: для заказов в обработанной БД — таблица _processed
    table_name = EXPORT_TABLE_OVERRIDE.get(catalog_name, catalog_name)
    if table_name != catalog_name:
        verbose_print(f"  [ИНФО] Каталог '{catalog_name}': фильтрация по таблице '{table_name}'")
    
    # Для заказов отбор по «кодам» — по полю Номер документа
    code_column = EXPORT_CODE_COLUMN_OVERRIDE.get(catalog_name, "Код")
    if code_column != "Код":
        verbose_print(f"  [ИНФО] Отбор по кодам: колонка '{code_column}' (номер заказа)")
    
    # Строгая проверка: должна использоваться только обработанная БД
    if not sqlite_db_path.endswith('_processed.db') and not os.path.basename(sqlite_db_path).startswith(catalog_name + '_processed'):
        print(f"\n[ОШИБКА] Должна использоваться только обработанная БД (с суффиксом _processed.db)")
        print(f"[ОШИБКА] Указанная БД: {sqlite_db_path}")
        return False
    
    # Проверяем существование обработанной БД
    if not os.path.exists(sqlite_db_path):
        print(f"\n[ОШИБКА] Обработанная база данных не найдена: {sqlite_db_path}")
        return False
    
    # Создаем временную БД с отфильтрованными данными
    temp_db_path = None
    try:
        import time
        total_start_time = time.time()
        
        # Создаем временный файл для БД
        temp_fd, temp_db_path = tempfile.mkstemp(suffix='.db', prefix=f'{catalog_name}_filtered_')
        os.close(temp_fd)
        
        uuid_json_col = EXPORT_UUID_JSON_COLUMN.get(catalog_name)
        if uuid_json_col:
            verbose_print(f"  [ИНФО] UUID в JSON-колонке: {uuid_json_col}")

        verbose_print(f"\n[1/3] Фильтрация данных...")
        filter_start = time.time()
        if not filter_items(
            sqlite_db_path, table_name, codes, uuids, temp_db_path,
            code_column=code_column, uuid_json_column=uuid_json_col
        ):
            verbose_print(f"[1/3] Фильтрация завершена с ошибкой")
            return False
        filter_time = time.time() - filter_start
        verbose_print(f"[1/3] Фильтрация завершена за {filter_time:.2f} сек")
        
        verbose_print(f"\n[2/3] Экспорт в приемник 1С...")
        export_start = time.time()
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Для договоров отключаем фильтр по дате (т.к. мы экспортируем конкретные UUID)
        if catalog_name in ("contractor_contracts", "custom_contracts_external_to_contractor_contracts"):
            from tools.writer_utils import set_ignore_date_filter
            set_ignore_date_filter(True)
            verbose_print("  [ИНФО] Отключен фильтр по дате для экспорта договоров по UUID")
            
        try:
            success = load_from_db_to_1c(
                base_dir=base_dir,
                catalog_name=catalog_name,
                sqlite_db_file=temp_db_path,
                target_db_path=target_db_path,
                process_func=None,
                mode=mode
            )
        except Exception as export_error:
            print(f"\n[ОШИБКА] Ошибка при экспорте в 1С: {export_error}")
            import traceback
            traceback.print_exc()
            success = False
        finally:
            export_time = time.time() - export_start
            verbose_print(f"[2/3] Экспорт завершен за {export_time:.2f} сек")
        
        total_time = time.time() - total_start_time
        
        if success:
            verbose_print(f"\n[3/3] Экспорт завершен успешно!")
        else:
            verbose_print(f"\n[3/3] Ошибка при экспорте")
        
        return success
        
    except Exception as e:
        print(f"\n[КРИТИЧЕСКАЯ ОШИБКА] {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Удаляем временную БД
        if temp_db_path and os.path.exists(temp_db_path):
            try:
                os.remove(temp_db_path)
                verbose_print(f"\nВременная БД удалена: {temp_db_path}")
            except Exception as e:
                print(f"Предупреждение: Не удалось удалить временную БД: {e}")
        
        sys.stdout.flush()
        sys.stderr.flush()


def main():
    """Основная функция для запуска скрипта из командной строки."""
    parser = argparse.ArgumentParser(
        description="Экспорт выборочных элементов справочника в приемник с отбором по коду",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

1. Экспорт с указанием кодов в командной строке (БД определяется автоматически):
   python export_by_code.py --catalog contractors --target-1c target --codes "001" "002" "003"

2. Экспорт с кодами из файла:
   python export_by_code.py --catalog contractors --target-1c target --codes-file codes.txt

3. Тестовый режим (первые 50 записей):
   python export_by_code.py --catalog contractors --target-1c target --codes "001" "002" --mode test

4. С явным указанием пути к БД (если нужно):
   python export_by_code.py --catalog contractors --sqlite-db BD/contractors_processed.db --target-1c target --codes "001"
        """
    )
    
    parser.add_argument(
        "--catalog",
        required=True,
        help="Имя справочника (например, contractors, employees)"
    )
    
    parser.add_argument(
        "--sqlite-db",
        required=False,
        help="Путь к обработанной БД SQLite (если не указан, формируется автоматически как BD/{catalog}_processed.db)"
    )
    
    parser.add_argument(
        "--target-1c",
        required=False,
        help="Путь к приемнику 1С (как в main.py)"
    )
    
    parser.add_argument(
        "--codes",
        nargs="+",
        help="Список кодов для фильтрации (можно указать несколько)"
    )
    
    parser.add_argument(
        "--codes-file",
        help="Путь к файлу с кодами (по одному коду на строку)"
    )

    parser.add_argument(
        "--uuids",
        nargs="+",
        help="Список UUID для фильтрации (можно указать несколько)"
    )
    
    parser.add_argument(
        "--uuids-file",
        help="Путь к файлу с UUID (по одному на строку)"
    )
    
    parser.add_argument(
        "--mode",
        default="full",
        choices=["test", "full"],
        help="Режим экспорта: test (первые 50 записей) или full (все записи)"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Включить подробный вывод (verbose mode)"
    )
    
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Использовать продакшн базу приемника (TARGET_CONNECTION_STRING_PROD)"
    )
    
    args = parser.parse_args()
    
    # Если указан --prod, используем продакшн базу приемника
    if args.prod:
        prod_connection_string = os.getenv("TARGET_CONNECTION_STRING_PROD")
        if not prod_connection_string:
            print("Ошибка: при использовании --prod необходимо указать TARGET_CONNECTION_STRING_PROD в .env файле")
            sys.exit(1)
        args.target_1c = prod_connection_string
        set_prod_mode(True)
    
    # Включаем verbose режим, если указан флаг
    if args.verbose:
        set_verbose(True)
    
    # Получаем коды
    codes = []
    if args.codes:
        codes.extend(args.codes)
    
    if args.codes_file:
        file_codes = get_codes_from_file(args.codes_file)
        codes.extend(file_codes)
    
    # Убираем дубликаты, сохраняя порядок
    if codes:
        seen = set()
        unique_codes = []
        for code in codes:
            if code not in seen:
                seen.add(code)
                unique_codes.append(code)
        codes = unique_codes
    
    # Получаем UUID
    uuids = []
    if args.uuids:
        uuids.extend(args.uuids)
    
    if args.uuids_file:
        file_uuids = get_codes_from_file(args.uuids_file) # Используем ту же функцию чтения
        uuids.extend(file_uuids)
        
    if uuids:
        seen = set()
        unique_uuids = []
        for u in uuids:
            if u not in seen:
                seen.add(u)
                unique_uuids.append(u)
        uuids = unique_uuids

    if not codes and not uuids:
        print("Ошибка: Не указаны ни коды, ни UUID для фильтрации. Используйте --codes/--codes-file или --uuids/--uuids-file")
        sys.exit(1)
    
    if codes:
        verbose_print(f"\nКоды для фильтрации ({len(codes)} шт.): {', '.join(codes[:10])}{'...' if len(codes) > 10 else ''}")
    if uuids:
        verbose_print(f"\nUUID для фильтрации ({len(uuids)} шт.): {', '.join(uuids[:10])}{'...' if len(uuids) > 10 else ''}")
    
    # Автоматически формируем путь к обработанной БД, если не указан
    if not args.sqlite_db:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sqlite_db_path = os.path.join(base_dir, 'BD', f'{args.catalog}_processed.db')
        verbose_print(f"\n[ИНФО] Путь к БД не указан, используется автоматический: {sqlite_db_path}")
    else:
        sqlite_db_path = args.sqlite_db
    
    # Выполняем экспорт
    success = export_catalog_by_codes(
        catalog_name=args.catalog,
        sqlite_db_path=sqlite_db_path,
        target_db_path=args.target_1c,
        codes=codes,
        uuids=uuids,
        mode=args.mode
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

