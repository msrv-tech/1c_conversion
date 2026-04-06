# -*- coding: utf-8 -*-
"""
Скрипт для заполнения незаполненных ссылочных объектов из reference_objects.db.

Ищет объекты с filled=0, находит их в processed_db и записывает в приемник.
"""

import os
import sys
import sqlite3
import json
from typing import Dict, List, Optional
from collections import defaultdict

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.reference_objects import get_reference_objects_db_path, get_reference_objects, mark_reference_filled
from tools.logger import verbose_print
from tools.onec_connector import connect_to_1c


def load_catalog_mapping(mapping_file: Optional[str] = None) -> Dict:
    """
    Загружает маппинг справочников из JSON файла.
    """
    if mapping_file is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        mapping_file = os.path.join(project_root, "CONF", "catalog_mapping.json")
    
    if not os.path.exists(mapping_file):
        verbose_print(f"  ⚠ Файл маппинг не найден: {mapping_file}")
        return {}
    
    try:
        with open(mapping_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка при загрузке маппинга: {e}")
        return {}


def get_all_catalog_infos_for_type(ref_type: str, mapping: Optional[Dict] = None) -> List[Dict]:
    """
    Находит все маппинги, которые ведут в данный тип приемника.
    Учитывает как прямые маппинги, так и трансформации (Заказ -> Договор).
    """
    if mapping is None:
        mapping = load_catalog_mapping()
    
    results = []
    # 1. Прямое совпадение
    if ref_type in mapping:
        results.append(mapping[ref_type])
    
    # 2. Поиск трансформаций типа "Source -> Target"
    for key, info in mapping.items():
        if "→" in key or "->" in key:
            if key.endswith(f"→{ref_type}") or key.endswith(f"->{ref_type}"):
                if info not in results:
                    results.append(info)
    
    return results


def get_statistics_by_type(reference_objects_db: str) -> Dict[str, int]:
    """
    Получает статистику незаполненных объектов по типам.
    """
    conn = sqlite3.connect(reference_objects_db)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT ref_type, COUNT(*) as cnt
        FROM reference_objects
        WHERE filled = 0
        GROUP BY ref_type
        ORDER BY cnt DESC
    """)
    
    stats = {}
    for row in cursor.fetchall():
        stats[row[0]] = row[1]
    
    conn.close()
    return stats


def get_catalog_name_from_type_mapping_db(ref_type: str, mapping_db_path: str = "CONF/type_mapping.db") -> Optional[str]:
    """
    Получает catalog_name из type_mapping.db по ref_type (target_full_name).
    """
    if not os.path.exists(mapping_db_path):
        return None
    
    try:
        conn = sqlite3.connect(mapping_db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT target_name, source_name, source_full_name
            FROM object_mapping
            WHERE object_type = 'catalog' AND target_full_name = ?
        """, (ref_type,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            target_name = row['target_name']
            mapping = load_catalog_mapping()
            if ref_type in mapping:
                catalog_name = mapping[ref_type].get('catalog_name')
                if catalog_name:
                    return catalog_name
            return target_name
        
        mapping = load_catalog_mapping()
        if ref_type in mapping:
            catalog_name = mapping[ref_type].get('catalog_name')
            if catalog_name:
                return catalog_name
        
        return None
        
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка при поиске в type_mapping.db: {e}")
        return None


def find_objects_in_processed_db(
    processed_db: str,
    ref_type: str,
    uuids: List[str],
    mapping: Optional[Dict] = None,
    mapping_db_path: Optional[str] = None
) -> List[tuple[Dict, str, str]]: 
    """
    Ищет объекты в обработанных БД по типу и списку UUID.
    Возвращает список кортежей (объект, путь_к_бд, имя_таблицы).
    """
    if not uuids:
        return []

    allowed_prefixes = ("Справочник.", "ChartOf", "Документ.Заказ")
    if not any(ref_type.startswith(p) for p in allowed_prefixes):
        return []

    catalog_infos = get_all_catalog_infos_for_type(ref_type, mapping)
    
    if not catalog_infos and mapping_db_path:
        catalog_name_from_db = get_catalog_name_from_type_mapping_db(ref_type, mapping_db_path)
        if catalog_name_from_db:
            catalog_infos = [{"catalog_name": catalog_name_from_db}]

    candidate_paths: List[str] = []

    def _append_candidate(path: Optional[str]):
        if not path:
            return
        abs_path = os.path.abspath(path)
        if abs_path not in candidate_paths:
            candidate_paths.append(abs_path)

    if os.path.isdir(processed_db):
        for info in catalog_infos:
            catalog_name = info.get("catalog_name")
            if catalog_name:
                _append_candidate(os.path.join(processed_db, f"{catalog_name}_processed.db"))
                _append_candidate(os.path.join(processed_db, f"{catalog_name}.db"))

        for name in sorted(os.listdir(processed_db)):
            if not name.endswith(".db"):
                continue
            if name in {"filters.db", "reference_objects.db", "reference_objects_prod.db"}:
                continue
            _append_candidate(os.path.join(processed_db, name))
    else:
        _append_candidate(processed_db)

    all_found = []
    remaining_uuids = list(uuids)

    for db_path in candidate_paths:
        if not remaining_uuids:
            break
            
        if catalog_infos:
            for info in catalog_infos:
                items_with_table = _find_in_single_processed_db_with_table(db_path, ref_type, remaining_uuids, info, mapping_db_path)
                if items_with_table:
                    items, table_name = items_with_table
                    for item in items:
                        all_found.append((item, db_path, table_name))
                    found_uuids = {item.get('uuid') for item in items if item.get('uuid')}
                    remaining_uuids = [u for u in remaining_uuids if u not in found_uuids]
        else:
            items_with_table = _find_in_single_processed_db_with_table(db_path, ref_type, remaining_uuids, None, mapping_db_path)
            if items_with_table:
                items, table_name = items_with_table
                for item in items:
                    all_found.append((item, db_path, table_name))
                found_uuids = {item.get('uuid') for item in items if item.get('uuid')}
                remaining_uuids = [u for u in remaining_uuids if u not in found_uuids]
    
    return all_found


def _find_in_single_processed_db_with_table(
    db_path: str,
    ref_type: str,
    uuids: List[str],
    catalog_info: Optional[Dict],
    mapping_db_path: Optional[str] = None,
) -> Optional[tuple[List[Dict], str]]:
    if not os.path.exists(db_path):
        return None

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        table_name = None
        # Если из маппинга уже известно имя таблицы справочника, не подставлять «единственную таблицу»
        # в чужом .db (например parties_mc_balances в parties_mc_balances_processed.db) —
        # writer потом ищет таблицу catalog_name («nomenclature») и пишет «Таблица не найдена».
        strict_catalog = False

        if catalog_info:
            catalog_name = catalog_info.get("catalog_name")
            if catalog_name:
                strict_catalog = True
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND (name = ? OR name = ?)",
                    (catalog_name, f"{catalog_name}_processed"),
                )
                exact_match = cursor.fetchone()
                if exact_match:
                    table_name = exact_match[0]

        if not table_name and mapping_db_path and os.path.exists(mapping_db_path):
            catalog_name_from_db = get_catalog_name_from_type_mapping_db(ref_type, mapping_db_path)
            if catalog_name_from_db:
                strict_catalog = True
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND (name = ? OR name = ?)",
                    (catalog_name_from_db, f"{catalog_name_from_db}_processed"),
                )
                exact_match = cursor.fetchone()
                if exact_match:
                    table_name = exact_match[0]

        if not table_name and strict_catalog:
            return None

        if not table_name:
            table_name = _guess_table_name(cursor, ref_type)

        if not table_name:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            all_tables = [row[0] for row in cursor.fetchall()]
            if len(all_tables) == 1:
                table_name = all_tables[0]

        if not table_name:
            return None

        cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns_info = cursor.fetchall()
        if not columns_info:
            return None

        column_names = [col[1] for col in columns_info]
        if 'uuid' not in column_names:
            return None

        chunk_size = 500
        result = []
        for i in range(0, len(uuids), chunk_size):
            chunk = uuids[i:i+chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            query = f'SELECT * FROM "{table_name}" WHERE uuid IN ({placeholders})'
            cursor.execute(query, chunk)
            rows = cursor.fetchall()
            
            for row in rows:
                item = {}
                for j, col_name in enumerate(column_names):
                    item[col_name] = row[j]
                result.append(item)

        return result, table_name
    except sqlite3.Error as error:
        print(f"Ошибка при чтении из базы данных {db_path}: {error}")
        return None
    finally:
        conn.close()


def _guess_table_name(cursor, ref_type: str) -> Optional[str]:
    if ref_type.startswith("Справочник."):
        lookup = ref_type.replace("Справочник.", "").lower()
    else:
        parts = ref_type.split(".")
        lookup = parts[-1].lower() if parts else ref_type.lower()

    if not lookup:
        return None

    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND lower(name) LIKE ?",
        (f"%{lookup}%",),
    )
    table = cursor.fetchone()
    if table:
        return table[0]

    return None


def write_objects_by_type(
    com_object,
    processed_db: str,
    reference_objects_db: str,
    ref_type: str,
    objects_with_source: List[tuple[Dict, str, str]], 
    mapping: Optional[Dict] = None,
    mapping_db_path: Optional[str] = None
) -> tuple[int, int]:
    """
    Записывает объекты определенного типа в приемник, группируя их по источникам и именам таблиц.
    """
    if not objects_with_source:
        return 0, 0

    objects_by_source = defaultdict(list)
    for obj, source_path, table_name in objects_with_source:
        objects_by_source[(source_path, table_name)].append(obj)

    total_written = 0
    total_errors = 0

    for (source_path, table_name), objects in objects_by_source.items():
        db_name = os.path.basename(source_path).replace("_processed.db", "").replace(".db", "")
        
        catalog_info = None
        all_infos = get_all_catalog_infos_for_type(ref_type, mapping)
        
        for info in all_infos:
            if info.get("catalog_name") == db_name:
                catalog_info = info
                break
        
        if not catalog_info and all_infos:
            catalog_info = all_infos[0]
            
        if not catalog_info:
            total_errors += len(objects)
            continue

        catalog_name = catalog_info.get("catalog_name")
        writer_name = catalog_info.get("writer")
        
        if not catalog_name or not writer_name:
            verbose_print(f"  ⚠ Неполная информация о справочнике {ref_type} (источник {db_name})")
            total_errors += len(objects)
            continue
        
        try:
            writer_module = __import__(f"OUT.{writer_name}", fromlist=[f"write_{catalog_name}_to_1c"])
            write_func = getattr(writer_module, f"write_{catalog_name}_to_1c")
        except (ImportError, AttributeError) as e:
            verbose_print(f"  ⚠ Не удалось загрузить writer {writer_name} для {catalog_name}: {e}")
            total_errors += len(objects)
            continue
        
        import tempfile
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        
        try:
            temp_conn = sqlite3.connect(temp_db.name)
            temp_cursor = temp_conn.cursor()
            
            if not objects:
                continue
            
            first_obj = objects[0]
            columns = list(first_obj.keys())

            columns_def = ', '.join([f'"{col}" TEXT' for col in columns])
            temp_cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_def})')

            for obj in objects:
                values = [str(obj.get(col, '')) for col in columns]
                placeholders = ','.join(['?' for _ in columns])
                col_names_str = ','.join([f'"{col}"' for col in columns])
                temp_cursor.execute(
                    f'INSERT INTO "{table_name}" ({col_names_str}) VALUES ({placeholders})',
                    values
                )
            
            temp_conn.commit()
            temp_conn.close()

            if source_path and os.path.exists(source_path):
                try:
                    source_conn = sqlite3.connect(source_path)
                    source_cursor = source_conn.cursor()
                    source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    all_tables = [row[0] for row in source_cursor.fetchall()]
                    tabular_tables = [t for t in all_tables if t.startswith(f"{catalog_name}_") or t.startswith(f"{table_name}_")]
                    
                    if tabular_tables:
                        uuids = [obj.get('uuid') for obj in objects if obj.get('uuid')]
                        if uuids:
                            temp_conn = sqlite3.connect(temp_db.name)
                            temp_cursor = temp_conn.cursor()
                            for tab_table in tabular_tables:
                                source_cursor.execute(f'PRAGMA table_info("{tab_table}")')
                                tab_cols = source_cursor.fetchall()
                                tab_col_names = [col[1] for col in tab_cols]
                                tab_columns_def = ", ".join([f'"{c[1]}" {c[2]}' for c in tab_cols])
                                temp_cursor.execute(f'CREATE TABLE IF NOT EXISTS "{tab_table}" ({tab_columns_def})')
                                
                                if 'parent_uuid' in tab_col_names:
                                    for i in range(0, len(uuids), 500):
                                        chunk = uuids[i:i+500]
                                        placeholders = ",".join(["?"] * len(chunk))
                                        source_cursor.execute(f'SELECT * FROM "{tab_table}" WHERE parent_uuid IN ({placeholders})', chunk)
                                        tab_rows = source_cursor.fetchall()
                                        if tab_rows:
                                            tab_placeholders = ",".join(["?"] * len(tab_col_names))
                                            tab_cols_str = ",".join([f'"{c}"' for c in tab_col_names])
                                            temp_cursor.executemany(f'INSERT INTO "{tab_table}" ({tab_cols_str}) VALUES ({tab_placeholders})', tab_rows)
                            temp_conn.commit()
                            temp_conn.close()
                    source_conn.close()
                except Exception as e:
                    verbose_print(f"    ⚠ Ошибка при копировании ТЧ: {e}")
            
            from tools.writer_utils import set_include_deleted, set_ignore_date_filter
            try:
                set_include_deleted(True)
                set_ignore_date_filter(True)
                success = write_func(temp_db.name, com_object, None)
            finally:
                set_include_deleted(False)
                set_ignore_date_filter(False)
            
            if success:
                ref_conn = sqlite3.connect(reference_objects_db)
                for obj in objects:
                    uuid = obj.get('uuid', '')
                    if uuid:
                        mark_reference_filled(ref_conn, uuid, ref_type)
                ref_conn.commit()
                ref_conn.close()
                total_written += len(objects)
            else:
                total_errors += len(objects)
        finally:
            if os.path.exists(temp_db.name):
                os.unlink(temp_db.name)

    return total_written, total_errors


def fill_unfilled_references(
    processed_db: str,
    target_1c: str,
    reference_objects_db: Optional[str] = None,
    mapping_file: Optional[str] = None,
    ref_type_filter: Optional[str] = None,
    mapping_db_path: Optional[str] = None,
    catalog_name_filter: Optional[str] = None
) -> bool:
    """
    Заполняет незаполненные ссылочные объекты из reference_objects.db.
    """
    if reference_objects_db is None:
        reference_objects_db = get_reference_objects_db_path()
    
    verbose_print("=" * 80)
    verbose_print("ЗАПОЛНЕНИЕ НЕЗАПОЛНЕННЫХ ССЫЛОЧНЫХ ОБЪЕКТОВ")
    verbose_print("=" * 80)
    verbose_print(f"БД ссылочных объектов: {reference_objects_db}")
    
    if mapping_db_path is None:
        mapping_db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "CONF", "type_mapping.db")
    
    mapping = load_catalog_mapping(mapping_file)
    
    if catalog_name_filter and not ref_type_filter:
        for r_type, info in mapping.items():
            if info.get('catalog_name') == catalog_name_filter:
                ref_type_filter = r_type
                break
        
    stats_before = get_statistics_by_type(reference_objects_db)
    if not stats_before:
        verbose_print("  ✓ Незаполненных объектов не найдено")
        return True
    
    com_object = connect_to_1c(target_1c)
    if not com_object:
        return False
    
    ref_conn = sqlite3.connect(reference_objects_db)
    unfilled_objects = get_reference_objects(ref_conn, ref_type=ref_type_filter, filled=0)
    ref_conn.close()
    
    if not unfilled_objects:
        verbose_print("  ✓ Нет объектов для обработки")
        return True
    
    objects_by_type = defaultdict(list)
    for obj in unfilled_objects:
        r_type = obj.get('ref_type', '')
        if r_type:
            objects_by_type[r_type].append(obj)
    
    total_written = 0
    total_errors = 0
    filled_by_type: Dict[str, int] = {}
    
    for r_type, type_objects in objects_by_type.items():
        verbose_print(f"\nОбработка типа: {r_type} ({len(type_objects)} объектов)")
        uuids = [obj.get('ref_uuid', '') for obj in type_objects if obj.get('ref_uuid')]
        
        objects_with_source = find_objects_in_processed_db(processed_db, r_type, uuids, mapping, mapping_db_path)
        
        if not objects_with_source:
            verbose_print(f"  ⚠ Объекты не найдены в обработанной БД")
            total_errors += len(type_objects)
            continue
        
        written, errors = write_objects_by_type(
            com_object, processed_db, reference_objects_db, r_type, objects_with_source, mapping, mapping_db_path
        )
        
        total_written += written
        total_errors += errors
        if written > 0:
            filled_by_type[r_type] = written
    
    stats_after = get_statistics_by_type(reference_objects_db)
    
    print("\n" + "=" * 80)
    print("СТАТИСТИКА ЗАПОЛНЕНИЯ")
    print("=" * 80)
    print(f"Заполнено в ходе прогона: {total_written}")
    if filled_by_type:
        for r_type, cnt in sorted(filled_by_type.items(), key=lambda x: -x[1]):
            print(f"  - {r_type}: {cnt}")
    remaining = sum(stats_after.values())
    if remaining > 0:
        print(f"Осталось незаполненных: {remaining}")
        for r_type, cnt in sorted(stats_after.items(), key=lambda x: -x[1]):
            print(f"  - {r_type}: {cnt}")
    print(f"Ошибок: {total_errors}")
    return total_errors == 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--processed-db', required=True)
    parser.add_argument('--target-1c', required=True)
    parser.add_argument('--reference-objects-db')
    parser.add_argument('--mapping-file')
    parser.add_argument('--mapping-db')
    parser.add_argument('--ref-type')
    parser.add_argument('--catalog')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    
    if args.verbose:
        from tools.logger import set_verbose
        set_verbose(True)
    
    success = fill_unfilled_references(
        args.processed_db, args.target_1c, args.reference_objects_db, args.mapping_file, args.ref_type, args.mapping_db, args.catalog
    )
    sys.exit(0 if success else 1)
