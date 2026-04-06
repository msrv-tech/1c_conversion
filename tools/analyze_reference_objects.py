# -*- coding: utf-8 -*-
"""
Скрипт для анализа базы данных reference_objects_prod.db.

Показывает статистику по типам объектов:
- Общее количество объектов каждого типа
- Количество полных объектов (filled=1)
- Количество объектов по ссылкам (filled=0)
"""

import os
import sys
import sqlite3

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.reference_objects import get_reference_objects_db_path, set_prod_mode


def analyze_reference_objects(reference_objects_db: str = None) -> None:
    """
    Проводит анализ базы данных reference_objects_prod.
    
    Args:
        reference_objects_db: Путь к БД ссылочных объектов (если None, используется прод БД по умолчанию)
    """
    if reference_objects_db is None:
        # Устанавливаем прод режим для анализа прод БД
        set_prod_mode(True)
        reference_objects_db = get_reference_objects_db_path()
    
    print("=" * 80)
    print("АНАЛИЗ БАЗЫ ДАННЫХ reference_objects_prod.db")
    print("=" * 80)
    print(f"Путь к БД: {reference_objects_db}")
    
    # Проверяем существование БД
    if not os.path.exists(reference_objects_db):
        print(f"\n⚠ Ошибка: База данных не найдена: {reference_objects_db}")
        return
    
    # Подключаемся к БД
    conn = sqlite3.connect(reference_objects_db)
    cursor = conn.cursor()
    
    # Проверяем существование таблицы
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='reference_objects'
    """)
    if not cursor.fetchone():
        print("\n⚠ Ошибка: Таблица reference_objects не найдена в БД")
        conn.close()
        return
    
    # Общая статистика
    cursor.execute("SELECT COUNT(*) FROM reference_objects")
    total_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM reference_objects WHERE filled = 1")
    filled_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM reference_objects WHERE filled = 0")
    unfilled_count = cursor.fetchone()[0]
    
    print(f"\nОБЩАЯ СТАТИСТИКА:")
    print(f"  Всего объектов: {total_count}")
    print(f"  Полных объектов (filled=1): {filled_count}")
    print(f"  Объектов по ссылкам (filled=0): {unfilled_count}")
    
    if total_count > 0:
        filled_percent = (filled_count / total_count) * 100
        unfilled_percent = (unfilled_count / total_count) * 100
        print(f"  Полных: {filled_percent:.1f}%")
        print(f"  По ссылкам: {unfilled_percent:.1f}%")
    
    # Статистика по типам
    print("\n" + "=" * 80)
    print("СТАТИСТИКА ПО ТИПАМ ОБЪЕКТОВ:")
    print("=" * 80)
    
    cursor.execute("""
        SELECT 
            ref_type,
            COUNT(*) as total,
            SUM(CASE WHEN filled = 1 THEN 1 ELSE 0 END) as filled,
            SUM(CASE WHEN filled = 0 THEN 1 ELSE 0 END) as unfilled
        FROM reference_objects
        GROUP BY ref_type
        ORDER BY total DESC
    """)
    
    results = cursor.fetchall()
    
    if not results:
        print("  Нет данных в таблице")
    else:
        # Форматируем вывод
        print(f"\n{'Тип объекта':<50} {'Всего':>10} {'Полных':>10} {'По ссылкам':>12} {'% полных':>10}")
        print("-" * 92)
        
        for row in results:
            ref_type, total, filled, unfilled = row
            filled = filled or 0
            unfilled = unfilled or 0
            percent = (filled / total * 100) if total > 0 else 0
            
            # Обрезаем длинные типы
            type_display = ref_type if len(ref_type) <= 48 else ref_type[:45] + "..."
            
            print(f"{type_display:<50} {total:>10} {filled:>10} {unfilled:>12} {percent:>9.1f}%")
    
    # Дополнительная статистика по датам
    print("\n" + "=" * 80)
    print("СТАТИСТИКА ПО ДАТАМ СОЗДАНИЯ:")
    print("=" * 80)
    
    cursor.execute("""
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as total,
            SUM(CASE WHEN filled = 1 THEN 1 ELSE 0 END) as filled,
            SUM(CASE WHEN filled = 0 THEN 1 ELSE 0 END) as unfilled
        FROM reference_objects
        WHERE created_at IS NOT NULL
        GROUP BY DATE(created_at)
        ORDER BY date DESC
        LIMIT 10
    """)
    
    date_results = cursor.fetchall()
    
    if date_results:
        print(f"\n{'Дата':<15} {'Всего':>10} {'Полных':>10} {'По ссылкам':>12}")
        print("-" * 47)
        for row in date_results:
            date, total, filled, unfilled = row
            filled = filled or 0
            unfilled = unfilled or 0
            print(f"{date:<15} {total:>10} {filled:>10} {unfilled:>12}")
    else:
        print("  Нет данных по датам")
    
    conn.close()
    print("\n" + "=" * 80)


if __name__ == "__main__":
    import sys
    
    # Устанавливаем кодировку для Windows
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    reference_objects_db = None
    if len(sys.argv) > 1:
        reference_objects_db = sys.argv[1]
    
    analyze_reference_objects(reference_objects_db)

