# -*- coding: utf-8 -*-
"""
Модуль для работы с ссылочными объектами.

Ведет единую таблицу всех ссылочных объектов с признаком полной записи.
"""

import json
import os
import sqlite3
from typing import Dict, List, Optional

# Глобальный флаг для режима prod
_IS_PROD_MODE: bool = False


def set_prod_mode(is_prod: bool) -> None:
    """
    Устанавливает режим продакшн.
    
    Args:
        is_prod: True - использовать reference_objects_prod.db, False - использовать reference_objects.db
    """
    global _IS_PROD_MODE
    _IS_PROD_MODE = is_prod


def get_reference_objects_db_path(base_dir: Optional[str] = None) -> str:
    """
    Возвращает путь к единой БД для ссылочных объектов.
    
    Args:
        base_dir: Базовая директория проекта (если None, используется текущая директория)
    
    Returns:
        Путь к БД reference_objects.db или reference_objects_prod.db (в зависимости от режима)
    """
    if base_dir is None:
        # Определяем базовую директорию проекта (папка BD)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        base_dir = os.path.join(project_root, "BD")
    else:
        # Если передан base_dir, используем его
        if not os.path.exists(base_dir):
            os.makedirs(base_dir, exist_ok=True)
    
    # Выбираем имя файла в зависимости от режима
    db_filename = "reference_objects_prod.db" if _IS_PROD_MODE else "reference_objects.db"
    return os.path.join(base_dir, db_filename)


def ensure_reference_objects_table(connection: sqlite3.Connection) -> None:
    """
    Создает таблицу для ссылочных объектов, если её нет.
    
    Args:
        connection: Подключение к SQLite
    """
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reference_objects (
            ref_uuid TEXT NOT NULL,
            ref_type TEXT NOT NULL,
            ref_presentation TEXT,
            source_data TEXT,
            filled INTEGER DEFAULT 0,
            filled_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            parent_type TEXT,
            parent_name TEXT,
            parent_uuid TEXT,
            field_name TEXT,
            PRIMARY KEY (ref_uuid, ref_type)
        )
    """)
    connection.commit()


def save_reference_object(
    connection: sqlite3.Connection,
    ref_uuid: str,
    ref_type: str,
    ref_presentation: str = "",
    source_data: Optional[Dict] = None,
    filled: bool = False,
    parent_type: str = "",
    parent_name: str = "",
    parent_uuid: str = "",
    field_name: str = "",
) -> None:
    """
    Сохраняет или обновляет информацию о ссылочном объекте.
    
    Логика:
    - Если объект уже существует и filled=1 (полная запись), не перезаписываем
    - Если объект не существует или filled=0, создаем/обновляем запись
    
    Args:
        connection: Подключение к SQLite
        ref_uuid: UUID объекта
        ref_type: Тип объекта (например, "Справочник.Контрагенты")
        ref_presentation: Представление объекта
        source_data: Полные данные из источника (JSON)
        filled: Признак полной записи (True - записано через основной обработчик, False - создано через реквизит)
        parent_type: Тип родительского объекта (для контекста)
        parent_name: Имя родительского объекта (для контекста)
        parent_uuid: UUID родительского объекта (для контекста)
        field_name: Имя поля (для контекста)
    """
    ensure_reference_objects_table(connection)
    cursor = connection.cursor()
    
    # Проверяем, существует ли запись и заполнена ли она полностью
    cursor.execute("""
        SELECT filled FROM reference_objects 
        WHERE ref_uuid = ? AND ref_type = ?
    """, (ref_uuid, ref_type))
    existing = cursor.fetchone()
    
    # Если запись существует и уже заполнена полностью, не перезаписываем
    if existing and existing[0] == 1 and not filled:
        return
    
    # Подготавливаем данные
    source_data_json = json.dumps(source_data, ensure_ascii=False) if source_data else None
    
    # Вставляем или обновляем запись
    try:
        if existing:
            # Обновляем существующую запись
            if filled:
                # Обновляем с признаком полной записи
                cursor.execute("""
                    UPDATE reference_objects 
                    SET ref_presentation = ?,
                        source_data = ?,
                        filled = 1,
                        filled_at = CURRENT_TIMESTAMP,
                        parent_type = ?,
                        parent_name = ?,
                        parent_uuid = ?,
                        field_name = ?
                    WHERE ref_uuid = ? AND ref_type = ?
                """, (ref_presentation, source_data_json, parent_type, parent_name, parent_uuid, field_name, ref_uuid, ref_type))
            else:
                # Обновляем только если еще не заполнено
                cursor.execute("""
                    UPDATE reference_objects 
                    SET ref_presentation = ?,
                        source_data = ?,
                        parent_type = ?,
                        parent_name = ?,
                        parent_uuid = ?,
                        field_name = ?
                    WHERE ref_uuid = ? AND ref_type = ? AND filled = 0
                """, (ref_presentation, source_data_json, parent_type, parent_name, parent_uuid, field_name, ref_uuid, ref_type))
        else:
            # Создаем новую запись
            cursor.execute("""
                INSERT INTO reference_objects 
                (ref_uuid, ref_type, ref_presentation, source_data, filled, 
                 parent_type, parent_name, parent_uuid, field_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ref_uuid, ref_type, ref_presentation, source_data_json, 1 if filled else 0,
                  parent_type, parent_name, parent_uuid, field_name))
        
        connection.commit()
    except sqlite3.Error as e:
        print(f"    ⚠ Ошибка при сохранении ссылочного объекта: {e}")


def get_reference_objects(
    connection: sqlite3.Connection,
    ref_type: Optional[str] = None,
    filled: Optional[int] = None
) -> List[Dict]:
    """
    Получает список ссылочных объектов из БД.
    
    Args:
        connection: Подключение к SQLite
        ref_type: Фильтр по типу (опционально)
        filled: Фильтр по статусу заполнения (0 - не заполнено, 1 - заполнено)
    
    Returns:
        Список словарей с информацией о ссылочных объектах
    """
    ensure_reference_objects_table(connection)
    cursor = connection.cursor()
    
    query = "SELECT * FROM reference_objects WHERE 1=1"
    params = []
    
    if ref_type:
        query += " AND ref_type = ?"
        params.append(ref_type)
    
    if filled is not None:
        query += " AND filled = ?"
        params.append(filled)
    
    query += " ORDER BY created_at"
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    # Получаем имена колонок
    column_names = [desc[0] for desc in cursor.description]
    
    result = []
    for row in rows:
        item = {}
        for i, col_name in enumerate(column_names):
            value = row[i]
            if col_name == 'source_data' and value:
                try:
                    value = json.loads(value)
                except:
                    pass
            item[col_name] = value
        result.append(item)
    
    return result


def mark_reference_filled(
    connection: sqlite3.Connection,
    ref_uuid: str,
    ref_type: str
) -> None:
    """
    Помечает объект как заполненный.
    
    Args:
        connection: Подключение к SQLite
        ref_uuid: UUID объекта
        ref_type: Тип объекта
    """
    ensure_reference_objects_table(connection)
    cursor = connection.cursor()
    cursor.execute("""
        UPDATE reference_objects 
        SET filled = 1, filled_at = CURRENT_TIMESTAMP
        WHERE ref_uuid = ? AND ref_type = ?
    """, (ref_uuid, ref_type))
    connection.commit()


def mark_references_unfilled(
    connection: sqlite3.Connection,
    ref_type: str
) -> int:
    """
    Помечает все объекты указанного типа как незаполненные (для повторной догрузки).
    
    Args:
        connection: Подключение к SQLite
        ref_type: Тип объекта (например, "Справочник.РасходыБудущихПериодов")
    
    Returns:
        Количество обновлённых записей
    """
    ensure_reference_objects_table(connection)
    cursor = connection.cursor()
    cursor.execute("""
        UPDATE reference_objects 
        SET filled = 0, filled_at = NULL
        WHERE ref_type = ?
    """, (ref_type,))
    connection.commit()
    return cursor.rowcount

