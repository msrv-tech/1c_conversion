# -*- coding: utf-8 -*-
"""
Модуль для работы с базой данных SQLite
Содержит только общие процедуры подключения и управления БД
Функции работы с конкретными справочниками находятся в соответствующих модулях IN/ и OUT/
"""

import json
import os
import sqlite3
from typing import Iterable, List

from tools.encoding_fix import fix_encoding

fix_encoding()


def _prepare_db_path(db_file: str) -> str:
    if not db_file:
        raise ValueError("Не указан путь к файлу базы данных SQLite.")

    db_file = os.path.abspath(db_file)
    directory = os.path.dirname(db_file)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    return db_file


def connect_to_sqlite(db_file: str):
    """
    Подключается к базе данных SQLite
    
    Args:
        db_file: Путь к файлу базы данных
    
    Returns:
        Объект подключения к SQLite или None при ошибке
    """
    db_file = _prepare_db_path(db_file)

    try:
        connection = sqlite3.connect(db_file)
        # Включаем поддержку внешних ключей
        connection.execute("PRAGMA foreign_keys = ON")
        return connection
    except sqlite3.Error as e:
        print(f"Ошибка подключения к SQLite: {e}")
        return None


def ensure_database_exists(db_file: str):
    """
    Проверяет существование базы данных и создает её при необходимости
    
    Args:
        db_file: Путь к файлу базы данных
    
    Returns:
        True если БД существует или создана, False при ошибке
    """
    db_file = _prepare_db_path(db_file)

    if os.path.exists(db_file):
        return True

    try:
        connection = sqlite3.connect(db_file)
        connection.close()
        return True
    except sqlite3.Error as error:
        print(f"Ошибка создания базы данных SQLite: {error}")
        return False


def process_reference_fields(rows: List[dict], reference_columns: Iterable[str]) -> List[dict]:
    """
    Обрабатывает поля-ссылки и перечисления в строках данных перед сохранением в БД.
    
    Для перечислений оставляет значение как строку вида "Перечисление.Имя.Значение"
    (значение уже обработано в execute_query).
    
    Для ссылок конвертирует метаданные в JSON формат с presentation, uuid и type.
    
    Args:
        rows: Список словарей с данными строк
        reference_columns: Список имен колонок, которые являются ссылками или перечислениями
    
    Returns:
        Список обработанных строк
    """
    for row in rows:
        for column in reference_columns:
            presentation_key = f"{column}_Представление"
            uuid_key = f"{column}_UUID"
            type_key = f"{column}_Тип"

            # Проверяем наличие метаданных
            has_meta_columns = (
                presentation_key in row or uuid_key in row or type_key in row
            )
            if not has_meta_columns:
                continue

            # Получаем тип первым, чтобы проверить, является ли поле перечислением
            type_value = row.pop(type_key, "")
            
            # Проверяем, является ли поле перечислением
            # Если это перечисление, значение уже обработано в execute_query как строка Перечисление.Имя.Значение
            # Не нужно конвертировать в JSON
            if type_value and type_value.startswith("Перечисление."):
                # Для перечислений оставляем значение как есть (уже строка Перечисление.Имя.Значение)
                # Удаляем метаданные, которые не нужны
                row.pop(presentation_key, None)
                row.pop(uuid_key, None)
                # Значение column уже установлено в execute_query
                if column not in row or not row[column]:
                    row[column] = ""
                continue

            # Для ссылок (не перечислений) - конвертируем в JSON
            presentation = row.pop(presentation_key, "")
            uuid_value = row.pop(uuid_key, "")
            
            # Для поля "Родитель" в иерархических справочниках добавляем ЭтоГруппа в JSON
            # НЕ удаляем Родитель_ЭтоГруппа, чтобы сохранить в БД для использования в процессоре
            is_group_key = f"{column}_ЭтоГруппа"
            is_group_value = None
            if is_group_key in row:
                is_group_value = row[is_group_key]  # НЕ удаляем, оставляем в row для сохранения в БД
                # Преобразуем в булево значение
                if isinstance(is_group_value, bool):
                    pass  # Уже булево
                elif isinstance(is_group_value, (int, str)):
                    is_group_value = str(is_group_value).lower() in ('1', 'true', 'истина', 'да')
                elif is_group_value is None:
                    is_group_value = False

            if presentation or uuid_value or type_value:
                json_data = {
                    "presentation": presentation,
                    "uuid": uuid_value,
                    "type": type_value,
                }
                # Добавляем ЭтоГруппа для поля "Родитель"
                if is_group_value is not None:
                    json_data["is_group"] = is_group_value
                
                row[column] = json.dumps(
                    json_data,
                    ensure_ascii=False,
                )
            else:
                row[column] = ""
    
    return rows
