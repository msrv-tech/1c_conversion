# -*- coding: utf-8 -*-
"""
Утилиты для модулей выгрузки данных в 1С.

Содержит общие функции для работы с ссылочными полями и чтения данных из БД.
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Dict, List, Optional

from tools.onec_connector import find_object_by_uuid, safe_getattr, call_if_callable
from tools.db_manager import connect_to_sqlite

_DEFAULT_FETCH_LIMIT: Optional[int] = None
_INCLUDE_DELETED: bool = False  # Флаг для включения объектов с ПометкаУдаления=1 (для догрузки по ссылкам)
_IGNORE_DATE_FILTER: bool = False  # Флаг для игнорирования фильтра по дате (для догрузки по ссылкам)

# Кеш для организации (чтобы не запрашивать каждый раз)
_organization_cache: Optional[Dict[str, str]] = None

# Кеш для предопределенных элементов
# Ключ: путь к предопределенному элементу (например, "Справочники.СтраныМира.Россия")
# Значение: JSON строка с данными элемента (uuid, presentation, type)
_predefined_elements_cache: Dict[str, str] = {}


def set_default_fetch_limit(limit: Optional[int]) -> None:
    """
    Устанавливает глобальный лимит строк для чтения из БД при экспорте.
    Если limit=None, чтение происходит без ограничений.
    """
    global _DEFAULT_FETCH_LIMIT
    _DEFAULT_FETCH_LIMIT = limit


def set_include_deleted(include: bool) -> None:
    """
    Устанавливает флаг включения объектов с ПометкаУдаления=1 при чтении из БД.
    Используется при догрузке объектов по ссылкам.
    
    Args:
        include: True - включать объекты с ПометкаУдаления=1, False - пропускать (по умолчанию)
    """
    global _INCLUDE_DELETED
    _INCLUDE_DELETED = include


def set_ignore_date_filter(ignore: bool) -> None:
    """
    Устанавливает флаг игнорирования фильтра по дате при чтении из БД.
    Используется при догрузке объектов по ссылкам (например, для договоров - игнорировать фильтр по 2025 году).
    
    Args:
        ignore: True - игнорировать фильтр по дате, False - применять фильтр (по умолчанию)
    """
    global _IGNORE_DATE_FILTER
    _IGNORE_DATE_FILTER = ignore


def parse_reference_field(value) -> Optional[Dict]:
    """
    Парсит JSON-значение ссылочного поля.
    
    Args:
        value: JSON строка или обычное значение
        
    Returns:
        Словарь с presentation, uuid, type или None
    """
    if not value:
        return None
    
    if isinstance(value, str) and value.startswith('{') and '"presentation"' in value:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    
    return None


def get_reference_by_uuid(com_object, uuid_value: str, type_name: str):
    """
    Получает ссылку на объект по UUID.
    Использует универсальную функцию find_object_by_uuid из tools.onec_connector.
    
    Args:
        com_object: COM-объект подключения к 1С
        uuid_value: UUID объекта
        type_name: Тип объекта (например, "Справочник.Контрагенты")
        
    Returns:
        Ссылка на объект или None
    """
    return find_object_by_uuid(com_object, uuid_value, type_name)


def get_from_db(connection, table_name: str, limit: Optional[int] = None) -> List[Dict]:
    """
    Читает данные из обработанной базы данных SQLite.
    Читает все колонки из указанной таблицы.
    
    Args:
        connection: Объект подключения к SQLite
        table_name: Имя таблицы
        limit: Максимальное количество записей (None = все)
    
    Returns:
        Список словарей с данными
    """
    if not connection:
        print("Нет подключения к SQLite")
        return []
    
    if limit is None:
        limit = _DEFAULT_FETCH_LIMIT

    try:
        cursor = connection.cursor()
        
        # Получаем все колонки таблицы
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        if not columns_info:
            print(f"Таблица '{table_name}' не найдена")
            return []
        
        column_names = [col[1] for col in columns_info]
        
        # Читаем данные
        if limit:
            query = f'SELECT * FROM "{table_name}" LIMIT ?'
            cursor.execute(query, (limit,))
        else:
            query = f'SELECT * FROM "{table_name}"'
            cursor.execute(query)
        
        rows = cursor.fetchall()
        items = []
        
        # Проверяем наличие колонки ПометкаУдаления
        has_deletion_mark = 'ПометкаУдаления' in column_names
        deletion_mark_index = column_names.index('ПометкаУдаления') if has_deletion_mark else -1
        
        # Проверяем наличие колонки Дата (для фильтрации договоров по году)
        has_date = 'Дата' in column_names
        date_index = column_names.index('Дата') if has_date else -1
        
        for row in rows:
            # Пропускаем объекты с ПометкаУдаления=1 (если флаг не установлен)
            if not _INCLUDE_DELETED and has_deletion_mark and deletion_mark_index >= 0:
                deletion_mark_value = row[deletion_mark_index]
                # ПометкаУдаления может быть INTEGER (1/0), булевым значением или строкой ("True"/"False"/"1"/"0")
                is_deleted = False
                if deletion_mark_value == 1 or deletion_mark_value is True:
                    is_deleted = True
                elif isinstance(deletion_mark_value, str):
                    # Проверяем строковые значения
                    value_lower = deletion_mark_value.strip().lower()
                    if value_lower in ('1', 'true', 'истина', 'да'):
                        is_deleted = True
                
                if is_deleted:
                    continue
            
            # Фильтр по дате: для договоров (contractor_contracts и custom_contracts_external_to_contractor_contracts) только за 2025 год
            # При догрузке по ссылкам этот фильтр игнорируется
            if not _IGNORE_DATE_FILTER and has_date and date_index >= 0 and table_name in ("contractor_contracts", "custom_contracts_external_to_contractor_contracts"):
                date_value = row[date_index]
                # Пропускаем договоры без даты или с пустой датой
                if not date_value or (isinstance(date_value, str) and not date_value.strip()):
                    continue
                
                # Парсим дату (может быть строкой в формате YYYY-MM-DD или YYYY-MM-DD HH:MM:SS)
                try:
                    year = None
                    if isinstance(date_value, str):
                        # Извлекаем год из строки даты
                        date_parts = date_value.split('-')
                        if len(date_parts) >= 1:
                            year = int(date_parts[0])
                    else:
                        # Если это не строка, пробуем извлечь год другим способом
                        # Может быть datetime объект или другой формат
                        date_str = str(date_value)
                        if len(date_str) >= 4:
                            year_str = date_str[:4]
                            try:
                                year = int(year_str)
                            except ValueError:
                                pass
                    
                    # Если год не 2025, пропускаем договор
                    if year is None or year != 2025:
                        continue
                except (ValueError, AttributeError, TypeError):
                    # Если не удалось распарсить дату, пропускаем договор (безопаснее)
                    continue
            
            item = {}
            for i, col_name in enumerate(column_names):
                value = row[i]
                item[col_name] = value
            
            items.append(item)
        
        cursor.close()
        return items
        
    except sqlite3.Error as e:
        print(f"Ошибка при чтении из SQLite: {e}")
        import traceback
        traceback.print_exc()
        return []


def get_default_organization_json(db_path: Optional[str] = None) -> Optional[str]:
    """
    Возвращает захардкоженный UUID организации.
    Поскольку у нас одна организация, возвращаем захардкоженный UUID.
    Возвращает JSON строку в формате для ссылочных полей.
    
    Args:
        db_path: Не используется (оставлен для совместимости)
    
    Returns:
        JSON строка с данными организации (uuid, presentation, type) или None
    """
    global _organization_cache
    
    # Используем кеш, если организация уже получена
    if _organization_cache is not None:
        return json.dumps(_organization_cache, ensure_ascii=False)
    
    # Захардкоженный UUID организации
    # TODO: Заменить на актуальный UUID организации из приемника
    ORG_UUID = "0c3fb101-7605-11da-b92a-505054503030"
    ORG_NAME = "АО \"целевая организация\""
    
    # Сохраняем в кеш
    _organization_cache = {
        "uuid": ORG_UUID,
        "presentation": ORG_NAME,
        "type": "Справочник.Организации"
    }
    
    return json.dumps(_organization_cache, ensure_ascii=False)


def get_predefined_element_by_name(com_object, type_name: str, element_name: str) -> Optional[object]:
    """
    Универсальный поиск предопределенного элемента по типу и имени.
    Использует метод GetPredefinedItemName из COM API, как описано в форуме:
    https://forum.infostart.ru/forum9/topic160155/
    
    Args:
        com_object: COM-объект подключения к 1С
        type_name: Тип элемента (например, "Справочник.СтраныМира", "Перечисление.ВидыСоглашений")
        element_name: Имя предопределенного элемента (используется как есть, без нормализации)
    
    Returns:
        COM-объект предопределенного элемента или None
    """
    if com_object is None or not element_name:
        return None
    
    try:
        from tools.logger import verbose_print
        from tools.onec_connector import safe_getattr, call_if_callable
        
        # Определяем тип объекта (справочник или перечисление)
        if type_name.startswith("Справочник."):
            catalog_name = type_name.replace("Справочник.", "")
            
            # Получаем менеджер справочника
            catalogs = safe_getattr(com_object, "Справочники", None)
            if not catalogs:
                return None
            
            catalog_manager = safe_getattr(catalogs, catalog_name, None)
            if not catalog_manager:
                return None
            
            # Пробуем прямой доступ по имени (как передали, так и используем)
            predefined_element = safe_getattr(catalog_manager, element_name, None)
            if predefined_element:
                # Проверяем, что это действительно предопределенный элемент через GetPredefinedItemName
                try:
                    # Используем метод GetPredefinedItemName для проверки
                    # Согласно форуму: Catalogs[CatalogName].GetPredefinedItemName(COMObject)
                    get_predefined_name_method = safe_getattr(catalog_manager, "GetPredefinedItemName", None)
                    if get_predefined_name_method:
                        predefined_name = call_if_callable(get_predefined_name_method, predefined_element)
                        if predefined_name:
                            verbose_print(f"    ✓ Найден предопределенный элемент {type_name}.{element_name} (проверено через GetPredefinedItemName)")
                            return predefined_element
                except Exception:
                    # Если GetPredefinedItemName не работает, используем прямой доступ
                    pass
                
                # Если прямой доступ сработал, возвращаем элемент
                verbose_print(f"    ✓ Найден предопределенный элемент {type_name}.{element_name} (прямой доступ)")
                return predefined_element
        
        elif type_name.startswith("Перечисление."):
            enum_name = type_name.replace("Перечисление.", "")
            
            # Получаем перечисление
            enums = safe_getattr(com_object, "Перечисления", None)
            if not enums:
                return None
            
            enum_ref = safe_getattr(enums, enum_name, None)
            if not enum_ref:
                return None
            
            # Пробуем получить значение перечисления напрямую по имени (как передали, так и используем)
            enum_value = safe_getattr(enum_ref, element_name, None)
            if enum_value:
                verbose_print(f"    ✓ Найден предопределенный элемент {type_name}.{element_name}")
                return enum_value
        
    except Exception as e:
        from tools.logger import verbose_print
        verbose_print(f"    ⚠ Ошибка при поиске предопределенного элемента '{type_name}.{element_name}': {e}")
    
    return None


def get_predefined_element_json(com_object, element_path: str, type_name: str = "") -> Optional[str]:
    """
    Получает предопределенный элемент из 1С и возвращает его в формате JSON.
    Использует универсальный поиск через get_predefined_element_by_name.
    Использует кеш для избежания повторных обращений к COM объекту.
    
    Args:
        com_object: COM-объект подключения к 1С
        element_path: Путь к предопределенному элементу (например, "Справочники.СтраныМира.Россия")
        type_name: Тип элемента (например, "Справочник.СтраныМира"). 
                   Если не указан, будет определен автоматически.
    
    Returns:
        JSON строка с данными элемента (uuid, presentation, type) или None
    """
    global _predefined_elements_cache
    
    # Проверяем кеш
    if element_path in _predefined_elements_cache:
        return _predefined_elements_cache[element_path]
    
    if com_object is None:
        return None
    
    try:
        from tools.logger import verbose_print
        from tools.onec_connector import safe_getattr, call_if_callable
        
        # Разбиваем путь на части
        parts = element_path.split('.')
        if len(parts) < 3:
            verbose_print(f"    ⚠ Неверный формат пути к предопределенному элементу: '{element_path}'")
            return None
        
        # Определяем тип и имя элемента
        if "Справочники" in element_path:
            catalog_name = parts[1] if len(parts) > 1 else ""
            element_name = parts[2] if len(parts) > 2 else ""
            if not type_name:
                type_name = f"Справочник.{catalog_name}"
        elif "Перечисления" in element_path:
            enum_name = parts[1] if len(parts) > 1 else ""
            element_name = parts[2] if len(parts) > 2 else ""
            if not type_name:
                type_name = f"Перечисление.{enum_name}"
        else:
            verbose_print(f"    ⚠ Неизвестный тип предопределенного элемента: '{element_path}'")
            return None
        
        # Используем универсальный поиск
        predefined_obj = get_predefined_element_by_name(com_object, type_name, element_name)
        if not predefined_obj:
            # Пробуем прямой доступ по пути (fallback)
            current_obj = com_object
            for part in parts:
                current_obj = safe_getattr(current_obj, part, None)
                if current_obj is None:
                    verbose_print(f"    ⚠ Предопределенный элемент '{element_path}' не найден")
                    return None
            predefined_obj = current_obj
        
        if predefined_obj:
            # Получаем UUID
            uuid_attr = safe_getattr(predefined_obj, "УникальныйИдентификатор", None)
            uuid_value = call_if_callable(uuid_attr) if uuid_attr else None
            
            # Получаем представление (пробуем разные варианты)
            presentation = None
            for attr_name in ["Наименование", "Представление", "Name"]:
                presentation_attr = safe_getattr(predefined_obj, attr_name, None)
                presentation = call_if_callable(presentation_attr) if presentation_attr else None
                if presentation:
                    break
            
            # Если представление не найдено, используем имя элемента
            if not presentation:
                presentation = element_name
            
            element_json = {
                "uuid": str(uuid_value) if uuid_value else "",
                "presentation": str(presentation) if presentation else element_name,
                "type": type_name
            }
            
            json_str = json.dumps(element_json, ensure_ascii=False)
            
            # Сохраняем в кеш
            _predefined_elements_cache[element_path] = json_str
            
            verbose_print(f"    ✓ Получен предопределенный элемент: {element_path} ({presentation})")
            return json_str
            
    except Exception as e:
        from tools.logger import verbose_print
        verbose_print(f"    ⚠ Ошибка при получении предопределенного элемента '{element_path}': {e}")
    
    return None


def clear_predefined_elements_cache() -> None:
    """
    Очищает кэш предопределенных элементов.
    Полезно для освобождения памяти или принудительного обновления данных.
    """
    global _predefined_elements_cache
    _predefined_elements_cache.clear()

