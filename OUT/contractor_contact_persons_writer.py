# -*- coding: utf-8 -*-
"""
Модуль выгрузки контактныелицаконтрагентов из обработанной БД в 1С приемник.
Данные в БД уже в формате приемника, запись происходит с сохранением UUID.
"""

import os
import sys
import json
import sqlite3

# Добавляем родительскую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db
from tools.base_writer import (
    prepare_catalog_item,
    finalize_catalog_item,
    _get_enum_from_string,
)
from tools.onec_connector import safe_getattr, call_if_callable
from tools.logger import verbose_print  # noqa: E402

fix_encoding()


def _write_item(com_object, item_data, processed_db=None):
    """
    Записывает элемент контактного лица контрагента в 1С с сохранением UUID.
    Использует базовые функции prepare_catalog_item и finalize_catalog_item,
    дозаполняет только специфичные данные (табличная часть КонтактнаяИнформация).
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item_data: Словарь с данными элемента
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        
    Returns:
        True если успешно, False если ошибка
    """
    from tools.logger import verbose_print
    
    item_name = item_data.get('Наименование', 'Без наименования')
    uuid_value = item_data.get('uuid', '')
    
    # Маппинг имен полей (Владелец -> ОбъектВладелец в приемнике)
    field_name_mapping = {
        "Владелец": "ОбъектВладелец"
    }
    
    # Стандартные поля для контактных лиц
    standard_fields = ["Наименование", "ПометкаУдаления", "Комментарий", "Должность"]
    
    # ЭТАПЫ 1-8: Подготовка и заполнение базовых полей через базовую функцию
    item = prepare_catalog_item(
        com_object=com_object,
        item_data=item_data,
        catalog_name="КонтактныеЛица",
        type_name="Справочник.КонтактныеЛица",
        standard_fields=standard_fields,
        processed_db=processed_db,
        field_mapping=None,  # Загрузится автоматически из type_mapping.db
        field_name_mapping=field_name_mapping
    )
    
    if not item:
        verbose_print(f"  ✗ Не удалось подготовить элемент '{item_name}'")
        return False
    
    # Дозаполнение поля ВидКонтактногоЛица
    verbose_print(f"  [ДОЗАПОЛНЕНИЕ] Установка ВидКонтактногоЛица")
    try:
        enum_obj = _get_enum_from_string(com_object, "Перечисление.ВидыКонтактныхЛиц.КонтактноеЛицоКонтрагента")
        if enum_obj:
            item.ВидКонтактногоЛица = enum_obj
            verbose_print(f"  → Установлено ВидКонтактногоЛица: КонтактноеЛицоКонтрагента")
        else:
            verbose_print(f"  ⚠ Не удалось получить перечисление ВидыКонтактныхЛиц.КонтактноеЛицоКонтрагента")
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка при установке ВидКонтактногоЛица: {e}")
    
    # ЭТАП 9: ЗАПОЛНЕНИЕ ТАБЛИЧНЫХ ЧАСТЕЙ (специфичная логика)
    verbose_print(f"  [ЭТАП 9] Заполнение табличной части КонтактнаяИнформация")
    if processed_db and uuid_value:
        _write_contact_info_tabular_section(com_object, item, uuid_value, processed_db)
    
    # ЭТАПЫ 10-12: Завершение записи через базовую функцию
    return finalize_catalog_item(
        com_object=com_object,
        item=item,
        item_data=item_data,
        catalog_name="КонтактныеЛица",
        type_name="Справочник.КонтактныеЛица",
        processed_db=processed_db
    )


def _write_contact_info_tabular_section(com_object, contact_person_obj, contact_person_uuid, processed_db):
    """
    Записывает табличную часть КонтактнаяИнформация для контактного лица контрагента
    используя функцию ДобавитьКонтактнуюИнформацию из 1С.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        contact_person_obj: Объект контактного лица контрагента
        contact_person_uuid: UUID контактного лица контрагента
        processed_db: Путь к обработанной БД
    """
    verbose_print(f"    → Заполнение табличной части КонтактнаяИнформация для контактного лица контрагента {contact_person_uuid[:8]}...")
    
    try:
        # Читаем данные из БД
        conn = sqlite3.connect(processed_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT * FROM contractor_contact_persons_contact_info 
                WHERE parent_uuid = ?
                ORDER BY НомерСтроки
            ''', (contact_person_uuid,))
            
            rows = cursor.fetchall()
        except sqlite3.OperationalError:
            # Таблица может отсутствовать
            rows = []
        finally:
            conn.close()
        
        if not rows:
            return
        
        # Получаем функцию ДобавитьКонтактнуюИнформацию
        # Пробуем через ГлобальныйКонтекст или напрямую
        add_contact_info_func = None
        try:
            global_context = safe_getattr(com_object, "ГлобальныйКонтекст", None)
            if global_context:
                add_contact_info_func = safe_getattr(global_context, "ДобавитьКонтактнуюИнформацию", None)
        except Exception:
            pass
        
        if not add_contact_info_func:
            # Пробуем напрямую
            try:
                add_contact_info_func = safe_getattr(com_object, "ДобавитьКонтактнуюИнформацию", None)
            except Exception:
                pass
        
        if not add_contact_info_func:
            verbose_print(f"    ⚠ Функция ДобавитьКонтактнуюИнформацию не найдена, используем прямой доступ к табличной части")
            # Fallback на старый способ
            _write_contact_info_direct(com_object, contact_person_obj, rows)
            return
        
        # Записываем строки через функцию ДобавитьКонтактнуюИнформацию
        rows_written = 0
        for row in rows:
            try:
                row_dict = dict(row)
                
                # Получаем значение (представление или значение)
                значение_или_представление = ""
                if 'Представление' in row_dict and row_dict['Представление']:
                    значение_или_представление = row_dict['Представление']
                elif 'Значение' in row_dict and row_dict['Значение']:
                    значение_или_представление = row_dict['Значение']
                
                if not значение_или_представление:
                    verbose_print(f"    ⚠ Пропуск строки: нет Представление и Значение")
                    continue
                
                # Получаем ВидКонтактнойИнформации (предопределенный элемент)
                вид_ref = None
                if 'Вид' in row_dict and row_dict['Вид']:
                    try:
                        вид_value = row_dict['Вид']
                        
                        # Маппинг наименований на имена предопределенных элементов
                        # Из справочника ВидыКонтактнойИнформации для КонтактныеЛица
                        predefined_name_mapping = {
                            "Телефон мобильный": "ТелефонМобильныйКонтактныеЛица",
                            "Телефон рабочий": "ТелефонРабочийКонтактныеЛица",
                            "Email": "EmailКонтактныеЛица",
                            "Электронная почта": "EmailКонтактныеЛица",
                            "Адрес для информирования": "АдресДляИнформированияКонтактныеЛица",
                            "Другое": "ДругаяИнформацияКонтактныеЛица",
                        }
                        
                        # Очищаем вид_value
                        вид_value_clean = вид_value.replace(" контактные лица", "").replace("КонтактныеЛица", "").replace(" контактного лица", "").strip()
                        
                        # Определяем имя предопределенного элемента
                        predefined_name = predefined_name_mapping.get(вид_value_clean, None)
                        
                        if not predefined_name:
                            # Попробуем найти вхождение по ключевым словам
                            вид_value_lower = вид_value_clean.lower()
                            if "мобильный" in вид_value_lower:
                                predefined_name = "ТелефонМобильныйКонтактныеЛица"
                            elif "рабочий" in вид_value_lower and "телефон" in вид_value_lower:
                                predefined_name = "ТелефонРабочийКонтактныеЛица"
                            elif "телефон" in вид_value_lower:
                                predefined_name = "ТелефонМобильныйКонтактныеЛица"
                            elif "email" in вид_value_lower or "почта" in вид_value_lower:
                                predefined_name = "EmailКонтактныеЛица"
                            elif "информировани" in вид_value_lower:
                                predefined_name = "АдресДляИнформированияКонтактныеЛица"
                            else:
                                predefined_name = "ДругаяИнформацияКонтактныеЛица"
                        
                        if predefined_name:
                            # Получаем предопределенный элемент напрямую через справочник
                            try:
                                catalogs = safe_getattr(com_object, "Справочники", None)
                                if catalogs:
                                    виды_каталог = safe_getattr(catalogs, "ВидыКонтактнойИнформации", None)
                                    if виды_каталог:
                                        вид_ref = safe_getattr(виды_каталог, predefined_name, None)
                                        if вид_ref:
                                            вид_ref = call_if_callable(вид_ref)
                                            verbose_print(f"    ✓ Получен предопределенный элемент: {predefined_name}")
                            except Exception as e:
                                verbose_print(f"    ⚠ Ошибка при получении предопределенного элемента {predefined_name}: {e}")
                        
                        # Если не нашли через маппинг, пробуем использовать значение как имя предопределенного элемента
                        if not вид_ref:
                            try:
                                catalogs = safe_getattr(com_object, "Справочники", None)
                                if catalogs:
                                    виды_каталог = safe_getattr(catalogs, "ВидыКонтактнойИнформации", None)
                                    if виды_каталог:
                                        # Пробуем использовать вид_value как имя предопределенного элемента
                                        вид_ref = safe_getattr(виды_каталог, вид_value, None)
                                        if вид_ref:
                                            вид_ref = call_if_callable(вид_ref)
                                            verbose_print(f"    ✓ Получен предопределенный элемент по имени: {вид_value}")
                            except Exception:
                                pass
                        
                        # БОЛЬШЕ НЕ СОЗДАЕМ НОВЫЕ ЭЛЕМЕНТЫ (удален fallback на find_or_create_reference_by_name)
                                
                    except Exception as e:
                        verbose_print(f"    ⚠ Ошибка при получении ВидКонтактнойИнформации: {e}")
                
                if not вид_ref:
                    verbose_print(f"    ⚠ Пропуск строки: не найден ВидКонтактнойИнформации (Вид: {row_dict.get('Вид', 'N/A')})")
                    continue
                
                # Вызываем функцию ДобавитьКонтактнуюИнформацию
                try:
                    # Параметры: СсылкаИлиОбъект, ЗначениеИлиПредставление, ВидКонтактнойИнформации, Дата, Замещать, РаспознатьАдрес
                    # Дата = Неопределено (по умолчанию), Замещать = Истина, РаспознатьАдрес = Истина
                    call_if_callable(
                        add_contact_info_func,
                        contact_person_obj,
                        значение_или_представление,
                        вид_ref,
                        None,  # Дата = Неопределено
                        True,  # Замещать = Истина
                        True   # РаспознатьАдрес = Истина
                    )
                    rows_written += 1
                    verbose_print(f"    ✓ Добавлена контактная информация: {значение_или_представление[:50]}... (вид: {row_dict.get('Вид', 'N/A')})")
                except Exception as e:
                    verbose_print(f"    ⚠ Ошибка при вызове ДобавитьКонтактнуюИнформацию: {e}")
                    continue
                
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при обработке строки: {e}")
                continue
        
        if rows_written > 0:
            verbose_print(f"    ✓ Заполнена контактная информация через ДобавитьКонтактнуюИнформацию ({rows_written} записей)")
        else:
            verbose_print(f"    ⚠ Не удалось заполнить ни одной записи контактной информации")
        
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при записи контактной информации: {e}")
        import traceback
        traceback.print_exc()


def _write_contact_info_direct(com_object, contact_person_obj, rows):
    """
    Fallback: запись контактной информации напрямую в табличную часть (старый способ).
    Используется, если функция ДобавитьКонтактнуюИнформацию недоступна.
    """
    verbose_print(f"    → Используется прямой доступ к табличной части (fallback)")
    
    try:
        # Получаем табличную часть КонтактнаяИнформация
        tabular_section = safe_getattr(contact_person_obj, "КонтактнаяИнформация", None)
        if not tabular_section:
            verbose_print(f"    ⚠ Табличная часть КонтактнаяИнформация не найдена")
            return
        
        tabular_section = call_if_callable(tabular_section)
        if not tabular_section:
            verbose_print(f"    ⚠ Не удалось получить табличную часть КонтактнаяИнформация")
            return
        
        # Очищаем существующие строки табличной части
        try:
            tabular_section.Очистить()
        except Exception:
            pass
        
        # Записываем строки
        rows_written = 0
        for row in rows:
            try:
                new_row = tabular_section.Добавить()
                row_dict = dict(row)
                
                # Устанавливаем Тип (перечисление)
                if 'Тип' in row_dict and row_dict['Тип']:
                    try:
                        from tools.writer_utils import get_predefined_element_by_name
                        
                        type_value = row_dict['Тип']
                        enum_value = get_predefined_element_by_name(com_object, "Перечисление.ТипыКонтактнойИнформации", type_value)
                        if enum_value:
                            new_row.Тип = enum_value
                    except Exception:
                        pass
                
                # Устанавливаем Вид
                if 'Вид' in row_dict and row_dict['Вид']:
                    try:
                        # Используем тот же маппинг, что и выше
                        вид_value = row_dict['Вид']
                        
                        # Очищаем вид_value
                        вид_value_clean = вид_value.replace(" контактные лица", "").replace("КонтактныеЛица", "").replace(" контактного лица", "").strip()
                        
                        # Пытаемся сопоставить с предопределенным
                        predefined_name = predefined_name_mapping.get(вид_value_clean, None)
                        if not predefined_name:
                            # Краткий keyword-based поиск для fallback
                            v_lower = вид_value_clean.lower()
                            if "мобильный" in v_lower: predefined_name = "ТелефонМобильныйКонтактныеЛица"
                            elif "рабочий" in v_lower: predefined_name = "ТелефонРабочийКонтактныеЛица"
                            elif "email" in v_lower or "почта" in v_lower: predefined_name = "EmailКонтактныеЛица"
                            else: predefined_name = "ДругаяИнформацияКонтактныеЛица"
                            
                        # Ищем предопределенный элемент
                        вид_ref = None
                        try:
                            catalogs = safe_getattr(com_object, "Справочники", None)
                            if catalogs:
                                виды_каталог = safe_getattr(catalogs, "ВидыКонтактнойИнформации", None)
                                if виды_каталог:
                                    вид_ref = safe_getattr(виды_каталог, predefined_name, None)
                                    if вид_ref:
                                        вид_ref = call_if_callable(вид_ref)
                        except Exception:
                            pass
                        
                        if вид_ref:
                            new_row.Вид = вид_ref
                    except Exception as e:
                        verbose_print(f"    ⚠ Ошибка при установке Вида в fallback: {e}")
                
                # Устанавливаем Представление
                if 'Представление' in row_dict and row_dict['Представление']:
                    try:
                        new_row.Представление = row_dict['Представление']
                    except Exception:
                        pass
                
                # Устанавливаем Значение
                if 'Значение' in row_dict and row_dict['Значение']:
                    try:
                        new_row.Значение = row_dict['Значение']
                    except Exception:
                        pass
                
                rows_written += 1
                
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при добавлении строки: {e}")
                continue
        
        if rows_written > 0:
            verbose_print(f"    ✓ Заполнена табличная часть КонтактнаяИнформация ({rows_written} строк)")
        
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при прямой записи табличной части: {e}")


def write_contractor_contact_persons_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает контактныелицаконтрагентов из обработанной БД в 1С приемник.
    Запись происходит в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.
    
    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) - обязательный параметр
        process_func: Не используется (данные уже обработаны)
    
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА КОНТАКТНЫЕЛИЦАКОНТРАГЕНТОВ ИЗ ОБРАБОТАННОЙ БД В 1С ПРИЕМНИК")
    verbose_print("=" * 80)
    
    if com_object is None:
        print("Ошибка: com_object обязателен")
        return False
    
    # Шаг 1: Подключение к БД
    verbose_print("\n[1/3] Подключение к обработанной базе данных SQLite...")
    db_connection = connect_to_sqlite(sqlite_db_file)
    
    if not db_connection:
        print("Ошибка: Не удалось подключиться к базе данных SQLite")
        return False
    
    # Шаг 2: Чтение элементов из БД
    verbose_print("\n[2/3] Чтение контактныелицаконтрагентов из обработанной БД...")
    items = get_from_db(db_connection, "contractor_contact_persons")
    db_connection.close()
    
    if not items:
        verbose_print("КонтактныеЛица не найдены в базе данных")
        return False
    
    verbose_print(f"Прочитано контактныелицаконтрагентов: {len(items)}")
    
    # Записываем элементы
    verbose_print(f"\nНачинаем запись {len(items)} контактныелицаконтрагентов...")
    written_count = 0
    error_count = 0
    
    for i, item in enumerate(items, 1):
        if i % 10 == 0 or i == 1:
            verbose_print(f"\n[{i}/{len(items)}]")
        
        if _write_item(com_object, item, sqlite_db_file):
            written_count += 1
        else:
            error_count += 1
    
    verbose_print("\n" + "=" * 80)
    verbose_print("ИТОГИ ЗАПИСИ:")
    verbose_print(f"  Успешно записано: {written_count}")
    verbose_print(f"  Ошибок: {error_count}")
    verbose_print(f"  Всего обработано: {len(items)}")
    verbose_print("=" * 80)
    
    return error_count == 0
