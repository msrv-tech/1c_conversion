# -*- coding: utf-8 -*-
"""
Модуль выгрузки физическиелица из обработанной БД в 1С приемник.
Данные в БД уже в формате приемника, запись происходит с сохранением UUID.
Использует базовый writer, который автоматически обрабатывает табличные части.
Для контактной информации используется процедура ДобавитьКонтактнуюИнформацию.
"""

import os
import sys
import sqlite3

# Добавляем родительскую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db
from tools.base_writer import write_catalog_item, find_object_by_uuid
from tools.onec_connector import safe_getattr, call_if_callable
from tools.logger import verbose_print  # noqa: E402

fix_encoding()


def _write_contact_info(com_object, person_uuid, processed_db):
    """
    Записывает табличную часть КонтактнаяИнформация для физлица
    используя функцию ДобавитьКонтактнуюИнформацию из 1С.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        person_uuid: UUID физлица
        processed_db: Путь к обработанной БД
    """
    verbose_print(f"    → Заполнение табличной части КонтактнаяИнформация для физлица {person_uuid[:8]}...")
    
    try:
        # Читаем данные из БД
        conn = sqlite3.connect(processed_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT * FROM individual_persons_contact_info 
                WHERE parent_uuid = ?
                ORDER BY НомерСтроки
            ''', (person_uuid,))
            
            rows = cursor.fetchall()
        except sqlite3.OperationalError:
            # Таблица может отсутствовать
            rows = []
        finally:
            conn.close()
        
        if not rows:
            return
        
        # Получаем объект физлица
        person_obj = find_object_by_uuid(com_object, person_uuid, "Справочник.ФизическиеЛица")
        if not person_obj:
            verbose_print(f"    ⚠ Не найден объект физлица с UUID {person_uuid[:8]}...")
            return
        
        # Получаем объект (не ссылку) для записи
        try:
            person_obj = person_obj.ПолучитьОбъект()
        except Exception:
            pass
        
        # Получаем функцию ДобавитьКонтактнуюИнформацию
        add_contact_info_func = None
        try:
            global_context = safe_getattr(com_object, "ГлобальныйКонтекст", None)
            if global_context:
                add_contact_info_func = safe_getattr(global_context, "ДобавитьКонтактнуюИнформацию", None)
        except Exception:
            pass
        
        if not add_contact_info_func:
            try:
                add_contact_info_func = safe_getattr(com_object, "ДобавитьКонтактнуюИнформацию", None)
            except Exception:
                pass
        
        if not add_contact_info_func:
            verbose_print(f"    ⚠ Функция ДобавитьКонтактнуюИнформацию не найдена")
            return
        
        # Маппинг наименований на имена предопределенных элементов
        predefined_name_mapping = {
            "Телефон мобильный": "ТелефонМобильныйФизическиеЛица",
            "Телефон рабочий": "ТелефонРабочийФизическиеЛица",
            "Телефон домашний": "ТелефонДомашнийФизическиеЛица",
            "Email": "EmailФизическиеЛица",
            "Электронная почта": "EmailФизическиеЛица",
            "Адрес для информирования": "АдресДляИнформированияФизическиеЛица",
            "Адрес регистрации": "АдресРегистрацииФизическиеЛица",
            "Адрес проживания": "АдресПроживанияФизическиеЛица",
            "Другое": "ДругаяИнформацияФизическиеЛица",
        }
        
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
                    continue
                
                # Получаем ВидКонтактнойИнформации (предопределенный элемент)
                вид_ref = None
                if 'Вид_Представление' in row_dict and row_dict['Вид_Представление']:
                    вид_value = row_dict['Вид_Представление']
                    
                    # Очищаем вид_value
                    вид_value_clean = вид_value.replace(" физические лица", "").replace("ФизическиеЛица", "").replace(" физ. лица", "").strip()
                    
                    # Определяем имя предопределенного элемента
                    predefined_name = predefined_name_mapping.get(вид_value_clean, None)
                    
                    if not predefined_name:
                        # Попробуем найти вхождение по ключевым словам
                        вид_value_lower = вид_value_clean.lower()
                        if "мобильный" in вид_value_lower:
                            predefined_name = "ТелефонМобильныйФизическиеЛица"
                        elif "рабочий" in вид_value_lower and "телефон" in вид_value_lower:
                            predefined_name = "ТелефонРабочийФизическиеЛица"
                        elif "домашний" in вид_value_lower and "телефон" in вид_value_lower:
                            predefined_name = "ТелефонДомашнийФизическиеЛица"
                        elif "телефон" in вид_value_lower:
                            predefined_name = "ТелефонМобильныйФизическиеЛица"
                        elif "email" in вид_value_lower or "почта" in вид_value_lower:
                            predefined_name = "EmailФизическиеЛица"
                        elif "регистраци" in вид_value_lower:
                            predefined_name = "АдресРегистрацииФизическиеЛица"
                        elif "проживан" in вид_value_lower:
                            predefined_name = "АдресПроживанияФизическиеЛица"
                        elif "информировани" in вид_value_lower:
                            predefined_name = "АдресДляИнформированияФизическиеЛица"
                        elif "адрес" in вид_value_lower:
                            predefined_name = "АдресПроживанияФизическиеЛица"
                        else:
                            predefined_name = "ДругаяИнформацияФизическиеЛица"
                    
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
                        except Exception:
                            pass
                
                if not вид_ref:
                    verbose_print(f"    ⚠ Пропуск строки: не удалось сопоставить ВидКонтактнойИнформации для '{вид_value}'")
                    continue
                
                # Вызываем функцию ДобавитьКонтактнуюИнформацию
                try:
                    call_if_callable(
                        add_contact_info_func,
                        person_obj,
                        значение_или_представление,
                        вид_ref,
                        None,  # Дата = Неопределено
                        True,  # Замещать = Истина
                        True   # РаспознатьАдрес = Истина
                    )
                    rows_written += 1
                except Exception as e:
                    verbose_print(f"    ⚠ Ошибка при вызове ДобавитьКонтактнуюИнформацию: {e}")
                    continue
                
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при обработке строки: {e}")
                continue
        
        if rows_written > 0:
            verbose_print(f"    ✓ Заполнена контактная информация через ДобавитьКонтактнуюИнформацию ({rows_written} записей)")
        
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при записи контактной информации: {e}")


def _write_item(com_object, item_data, processed_db=None):
    """
    Записывает элемент физлица в 1С с сохранением UUID.
    Использует базовый write_catalog_item, который автоматически обрабатывает табличные части.
    После записи заполняет контактную информацию через ДобавитьКонтактнуюИнформацию.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item_data: Словарь с данными элемента
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        
    Returns:
        True если успешно, False если ошибка
    """
    result = write_catalog_item(
        com_object,
        item_data,
        "ФизическиеЛица",
        "Справочник.ФизическиеЛица",
        ["Код", "Наименование", "ПометкаУдаления", "Комментарий"],
        processed_db=processed_db
    )
    
    # После успешной записи заполняем контактную информацию
    if result and processed_db and 'uuid' in item_data:
        _write_contact_info(com_object, item_data['uuid'], processed_db)
    
    return result


def write_individual_persons_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает физическиелица из обработанной БД в 1С приемник.
    Запись происходит в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.
    
    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) - обязательный параметр
        process_func: Не используется (данные уже обработаны)
    
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ФИЗИЧЕСКИЕЛИЦА ИЗ ОБРАБОТАННОЙ БД В 1С ПРИЕМНИК")
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
    verbose_print("\n[2/3] Чтение физическиелица из обработанной БД...")
    items = get_from_db(db_connection, "individual_persons")
    db_connection.close()
    
    if not items:
        verbose_print("ФизическиеЛица не найдены в базе данных")
        return False
    
    verbose_print(f"Прочитано физическиелица: {len(items)}")
    
    # Шаг 3: Запись в 1С (режим обмена устанавливается для каждого объекта)
    
    # Записываем элементы
    verbose_print(f"\nНачинаем запись {len(items)} физическиелица...")
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

