# -*- coding: utf-8 -*-
"""
Модуль выгрузки договорыконтрагентов из обработанной БД в 1С приемник.
Данные в БД уже в формате приемника, запись происходит с сохранением UUID.
"""

import os
import sys

# Добавляем родительскую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, get_default_organization_json
from tools.base_writer import write_catalog_item, setup_exchange_mode, find_or_create_reference_by_name
from tools.logger import verbose_print  # noqa: E402
from tools.onec_connector import safe_getattr, call_if_callable

fix_encoding()


def _post_process_contracts(com_object):
    """
    Выполняет постобработку договоров после записи:
    - Заполняет ВидДоговораУХ, если не заполнен
    - Создает версии соглашений для договоров
    
    Выполняет код 1С через Execute с передачей контекста COM-объекта.
    См. https://infostart.ru/1c/articles/309179/
    """
    try:
        verbose_print("  Выполняем постобработку договоров через Execute...")
        
        # Код 1С для выполнения (согласно статье, нужно передать код и контекст)
        execute_code = """Запрос = Новый Запрос("Выбрать Ссылка из Справочник.ДоговорыКонтрагентов где ВерсияСоглашения = Неопределено И Не ЭтоГруппа И ДЛИНАСТРОКИ(Код)>0");
Выборка = Запрос.Выполнить().Выбрать();
СчетчикОбработано = 0;
СчетчикОшибок = 0;
Пока Выборка.Следующий() Цикл
    Попытка
        ДоговорОбъект = Выборка.Ссылка.ПолучитьОбъект();
        ВерсияСоглашенияКоммерческийДоговорОбъект = Документы.ВерсияСоглашенияКоммерческийДоговор.СоздатьДокумент();
        ЗаполнитьЗначенияСвойств(ВерсияСоглашенияКоммерческийДоговорОбъект, ДоговорОбъект,,"Номер");
        ВерсияСоглашенияКоммерческийДоговорОбъект.Заполнить(ДоговорОбъект);
        ВерсияСоглашенияКоммерческийДоговорОбъект.ДоговорКонтрагента = ДоговорОбъект.Ссылка;        
        ВерсияСоглашенияКоммерческийДоговорОбъект.Контрагент = ДоговорОбъект.Владелец;
        ВерсияСоглашенияКоммерческийДоговорОбъект.СуммаНДС = ДоговорОбъект.СуммаНДС;
        ВерсияСоглашенияКоммерческийДоговорОбъект.Сумма = ДоговорОбъект.Сумма;    
        ВерсияСоглашенияКоммерческийДоговорОбъект.ЗаполнитьВычисляемыеРеквизитыПоДаннымДоговора(ДоговорОбъект);    
        Если ДоговорОбъект.ВидСоглашения = Перечисления.ВидыСоглашений.Спецификация Тогда
            ВерсияСоглашенияКоммерческийДоговорОбъект.БазовыйДоговор = ДоговорОбъект.БазовыйДоговор;        
        КонецЕсли;
        
        Если ДоговорОбъект.ПометкаУдаления Тогда
            ВерсияСоглашенияКоммерческийДоговорОбъект.Записать(РежимЗаписиДокумента.Запись);
        Иначе
            ВерсияСоглашенияКоммерческийДоговорОбъект.Записать(РежимЗаписиДокумента.Проведение);
        КонецЕсли;
    Исключение
        СчетчикОшибок = СчетчикОшибок + 1;
        ОшибкаТекст = ОписаниеОшибки();
        Сообщить("Ошибка при обработке договора " + Выборка.Ссылка + ": " + ОшибкаТекст);
        ЖурналРегистрации.ДобавитьСообщениеДляЖурналаРегистрации("СоздатьВерсиюСоглашенияПоДоговорам", УровеньЖурналаРегистрации.Ошибка,,Выборка.Ссылка, ОшибкаТекст);
        Продолжить;
    КонецПопытки;
КонецЦикла;
Сообщить("Обработано договоров: " + СчетчикОбработано + ", ошибок: " + СчетчикОшибок);"""
        
        # Выполняем код через COMExecute
        # Вариант 1: напрямую на объекте базы (как в примере: ОбрабатываемаяБазаДанных.COMExecute(...))
        # Вариант 2: через общий модуль custom_Сервер.COMExecute(...)
        try:
            verbose_print("  Выполняем код через COMExecute...")
            
            # ВАРИАНТ 1: Пробуем вызвать COMExecute напрямую на com_object
            verbose_print("  [Вариант 1] Пробуем com_object.COMExecute(Код) напрямую...")
            try:
                result = com_object.COMExecute(execute_code)
                verbose_print(f"  ✓ Код выполнен успешно через com_object.COMExecute. Результат: {result}")
                return
            except AttributeError:
                verbose_print("  ⚠ Метод COMExecute не найден напрямую на com_object")
            except Exception as e:
                verbose_print(f"  ⚠ Ошибка при выполнении через com_object.COMExecute: {e}")
                import traceback
                verbose_print(f"  Детали:\n{traceback.format_exc()}")
            
            # ВАРИАНТ 2: Пробуем через общий модуль custom_Сервер
            verbose_print("  [Вариант 2] Пробуем через общий модуль custom_Сервер.COMExecute(Код)...")
            
            custom_сервер = None
            
            # Способ 1: напрямую через com_object
            try:
                custom_сервер = com_object.custom_Сервер
                if custom_сервер is not None:
                    verbose_print("    Модуль найден через com_object.custom_Сервер")
            except AttributeError:
                pass
            
            # Способ 2: через ОбщиеМодули
            if custom_сервер is None:
                try:
                    общие_модули = com_object.ОбщиеМодули
                    if общие_модули is not None:
                        custom_сервер = общие_модули.custom_Сервер
                        if custom_сервер is not None:
                            verbose_print("    Модуль найден через com_object.ОбщиеМодули.custom_Сервер")
                except AttributeError:
                    pass
            
            # Способ 3: через Расширения (если модуль в расширении)
            if custom_сервер is None:
                try:
                    расширения = com_object.Расширения
                    if расширения is not None:
                        try:
                            расширение = расширения.custom_Конвертация
                            if расширение is not None:
                                custom_сервер = расширение.custom_Сервер
                                if custom_сервер is not None:
                                    verbose_print("    Модуль найден через Расширения.custom_Конвертация.custom_Сервер")
                        except AttributeError:
                            pass
                except AttributeError:
                    pass
            
            if custom_сервер is None:
                verbose_print("  ⚠ Не удалось найти модуль custom_Сервер")
                verbose_print("  Убедитесь, что:")
                verbose_print("    1. Общий модуль custom_Сервер существует в конфигурации")
                verbose_print("    2. Процедура COMExecute объявлена с ключевым словом Экспорт")
                verbose_print("    3. Модуль доступен через COM-соединение")
                return
            
            # Вызываем процедуру через модуль
            try:
                result = custom_сервер.COMExecute(execute_code)
                verbose_print(f"  ✓ Код выполнен успешно через custom_Сервер.COMExecute. Результат: {result}")
                return
            except AttributeError as e:
                verbose_print(f"  ⚠ Не удалось найти процедуру COMExecute в модуле: {e}")
            except Exception as e:
                verbose_print(f"  ⚠ Ошибка при выполнении через custom_Сервер.COMExecute: {e}")
                import traceback
                verbose_print(f"  Детали:\n{traceback.format_exc()}")
            
        except Exception as e:
            verbose_print(f"  ⚠ Критическая ошибка при выполнении через COMExecute: {e}")
            import traceback
            verbose_print(f"  Детали:\n{traceback.format_exc()}")
        
        except Exception as e:
            verbose_print(f"  ⚠ Ошибка при выполнении через Execute: {e}")
            import traceback
            verbose_print(f"  Детали ошибки:\n{traceback.format_exc()}")
            verbose_print("  Рекомендуется:")
            verbose_print("    1. Убедиться, что метод Execute доступен в COM-объекте")
            verbose_print("    2. Проверить, что код 1С синтаксически корректен")
            verbose_print("    3. Создать процедуру в модуле 1С и вызвать её через Execute")
        
    except Exception as e:
        verbose_print(f"  ⚠ Критическая ошибка при постобработке договоров: {e}")
        import traceback
        verbose_print(f"  Детали:\n{traceback.format_exc()}")
        # Не прерываем выполнение, так как это постобработка


def _write_item(com_object, item_data, processed_db=None):
    """
    Записывает элемент в 1С с сохранением UUID.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item_data: Словарь с данными элемента
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        
    Returns:
        True если успешно, False если ошибка
    """
    # Заполняем поле Организация, если оно не заполнено или пустое
    org_field = item_data.get("Организация", "")
    need_fill = False
    
    if not org_field:
        need_fill = True
    else:
        # Проверяем, является ли это пустым JSON
        try:
            import json
            if isinstance(org_field, str) and org_field.strip().startswith('{'):
                org_json = json.loads(org_field)
                if not org_json.get("uuid") or org_json.get("uuid") == "00000000-0000-0000-0000-000000000000":
                    need_fill = True
        except (json.JSONDecodeError, AttributeError):
            # Если не JSON или ошибка парсинга, считаем пустым
            need_fill = True
    
    if need_fill:
        # Получаем организацию из БД
        org_json_str = get_default_organization_json(processed_db)
        if org_json_str:
            item_data["Организация"] = org_json_str
            try:
                import json
                org_data = json.loads(org_json_str)
                verbose_print(f"    ✓ Заполнено поле Организация: {org_data.get('presentation', '')}")
            except Exception:
                pass
    
    # Заполняем поле ВидДоговораУХ на основе ВидДоговора
    vid_dogovora_field = item_data.get("ВидДоговора", "")
    vid_dogovora_uh_field = item_data.get("ВидДоговораУХ", "")
    
    # Проверяем, нужно ли заполнять ВидДоговораУХ
    need_fill_vid_dogovora_uh = False
    if not vid_dogovora_uh_field:
        need_fill_vid_dogovora_uh = True
    else:
        # Если поле уже в формате предопределенного значения (Справочник.Имя.Код), не перезаписываем
        if isinstance(vid_dogovora_uh_field, str) and vid_dogovora_uh_field.startswith("Справочник."):
            need_fill_vid_dogovora_uh = False
        else:
            # Проверяем, является ли это пустым JSON
            try:
                import json
                if isinstance(vid_dogovora_uh_field, str) and vid_dogovora_uh_field.strip().startswith('{'):
                    vid_uh_json = json.loads(vid_dogovora_uh_field)
                    if not vid_uh_json.get("uuid") or vid_uh_json.get("uuid") == "00000000-0000-0000-0000-000000000000":
                        need_fill_vid_dogovora_uh = True
                else:
                    need_fill_vid_dogovora_uh = True
            except (json.JSONDecodeError, AttributeError):
                need_fill_vid_dogovora_uh = True
    
    if need_fill_vid_dogovora_uh and vid_dogovora_field:
        # Извлекаем значение перечисления из поля ВидДоговора
        enum_value = None
        try:
            import json
            # Пытаемся извлечь значение из JSON
            if isinstance(vid_dogovora_field, str) and vid_dogovora_field.strip().startswith('{'):
                enum_json = json.loads(vid_dogovora_field)
                enum_value = enum_json.get("presentation", "")
            elif isinstance(vid_dogovora_field, str):
                # Если это строка вида "Перечисление.ВидыДоговоровКонтрагентов.СПокупателем"
                if vid_dogovora_field.startswith("Перечисление."):
                    parts = vid_dogovora_field.split(".")
                    if len(parts) >= 3:
                        enum_value = parts[-1]  # Берем последнюю часть (значение перечисления)
                else:
                    enum_value = vid_dogovora_field
        except Exception:
            pass
        
        if enum_value:
            # Маппинг значений перечисления на наименования в справочнике
            # Значения перечисления могут быть без пробелов, а в справочнике - с пробелами
            enum_to_catalog_mapping = {
                "СПокупателем": "С покупателем",
                "СПоставщиком": "С поставщиком",
                "СКомитентом": "С комитентом",
                "СКомиссионером": "С комиссионером",
                "СКомитентомНаЗакупку": "С комитентом на закупку",
                "СКомиссионеромНаЗакупку": "С комиссионером на закупку",
                "СФакторинговойКомпанией": "С факторинговой компанией",
                "СТранспортнойКомпанией": "С транспортной компанией",
                "ЗаемПолученный": "Заем полученный",
                "Прочее": "Прочее",
            }
            
            # Преобразуем значение перечисления в наименование для поиска
            catalog_name = enum_to_catalog_mapping.get(enum_value, enum_value)
            
            # Если не нашли точное совпадение, пробуем преобразовать автоматически
            # (добавляем пробелы перед заглавными буквами, кроме первой)
            if catalog_name == enum_value and len(enum_value) > 1:
                import re
                # Добавляем пробелы перед заглавными буквами
                catalog_name = re.sub(r'(?<!^)(?=[А-ЯЁ])', ' ', enum_value)
            
            # Ищем элемент в справочнике ВидыДоговоровКонтрагентовУХ по наименованию
            try:
                ref_obj = find_or_create_reference_by_name(
                    com_object,
                    "Справочник.ВидыДоговоровКонтрагентовУХ",
                    catalog_name,
                    None,  # ref_uuid
                    None,  # parent_type
                    None,  # parent_name
                    None,  # parent_uuid
                    "ВидДоговораУХ",
                    {},  # source_data
                    processed_db
                )
                
                if ref_obj:
                    # Формируем JSON для поля ВидДоговораУХ
                    try:
                        ref_uuid = None
                        ref_presentation = catalog_name
                        
                        # Пытаемся получить UUID из ссылки
                        try:
                            uuid_attr = safe_getattr(ref_obj, "УникальныйИдентификатор", None)
                            if uuid_attr:
                                uuid_attr = call_if_callable(uuid_attr)
                                if uuid_attr:
                                    uuid_str_attr = safe_getattr(uuid_attr, "Строка", None)
                                    if uuid_str_attr:
                                        ref_uuid = call_if_callable(uuid_str_attr)
                        except Exception:
                            pass
                        
                        # Пытаемся получить представление из ссылки
                        try:
                            presentation_attr = safe_getattr(ref_obj, "Представление", None)
                            if presentation_attr:
                                ref_presentation = call_if_callable(presentation_attr) or catalog_name
                        except Exception:
                            pass
                        
                        vid_uh_json = {
                            "uuid": ref_uuid or "",
                            "presentation": ref_presentation,
                            "type": "Справочник.ВидыДоговоровКонтрагентовУХ"
                        }
                        item_data["ВидДоговораУХ"] = json.dumps(vid_uh_json, ensure_ascii=False)
                        verbose_print(f"    ✓ Заполнено поле ВидДоговораУХ: {ref_presentation} (из перечисления '{enum_value}')")
                    except Exception as e:
                        verbose_print(f"    ⚠ Ошибка при формировании JSON для ВидДоговораУХ: {e}")
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при поиске элемента ВидыДоговоровКонтрагентовУХ '{catalog_name}': {e}")
    
    return write_catalog_item(
        com_object,
        item_data,
        "ДоговорыКонтрагентов",
        "Справочник.ДоговорыКонтрагентов",
        ["Код", "Наименование", "ПометкаУдаления", "Комментарий"],
        processed_db=processed_db
    )


def write_contractor_contracts_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает договорыконтрагентов из обработанной БД в 1С приемник.
    Запись происходит в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.
    
    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) - обязательный параметр
        process_func: Не используется (данные уже обработаны)
    
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ДОГОВОРЫКОНТРАГЕНТОВ ИЗ ОБРАБОТАННОЙ БД В 1С ПРИЕМНИК")
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
    verbose_print("\n[2/3] Чтение договорыконтрагентов из обработанной БД...")
    items = get_from_db(db_connection, "contractor_contracts")
    db_connection.close()
    
    if not items:
        verbose_print("ДоговорыКонтрагентов не найдены в базе данных")
        return False
    
    verbose_print(f"Прочитано договорыконтрагентов: {len(items)}")
    
    # Шаг 3: Подключение к 1С и запись
    setup_exchange_mode(com_object)
    
    # Записываем элементы
    verbose_print(f"\nНачинаем запись {len(items)} договорыконтрагентов...")
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

