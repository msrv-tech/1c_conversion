# -*- coding: utf-8 -*-
"""
Модуль выгрузки контрагентов из обработанной БД в 1С приемник.
Данные в БД уже в формате приемника, запись происходит с сохранением UUID.
"""

import json
import os
import sqlite3
import sys

# Добавляем родительскую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite

from tools.writer_utils import get_from_db, get_predefined_element_json
from tools.base_writer import write_catalog_item, setup_exchange_mode
from tools.onec_connector import find_object_by_uuid, safe_getattr, call_if_callable

fix_encoding()


def _write_contractor(com_object, contractor_data, processed_db=None):
    """
    Записывает контрагента в 1С с сохранением UUID.
    Сначала заполняет основные поля и табличную часть, затем записывает один раз.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        contractor_data: Словарь с данными контрагента
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        
    Returns:
        True если успешно, False если ошибка
    """
    from tools.logger import verbose_print
    from tools.onec_connector import find_object_by_uuid, safe_getattr, call_if_callable
    from tools.writer_utils import parse_reference_field, get_reference_by_uuid
    from tools.base_writer import setup_exchange_mode, create_reference_by_uuid
    import json
    
    # Заполняем поле СтранаРегистрации предопределенным элементом "Россия", если оно не заполнено
    country_field = contractor_data.get("СтранаРегистрации", "")
    need_fill_country = False
    
    if not country_field:
        need_fill_country = True
    else:
        # Проверяем, является ли это пустым JSON
        try:
            if isinstance(country_field, str) and country_field.strip().startswith('{'):
                country_json = json.loads(country_field)
                if not country_json.get("uuid") or country_json.get("uuid") == "00000000-0000-0000-0000-000000000000":
                    need_fill_country = True
        except (json.JSONDecodeError, AttributeError):
            # Если не JSON или ошибка парсинга, считаем пустым
            need_fill_country = True
    
    if need_fill_country:
        # Получаем предопределенный элемент "Россия" через функцию с кешированием
        country_json_str = get_predefined_element_json(
            com_object,
            "Справочники.СтраныМира.Россия",
            "Справочник.СтраныМира"
        )
        if country_json_str:
            contractor_data["СтранаРегистрации"] = country_json_str
            try:
                country_data = json.loads(country_json_str)
                verbose_print(f"    ✓ Заполнено поле СтранаРегистрации: {country_data.get('presentation', '')}")
            except Exception:
                pass
    
    # Устанавливаем режим обмена данными
    setup_exchange_mode(com_object)
    
    contractor_uuid = contractor_data.get("uuid", "")
    item_name = contractor_data.get('Наименование', 'Без наименования')
    
    try:
        # Получаем справочник
        catalogs = com_object.Справочники
        catalog_ref = safe_getattr(catalogs, "Контрагенты", None)
        
        if catalog_ref is None:
            verbose_print(f"  ✗ Контрагенты '{item_name}': Справочник 'Контрагенты' не найден")
            return False
        
        # Проверяем, является ли элемент группой
        is_group = False
        if 'ЭтоГруппа' in contractor_data:
            group_value = contractor_data['ЭтоГруппа']
            if isinstance(group_value, bool):
                is_group = group_value
            elif isinstance(group_value, (int, str)):
                is_group = str(group_value).lower() in ('1', 'true', 'истина', 'да')
        
        # Проверяем, существует ли элемент с таким UUID
        item = None
        is_existing = False
        if contractor_uuid and contractor_uuid != "00000000-0000-0000-0000-000000000000":
            ref = find_object_by_uuid(com_object, contractor_uuid, "Справочник.Контрагенты")
            if ref:
                item = ref.ПолучитьОбъект()
                is_existing = True
                verbose_print(f"  → Контрагенты '{item_name}': найден существующий элемент по UUID, загружаем для обновления")
        
        # Если не нашли существующий, создаем новый
        if item is None:
            if is_group:
                create_group_method = safe_getattr(catalog_ref, "СоздатьГруппу", None)
                if create_group_method:
                    item = call_if_callable(create_group_method)
                    verbose_print(f"  → Контрагенты '{item_name}': создана группа")
                else:
                    item = catalog_ref.СоздатьЭлемент()
                    is_group = False
                    verbose_print(f"  → Контрагенты '{item_name}': создан элемент (метод СоздатьГруппу не найден)")
            else:
                item = catalog_ref.СоздатьЭлемент()
            
            # Устанавливаем UUID для нового элемента
            # ВАЖНО: UUID устанавливаем ПЕРВЫМ, так как УстановитьСсылкуНового может сбросить другие поля
            if contractor_uuid and contractor_uuid != "00000000-0000-0000-0000-000000000000":
                try:
                    uuid_obj = com_object.NewObject("УникальныйИдентификатор", contractor_uuid)
                    get_ref_method = safe_getattr(catalog_ref, "ПолучитьСсылку", None)
                    if get_ref_method:
                        ref_by_uuid = call_if_callable(get_ref_method, uuid_obj)
                        if ref_by_uuid:
                            set_ref_method = safe_getattr(item, "УстановитьСсылкуНового", None)
                            if set_ref_method:
                                call_if_callable(set_ref_method, ref_by_uuid)
                                verbose_print(f"  → Контрагенты '{item_name}': установлен UUID {contractor_uuid[:30]}...")
                except Exception as e:
                    verbose_print(f"  → Контрагенты '{item_name}': создан новый элемент (ошибка установки UUID: {e})")
        
        # Устанавливаем режим обмена данными для элемента
        try:
            item_exchange = safe_getattr(item, "ОбменДанными", None)
            if item_exchange:
                item_exchange = call_if_callable(item_exchange)
                if item_exchange:
                    try:
                        item_exchange.Загрузка = True
                    except Exception:
                        pass
        except Exception:
            pass
        
        # Устанавливаем стандартные поля
        standard_fields = ['Код', 'Наименование', 'ПометкаУдаления', 'ИНН', 'КПП', 'НаименованиеПолное', 'Комментарий', 'ОбособленноеПодразделение']
        if not is_existing:
            if 'Код' in contractor_data and contractor_data['Код']:
                item.Код = contractor_data['Код']
        
        for field_name in standard_fields:
            if field_name in contractor_data:
                # Обрабатываем булевы поля
                if field_name in ('ПометкаУдаления', 'ОбособленноеПодразделение'):
                    try:
                        value = contractor_data[field_name]
                        if isinstance(value, bool):
                            bool_value = value
                        elif isinstance(value, (int, str)):
                            bool_value = str(value).lower() in ('1', 'true', 'истина', 'да')
                        else:
                            bool_value = bool(value)
                        setattr(item, field_name, bool_value)
                        if field_name == 'ОбособленноеПодразделение':
                            verbose_print(f"  → Установлено ОбособленноеПодразделение = {bool_value} для '{item_name}'")
                    except Exception as e:
                        verbose_print(f"  ⚠ Ошибка при установке {field_name}: {e}")
                        pass
                elif contractor_data[field_name]:
                    try:
                        setattr(item, field_name, contractor_data[field_name])
                    except Exception:
                        pass
        
        # ВАЖНО: Устанавливаем ссылочные поля ПОСЛЕ UUID (УстановитьСсылкуНового может сбросить их)
        # Сначала Родитель (Группу) - используем create_reference_by_uuid для автоматического создания
        if 'Родитель' in contractor_data and contractor_data['Родитель']:
            try:
                parent_data = parse_reference_field(contractor_data['Родитель'])
                if parent_data:
                    parent_uuid = parent_data.get('uuid', '')
                    parent_type = parent_data.get('type', '')
                    parent_presentation = parent_data.get('presentation', '')
                    if parent_uuid and parent_type and parent_uuid != "00000000-0000-0000-0000-000000000000":
                        # Используем create_reference_by_uuid - создаст элемент, если не найден
                        # ВАЖНО: родитель должен быть записан ДО установки его для дочернего элемента
                        parent_ref = create_reference_by_uuid(
                            com_object,
                            parent_uuid,
                            parent_type,
                            ref_presentation=parent_presentation,
                            source_data=parent_data,
                            processed_db=processed_db
                        )
                        if parent_ref:
                            item.Родитель = parent_ref
                            # Проверяем, что родитель действительно установлен
                            try:
                                check_parent = safe_getattr(item, "Родитель", None)
                                check_parent = call_if_callable(check_parent) if callable(check_parent) else check_parent
                                if check_parent:
                                    parent_name_check = check_parent.Наименование if hasattr(check_parent, 'Наименование') else 'N/A'
                                    verbose_print(f"  → Установлен Родитель для контрагента '{item_name}': {parent_name_check}")
                                else:
                                    verbose_print(f"  ⚠ ПРОБЛЕМА: Родитель не установлен после присваивания для '{item_name}'")
                            except Exception as check_e:
                                verbose_print(f"  ⚠ Ошибка при проверке родителя: {check_e}")
                                verbose_print(f"  → Родитель установлен для контрагента '{item_name}' (проверка не удалась)")
                        else:
                            verbose_print(f"  ⚠ Родитель не найден и не создан по UUID {parent_uuid[:8]}... (тип: {parent_type})")
                    else:
                        verbose_print(f"  ⚠ Родитель: пустой UUID или тип")
                else:
                    verbose_print(f"  ⚠ Родитель: не удалось распарсить данные")
            except Exception as e:
                verbose_print(f"  ⚠ Ошибка при установке Родителя: {e}")
        else:
            verbose_print(f"  → Родитель не указан в данных")
        
        # Затем СтранаРегистрации (если есть) - ищем только как предопределенный элемент с использованием кэша
        if 'СтранаРегистрации' in contractor_data and contractor_data['СтранаРегистрации']:
            try:
                country_data = parse_reference_field(contractor_data['СтранаРегистрации'])
                if country_data:
                    country_type = country_data.get('type', '')
                    country_presentation = country_data.get('presentation', '')
                    if country_type and country_presentation:
                        # Ищем сразу как предопределенный элемент по имени (без поиска по UUID)
                        from tools.writer_utils import get_predefined_element_by_name
                        
                        # Пробуем найти предопределенный элемент по имени (из presentation)
                        # Имя предопределенного элемента может быть в разных форматах, пробуем несколько вариантов
                        predefined_names = [country_presentation]
                        # Если presentation содержит запятую, берем первую часть
                        if ',' in country_presentation:
                            predefined_names.append(country_presentation.split(',')[0].strip())
                        
                        country_ref = None
                        for predefined_name in predefined_names:
                            if predefined_name:
                                country_ref = get_predefined_element_by_name(com_object, country_type, predefined_name)
                                if country_ref:
                                    verbose_print(f"  → СтранаРегистрации найдена как предопределенный элемент '{predefined_name}'")
                                    break
                        
                        if country_ref:
                            item.СтранаРегистрации = country_ref
                            verbose_print(f"  → Установлена СтранаРегистрации для контрагента '{item_name}'")
                        else:
                            verbose_print(f"  ⚠ СтранаРегистрации не найдена как предопределенный элемент '{country_presentation}' (тип: {country_type})")
                    else:
                        verbose_print(f"  ⚠ СтранаРегистрации: пустой тип или представление")
                else:
                    verbose_print(f"  ⚠ СтранаРегистрации: не удалось распарсить данные")
            except Exception as e:
                verbose_print(f"  ⚠ Ошибка при установке СтранаРегистрации: {e}")
        else:
            verbose_print(f"  → СтранаРегистрации не указана в данных")
        
        # Устанавливаем остальные поля (ссылочные, перечисления и прочие)
        for field_name, field_value in contractor_data.items():
            if field_name in ('uuid', 'Ссылка', 'ЭтоГруппа', 'Родитель', 'СтранаРегистрации') or field_name in standard_fields:
                continue
            if field_name.endswith('_UUID') or field_name.endswith('_Представление') or field_name.endswith('_Тип'):
                continue
            
            if not field_value:
                continue
            
            # Проверяем, является ли поле перечислением
            # Сначала проверяем, не является ли это JSON с типом перечисления
            enum_string = None
            if isinstance(field_value, str) and field_value.strip().startswith('{'):
                try:
                    json_data = json.loads(field_value)
                    if isinstance(json_data, dict):
                        json_type = json_data.get('type', '')
                        if json_type and json_type.startswith("Перечисление."):
                            # Преобразуем JSON перечисления в строку перечисления
                            enum_name = json_type.replace("Перечисление.", "")
                            enum_value = json_data.get('presentation', '')
                            if enum_value:
                                # Пробуем найти значение перечисления по представлению
                                # Формат: "Перечисление.ИмяПеречисления.Значение"
                                enum_string = f"Перечисление.{enum_name}.{enum_value}"
                except (json.JSONDecodeError, ValueError):
                    pass
            
            # Если не нашли в JSON, проверяем строку напрямую
            if not enum_string and isinstance(field_value, str) and field_value.startswith("Перечисление."):
                enum_string = field_value
            
            # Если нашли перечисление, обрабатываем его
            if enum_string:
                from tools.base_writer import _get_enum_from_string
                enum_obj = _get_enum_from_string(com_object, enum_string)
                if enum_obj:
                    try:
                        setattr(item, field_name, enum_obj)
                        verbose_print(f"  → Установлено перечисление {field_name} = {enum_string} для контрагента '{item_name}'")
                    except Exception as e:
                        verbose_print(f"  ⚠ Ошибка при установке перечисления {field_name} = {enum_string}: {e}")
                else:
                    verbose_print(f"  ⚠ Не удалось получить объект перечисления для {field_name} = {enum_string}")
                continue
            
            # Парсим ссылочные поля
            ref_data = parse_reference_field(field_value)
            if ref_data:
                ref_uuid = ref_data.get('uuid', '')
                ref_type = ref_data.get('type', '')
                
                if ref_uuid and ref_type and ref_uuid != "00000000-0000-0000-0000-000000000000":
                    ref_obj = get_reference_by_uuid(com_object, ref_uuid, ref_type)
                    if ref_obj:
                        try:
                            setattr(item, field_name, ref_obj)
                            verbose_print(f"  → Установлено поле {field_name} для контрагента '{item_name}'")
                        except Exception as e:
                            verbose_print(f"  ⚠ Ошибка при установке поля {field_name}: {e}")
        
        # Заполняем табличные части ДО записи
        if processed_db and contractor_uuid:
            _write_contact_info_tabular_section(com_object, item, contractor_uuid, processed_db)
        
        # Записываем контрагента один раз (после заполнения всех полей и табличных частей)
        item.Записать()
        verbose_print(f"  ✓ Контрагенты '{item_name}': записан (поля: {len(standard_fields)}, табличная часть заполнена)")
        
        # Сохраняем информацию о записанном объекте в БД (filled=True - полная запись через основной обработчик)
        if contractor_uuid and contractor_uuid != "00000000-0000-0000-0000-000000000000":
            try:
                import sqlite3
                from tools.reference_objects import get_reference_objects_db_path, save_reference_object
                refs_db_path = get_reference_objects_db_path()
                conn = sqlite3.connect(refs_db_path)
                
                # Получаем полные данные из processed_db, если доступны
                source_data = None
                if processed_db:
                    try:
                        source_conn = sqlite3.connect(processed_db)
                        source_cursor = source_conn.cursor()
                        
                        # Ищем в таблице contractors
                        source_cursor.execute("SELECT * FROM contractors WHERE uuid = ?", (contractor_uuid,))
                        row = source_cursor.fetchone()
                        if row:
                            column_names = [desc[0] for desc in source_cursor.description]
                            source_data = {}
                            for i, col_name in enumerate(column_names):
                                source_data[col_name] = row[i]
                        
                        source_conn.close()
                    except Exception:
                        pass
                
                # Если source_data не получено, используем contractor_data
                if not source_data:
                    source_data = contractor_data
                
                save_reference_object(
                    conn,
                    contractor_uuid,
                    "Справочник.Контрагенты",
                    item_name,
                    source_data,
                    filled=True,  # Полная запись через основной обработчик
                    parent_type="catalog",
                    parent_name="Контрагенты",
                    parent_uuid="",
                    field_name=""
                )
                conn.close()
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при сохранении информации о записанном объекте: {e}")
        
        # Проверяем родителя после записи
        try:
            saved_parent = safe_getattr(item, "Родитель", None)
            if saved_parent:
                saved_parent = call_if_callable(saved_parent) if callable(saved_parent) else saved_parent
                if saved_parent:
                    saved_parent_name = saved_parent.Наименование if hasattr(saved_parent, 'Наименование') else 'N/A'
                    verbose_print(f"  ✓ Родитель сохранен после записи: {saved_parent_name}")
                else:
                    verbose_print(f"  ⚠ ПРОБЛЕМА: Родитель НЕ сохранен после записи для '{item_name}'")
            else:
                verbose_print(f"  ⚠ ПРОБЛЕМА: Родитель отсутствует после записи для '{item_name}'")
        except Exception as check_e:
            verbose_print(f"  ⚠ Ошибка при проверке родителя после записи: {check_e}")
        
        # Создаем банковские счета (элементы справочника, не табличная часть)
        # Получаем ссылку на контрагента после записи для установки владельца
        if processed_db and contractor_uuid:
            try:
                # Получаем ссылку через UUID (надежнее, чем через item.Ссылка)
                uuid_obj = com_object.NewObject("УникальныйИдентификатор", contractor_uuid)
                catalogs = com_object.Справочники
                catalog_ref = safe_getattr(catalogs, "Контрагенты", None)
                contractor_ref = None
                
                if catalog_ref:
                    get_ref_method = safe_getattr(catalog_ref, "ПолучитьСсылку", None)
                    if get_ref_method:
                        contractor_ref = call_if_callable(get_ref_method, uuid_obj)
                        if contractor_ref:
                            verbose_print(f"    → Ссылка на контрагента получена через UUID, создаем банковские счета...")
                
                if contractor_ref:
                    _write_bank_accounts_tabular_section(com_object, contractor_ref, contractor_uuid, processed_db)
                else:
                    verbose_print(f"    ⚠ Не удалось получить ссылку на контрагента для банковских счетов (UUID: {contractor_uuid[:8]}...)")
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при создании банковских счетов: {e}")
                import traceback
                traceback.print_exc()
        
        return True
        
    except Exception as e:
        verbose_print(f"  ✗ Контрагенты '{item_name}': ошибка при записи: {e}")
        import traceback
        traceback.print_exc()
        return False


def _write_contact_info_tabular_section(com_object, contractor_obj, contractor_uuid, processed_db):
    """
    Записывает контактную информацию для контрагента
    используя функцию ДобавитьКонтактнуюИнформацию из 1С.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        contractor_obj: Объект контрагента
        contractor_uuid: UUID контрагента
        processed_db: Путь к обработанной БД
    """
    from tools.logger import verbose_print
    from tools.onec_connector import safe_getattr, call_if_callable
    import sqlite3
    
    verbose_print(f"    → Заполнение контактной информации для контрагента {contractor_uuid[:8]}...")
    
    try:
        # Очищаем существующую контактную информацию перед заполнением
        try:
            ci_ts = safe_getattr(contractor_obj, "КонтактнаяИнформация", None)
            if ci_ts:
                clear_method = safe_getattr(ci_ts, "Очистить", None)
                if clear_method:
                    call_if_callable(clear_method)
                    verbose_print(f"    ✓ Табличная часть КонтактнаяИнформация очищена")
        except Exception as e:
            verbose_print(f"    ⚠ Ошибка при очистке контактной информации: {e}")

        # Получаем функцию ДобавитьКонтактнуюИнформацию
        add_contact_info_func = None
        try:
            # Сначала ищем в общем модуле УправлениеКонтактнойИнформацией
            uk_module = safe_getattr(com_object, "УправлениеКонтактнойИнформацией", None)
            if uk_module:
                add_contact_info_func = safe_getattr(uk_module, "ДобавитьКонтактнуюИнформацию", None)
        except Exception:
            pass
            
        if not add_contact_info_func:
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
            
        # Читаем данные из БД
        conn = sqlite3.connect(processed_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT * FROM contractors_contact_info 
                WHERE parent_uuid = ?
                ORDER BY НомерСтроки
            ''', (contractor_uuid,))
            
            rows = cursor.fetchall()
        except sqlite3.OperationalError:
            rows = []
        finally:
            conn.close()
            
        if not rows:
            return
            
        # Маппинг наименований на имена предопределенных элементов для Контрагентов
        predefined_name_mapping = {
            "Юридический адрес": "ЮрАдресКонтрагента",
            "Фактический адрес": "ФактАдресКонтрагента",
            "Почтовый адрес": "ПочтовыйАдресКонтрагента",
            "Телефон": "ТелефонКонтрагента",
            "Email": "EmailКонтрагенты",
            "Электронная почта": "EmailКонтрагенты",
            "Факс": "ФаксКонтрагенты",
            "Другое": "ДругаяИнформацияКонтрагенты",
            "Сайт": "ВебСтраницаКонтрагента",
            "Веб-страница": "ВебСтраницаКонтрагента",
            "Адрес доставки": "ФактАдресКонтрагента",
        }
        
        # Записываем строки через функцию ДобавитьКонтактнуюИнформацию
        rows_written = 0
        for row in rows:
            try:
                row_dict = dict(row)
                
                # Получаем значение (представление или значение)
                # В 1С ДобавитьКонтактнуюИнформацию ЗначениеИлиПредставление
                значение_или_представление = ""
                
                # Приоритет отдаем Значению (там может быть XML/JSON со структурой адреса), 
                # если его нет - Представлению
                if 'Значение' in row_dict and row_dict['Значение']:
                    значение_или_представление = row_dict['Значение']
                elif 'Представление' in row_dict and row_dict['Представление']:
                    значение_или_представление = row_dict['Представление']
                
                if not значение_или_представление:
                    continue
                
                # Получаем ВидКонтактнойИнформации (ссылка)
                вид_ref = None
                вид_value = ""
                
                if 'Вид_Представление' in row_dict and row_dict['Вид_Представление']:
                    вид_value = row_dict['Вид_Представление']
                elif 'Вид' in row_dict and row_dict['Вид']:
                    вид_value = row_dict['Вид']
                
                if вид_value:
                    # Очищаем вид_value от лишних слов для более точного поиска
                    вид_value_clean = вид_value.replace(" контрагента", "").replace("Контрагента", "").strip()
                    
                    # Пытаемся найти предопределенный элемент
                    predefined_name = predefined_name_mapping.get(вид_value_clean, None)
                    
                    if not predefined_name:
                        # Попробуем найти вхождение по ключевым словам
                        вид_value_lower = вид_value_clean.lower()
                        if "юридический адрес" in вид_value_lower:
                            predefined_name = "ЮридическийАдресКонтрагента"
                        elif "фактический адрес" in вид_value_lower or "адрес доставки" in вид_value_lower:
                            predefined_name = "ФактическийАдресКонтрагента"
                        elif "почтовый адрес" in вид_value_lower:
                            predefined_name = "ПочтовыйАдресКонтрагента"
                        elif "телефон" in вид_value_lower or "мобильный" in вид_value_lower:
                            predefined_name = "ТелефонКонтрагента"
                        elif "email" in вид_value_lower or "электронная почта" in вид_value_lower or "почта" in вид_value_lower:
                            predefined_name = "EmailКонтрагента"
                        elif "факс" in вид_value_lower:
                            predefined_name = "ФаксКонтрагента"
                        elif "сайт" in вид_value_lower or "веб" in вид_value_lower or "www" in вид_value_lower:
                            predefined_name = "ВебСтраницаКонтрагента"
                        else:
                            predefined_name = "ДругаяИнформацияКонтрагента"
                    
                    if predefined_name:
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
                        contractor_obj,
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
                verbose_print(f"    ⚠ Ошибка при обработке строки КИ: {e}")
                continue
                
        if rows_written > 0:
            verbose_print(f"    ✓ Заполнена контактная информация через ДобавитьКонтактнуюИнформацию ({rows_written} записей)")
            
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при заполнении контактной информации: {e}")



def _write_bank_accounts_tabular_section(com_object, contractor_ref, contractor_uuid, processed_db):
    """
    Создает/находит элементы справочника БанковскиеСчета для контрагента.
    Заполняет UUID, Наименование и Владелец (контрагент).
    Сохраняет в reference_objects как неполностью заполненные объекты.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        contractor_ref: Ссылка на контрагента (владелец)
        contractor_uuid: UUID контрагента
        processed_db: Путь к обработанной БД
    """
    from tools.logger import verbose_print
    from tools.base_writer import create_reference_by_uuid
    from tools.onec_connector import safe_getattr, call_if_callable, find_object_by_uuid
    from tools.writer_utils import parse_reference_field
    import sqlite3
    
    verbose_print(f"    → Создание банковских счетов для контрагента {contractor_uuid[:8]}...")
    
    # Проверяем, что ссылка на контрагента валидна
    if not contractor_ref:
        verbose_print(f"    ⚠ Ссылка на контрагента отсутствует, пропуск создания банковских счетов")
        return
    
    contractor_name = contractor_ref.Наименование if hasattr(contractor_ref, 'Наименование') else 'N/A'
    verbose_print(f"    → Контрагент: {contractor_name}")
    
    try:
        # Читаем данные из БД
        conn = sqlite3.connect(processed_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM contractors_bank_accounts 
            WHERE parent_uuid = ?
            ORDER BY НомерСтроки
        ''', (contractor_uuid,))
        
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            verbose_print(f"    → Нет банковских счетов в БД для контрагента {contractor_uuid[:8]}...")
            return
        
        verbose_print(f"    → Найдено {len(rows)} банковских счетов в БД для контрагента {contractor_uuid[:8]}...")
        # Логируем все счета для отладки
        for idx, row in enumerate(rows, 1):
            row_dict = dict(row)
            bank_account_ref_data = row_dict.get("БанковскийСчет", "")
            if bank_account_ref_data:
                try:
                    ref_data = parse_reference_field(bank_account_ref_data)
                    if ref_data:
                        uuid_val = ref_data.get('uuid', '')
                        presentation_val = ref_data.get('presentation', '')
                        verbose_print(f"      [{idx}] UUID: {uuid_val[:8] if uuid_val else 'ПУСТО'}..., presentation: {presentation_val[:50] if presentation_val else 'ПУСТО'}")
                    else:
                        verbose_print(f"      [{idx}] Не удалось распарсить JSON")
                except Exception as e:
                    verbose_print(f"      [{idx}] Ошибка парсинга: {e}")
            else:
                verbose_print(f"      [{idx}] Поле БанковскийСчет пустое")
        
        # Получаем справочник БанковскиеСчета
        catalogs = com_object.Справочники
        bank_accounts_catalog = safe_getattr(catalogs, "БанковскиеСчета", None)
        if not bank_accounts_catalog:
            verbose_print(f"    ⚠ Справочник БанковскиеСчета не найден")
            return
        
        # Создаем/находим банковские счета
        accounts_created = 0
        accounts_updated = 0
        for row in rows:
            # Преобразуем Row в словарь для удобства
            row_dict = dict(row)
            
            # Все данные берем из JSON поля БанковскийСчет
            bank_account_ref_data = row_dict.get("БанковскийСчет", "")
            if not bank_account_ref_data:
                verbose_print(f"    → Пропуск строки: поле БанковскийСчет пустое")
                continue
            
            # Парсим JSON и получаем все необходимые данные
            try:
                ref_data = parse_reference_field(bank_account_ref_data)
                if not ref_data:
                    verbose_print(f"    → Пропуск строки: не удалось распарсить JSON поля БанковскийСчет")
                    continue
                
                bank_account_uuid = ref_data.get('uuid', '')
                bank_account_name = ref_data.get('presentation', '')  # Может быть пустым
                
                if not bank_account_uuid or bank_account_uuid == "00000000-0000-0000-0000-000000000000":
                    verbose_print(f"    → Пропуск строки: пустой UUID в JSON поля БанковскийСчет")
                    continue
                
                if bank_account_name:
                    verbose_print(f"    → Обработка банковского счета: {bank_account_name} (UUID: {bank_account_uuid[:8]}...)")
                else:
                    verbose_print(f"    → Обработка банковского счета с пустым наименованием (UUID: {bank_account_uuid[:8]}...)")
                
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при парсинге БанковскийСчет: {e}")
                continue
            
            verbose_print(f"    → Обработка банковского счета: {bank_account_name} (UUID: {bank_account_uuid[:8]}...)")
            
            try:
                # Ищем существующий банковский счет по UUID
                bank_account_ref = find_object_by_uuid(
                    com_object,
                    bank_account_uuid,
                    "Справочник.БанковскиеСчета"
                )
                
                if bank_account_ref:
                    # Если найден, проверяем и обновляем владельца
                    bank_account_obj = bank_account_ref.ПолучитьОбъект()
                    if bank_account_obj:
                        try:
                            # Проверяем текущего владельца
                            current_owner = safe_getattr(bank_account_obj, "Владелец", None)
                            current_owner_ref = call_if_callable(current_owner) if callable(current_owner) else current_owner
                            
                            # Устанавливаем владельца, если он не установлен или отличается
                            if not current_owner_ref:
                                if contractor_ref:
                                    bank_account_obj.Владелец = contractor_ref
                                    bank_account_obj.Записать()
                                    verbose_print(f"    ✓ Установлен владелец для существующего банковского счета: {bank_account_name}")
                                else:
                                    verbose_print(f"    ⚠ Ссылка на контрагента отсутствует, владелец не обновлен")
                            else:
                                # Проверяем, совпадает ли владелец с текущим контрагентом
                                current_owner_uuid = None
                                try:
                                    if hasattr(current_owner_ref, 'УникальныйИдентификатор'):
                                        current_owner_uuid = str(current_owner_ref.УникальныйИдентификатор())
                                    elif hasattr(current_owner_ref, 'УникальныйИдентификатор'):
                                        uuid_method = safe_getattr(current_owner_ref, 'УникальныйИдентификатор', None)
                                        if uuid_method:
                                            current_owner_uuid = str(call_if_callable(uuid_method))
                                except Exception:
                                    pass
                                
                                if current_owner_uuid != contractor_uuid:
                                    # Владелец отличается, обновляем
                                    if contractor_ref:
                                        bank_account_obj.Владелец = contractor_ref
                                        bank_account_obj.Записать()
                                        verbose_print(f"    ✓ Обновлен владелец для банковского счета: {bank_account_name} (был другой владелец)")
                                    else:
                                        verbose_print(f"    ⚠ Ссылка на контрагента отсутствует, владелец не обновлен")
                                else:
                                    verbose_print(f"    ✓ Банковский счет уже имеет правильного владельца: {bank_account_name}")
                        except Exception as e:
                            verbose_print(f"    ⚠ Ошибка при обновлении владельца: {e}")
                            import traceback
                            traceback.print_exc()
                    accounts_updated += 1
                    verbose_print(f"    ✓ Найден существующий банковский счет: {bank_account_name}")
                else:
                    # Создаем новый банковский счет
                    try:
                        new_account = bank_accounts_catalog.СоздатьЭлемент()
                        
                        # Устанавливаем режим обмена данными
                        try:
                            item_exchange = safe_getattr(new_account, "ОбменДанными", None)
                            if item_exchange:
                                item_exchange = call_if_callable(item_exchange)
                                if item_exchange:
                                    item_exchange.Загрузка = True
                        except Exception:
                            pass
                        
                        # Устанавливаем UUID (ПЕРВЫМ, так как УстановитьСсылкуНового может сбросить другие поля)
                        try:
                            uuid_obj = com_object.NewObject("УникальныйИдентификатор", bank_account_uuid)
                            get_ref_method = safe_getattr(bank_accounts_catalog, "ПолучитьСсылку", None)
                            if get_ref_method:
                                ref_by_uuid = call_if_callable(get_ref_method, uuid_obj)
                                if ref_by_uuid:
                                    set_ref_method = safe_getattr(new_account, "УстановитьСсылкуНового", None)
                                    if set_ref_method:
                                        call_if_callable(set_ref_method, ref_by_uuid)
                                        verbose_print(f"    → Установлен UUID для банковского счета: {bank_account_uuid[:8]}...")
                        except Exception as e:
                            verbose_print(f"    ⚠ Ошибка при установке UUID: {e}")
                            import traceback
                            traceback.print_exc()
                        
                        # ВАЖНО: Устанавливаем Владелец ПОСЛЕ UUID (УстановитьСсылкуНового может сбросить владельца)
                        owner_set = False
                        try:
                            if contractor_ref:
                                new_account.Владелец = contractor_ref
                                owner_set = True
                                contractor_name = contractor_ref.Наименование if hasattr(contractor_ref, 'Наименование') else 'N/A'
                                verbose_print(f"    → Установлен владелец: {contractor_name}")
                                
                                # Проверяем, что владелец действительно установлен
                                try:
                                    check_owner = safe_getattr(new_account, "Владелец", None)
                                    check_owner = call_if_callable(check_owner) if callable(check_owner) else check_owner
                                    if check_owner:
                                        verbose_print(f"    → Проверка: владелец установлен корректно")
                                    else:
                                        verbose_print(f"    ⚠ Проверка: владелец не установлен после присваивания!")
                                except Exception as check_e:
                                    verbose_print(f"    ⚠ Ошибка при проверке владельца: {check_e}")
                            else:
                                verbose_print(f"    ⚠ Ссылка на контрагента отсутствует, владелец не установлен")
                        except Exception as e:
                            verbose_print(f"    ⚠ Ошибка при установке владельца: {e}")
                            import traceback
                            traceback.print_exc()
                        
                        # Устанавливаем Наименование
                        if bank_account_name:
                            try:
                                new_account.Наименование = bank_account_name
                                verbose_print(f"    → Установлено наименование: {bank_account_name}")
                            except Exception as e:
                                verbose_print(f"    ⚠ Ошибка при установке наименования: {e}")
                        
                        # Проверяем, что владелец установлен перед записью
                        if not owner_set:
                            verbose_print(f"    ⚠ ВНИМАНИЕ: владелец не был установлен перед записью!")
                        
                        # Записываем
                        new_account.Записать()
                        
                        # Проверяем владельца после записи
                        if owner_set:
                            try:
                                # Переоткрываем объект для проверки
                                saved_account_ref = find_object_by_uuid(
                                    com_object,
                                    bank_account_uuid,
                                    "Справочник.БанковскиеСчета"
                                )
                                if saved_account_ref:
                                    saved_account_obj = saved_account_ref.ПолучитьОбъект()
                                    if saved_account_obj:
                                        saved_owner = safe_getattr(saved_account_obj, "Владелец", None)
                                        saved_owner = call_if_callable(saved_owner) if callable(saved_owner) else saved_owner
                                        if saved_owner:
                                            verbose_print(f"    ✓ Банковский счет записан с владельцем (проверено после записи)")
                                        else:
                                            verbose_print(f"    ⚠ Банковский счет записан, но владелец не сохранен!")
                            except Exception as verify_e:
                                verbose_print(f"    ⚠ Ошибка при проверке владельца после записи: {verify_e}")
                        else:
                            verbose_print(f"    ⚠ Банковский счет записан БЕЗ владельца")
                        
                        accounts_created += 1
                        verbose_print(f"    ✓ Создан банковский счет: {bank_account_name} (UUID: {bank_account_uuid[:8]}...)")
                        
                        # Сохраняем в reference_objects как неполностью заполненный
                        try:
                            from tools.reference_objects import save_reference_object, get_reference_objects_db_path
                            import sqlite3
                            refs_db_path = get_reference_objects_db_path()
                            refs_conn = sqlite3.connect(refs_db_path)
                            
                            # Формируем source_data с минимальными данными
                            source_data = {
                                "uuid": bank_account_uuid,
                                "Наименование": bank_account_name,
                                "Владелец_UUID": contractor_uuid,
                            }
                            
                            save_reference_object(
                                refs_conn,
                                bank_account_uuid,
                                "Справочник.БанковскиеСчета",
                                bank_account_name,
                                source_data,
                                filled=False,  # Неполностью заполненный объект
                                parent_type="catalog",
                                parent_name="Контрагенты",
                                parent_uuid=contractor_uuid,
                                field_name="БанковскиеСчета"
                            )
                            refs_conn.close()
                        except Exception as e:
                            verbose_print(f"    ⚠ Ошибка при сохранении в reference_objects: {e}")
                            
                    except Exception as e:
                        verbose_print(f"    ⚠ Ошибка при создании банковского счета: {e}")
                        import traceback
                        traceback.print_exc()
                        continue
                        
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при обработке банковского счета: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        if accounts_created > 0 or accounts_updated > 0:
            verbose_print(f"    ✓ Обработано банковских счетов: создано {accounts_created}, обновлено {accounts_updated}, всего {len(rows)} в БД")
        
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при создании банковских счетов: {e}")
        import traceback
        traceback.print_exc()


def write_contractors_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает контрагентов из обработанной БД в 1С приемник.
    Запись происходит в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.
    
    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) - обязательный параметр
        process_func: Не используется (данные уже обработаны)
    
    Returns:
        True если успешно, False если ошибка
    """
    from tools.logger import verbose_print
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА КОНТРАГЕНТОВ ИЗ ОБРАБОТАННОЙ БД В 1С ПРИЕМНИК")
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
    
    # Шаг 2: Чтение контрагентов из БД
    verbose_print("\n[2/3] Чтение контрагентов из обработанной БД...")
    contractors = get_from_db(db_connection, "contractors")
    db_connection.close()
    
    if not contractors:
        verbose_print("Контрагенты не найдены в базе данных")
        return False
    
    verbose_print(f"Прочитано контрагентов: {len(contractors)}")
    
    # Шаг 3: Запись в 1С
    verbose_print("\n[3/3] Запись в 1С приемник...")
    
    # Устанавливаем режим обмена данными
    setup_exchange_mode(com_object)
    
    # Записываем контрагентов
    verbose_print(f"\nНачинаем запись {len(contractors)} контрагентов...")
    written_count = 0
    error_count = 0
    
    for i, contractor in enumerate(contractors, 1):
        if i % 10 == 0 or i == 1:
            verbose_print(f"\n[{i}/{len(contractors)}]")
        
        if _write_contractor(com_object, contractor, sqlite_db_file):
            written_count += 1
        else:
            error_count += 1
    
    verbose_print(f"\n{'='*80}")
    verbose_print(f"ИТОГИ ЗАПИСИ:")
    verbose_print(f"  Успешно записано: {written_count}")
    verbose_print(f"  Ошибок: {error_count}")
    verbose_print(f"  Всего обработано: {len(contractors)}")
    verbose_print(f"{'='*80}")
    
    return written_count > 0
