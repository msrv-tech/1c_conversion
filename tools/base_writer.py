# -*- coding: utf-8 -*-
"""
Базовые функции для записи данных в 1С.

Содержит универсальную функцию для записи элементов справочников с сохранением UUID.

ИНСТРУКЦИЯ ПО ПОРЯДКУ ЗАПИСИ ОБЪЕКТА В 1С
===========================================

Четкий порядок операций при записи элемента справочника в 1С:

ЭТАП 1: ПОДГОТОВКА И ПОИСК СУЩЕСТВУЮЩЕГО ЭЛЕМЕНТА
-------------------------------------------------
1.1. Получение справочника через com_object.Справочники[ИмяСправочника]
1.2. Определение is_group из item_data['ЭтоГруппа'] (для новых элементов)
1.3. Поиск существующего элемента по UUID через find_object_by_uuid()
     - Если найден: загружаем через ПолучитьОбъект() для обновления
     - Если не найден: переходим к этапу 2

ЭТАП 2: СОЗДАНИЕ НОВОГО ЭЛЕМЕНТА (если не найден существующий)
---------------------------------------------------------------
2.1. Создание элемента/группы:
     - Если is_group = True: catalog_ref.СоздатьГруппу()
     - Если is_group = False: catalog_ref.СоздатьЭлемент()
2.2. Установка UUID через УстановитьСсылкуНового():
     - uuid_obj = com_object.NewObject("УникальныйИдентификатор", uuid_value)
     - ref_by_uuid = catalog_ref.ПолучитьСсылку(uuid_obj)
     - item.УстановитьСсылкуНового(ref_by_uuid)

ЭТАП 3: УСТАНОВКА СТАНДАРТНЫХ ПОЛЕЙ
-----------------------------------
3.1. Установка Код (только для новых элементов, для существующих не меняем)
3.2. Установка стандартных полей:
     - Наименование
     - ПометкаУдаления (с преобразованием в булево)
     - Комментарий
     - Другие стандартные поля из standard_fields
3.3. Установка ЭтоГруппа (только для новых элементов, если указано в item_data)

ЭТАП 4: ЗАГРУЗКА МАППИНГА ПОЛЕЙ
--------------------------------
4.1. Загрузка маппинга из type_mapping.db для данного справочника
4.2. Получение информации о полях: target_field_name, search_method, status

ЭТАП 5: СОЗДАНИЕ НЕПОЛНЫХ ССЫЛОЧНЫХ ОБЪЕКТОВ
--------------------------------------------
5.1. Для ссылочных полей (например, Родитель), которые не найдены по UUID:
     - Использование create_reference_by_uuid() для создания объектов с минимальными данными
     - Созданные объекты сохраняются в reference_objects БД с filled=False
     - Это позволяет создать структуру ссылок до полного заполнения данных

ЭТАП 6: УСТАНОВКА ССЫЛОЧНЫХ ПОЛЕЙ (включая Владелец)
---------------------------------------------------
6.1. Для каждого ссылочного поля из item_data (включая Владелец):
     - Парсинг JSON ссылочного поля через parse_reference_field()
     - Извлечение: uuid, type, presentation
6.2. Поиск ссылочного объекта:
     - Если search_method = "string_to_reference_by_name": 
       поиск/создание через find_or_create_reference_by_name()
     - Иначе: поиск по UUID через get_reference_by_uuid()
6.3. Поиск предопределенных элементов (если не найден по UUID):
     - Использование get_predefined_element_by_name() с кэшированием
     - Для полей типа СтранаРегистрации - только предопределенные элементы
6.4. Создание неполного объекта (если не найден и не предопределенный):
     - Использование create_reference_by_uuid() для создания с минимальными данными
6.5. Установка ссылочного поля: item.Поле = ref_obj
     (ВАЖНО: Владелец обрабатывается так же, как и все остальные ссылочные поля)

ЭТАП 7: УСТАНОВКА ПЕРЕЧИСЛЕНИЙ
-------------------------------
7.1. Для полей со значениями вида "Перечисление.Имя.Значение":
     - Преобразование через _get_enum_from_string()
     - Установка: item.Поле = enum_obj

ЭТАП 8: УСТАНОВКА ОБЫЧНЫХ ПОЛЕЙ
--------------------------------
8.1. Для остальных полей (строки, числа, даты, булевы):
     - Преобразование булевых значений из строк ("True"/"False" -> True/False)
     - Установка: item.Поле = значение

ЭТАП 9: ЗАПОЛНЕНИЕ ТАБЛИЧНЫХ ЧАСТЕЙ
------------------------------------
9.1. Заполнение табличных частей элемента:
     - Получение табличной части: item.ТабличнаяЧасть
     - Для каждой строки из БД: добавление через Добавить() или Найти()
     - Заполнение полей строки табличной части
     (ВАЖНО: табличные части заполняются ДО записи элемента)

ЭТАП 10: УСТАНОВКА РЕЖИМА ОБМЕНА ДАННЫМИ
----------------------------------------
10.1. Установка режима обмена данными перед записью:
      - item.ОбменДанными.Загрузка = True
      (ВАЖНО: устанавливается для каждого конкретного объекта перед записью, 
       применяется как для новых, так и для существующих элементов)

ЭТАП 11: ЗАПИСЬ ЭЛЕМЕНТА
------------------------
11.1. Запись элемента: item.Записать()
      (ВАЖНО: запись выполняется ОДИН РАЗ после заполнения всех полей и табличных частей)

ЭТАП 12: СОХРАНЕНИЕ В БД reference_objects
------------------------------------------
12.1. Сохранение информации о записанном объекте:
      - UUID, тип, наименование
      - source_data (полные данные из processed_db)
      - filled=True (полная запись через основной обработчик)


ВАЖНЫЕ ПРИНЦИПЫ:
----------------
1. UUID устанавливается ТОЛЬКО для новых элементов через УстановитьСсылкуНового()
2. Владелец обрабатывается так же, как и все остальные ссылочные поля (в общем цикле)
3. Режим обмена данными устанавливается для каждого конкретного объекта ПЕРЕД записью
4. Табличные части заполняются ДО записи элемента
5. Элемент записывается ОДИН РАЗ после заполнения всех данных
6. create_reference_by_uuid() используется для создания неполных объектов (Родитель, Владелец и т.п.)
7. Предопределенные элементы ищутся через get_predefined_element_by_name() с кэшированием

КОГДА ИСПОЛЬЗОВАТЬ write_catalog_item() И КОГДА ПИСАТЬ СВОЮ ЛОГИКУ
====================================================================

ИСПОЛЬЗУЙТЕ write_catalog_item() КОГДА:
---------------------------------------
1. Справочник не имеет табличных частей или табличные части не требуют специальной обработки
2. Все поля справочника могут быть обработаны стандартным способом через маппинг
3. Не требуется дополнительная логика после записи элемента (например, создание связанных объектов)
4. Порядок записи соответствует стандартному плану (см. этапы выше)

Примеры использования write_catalog_item():
- Справочники без табличных частей: Банки, Валюты, ЕдиницыИзмерения, Склады и т.п.
- Простые справочники с базовыми полями и ссылками

ПИШИТЕ СВОЮ ЛОГИКУ (_write_item) КОГДА:
----------------------------------------
1. Справочник имеет табличные части, требующие специальной обработки:
   - Данные табличной части хранятся в отдельных таблицах БД
   - Требуется сложная логика заполнения строк табличной части
   - Нужна обработка ссылочных полей в строках табличной части
   
2. Требуется дополнительная логика после записи основного элемента:
   - Создание связанных объектов (например, банковские счета для контрагентов)
   - Установка специальных полей, которые не обрабатываются стандартным маппингом
   - Выполнение дополнительных проверок или операций
   
3. Нужен особый порядок операций, отличный от стандартного:
   - Установка специальных перечислений перед другими полями
   - Обработка владельца особым образом
   - Заполнение табличных частей в определенном порядке

Примеры кастомной логики:
- Контрагенты: есть табличная часть КонтактнаяИнформация + создание банковских счетов после записи
- КонтактныеЛица: есть табличная часть КонтактнаяИнформация с особым форматом данных
- Справочники с множественными табличными частями, требующими координации

ВАЖНО:
------
- Если используете write_catalog_item(), следуйте его сигнатуре и передавайте все необходимые параметры
- Если пишете свою логику, ОБЯЗАТЕЛЬНО следуйте плану из 12 этапов выше
- НЕ смешивайте подходы: либо полностью используйте write_catalog_item(), либо полностью свою логику
- При написании своей логики можно использовать вспомогательные функции из base_writer:
  * create_reference_by_uuid() - для создания неполных ссылочных объектов
  * find_or_create_reference_by_name() - для поиска/создания по наименованию
  * _get_enum_from_string() - для преобразования перечислений
"""

from __future__ import annotations

import json
from typing import Dict, List, Optional

from tools.onec_connector import safe_getattr, call_if_callable, find_object_by_uuid
from tools.writer_utils import parse_reference_field, get_reference_by_uuid
from tools.reference_objects import save_reference_object, get_reference_objects_db_path

# Кэш для поиска элементов справочников по наименованию
# Ключ: (ref_type, ref_presentation), Значение: ссылка на элемент
_reference_by_name_cache = {}

# Кэш для поиска элементов справочников по полному наименованию
# Ключ: (ref_type, ref_full_name), Значение: ссылка на элемент
_reference_by_full_name_cache = {}


def clear_reference_by_name_cache():
    """Очищает кэш поиска по наименованию. Полезно при длительных операциях."""
    global _reference_by_name_cache
    _reference_by_name_cache.clear()


def clear_reference_by_full_name_cache():
    """Очищает кэш поиска по полному наименованию. Полезно при длительных операциях."""
    global _reference_by_full_name_cache
    _reference_by_full_name_cache.clear()


def _find_account_by_code_in_receiver(com_object, account_code: str, account_type: str):
    """
    Ищет счет плана счетов в приемнике по коду.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        account_code: Код счета (например, "26", "26.01")
        account_type: Тип счета (например, "ПланСчетов.Хозрасчетный", "ПланСчетов.Налоговый")
        
    Returns:
        COM-объект счета или None
    """
    if not com_object or not account_code or not account_type:
        return None
    
    from tools.logger import verbose_print
    from tools.onec_connector import safe_getattr, call_if_callable
    
    try:
        # Определяем тип плана счетов (Хозрасчетный, Налоговый и т.д.)
        if account_type.startswith("ПланСчетов."):
            plan_type = account_type.replace("ПланСчетов.", "")
        elif account_type.startswith("ChartOfAccountsRef."):
            plan_type = account_type.replace("ChartOfAccountsRef.", "")
        else:
            verbose_print(f"    ⚠ Неизвестный тип счета: '{account_type}' (ожидается ПланСчетов.* или ChartOfAccountsRef.*)")
            return None
        
        verbose_print(f"    → Поиск счета '{account_code}' в плане счетов '{plan_type}' (исходный тип: {account_type})")
        
        # Получаем план счетов
        chart_of_accounts = safe_getattr(com_object, "ПланыСчетов", None)
        if not chart_of_accounts:
            verbose_print(f"    ⚠ Объект 'ПланыСчетов' не найден в приемнике")
            return None
        
        # Пробуем найти план счетов по имени
        plan_ref = safe_getattr(chart_of_accounts, plan_type, None)
        if not plan_ref:
            # Пробуем альтернативные имена
            alt_names = {
                "Хозрасчетный": ["Хозрасчетный", "Хозяйственный", "Основной"],
                "Налоговый": ["Налоговый", "НУ", "НалоговыйУчет"]
            }
            if plan_type in alt_names:
                for alt_name in alt_names[plan_type]:
                    plan_ref = safe_getattr(chart_of_accounts, alt_name, None)
                    if plan_ref:
                        verbose_print(f"    → Найден план счетов '{alt_name}' (вместо '{plan_type}')")
                        plan_type = alt_name
                        break
            
            if not plan_ref:
                # Пробуем получить список доступных планов счетов для отладки
                try:
                    available_plans = []
                    for attr in dir(chart_of_accounts):
                        if not attr.startswith('_') and not callable(getattr(chart_of_accounts, attr, None)):
                            try:
                                plan_obj = getattr(chart_of_accounts, attr, None)
                                if plan_obj:
                                    available_plans.append(attr)
                            except:
                                pass
                    verbose_print(f"    ⚠ План счетов '{plan_type}' не найден. Доступные планы: {', '.join(available_plans[:10])}")
                except:
                    verbose_print(f"    ⚠ План счетов '{plan_type}' не найден")
                return None
        
        # Используем метод НайтиПоКоду для поиска счета по коду
        # Это более простой и надежный способ, чем запрос
        try:
            # Вызываем метод НайтиПоКоду на плане счетов
            found_ref = call_if_callable(plan_ref.НайтиПоКоду, str(account_code))
            
            if found_ref:
                verbose_print(f"    ✓ Найден счет плана счетов по коду '{account_code}' в {plan_type}")
                return found_ref
        except Exception as e:
            verbose_print(f"    ⚠ Ошибка при поиске счета по коду '{account_code}' через НайтиПоКоду: {e}")
        
        return None
        
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при поиске счета плана счетов по коду '{account_code}': {e}")
        return None


def _find_catalog_by_code_in_receiver(com_object, catalog_code: str, catalog_type: str):
    """
    Ищет элемент справочника в приемнике по коду через НайтиПоКоду.
    
    Используется для справочников, где код является стабильным идентификатором
    (например, ОбщероссийскийКлассификаторОсновныхФондов).
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        catalog_code: Код элемента справочника
        catalog_type: Тип справочника (например, "Справочник.ОбщероссийскийКлассификаторОсновныхФондов")
        
    Returns:
        COM-объект ссылки на элемент или None
    """
    if not com_object or not catalog_type:
        return None
    catalog_code = (catalog_code or "").strip()
    if not catalog_code:
        return None
    
    from tools.logger import verbose_print
    from tools.onec_connector import safe_getattr, call_if_callable
    
    try:
        if not catalog_type.startswith("Справочник."):
            verbose_print(f"    ⚠ _find_catalog_by_code_in_receiver: ожидается Справочник.*, получен '{catalog_type}'")
            return None
        
        catalog_name = catalog_type.replace("Справочник.", "").strip()
        if not catalog_name:
            return None
        
        verbose_print(f"    → Поиск в справочнике '{catalog_name}' по коду '{catalog_code}'")
        
        catalogs = safe_getattr(com_object, "Справочники", None)
        if not catalogs:
            verbose_print(f"    ⚠ Объект 'Справочники' не найден в приемнике")
            return None
        
        catalog_ref = safe_getattr(catalogs, catalog_name, None)
        if not catalog_ref:
            verbose_print(f"    ⚠ Справочник '{catalog_name}' не найден в приемнике")
            return None
        
        try:
            found_ref = call_if_callable(catalog_ref.НайтиПоКоду, str(catalog_code))
            if found_ref:
                verbose_print(f"    ✓ Найден элемент справочника '{catalog_name}' по коду '{catalog_code}'")
                return found_ref
            verbose_print(f"    ⚠ Элемент справочника '{catalog_name}' не найден в приемнике по коду '{catalog_code}'")
        except Exception as e:
            verbose_print(f"    ⚠ Ошибка при поиске по коду '{catalog_code}' через НайтиПоКоду: {e}")
        
        return None
        
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при поиске справочника по коду '{catalog_code}': {e}")
        return None


def find_or_create_reference_by_name(
    com_object,
    ref_type: str,
    ref_presentation: str,
    ref_uuid: Optional[str] = None,
    parent_type: str = "",
    parent_name: str = "",
    parent_uuid: str = "",
    field_name: str = "",
    source_data: Optional[Dict] = None,
    processed_db: Optional[str] = None,
) -> Optional[object]:
    """
    Находит или создает элемент справочника по наименованию.
    Используется для полей с search_method = "string_to_reference_by_name".
    Использует кэш для избежания повторных запросов к базе.
    
    Args:
        com_object: COM-объект подключения к 1С
        ref_type: Тип объекта (например, "Справочник.РазделыИнвестиционныхПрограмм")
        ref_presentation: Наименование для поиска/создания
        ref_uuid: UUID для нового элемента (если нужно создать)
        parent_type: Тип родительского объекта (для логирования)
        parent_name: Имя родительского объекта (для логирования)
        parent_uuid: UUID родительского объекта (для логирования)
        field_name: Имя поля (для логирования)
        source_data: Данные источника (для определения is_group)
        processed_db: Путь к обработанной БД
    
    Returns:
        Ссылка на найденный или созданный элемент справочника, или None
    """
    from tools.logger import verbose_print
    
    # Нормализуем тип: СправочникСсылка. -> Справочник.
    if ref_type.startswith("СправочникСсылка."):
        ref_type = ref_type.replace("СправочникСсылка.", "Справочник.", 1)
    
    if not ref_presentation or not ref_type.startswith("Справочник."):
        return None
    
    
    # Проверяем кэш
    cache_key = (ref_type, ref_presentation)
    if cache_key in _reference_by_name_cache:
        cached_ref = _reference_by_name_cache[cache_key]
        # Проверяем, что ссылка еще валидна (опционально, можно пропустить)
        try:
            # Простая проверка - пытаемся получить наименование
            _ = cached_ref.Наименование
            verbose_print(f"    ✓ Использован кэш для {ref_type} '{ref_presentation}'")
            return cached_ref
        except Exception:
            # Если ссылка невалидна, удаляем из кэша и продолжаем поиск
            del _reference_by_name_cache[cache_key]
    
    catalog_name = ref_type.replace("Справочник.", "")
    
    try:
        catalogs = com_object.Справочники
        catalog_ref = safe_getattr(catalogs, catalog_name, None)
        if not catalog_ref:
            verbose_print(f"    ⚠ Справочник '{catalog_name}' не найден")
            return None
        
        # Сначала пробуем найти по наименованию
        # Пробуем использовать метод НайтиПоНаименованию, если он есть
        find_by_name_method = safe_getattr(catalog_ref, "НайтиПоНаименованию", None)
        if find_by_name_method:
            try:
                found_ref = call_if_callable(find_by_name_method, ref_presentation, True)  # True = точное совпадение
                if found_ref:
                    # Проверяем, что найденный элемент действительно имеет нужное наименование
                    try:
                        found_name = str(safe_getattr(found_ref, "Наименование", ""))
                        if not found_name or found_name != ref_presentation:
                            # Найден элемент с другим наименованием или без наименования - это не то, что нужно
                            if found_name:
                                verbose_print(f"    ⚠ Найден элемент с другим наименованием: '{found_name}' != '{ref_presentation}'")
                            else:
                                verbose_print(f"    ⚠ Найденный элемент не имеет наименования (невалидная ссылка)")
                            found_ref = None  # Продолжаем поиск через запрос
                        else:
                            verbose_print(f"    ✓ Найден существующий элемент {ref_type} '{ref_presentation}' по наименованию (метод НайтиПоНаименованию)")
                            # Сохраняем в кэш
                            _reference_by_name_cache[cache_key] = found_ref
                            return found_ref
                    except Exception as e:
                        verbose_print(f"    ⚠ Ошибка при проверке наименования найденного элемента: {e}")
                        found_ref = None  # Продолжаем поиск через запрос
            except Exception as e:
                verbose_print(f"    ⚠ Метод НайтиПоНаименованию не сработал: {e}")
                pass
        
        # Если метод НайтиПоНаименованию не найден или не сработал, используем запрос
        query = com_object.NewObject("Запрос")
        alias = catalog_name
        query.Текст = f"""
            ВЫБРАТЬ ПЕРВЫЕ 1
                {alias}.Ссылка КАК Ссылка
            ИЗ
                Справочник.{catalog_name} КАК {alias}
            ГДЕ
                {alias}.Наименование = &Наименование
                И НЕ {alias}.ПометкаУдаления
        """
        # Устанавливаем параметр
        try:
            param = query.Параметры.НайтиПоИмени("Наименование")
            if param:
                param.Значение = ref_presentation
            else:
                query.УстановитьПараметр("Наименование", ref_presentation)
        except Exception:
            query.УстановитьПараметр("Наименование", ref_presentation)
        
        try:
            result = query.Выполнить()
            selection = result.Выбрать()
            
            if selection.Следующий():
                found_ref = selection.Получить("Ссылка")
                if found_ref:
                    verbose_print(f"    ✓ Найден существующий элемент {ref_type} '{ref_presentation}' по наименованию (через запрос)")
                    # Сохраняем в кэш
                    _reference_by_name_cache[cache_key] = found_ref
                    return found_ref
        except Exception as e:
            verbose_print(f"    ⚠ Ошибка при поиске по наименованию через запрос: {e}")
            # Продолжаем и создаем новый элемент
        
        # Если не нашли, создаем новый элемент
        if not ref_uuid:
            import uuid
            ref_uuid = str(uuid.uuid4())
        
        verbose_print(f"    → Создаем новый элемент {ref_type} '{ref_presentation}' (не найден по наименованию)")
        
        # Определяем, является ли объект группой
        is_group = False
        if source_data and 'ЭтоГруппа' in source_data:
            group_value = source_data['ЭтоГруппа']
            if isinstance(group_value, bool):
                is_group = group_value
            elif isinstance(group_value, (int, str)):
                is_group = str(group_value).lower() in ('1', 'true', 'истина', 'да')
        
        # Создаем новый элемент или группу
        if is_group:
            create_group_method = safe_getattr(catalog_ref, "СоздатьГруппу", None)
            if create_group_method:
                new_item = call_if_callable(create_group_method)
                verbose_print(f"    → Создана группа {ref_type} '{ref_presentation}'")
            else:
                new_item = catalog_ref.СоздатьЭлемент()
                verbose_print(f"    → Создан элемент {ref_type} '{ref_presentation}' (метод СоздатьГруппу не найден)")
        else:
            new_item = catalog_ref.СоздатьЭлемент()
        
        # Устанавливаем режим обмена данными
        try:
            item_exchange = safe_getattr(new_item, "ОбменДанными", None)
            if item_exchange:
                item_exchange = call_if_callable(item_exchange)
                if item_exchange:
                    try:
                        item_exchange.Загрузка = True
                    except Exception:
                        pass
        except Exception:
            pass
        
        # Устанавливаем наименование
        new_item.Наименование = ref_presentation
        
        # Устанавливаем UUID
        try:
            item_exchange = safe_getattr(new_item, "ОбменДанными", None)
            if item_exchange:
                item_exchange = call_if_callable(item_exchange)
                if item_exchange:
                    try:
                        item_exchange.УникальныйИдентификатор = ref_uuid
                    except Exception:
                        pass
        except Exception:
            pass
        
        # Записываем
        new_item.Записать()
        
        verbose_print(f"    ✓ Создан новый элемент {ref_type} '{ref_presentation}' с UUID {ref_uuid[:30]}...")
        
        # Возвращаем ссылку на созданный элемент
        new_ref = new_item.Ссылка
        # Сохраняем в кэш
        _reference_by_name_cache[cache_key] = new_ref
        return new_ref
        
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при поиске/создании элемента '{ref_presentation}' в {ref_type}: {e}")
        import traceback
        traceback.print_exc()
        return None


def find_or_create_reference_by_full_name(
    com_object,
    ref_type: str,
    ref_full_name: str,
    ref_uuid: Optional[str] = None,
    parent_type: str = "",
    parent_name: str = "",
    parent_uuid: str = "",
    field_name: str = "",
    source_data: Optional[Dict] = None,
    processed_db: Optional[str] = None,
) -> Optional[object]:
    """
    Находит или создает элемент справочника по полному наименованию (НаименованиеПолное).
    Используется для полей с search_method = "string_to_reference_by_full_name".
    Использует кэш для избежания повторных запросов к базе.
    
    Args:
        com_object: COM-объект подключения к 1С
        ref_type: Тип объекта (например, "Справочник.custom_КлючевыеОбъекты")
        ref_full_name: Полное наименование для поиска/создания
        ref_uuid: UUID для нового элемента (если нужно создать)
        parent_type: Тип родительского объекта (для логирования)
        parent_name: Имя родительского объекта (для логирования)
        parent_uuid: UUID родительского объекта (для логирования)
        field_name: Имя поля (для логирования)
        source_data: Данные источника (для определения is_group)
        processed_db: Путь к обработанной БД
    
    Returns:
        Ссылка на найденный или созданный элемент справочника, или None
    """
    from tools.logger import verbose_print
    
    # Нормализуем тип: СправочникСсылка. -> Справочник.
    if ref_type.startswith("СправочникСсылка."):
        ref_type = ref_type.replace("СправочникСсылка.", "Справочник.", 1)
    
    if not ref_full_name or not ref_type.startswith("Справочник."):
        return None
    
    
    # Проверяем кэш
    cache_key = (ref_type, ref_full_name)
    if cache_key in _reference_by_full_name_cache:
        cached_ref = _reference_by_full_name_cache[cache_key]
        # Проверяем, что ссылка еще валидна (опционально, можно пропустить)
        try:
            # Простая проверка - пытаемся получить полное наименование
            _ = cached_ref.НаименованиеПолное
            verbose_print(f"    ✓ Использован кэш для {ref_type} '{ref_full_name}' (по полному наименованию)")
            return cached_ref
        except Exception:
            # Если ссылка невалидна, удаляем из кэша и продолжаем поиск
            del _reference_by_full_name_cache[cache_key]
    
    catalog_name = ref_type.replace("Справочник.", "")
    
    try:
        catalogs = com_object.Справочники
        catalog_ref = safe_getattr(catalogs, catalog_name, None)
        if not catalog_ref:
            verbose_print(f"    ⚠ Справочник '{catalog_name}' не найден")
            return None
        
        # Используем запрос для поиска по НаименованиеПолное
        query = com_object.NewObject("Запрос")
        alias = catalog_name
        query.Текст = f"""
            ВЫБРАТЬ ПЕРВЫЕ 1
                {alias}.Ссылка КАК Ссылка
            ИЗ
                Справочник.{catalog_name} КАК {alias}
            ГДЕ
                {alias}.НаименованиеПолное = &НаименованиеПолное
                И НЕ {alias}.ПометкаУдаления
        """
        # Устанавливаем параметр
        try:
            param = query.Параметры.НайтиПоИмени("НаименованиеПолное")
            if param:
                param.Значение = ref_full_name
            else:
                query.УстановитьПараметр("НаименованиеПолное", ref_full_name)
        except Exception:
            query.УстановитьПараметр("НаименованиеПолное", ref_full_name)
        
        try:
            result = query.Выполнить()
            selection = result.Выбрать()
            
            if selection.Следующий():
                found_ref = selection.Получить("Ссылка")
                if found_ref:
                    # Проверяем, что найденный элемент действительно имеет нужное полное наименование
                    try:
                        found_full_name = str(safe_getattr(found_ref, "НаименованиеПолное", ""))
                        if not found_full_name or found_full_name != ref_full_name:
                            # Найден элемент с другим полным наименованием или без полного наименования
                            if found_full_name:
                                verbose_print(f"    ⚠ Найден элемент с другим полным наименованием: '{found_full_name}' != '{ref_full_name}'")
                            else:
                                verbose_print(f"    ⚠ Найденный элемент не имеет полного наименования (невалидная ссылка)")
                            found_ref = None  # Продолжаем и создаем новый элемент
                        else:
                            verbose_print(f"    ✓ Найден существующий элемент {ref_type} '{ref_full_name}' по полному наименованию (через запрос)")
                            # Сохраняем в кэш
                            _reference_by_full_name_cache[cache_key] = found_ref
                            return found_ref
                    except Exception as e:
                        verbose_print(f"    ⚠ Ошибка при проверке полного наименования найденного элемента: {e}")
                        found_ref = None  # Продолжаем и создаем новый элемент
        except Exception as e:
            verbose_print(f"    ⚠ Ошибка при поиске по полному наименованию через запрос: {e}")
            # Продолжаем и создаем новый элемент
        
        # Если не нашли, создаем новый элемент
        if not ref_uuid:
            import uuid
            ref_uuid = str(uuid.uuid4())
        
        verbose_print(f"    → Создаем новый элемент {ref_type} '{ref_full_name}' (не найден по полному наименованию)")
        
        # Определяем, является ли объект группой
        is_group = False
        if source_data and 'ЭтоГруппа' in source_data:
            group_value = source_data['ЭтоГруппа']
            if isinstance(group_value, bool):
                is_group = group_value
            elif isinstance(group_value, (int, str)):
                is_group = str(group_value).lower() in ('1', 'true', 'истина', 'да')
        
        # Создаем новый элемент или группу
        if is_group:
            create_group_method = safe_getattr(catalog_ref, "СоздатьГруппу", None)
            if create_group_method:
                new_item = call_if_callable(create_group_method)
                verbose_print(f"    → Создана группа {ref_type} '{ref_full_name}'")
            else:
                new_item = catalog_ref.СоздатьЭлемент()
                verbose_print(f"    → Создан элемент {ref_type} '{ref_full_name}' (метод СоздатьГруппу не найден)")
        else:
            new_item = catalog_ref.СоздатьЭлемент()
        
        # Устанавливаем режим обмена данными
        try:
            item_exchange = safe_getattr(new_item, "ОбменДанными", None)
            if item_exchange:
                item_exchange = call_if_callable(item_exchange)
                if item_exchange:
                    try:
                        item_exchange.Загрузка = True
                    except Exception:
                        pass
        except Exception:
            pass
        
        # Устанавливаем полное наименование и наименование
        new_item.НаименованиеПолное = ref_full_name
        new_item.Наименование = ref_full_name  # Также устанавливаем Наименование
        
        # Устанавливаем UUID
        try:
            item_exchange = safe_getattr(new_item, "ОбменДанными", None)
            if item_exchange:
                item_exchange = call_if_callable(item_exchange)
                if item_exchange:
                    try:
                        item_exchange.УникальныйИдентификатор = ref_uuid
                    except Exception:
                        pass
        except Exception:
            pass
        
        # Записываем
        new_item.Записать()
        
        verbose_print(f"    ✓ Создан новый элемент {ref_type} '{ref_full_name}' с UUID {ref_uuid[:30]}...")
        
        # Возвращаем ссылку на созданный элемент
        new_ref = new_item.Ссылка
        # Сохраняем в кэш
        _reference_by_full_name_cache[cache_key] = new_ref
        return new_ref
        
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при поиске/создании элемента '{ref_full_name}' в {ref_type} (по полному наименованию): {e}")
        import traceback
        traceback.print_exc()
        return None


def _is_date_string(value: str) -> bool:
    """
    Проверяет, является ли строка датой в формате SQLite/ISO.
    
    Args:
        value: Строка для проверки
        
    Returns:
        True если строка похожа на дату
    """
    if not isinstance(value, str) or not value:
        return False
    
    # Проверяем форматы: "YYYY-MM-DD", "YYYY-MM-DD HH:MM:SS", "YYYY-MM-DD HH:MM:SS+00:00"
    import re
    date_patterns = [
        r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
        r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
        r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS+00:00
    ]
    
    for pattern in date_patterns:
        if re.match(pattern, value):
            return True
    
    return False


def _convert_string_to_date(com_object, date_string: str):
    """
    Преобразует строку даты в объект даты 1С.
    
    ВАЖНО: Если в строке есть время 00:00:00, это может быть дата без времени,
    которая была сохранена с временем. В этом случае устанавливаем полдень (12:00),
    чтобы избежать смещения на день из-за часовых поясов при преобразовании через win32com.
    
    Если время не 00:00:00, используем его как есть.
    
    Args:
        com_object: COM-объект подключения к 1С
        date_string: Строка даты в формате "YYYY-MM-DD" или "YYYY-MM-DD HH:MM:SS" или "YYYY-MM-DD HH:MM:SS+00:00"
        
    Returns:
        datetime объект (который win32com преобразует в PyTime) или None при ошибке
    """
    if not date_string or not isinstance(date_string, str):
        return None
    
    try:
        from datetime import datetime
        import re
        
        # Убираем timezone если есть
        date_str_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', date_string.strip())
        
        # Парсим дату
        if ' ' in date_str_clean:
            # Формат с временем: "YYYY-MM-DD HH:MM:SS"
            dt = datetime.strptime(date_str_clean, '%Y-%m-%d %H:%M:%S')
            # Если время 00:00:00, это скорее всего дата без времени, сохраненная с временем
            # Устанавливаем полдень, чтобы избежать смещения на день из-за часовых поясов
            if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
                dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
        else:
            # Формат только дата: "YYYY-MM-DD"
            # Устанавливаем время на полдень (12:00), чтобы избежать проблем с часовыми поясами
            dt = datetime.strptime(date_str_clean, '%Y-%m-%d')
            dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
        
        # Возвращаем naive datetime (без timezone)
        # win32com автоматически преобразует его в PyTime для 1С
        return dt
        
    except Exception as e:
        from tools.logger import verbose_print
        verbose_print(f"    ⚠ Ошибка при преобразовании даты '{date_string}': {e}")
        import traceback
        traceback.print_exc()
        return None


def _get_enum_from_string(com_object, enum_string: str):
    """
    Преобразует строку перечисления в формате "Перечисление.ИмяПеречисления.Значение" 
    в COM-объект перечисления.
    
    Значения уже в формате приемника (после маппинга), поэтому просто получаем их из COM API.
    
    Args:
        com_object: COM-объект подключения к 1С
        enum_string: Строка в формате "Перечисление.ИмяПеречисления.Значение"
        
    Returns:
        COM-объект перечисления или None
    """
    if not enum_string or not isinstance(enum_string, str):
        return None
    
    # Парсим строку вида "Перечисление.ЮридическоеФизическоеЛицо.ЮридическоеЛицо"
    if not enum_string.startswith("Перечисление."):
        return None
    
    parts = enum_string.split(".", 2)
    if len(parts) != 3:
        return None
    
    enum_prefix, enum_name, enum_value_name = parts
    
    from tools.logger import verbose_print
    
    try:
        # Получаем объект перечисления
        enums = com_object.Перечисления
        enum_ref = safe_getattr(enums, enum_name, None)
        if not enum_ref:
            verbose_print(f"    ⚠ Перечисление '{enum_name}' не найдено")
            return None
        
        # Прямой доступ через атрибут (Перечисления[Имя].Значение)
        enum_value = safe_getattr(enum_ref, enum_value_name, None)
        if enum_value:
            if callable(enum_value):
                enum_value = call_if_callable(enum_value)
            if enum_value:
                return enum_value
        
        verbose_print(f"    ⚠ Значение '{enum_value_name}' не найдено в перечислении '{enum_name}'")
        return None
        
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при получении перечисления из строки '{enum_string}': {e}")
        return None


def _get_predefined_reference_from_string(com_object, ref_string: str):
    """
    Преобразует строку предопределенного элемента справочника в формате 
    "Справочник.ИмяСправочника.КодЭлемента" в COM-объект ссылки.
    
    Args:
        com_object: COM-объект подключения к 1С
        ref_string: Строка в формате "Справочник.ИмяСправочника.КодЭлемента"
        
    Returns:
        COM-объект ссылки на предопределенный элемент или None
    """
    if not ref_string or not isinstance(ref_string, str):
        return None
    
    # Парсим строку вида "Справочник.ВидыДоговоровКонтрагентовУХ.СПоставщиком"
    if not ref_string.startswith("Справочник."):
        return None
    
    parts = ref_string.split(".", 2)
    if len(parts) != 3:
        return None
    
    ref_prefix, catalog_name, predefined_code = parts
    
    from tools.logger import verbose_print
    
    try:
        # Получаем объект справочника
        catalogs = com_object.Справочники
        catalog_ref = safe_getattr(catalogs, catalog_name, None)
        if not catalog_ref:
            verbose_print(f"    ⚠ Справочник '{catalog_name}' не найден")
            return None
        
        # Прямой доступ через атрибут (Справочники[Имя].КодПредопределенногоЭлемента)
        predefined_element = safe_getattr(catalog_ref, predefined_code, None)
        if predefined_element:
            if callable(predefined_element):
                predefined_element = call_if_callable(predefined_element)
            if predefined_element:
                return predefined_element
        
        verbose_print(f"    ⚠ Предопределенный элемент '{predefined_code}' не найден в справочнике '{catalog_name}'")
        return None
        
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при получении предопределенного элемента справочника из строки '{ref_string}': {e}")
        return None


def create_reference_by_uuid(
    com_object,
    ref_uuid: str,
    ref_type: str,
    ref_presentation: str = "",
    source_data: Optional[Dict] = None,
    processed_db: Optional[str] = None,
) -> Optional[object]:
    from tools.logger import verbose_print
    """
    Создает объект ссылочного типа по UUID с минимальными данными.
    Если объект не найден, создает новый с UUID и Наименованием.
    
    Args:
        com_object: COM-объект подключения к 1С
        ref_uuid: UUID объекта
        ref_type: Тип объекта (например, "Справочник.Контрагенты")
        ref_presentation: Представление объекта (используется как Наименование)
        source_data: Данные источника (для определения is_group)
        processed_db: Путь к обработанной БД
    
    Returns:
        Ссылка на объект или None
    """
    if not ref_uuid or ref_uuid == "00000000-0000-0000-0000-000000000000":
        return None
    
    # Сначала пробуем найти существующий объект по UUID
    ref_obj = find_object_by_uuid(com_object, ref_uuid, ref_type)
    if ref_obj:
        return ref_obj
    
    # Если не найден, создаем новый
    try:
        if ref_type.startswith("Справочник."):
            catalog_name = ref_type.replace("Справочник.", "")
            catalogs = com_object.Справочники
            catalog_ref = safe_getattr(catalogs, catalog_name, None)
            
            if not catalog_ref:
                # Сохраняем в БД (не удалось создать - тип не поддерживается)
                try:
                    import sqlite3
                    refs_db_path = get_reference_objects_db_path()
                    conn = sqlite3.connect(refs_db_path)
                    save_reference_object(
                        conn,
                        ref_uuid,
                        ref_type,
                        ref_presentation,
                        None,
                        filled=False,
                        parent_type="",
                        parent_name="",
                        parent_uuid="",
                        field_name=""
                    )
                    conn.close()
                except Exception as e:
                    verbose_print(f"    ⚠ Ошибка при сохранении несопоставленной ссылки: {e}")
                return None
            
            # Проверяем, является ли объект группой (из source_data или processed_db)
            # Важно: проверяем по реквизиту ЭтоГруппа или is_group из JSON ссылочного поля
            is_group = False
            
            # Сначала проверяем is_group из JSON ссылочного поля (если есть)
            if source_data and 'is_group' in source_data:
                group_value = source_data['is_group']
                if isinstance(group_value, bool):
                    is_group = group_value
                elif isinstance(group_value, (int, str)):
                    is_group = str(group_value).lower() in ('1', 'true', 'истина', 'да')
                verbose_print(f"    → Определен is_group={is_group} из source_data['is_group']={group_value}")
            
            # Если не нашли is_group, проверяем ЭтоГруппа
            if not is_group and source_data and 'ЭтоГруппа' in source_data:
                group_value = source_data['ЭтоГруппа']
                if isinstance(group_value, bool):
                    is_group = group_value
                elif isinstance(group_value, (int, str)):
                    is_group = str(group_value).lower() in ('1', 'true', 'истина', 'да')
            elif not is_group and processed_db:
                # Пробуем найти в processed_db
                try:
                    import sqlite3
                    import os
                    source_conn = sqlite3.connect(processed_db)
                    source_cursor = source_conn.cursor()
                    
                    if ref_type.startswith("Справочник."):
                        table_name = ref_type.replace("Справочник.", "").lower()
                        source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?", (f"%{table_name}%",))
                        tables = [row[0] for row in source_cursor.fetchall()]
                        
                        if tables:
                            table_name = tables[0]
                            source_cursor.execute(f"SELECT ЭтоГруппа FROM {table_name} WHERE uuid = ?", (ref_uuid,))
                            row = source_cursor.fetchone()
                            if row and row[0] is not None:
                                group_value = row[0]
                                if isinstance(group_value, bool):
                                    is_group = group_value
                                elif isinstance(group_value, (int, str)):
                                    is_group = str(group_value).lower() in ('1', 'true', 'истина', 'да')
                    
                    source_conn.close()
                    
                    # Если не нашли в processed_db, пробуем найти в исходной БД (без суффикса _processed)
                    # Это нужно для родительских групп, которые могут не попасть в processed_db
                    if not is_group:
                        source_db_path = processed_db.replace("_processed.db", ".db")
                        if os.path.exists(source_db_path) and source_db_path != processed_db:
                            try:
                                source_conn = sqlite3.connect(source_db_path)
                                source_cursor = source_conn.cursor()
                                
                                if ref_type.startswith("Справочник."):
                                    table_name = ref_type.replace("Справочник.", "").lower()
                                    source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?", (f"%{table_name}%",))
                                    tables = [row[0] for row in source_cursor.fetchall()]
                                    
                                    if tables:
                                        table_name = tables[0]
                                        source_cursor.execute(f"SELECT ЭтоГруппа FROM {table_name} WHERE uuid = ?", (ref_uuid,))
                                        row = source_cursor.fetchone()
                                        if row and row[0] is not None:
                                            group_value = row[0]
                                            if isinstance(group_value, bool):
                                                is_group = group_value
                                            elif isinstance(group_value, (int, str)):
                                                is_group = str(group_value).lower() in ('1', 'true', 'истина', 'да')
                                
                                source_conn.close()
                            except Exception:
                                pass
                except Exception:
                    pass
            
            # Создаем новый элемент или группу
            create_group_method = None
            verbose_print(f"    → Создание объекта: is_group={is_group}, тип={ref_type}, представление='{ref_presentation}'")
            if is_group:
                create_group_method = safe_getattr(catalog_ref, "СоздатьГруппу", None)
                if create_group_method:
                    new_item = call_if_callable(create_group_method)
                    verbose_print(f"    → Создана группа {ref_type} '{ref_presentation}' с UUID {ref_uuid[:30]}... (через СоздатьГруппу)")
                else:
                    new_item = catalog_ref.СоздатьЭлемент()
                    verbose_print(f"    → Создан элемент {ref_type} '{ref_presentation}' (метод СоздатьГруппу не найден) с UUID {ref_uuid[:30]}...")
            else:
                new_item = catalog_ref.СоздатьЭлемент()
                verbose_print(f"    → Создан элемент {ref_type} '{ref_presentation}' с UUID {ref_uuid[:30]}... (не группа)")
            
            # Устанавливаем режим обмена данными для элемента
            try:
                item_exchange = safe_getattr(new_item, "ОбменДанными", None)
                if item_exchange:
                    item_exchange = call_if_callable(item_exchange)
                    if item_exchange:
                        try:
                            item_exchange.Загрузка = True
                        except Exception:
                            pass
            except Exception:
                pass
            
            # Устанавливаем UUID
            try:
                uuid_obj = com_object.NewObject("УникальныйИдентификатор", ref_uuid)
                get_ref_method = safe_getattr(catalog_ref, "ПолучитьСсылку", None)
                if get_ref_method:
                    ref_by_uuid = call_if_callable(get_ref_method, uuid_obj)
                    if ref_by_uuid:
                        set_ref_method = safe_getattr(new_item, "УстановитьСсылкуНового", None)
                        if set_ref_method:
                            call_if_callable(set_ref_method, ref_by_uuid)
            except Exception as e:
                verbose_print(f"    ⚠ Не удалось установить UUID для {ref_type}: {e}")
            
            # Устанавливаем Наименование
            if ref_presentation:
                try:
                    new_item.Наименование = ref_presentation
                except Exception:
                    pass
            
            # Устанавливаем ЭтоГруппа для групп
            if is_group:
                try:
                    new_item.ЭтоГруппа = True
                except Exception:
                    pass
            
            # Записываем в режиме обмена данными
            try:
                new_item.Записать()
                # Сообщение о создании уже выведено выше для групп
                if not is_group:
                    verbose_print(f"    → Создан объект {ref_type} '{ref_presentation}' с UUID {ref_uuid[:30]}...")
                
                # Сохраняем информацию о созданном объекте в БД (filled=False - создано через реквизит)
                try:
                    import sqlite3
                    refs_db_path = get_reference_objects_db_path()
                    conn = sqlite3.connect(refs_db_path)
                    
                    # Если source_data не передан, пытаемся получить из processed_db
                    if not source_data and processed_db:
                        try:
                            source_conn = sqlite3.connect(processed_db)
                            source_cursor = source_conn.cursor()
                            
                            # Определяем имя таблицы по типу
                            if ref_type.startswith("Справочник."):
                                table_name = ref_type.replace("Справочник.", "").lower()
                                # Пробуем найти таблицу
                                source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?", (f"%{table_name}%",))
                                tables = [row[0] for row in source_cursor.fetchall()]
                                
                                if tables:
                                    # Берем первую подходящую таблицу
                                    table_name = tables[0]
                                    source_cursor.execute(f"SELECT * FROM {table_name} WHERE uuid = ?", (ref_uuid,))
                                    row = source_cursor.fetchone()
                                    if row:
                                        # Получаем имена колонок
                                        column_names = [desc[0] for desc in source_cursor.description]
                                        source_data = {}
                                        for i, col_name in enumerate(column_names):
                                            source_data[col_name] = row[i]
                            
                            source_conn.close()
                        except Exception as e:
                            verbose_print(f"    ⚠ Не удалось получить данные из processed_db: {e}")
                    
                    save_reference_object(
                        conn,
                        ref_uuid,
                        ref_type,
                        ref_presentation,
                        source_data,
                        filled=False,  # Создано через реквизит, не полная запись
                        parent_type="",
                        parent_name="",
                        parent_uuid="",
                        field_name=""
                    )
                    conn.close()
                except Exception as e:
                    verbose_print(f"    ⚠ Ошибка при сохранении созданного объекта в БД: {e}")
                
                # Получаем ссылку на созданный объект
                uuid_obj = com_object.NewObject("УникальныйИдентификатор", ref_uuid)
                get_ref_method = safe_getattr(catalog_ref, "ПолучитьСсылку", None)
                if get_ref_method:
                    ref = call_if_callable(get_ref_method, uuid_obj)
                    return ref
                return None
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка при создании объекта {ref_type}: {e}")
                return None
        else:
            # Для других типов (документы и т.д.) пока не поддерживается
            try:
                import sqlite3
                refs_db_path = get_reference_objects_db_path()
                conn = sqlite3.connect(refs_db_path)
                save_reference_object(
                    conn,
                    ref_uuid,
                    ref_type,
                    ref_presentation,
                    None,
                    filled=False,
                    parent_type="",
                    parent_name="",
                    parent_uuid="",
                    field_name=""
                )
                conn.close()
            except Exception:
                pass
            return None
            
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при создании объекта {ref_type}: {e}")
        # Сохраняем в БД
        try:
            import sqlite3
            refs_db_path = get_reference_objects_db_path()
            conn = sqlite3.connect(refs_db_path)
            save_reference_object(
                conn,
                ref_uuid,
                ref_type,
                ref_presentation,
                None,
                filled=False,
                parent_type="",
                parent_name="",
                parent_uuid="",
                field_name=""
            )
            conn.close()
        except Exception:
            pass
        return None


def write_catalog_item(
    com_object,
    item_data: Dict,
    catalog_name: str,
    type_name: str,
    standard_fields: Optional[List[str]] = None,
    processed_db: Optional[str] = None,
    field_mapping: Optional[Dict[str, Dict]] = None,
    field_name_mapping: Optional[Dict[str, str]] = None,
    source_object_name: Optional[str] = None,
    source_object_type: Optional[str] = None,
) -> bool:
    """
    Записывает элемент справочника в 1С с сохранением UUID.
    Использует prepare_catalog_item и finalize_catalog_item для унификации кода.
    Автоматически обрабатывает табличные части, если они есть в маппинге.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item_data: Словарь с данными элемента
        catalog_name: Имя справочника в 1С (например, "Контрагенты", "НоменклатурныеГруппы")
        type_name: Полное имя типа (например, "Справочник.Контрагенты")
        standard_fields: Список стандартных полей, которые нужно установить отдельно
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        field_mapping: Маппинг полей (если None, загружается из type_mapping.db)
        field_name_mapping: Маппинг имен полей (например, {"Владелец": "ОбъектВладелец"})
        
    Returns:
        True если успешно, False если ошибка
    """
    # ЭТАПЫ 1-8: Подготовка и заполнение базовых полей через базовую функцию
    item = prepare_catalog_item(
        com_object=com_object,
        item_data=item_data,
        catalog_name=catalog_name,
        type_name=type_name,
        standard_fields=standard_fields,
        processed_db=processed_db,
        field_mapping=field_mapping,
        field_name_mapping=field_name_mapping,
        source_object_name=source_object_name,
        source_object_type=source_object_type
    )
    
    if not item:
        return False
    
    # ЭТАП 9: ЗАПОЛНЕНИЕ ТАБЛИЧНЫХ ЧАСТЕЙ (автоматически, если есть маппинг)
    uuid_value = item_data.get('uuid', '')
    if processed_db and uuid_value:
        _write_tabular_sections_from_mapping(
            com_object, item, catalog_name, uuid_value, processed_db
        )
    
    # ЭТАПЫ 10-12: Завершение записи через базовую функцию
    return finalize_catalog_item(
        com_object=com_object,
        item=item,
        item_data=item_data,
        catalog_name=catalog_name,
        type_name=type_name,
        processed_db=processed_db
    )


def _write_tabular_sections_from_mapping(
    com_object, item, catalog_name: str, item_uuid: str, processed_db: str
):
    """
    Автоматически заполняет табличные части на основе маппинга из type_mapping.db.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item: COM-объект элемента справочника
        catalog_name: Имя справочника в 1С
        item_uuid: UUID элемента
        processed_db: Путь к обработанной БД
    """
    from tools.logger import verbose_print
    from tools.writer_utils import get_reference_by_uuid
    
    try:
        # Загружаем маппинг табличных частей из catalog_mapping.json
        import os
        import json
        import sqlite3
        
        mapping_db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "CONF", "type_mapping.db")
        if not os.path.exists(mapping_db_path):
            return
        
        mapping_conn = sqlite3.connect(mapping_db_path)
        mapping_conn.row_factory = sqlite3.Row
        mapping_cursor = mapping_conn.cursor()
        
        # Сначала находим имя объекта источника через маппинг объектов
        source_object_name = catalog_name
        mapping_cursor.execute("""
            SELECT source_name
            FROM object_mapping
            WHERE object_type = 'catalog' AND target_name = ?
        """, (catalog_name,))
        obj_row = mapping_cursor.fetchone()
        if obj_row:
            source_object_name = obj_row["source_name"]
            verbose_print(f"  → Найден маппинг объекта для табличных частей: {source_object_name} -> {catalog_name}")
        
        # Загружаем catalog_mapping.json для получения маппинга табличных частей
        catalog_mapping_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "CONF", "catalog_mapping.json")
        tabular_sections_mapping = {}
        if os.path.exists(catalog_mapping_path):
            with open(catalog_mapping_path, 'r', encoding='utf-8') as f:
                catalog_mapping = json.load(f)
                # Ищем справочник по полному пути источника в маппинге
                source_catalog_path = f"Справочник.{source_object_name}"
                if source_catalog_path in catalog_mapping:
                    tabular_sections_mapping = catalog_mapping[source_catalog_path].get("tabular_sections", {})
                    if tabular_sections_mapping:
                        verbose_print(f"  → Загружен маппинг табличных частей из catalog_mapping.json: {len(tabular_sections_mapping)} секций")
        
        # Загружаем маппинги из type_mapping.json для табличных частей
        type_mapping_json_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "CONF", "type_mapping.json")
        sections = {}
        
        if os.path.exists(type_mapping_json_path):
            try:
                with open(type_mapping_json_path, 'r', encoding='utf-8') as f:
                    type_mapping_json = json.load(f)
                    
                # Ищем объект в type_mapping.json
                source_catalog_path = f"catalog.{source_object_name}"
                target_catalog_path = f"catalog.{catalog_name}"
                
                for obj in type_mapping_json:
                    obj_name = obj.get("object", "")
                    # Проверяем, соответствует ли объект нашему справочнику
                    if obj_name == source_catalog_path or obj_name == target_catalog_path or obj_name.endswith(f".{source_object_name}") or obj_name.endswith(f".{catalog_name}"):
                        # Загружаем табличные части
                        tabular_sections = obj.get("tabular_sections", [])
                        for tab_section in tabular_sections:
                            section_name = tab_section.get("section", "")
                            if not section_name:
                                continue
                            
                            # Определяем target_section_name (может быть указан в маппинге или равен section_name)
                            target_section_name = section_name
                            
                            # Загружаем маппинги полей для этой секции
                            section_mappings = tab_section.get("mappings", [])
                            if section_name not in sections:
                                sections[section_name] = {
                                    "target_section": target_section_name,
                                    "fields": {}
                                }
                            
                            # Парсим маппинги полей
                            for mapping_str in section_mappings:
                                # Формат: "ПолеИсточника -> ПолеПриемника [ТипИсточника -> ТипПриемника]"
                                parts = mapping_str.split(" -> ")
                                if len(parts) >= 2:
                                    source_field = parts[0].strip()
                                    target_part = parts[1].strip()
                                    
                                    # Извлекаем target_field и типы
                                    target_field = target_part
                                    source_type = None
                                    target_type = None
                                    search_method = None
                                    
                                    # Проверяем наличие типов в квадратных скобках
                                    if " [" in target_part:
                                        field_and_type = target_part.split(" [")
                                        target_field = field_and_type[0].strip()
                                        type_part = field_and_type[1].rstrip("]").strip()
                                        
                                        # Парсим типы: "ТипИсточника -> ТипПриемника"
                                        if " -> " in type_part:
                                            type_parts = type_part.split(" -> ")
                                            source_type = type_parts[0].strip()
                                            target_type = type_parts[1].strip()
                                    
                                    # Проверяем наличие (m) - означает маппинг
                                    if " (m)" in target_field:
                                        target_field = target_field.replace(" (m)", "").strip()
                                        search_method = "mapped"
                                    
                                    if target_field:
                                        sections[section_name]["fields"][source_field] = {
                                            "target_field": target_field,
                                            "source_type": source_type,
                                            "target_type": target_type,
                                            "search_method": search_method,
                                        }
                        break
            except Exception as e:
                from tools.logger import verbose_print
                verbose_print(f"  ⚠ Ошибка при загрузке маппингов из type_mapping.json: {e}")
        
        # Дополняем маппингами из type_mapping.db
        mapping_cursor.execute("""
            SELECT DISTINCT section_name, target_section_name
            FROM field_mapping
            WHERE object_type = 'catalog' 
            AND object_name = ?
            AND field_kind IN ('tabular_attribute', 'tabular_requisite')
            AND (status = 'matched' OR is_manual = 1)
            AND section_name IS NOT NULL
            AND section_name != ''
        """, (source_object_name,))
        
        for row in mapping_cursor.fetchall():
            section_name = row["section_name"]
            target_section_name = row["target_section_name"] or section_name
            if section_name not in sections:
                sections[section_name] = {
                    "target_section": target_section_name,
                    "fields": {}
                }
        
        mapping_conn.close()
        
        if not sections:
            from tools.logger import verbose_print
            verbose_print(f"  [ЭТАП 9] Табличные части не найдены в маппинге (source_object_name: {source_object_name}, catalog_name: {catalog_name})")
            return
        
        from tools.logger import verbose_print
        verbose_print(f"  [ЭТАП 9] Заполнение табличных частей ({len(sections)} секций, source_object_name: {source_object_name})")
        
        # Загружаем маппинг полей для каждой табличной части из type_mapping.db
        mapping_conn = sqlite3.connect(mapping_db_path)
        mapping_conn.row_factory = sqlite3.Row
        mapping_cursor = mapping_conn.cursor()
        
        for section_name, section_info in sections.items():
            mapping_cursor.execute("""
                SELECT field_name, target_field_name, source_type, target_type, status, search_method
                FROM field_mapping
                WHERE object_type = 'catalog' 
                AND object_name = ?
                AND field_kind IN ('tabular_attribute', 'tabular_requisite')
                AND section_name = ?
                AND (status = 'matched' OR is_manual = 1)
            """, (source_object_name, section_name))
            
            # Дополняем маппинги из БД (если их еще нет из JSON)
            for row in mapping_cursor.fetchall():
                source_field = row["field_name"]
                if source_field not in section_info["fields"]:
                    target_field = row["target_field_name"]
                    if target_field:
                        section_info["fields"][source_field] = {
                            "target_field": target_field,
                            "source_type": row["source_type"],
                            "target_type": row["target_type"],
                            "search_method": row["search_method"] if row["search_method"] else None,
                        }
            
            for row in mapping_cursor.fetchall():
                source_field = row["field_name"]
                target_field = row["target_field_name"]
                if target_field:
                    section_info["fields"][source_field] = {
                        "target_field": target_field,
                        "source_type": row["source_type"],
                        "target_type": row["target_type"],
                        "search_method": row["search_method"] if row["search_method"] else None,
                    }
        
        mapping_conn.close()
        
        # Подключаемся к processed_db для чтения данных табличных частей
        conn = sqlite3.connect(processed_db)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Ищем все таблицы в БД, которые содержат parent_uuid и имеют данные для этого элемента
        # Каждый справочник загружается в свою БД, поэтому все табличные части уже там
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            AND name != 'sqlite_sequence'
        """)
        all_tables = [row[0] for row in cursor.fetchall()]
        
        # Для каждой табличной части ищем соответствующую таблицу в БД
        for section_name, section_info in sections.items():
            target_section_name = section_info["target_section"]
            fields_mapping = section_info["fields"]
            
            if not fields_mapping:
                continue
            
            # Ищем таблицу, которая содержит parent_uuid и имеет данные для этого UUID
            table_name = None
            
            # Сначала проверяем маппинг из catalog_mapping.json
            if section_name in tabular_sections_mapping:
                mapped_table_name = tabular_sections_mapping[section_name]
                # Проверяем, существует ли эта таблица и есть ли в ней данные
                try:
                    cursor.execute(f"PRAGMA table_info({mapped_table_name})")
                    columns = [col[1] for col in cursor.fetchall()]
                    if 'parent_uuid' in columns:
                        cursor.execute(f"SELECT COUNT(*) FROM {mapped_table_name} WHERE parent_uuid = ?", (item_uuid,))
                        count = cursor.fetchone()[0]
                        if count > 0:
                            table_name = mapped_table_name
                            verbose_print(f"      → Найдена таблица через catalog_mapping.json: {table_name}")
                except Exception:
                    pass
            
            # Если не найдено через маппинг, используем поиск по совпадению имени
            if not table_name:
                for table in all_tables:
                    # Проверяем, есть ли в таблице колонка parent_uuid
                    try:
                        cursor.execute(f"PRAGMA table_info({table})")
                        columns = [col[1] for col in cursor.fetchall()]
                        if 'parent_uuid' not in columns:
                            continue
                        
                        # Проверяем, есть ли данные для этого UUID
                        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE parent_uuid = ?", (item_uuid,))
                        count = cursor.fetchone()[0]
                        if count > 0:
                            # Проверяем, что имя таблицы соответствует секции (по частичному совпадению)
                            table_lower = table.lower()
                            section_lower = section_name.lower().replace(" ", "_")
                            # Ищем совпадение: либо таблица содержит имя секции, либо имя секции содержит часть имени таблицы
                            if section_lower in table_lower or any(part in table_lower for part in section_lower.split("_")):
                                table_name = table
                                break
                    except Exception:
                        continue
            
            if not table_name:
                verbose_print(f"      ⚠ Таблица для табличной части '{section_name}' не найдена")
                continue
            
            # Читаем данные из таблицы
            try:
                cursor.execute(f"SELECT * FROM {table_name} WHERE parent_uuid = ? ORDER BY НомерСтроки", (item_uuid,))
                rows = cursor.fetchall()
            except sqlite3.OperationalError:
                rows = []
            
            if not rows:
                continue
            
            verbose_print(f"      → Табличная часть {target_section_name}: {len(rows)} строк")
            
            # Получаем табличную часть из объекта
            tabular_section = safe_getattr(item, target_section_name, None)
            if not tabular_section:
                verbose_print(f"      ⚠ Табличная часть '{target_section_name}' не найдена в объекте")
                continue
            
            tabular_section = call_if_callable(tabular_section)
            if not tabular_section:
                verbose_print(f"      ⚠ Не удалось получить табличную часть '{target_section_name}'")
                continue
            
            # Очищаем табличную часть (если нужно)
            try:
                clear_method = safe_getattr(tabular_section, "Очистить", None)
                if clear_method:
                    call_if_callable(clear_method)
            except Exception:
                pass
            
            # Добавляем строки в табличную часть
            for row in rows:
                try:
                    # Создаем новую строку
                    add_method = safe_getattr(tabular_section, "Добавить", None)
                    if not add_method:
                        break
                    
                    new_row = call_if_callable(add_method)
                    if not new_row:
                        continue
                    
                    # Заполняем поля строки
                    for field_name in row.keys():
                        if field_name in ('parent_uuid', 'parent_link', 'НомерСтроки'):
                            continue
                        
                        if field_name not in fields_mapping:
                            continue
                        
                        field_value = row[field_name]
                        if field_value is None:
                            continue
                        
                        field_info = fields_mapping[field_name]
                        target_field = field_info["target_field"]
                        
                        # Обрабатываем ссылочные поля
                        if field_name.endswith('_UUID') or field_name.endswith('_Представление') or field_name.endswith('_Тип'):
                            continue
                        
                        # Проверяем, есть ли JSON данные в самом поле
                        ref_data = None
                        if isinstance(field_value, str) and field_value.strip().startswith('{'):
                            try:
                                ref_data = json.loads(field_value)
                            except (json.JSONDecodeError, ValueError):
                                pass
                        
                        # Если нет JSON, проверяем отдельные поля _UUID, _Представление, _Тип
                        # Но только если поле может быть ссылочным (есть информация о типе в маппинге)
                        if not ref_data:
                            uuid_field = f"{field_name}_UUID"
                            type_field = f"{field_name}_Тип"
                            presentation_field = f"{field_name}_Представление"
                            # Проверяем наличие uuid_field в row (может быть sqlite3.Row или dict)
                            # sqlite3.Row не поддерживает .get(), используем прямое обращение с try/except
                            try:
                                # Проверяем, является ли это поле ссылочным по наличию _UUID поля в row
                                # Если _UUID поля нет, это обычное поле и нужно обработать его дальше
                                if uuid_field in row and row[uuid_field]:
                                    ref_uuid = row[uuid_field]
                                    try:
                                        ref_type = row[type_field] if row[type_field] else ""
                                    except (KeyError, IndexError):
                                        ref_type = ""
                                    try:
                                        ref_presentation = row[presentation_field] if row[presentation_field] else ""
                                    except (KeyError, IndexError):
                                        ref_presentation = ""
                                    
                                    # Если есть UUID и тип, это ссылочное поле
                                    if ref_uuid and ref_type:
                                        ref_data = {
                                            'uuid': ref_uuid,
                                            'type': ref_type,
                                            'presentation': ref_presentation
                                        }
                                    # Если нет UUID или типа, но есть поле _UUID в row, это пустое ссылочное поле - пропускаем
                                    # (продолжаем, чтобы не обрабатывать как обычное поле)
                                # Если _UUID поля нет в row, это обычное поле - обрабатываем дальше (не делаем continue)
                            except (KeyError, IndexError, TypeError):
                                # Ошибка при проверке - обрабатываем как обычное поле (не делаем continue)
                                pass
                        
                        if ref_data:
                            ref_uuid = ref_data.get('uuid', '')
                            ref_type = ref_data.get('type', '')
                            ref_presentation = ref_data.get('presentation', '')
                            
                            if ref_uuid and ref_type:
                                ref_obj = None
                                
                                # Специальная обработка для полей типа ПланСчетов.* - поиск по коду из маппинга
                                if ref_type and (ref_type.startswith("ПланСчетов.") or ref_type.startswith("ChartOfAccountsRef.")):
                                    from tools.chart_of_accounts_mapper import extract_account_code, get_mapped_account_code, load_mapping
                                    from tools.logger import verbose_print
                                    
                                    # Преобразуем тип ChartOfAccountsRef.* в ПланСчетов.* для функции поиска
                                    search_type = ref_type
                                    if ref_type.startswith("ChartOfAccountsRef."):
                                        search_type = ref_type.replace("ChartOfAccountsRef.", "ПланСчетов.")
                                    
                                    # Извлекаем код счета из представления
                                    source_code = extract_account_code(ref_presentation)
                                    if source_code:
                                        # Загружаем маппинг плана счетов
                                        mapping_path = "CONF/chart_of_accounts_mapping.json"
                                        mapping, _ = load_mapping(mapping_path)
                                        
                                        # Получаем маппированный код
                                        mapped_code = get_mapped_account_code(source_code, mapping)
                                        if mapped_code:
                                            verbose_print(f"      → Маппинг счета: {source_code} -> {mapped_code} (тип: {ref_type} -> {search_type})")
                                            # Ищем счет по коду в приемнике (используем преобразованный тип)
                                            ref_obj = _find_account_by_code_in_receiver(com_object, mapped_code, search_type)
                                        else:
                                            verbose_print(f"      ⚠ Не удалось получить маппированный код для '{source_code}'")
                                    else:
                                        verbose_print(f"      ⚠ Не удалось извлечь код счета из представления: '{ref_presentation}'")
                                
                                # Для плана счетов НЕ ищем по UUID и НЕ создаем новый элемент
                                is_plan_accounts = ref_type and (ref_type.startswith("ПланСчетов.") or ref_type.startswith("ChartOfAccountsRef."))
                                
                                # Если не нашли через маппинг и это НЕ план счетов, пробуем стандартный поиск по UUID
                                if not ref_obj and not is_plan_accounts:
                                    ref_obj = get_reference_by_uuid(com_object, ref_uuid, ref_type)
                                
                                # Для плана счетов НЕ создаем новый элемент, только поиск по коду
                                if not ref_obj and is_plan_accounts:
                                    from tools.logger import verbose_print
                                    verbose_print(f"      ⚠ Счет плана счетов не найден и не будет создан: {ref_presentation} (UUID: {ref_uuid[:30]}...)")
                                elif not ref_obj:
                                    ref_obj = create_reference_by_uuid(
                                        com_object, ref_uuid, ref_type, ref_presentation, None, processed_db
                                    )
                                
                                if ref_obj:
                                    try:
                                        setattr(new_row, target_field, ref_obj)
                                    except Exception:
                                        pass
                                continue
                        
                        # Обрабатываем перечисления
                        if isinstance(field_value, str) and field_value.startswith("Перечисление."):
                            enum_obj = _get_enum_from_string(com_object, field_value)
                            if enum_obj:
                                try:
                                    setattr(new_row, target_field, enum_obj)
                                except Exception:
                                    pass
                            continue
                        
                        # Обрабатываем обычные поля (не ссылочные, не перечисления)
                        try:
                            if isinstance(field_value, str):
                                # Преобразуем булевы значения
                                if field_value.lower() == 'true':
                                    field_value = True
                                elif field_value.lower() == 'false':
                                    field_value = False
                                # Проверяем, является ли это числом
                                elif field_value.strip():
                                    try:
                                        # Пробуем преобразовать в число (int или float)
                                        if '.' in field_value or ',' in field_value:
                                            # Число с плавающей точкой
                                            field_value = float(field_value.replace(',', '.'))
                                        else:
                                            # Целое число
                                            field_value = int(field_value)
                                    except (ValueError, TypeError):
                                        # Не число - проверяем, является ли это датой
                                        if _is_date_string(field_value):
                                            date_obj = _convert_string_to_date(com_object, field_value)
                                            if date_obj:
                                                field_value = date_obj
                                        # Если не число и не дата - оставляем как строку
                            
                            # Устанавливаем значение поля
                            setattr(new_row, target_field, field_value)
                        except Exception as e:
                            from tools.logger import verbose_print
                            verbose_print(f"        ⚠ Ошибка при установке поля '{target_field}': {e}")
                            pass
                
                except Exception as e:
                    verbose_print(f"      ⚠ Ошибка при добавлении строки в {target_section_name}: {e}")
        
        conn.close()
    
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка при заполнении табличных частей: {e}")
        import traceback
        traceback.print_exc()


def setup_exchange_mode(com_object) -> bool:
    """
    Заглушка. Режим обмена данными теперь устанавливается для каждого конкретного объекта.
    Эта функция оставлена для обратной совместимости и не выполняет никаких действий.
    
    Args:
        com_object: COM-объект подключения к 1С (не используется)
        
    Returns:
        False (режим не устанавливается глобально)
    """
    # Режим обмена данными устанавливается для каждого конкретного объекта:
    # item.ОбменДанными.Загрузка = True
    # Эта функция больше не нужна, оставлена как заглушка для обратной совместимости
    return False


def prepare_catalog_item(
    com_object,
    item_data: Dict,
    catalog_name: str,
    type_name: str,
    standard_fields: Optional[List[str]] = None,
    processed_db: Optional[str] = None,
    field_mapping: Optional[Dict[str, Dict]] = None,
    field_name_mapping: Optional[Dict[str, str]] = None,
    source_object_name: Optional[str] = None,
    source_object_type: Optional[str] = None,
) -> Optional[object]:
    """
    Подготавливает элемент справочника: выполняет этапы 1-8 (поиск/создание, заполнение полей).
    Возвращает заполненный объект для дозаполнения в конкретном писателе.
    
    Этапы:
    1. Поиск существующего элемента по UUID
    2. Создание нового элемента (если не найден)
    3. Установка стандартных полей
    4. Загрузка маппинга полей
    5. Создание неполных ссылочных объектов
    6. Установка ссылочных полей
    7. Установка перечислений
    8. Установка обычных полей
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item_data: Словарь с данными элемента
        catalog_name: Имя справочника в 1С (например, "Контрагенты", "КонтактныеЛица")
        type_name: Полное имя типа (например, "Справочник.Контрагенты")
        standard_fields: Список стандартных полей
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        field_mapping: Маппинг полей (если None, загружается из type_mapping.db)
        field_name_mapping: Маппинг имен полей (например, {"Владелец": "ОбъектВладелец"})
        source_object_name: Имя исходного объекта для загрузки маппинга (если None, определяется автоматически)
        source_object_type: Тип исходного объекта ('catalog' или 'document', если None, определяется автоматически)
        
    Returns:
        COM-объект элемента справочника или None при ошибке
    """
    from tools.logger import verbose_print
    
    if standard_fields is None:
        standard_fields = ['Код', 'Наименование', 'ПометкаУдаления', 'Комментарий']
    
    if field_name_mapping is None:
        field_name_mapping = {}
    
    item_name = item_data.get('Наименование', 'Без наименования')
    uuid_value = item_data.get('uuid', '')
    
    try:
        # ЭТАП 1: ПОДГОТОВКА И ПОИСК СУЩЕСТВУЮЩЕГО ЭЛЕМЕНТА
        verbose_print(f"  [ЭТАП 1] Подготовка и поиск существующего элемента")
        verbose_print(f"  [ЭТАП 1] UUID: {uuid_value[:8] if uuid_value else 'нет'}..., Наименование: {item_name}")
        
        catalogs = com_object.Справочники
        catalog_ref = safe_getattr(catalogs, catalog_name, None)
        
        if catalog_ref is None:
            verbose_print(f"  ✗ {catalog_name} '{item_name}': Справочник '{catalog_name}' не найден")
            return None
        
        is_group = False
        if 'ЭтоГруппа' in item_data:
            group_value = item_data['ЭтоГруппа']
            if isinstance(group_value, bool):
                is_group = group_value
            elif isinstance(group_value, (int, str)):
                is_group = str(group_value).lower() in ('1', 'true', 'истина', 'да')
        
        item = None
        is_existing = False
        if uuid_value and uuid_value != "00000000-0000-0000-0000-000000000000":
            ref = find_object_by_uuid(com_object, uuid_value, type_name)
            if ref:
                item = ref.ПолучитьОбъект()
                is_existing = True
                verbose_print(f"  → {catalog_name} '{item_name}': найден существующий элемент по UUID, загружаем для обновления")
        
        # ЭТАП 2: СОЗДАНИЕ НОВОГО ЭЛЕМЕНТА (если не найден существующий)
        if item is None:
            verbose_print(f"  [ЭТАП 2] Создание нового элемента")
            if is_group:
                create_group_method = safe_getattr(catalog_ref, "СоздатьГруппу", None)
                if create_group_method:
                    item = call_if_callable(create_group_method)
                else:
                    item = catalog_ref.СоздатьЭлемент()
                    is_group = False
            else:
                item = catalog_ref.СоздатьЭлемент()
            
            # Устанавливаем UUID для нового элемента
            if uuid_value and uuid_value != "00000000-0000-0000-0000-000000000000":
                try:
                    uuid_obj = com_object.NewObject("УникальныйИдентификатор", uuid_value)
                    get_ref_method = safe_getattr(catalog_ref, "ПолучитьСсылку", None)
                    if get_ref_method:
                        ref_by_uuid = call_if_callable(get_ref_method, uuid_obj)
                        if ref_by_uuid:
                            set_ref_method = safe_getattr(item, "УстановитьСсылкуНового", None)
                            if set_ref_method:
                                call_if_callable(set_ref_method, ref_by_uuid)
                                verbose_print(f"  → UUID установлен: {uuid_value[:8]}...")
                except Exception as e:
                    verbose_print(f"  ⚠ Ошибка при установке UUID: {e}")
        
        # ЭТАП 3: УСТАНОВКА СТАНДАРТНЫХ ПОЛЕЙ
        verbose_print(f"  [ЭТАП 3] Установка стандартных полей")
        if not is_existing and 'Код' in item_data:
            try:
                item.Код = item_data['Код'] if item_data['Код'] is not None else ""
            except Exception as e:
                verbose_print(f"  ⚠ Ошибка при установке поля Код: {e}")
        
        for field_name in standard_fields:
            if field_name in item_data:
                if field_name == 'ПометкаУдаления':
                    try:
                        value = item_data[field_name]
                        if isinstance(value, bool):
                            bool_value = value
                        elif isinstance(value, (int, str)):
                            bool_value = str(value).lower() in ('1', 'true', 'истина', 'да')
                        else:
                            bool_value = bool(value)
                        setattr(item, field_name, bool_value)
                    except Exception:
                        pass
                elif item_data[field_name]:
                    try:
                        setattr(item, field_name, item_data[field_name])
                    except Exception:
                        pass
        
        if not is_existing and 'ЭтоГруппа' in item_data:
            try:
                group_value = item_data['ЭтоГруппа']
                if isinstance(group_value, bool):
                    bool_value = group_value
                elif isinstance(group_value, (int, str)):
                    bool_value = str(group_value).lower() in ('1', 'true', 'истина', 'да')
                else:
                    bool_value = bool(group_value)
                setattr(item, 'ЭтоГруппа', bool_value)
            except Exception:
                pass
        
        # ЭТАП 4: ЗАГРУЗКА МАППИНГА ПОЛЕЙ
        verbose_print(f"  [ЭТАП 4] Загрузка маппинга полей")
        if field_mapping is None:
            field_mapping = {}
            try:
                import os
                import sqlite3
                mapping_db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "CONF", "type_mapping.db")
                if os.path.exists(mapping_db_path):
                    mapping_conn = sqlite3.connect(mapping_db_path)
                    mapping_conn.row_factory = sqlite3.Row
                    mapping_cursor = mapping_conn.cursor()
                    
                    # Если source_object_name и source_object_type переданы извне, используем их
                    # Это позволяет частным writers указывать правильный источник маппинга
                    if source_object_name is None or source_object_type is None:
                        # Автоматически определяем имя объекта источника через маппинг объектов
                        resolved_source_object_name = catalog_name
                        mapping_cursor.execute("""
                            SELECT source_name, object_type
                            FROM object_mapping
                            WHERE target_name = ?
                        """, (catalog_name,))
                        obj_row = mapping_cursor.fetchone()
                        if obj_row:
                            resolved_source_object_name = obj_row["source_name"]
                            # sqlite3.Row не поддерживает .get(), используем try/except для безопасного доступа
                            try:
                                resolved_source_object_type = obj_row["object_type"] if obj_row["object_type"] else "catalog"
                            except (KeyError, IndexError):
                                resolved_source_object_type = "catalog"
                            verbose_print(f"  → Найден маппинг объекта: {resolved_source_object_name} ({resolved_source_object_type}) -> {catalog_name}")
                        else:
                            # Если маппинг объекта не найден, используем catalog_name и type 'catalog' по умолчанию
                            resolved_source_object_name = catalog_name
                            resolved_source_object_type = 'catalog'
                            verbose_print(f"  → Маппинг объекта не найден, используем по умолчанию: {resolved_source_object_name} ({resolved_source_object_type})")
                        
                        # Используем автоматически определенные значения только если не переданы извне
                        if source_object_name is None:
                            source_object_name = resolved_source_object_name
                        if source_object_type is None:
                            source_object_type = resolved_source_object_type
                    else:
                        verbose_print(f"  → Используем переданный исходный объект: {source_object_type}.{source_object_name} (для {catalog_name})")
                    
                    # Загружаем маппинг полей по имени объекта источника
                    mapping_cursor.execute("""
                        SELECT field_name, target_field_name, source_type, target_type, status, search_method
                        FROM field_mapping
                        WHERE object_type = ? AND object_name = ?
                    """, (source_object_type, source_object_name))
                    rows_fetched = mapping_cursor.fetchall()
                    for row in rows_fetched:
                        source_field = row["field_name"]
                        target_field = row["target_field_name"]
                        field_mapping[source_field] = {
                            "target_field": target_field,
                            "source_type": row["source_type"],
                            "target_type": row["target_type"],
                            "status": row["status"],
                            "search_method": row["search_method"] if row["search_method"] else None,
                        }
                        if target_field and target_field != source_field:
                            field_mapping[target_field] = field_mapping[source_field]
                    mapping_conn.close()
                    verbose_print(f"  → Загружено полей маппинга: {len(field_mapping)}")
            except Exception as e:
                verbose_print(f"  ⚠ Ошибка при загрузке маппинга: {e}")
        
        # ЭТАП 5-6: УСТАНОВКА ССЫЛОЧНЫХ ПОЛЕЙ
        verbose_print(f"  [ЭТАП 6] Установка ссылочных полей")
        from tools.writer_utils import parse_reference_field, get_reference_by_uuid
        
        for field_name, field_value in item_data.items():
            
            if field_name in ('uuid', 'Ссылка', 'ЭтоГруппа') or field_name in standard_fields:
                continue
            
            # Обрабатываем поля _UUID, _Представление, _Тип для string_to_reference_by_name
            if field_name.endswith('_UUID') or field_name.endswith('_Представление') or field_name.endswith('_Тип'):
                # Пропускаем служебные поля - они будут обработаны вместе с основным полем
                continue
            
            if not field_value:
                continue
            
            # Пропускаем предопределенные значения справочников (будут обработаны на этапе 7-8)
            if isinstance(field_value, str) and field_value.startswith("Справочник."):
                continue
            
            # Проверяем, есть ли отдельные поля _UUID, _Представление, _Тип для этого поля
            # Это означает, что базовый процессор обработал поле с search_method = "string_to_reference_by_name"
            uuid_field = f"{field_name}_UUID"
            presentation_field = f"{field_name}_Представление"
            type_field = f"{field_name}_Тип"
            
            ref_data = None
            if uuid_field in item_data or presentation_field in item_data or type_field in item_data:
                # Базовый процессор создал отдельные поля для string_to_reference_by_name
                ref_uuid = item_data.get(uuid_field, "")
                ref_presentation = item_data.get(presentation_field, "")
                ref_type = item_data.get(type_field, "")
                
                if ref_presentation or ref_type:
                    ref_data = {
                        "uuid": ref_uuid or "",
                        "presentation": ref_presentation or "",
                        "type": ref_type or ""
                    }
            
            # Если не нашли в отдельных полях, пробуем парсить как JSON
            if not ref_data:
                ref_data = parse_reference_field(field_value)
            
            if not ref_data:
                continue
            
            ref_uuid = ref_data.get('uuid', '')
            ref_type = ref_data.get('type', '')
            ref_presentation = ref_data.get('presentation', '')
            
            # Нормализуем тип: СправочникСсылка. -> Справочник.
            if ref_type.startswith("СправочникСсылка."):
                ref_type = ref_type.replace("СправочникСсылка.", "Справочник.", 1)
            
            # Приоритет: search_method из JSON (ref_data), иначе из field_mapping
            search_method = ref_data.get("search_method")
            if not search_method and field_name in field_mapping:
                search_method = field_mapping[field_name].get("search_method")
            if not search_method:
                for source_field, mapping_info in field_mapping.items():
                    if mapping_info.get("target_field") == field_name:
                        search_method = mapping_info.get("search_method")
                        field_mapping[field_name] = mapping_info
                        break
            
            ref_obj = None
            
            # reference_by_code: presentation содержит код, ищем через НайтиПоКоду
            if search_method == "reference_by_code" and ref_type and ref_type.startswith("Справочник."):
                code_value = (ref_presentation or "").strip()  # presentation = код при reference_by_code
                if code_value:
                    ref_obj = _find_catalog_by_code_in_receiver(com_object, str(code_value), ref_type)
                if not ref_obj and ref_uuid and ref_uuid != "00000000-0000-0000-0000-000000000000":
                    ref_obj = get_reference_by_uuid(com_object, ref_uuid, ref_type)
            # Для string_to_reference_by_name обрабатываем отдельно, даже если UUID пустой
            elif search_method == "string_to_reference_by_name" and ref_presentation:
                # Для string_to_reference_by_name НЕ используем UUID из БД,
                # чтобы сначала всегда искать по наименованию, а не создавать новый с новым UUID
                ref_obj = find_or_create_reference_by_name(
                    com_object, ref_type, ref_presentation, None,  # ref_uuid = None для поиска по наименованию
                    type_name, catalog_name, uuid_value, field_name,
                    None, processed_db  # source_data не нужен, так как ищем по наименованию
                )
            # Для string_to_reference_by_full_name обрабатываем отдельно, даже если UUID пустой
            elif search_method == "string_to_reference_by_full_name" and ref_presentation:
                # Для string_to_reference_by_full_name НЕ используем UUID из БД,
                # чтобы сначала всегда искать по полному наименованию, а не создавать новый с новым UUID
                ref_obj = find_or_create_reference_by_full_name(
                    com_object, ref_type, ref_presentation, None,  # ref_uuid = None для поиска по полному наименованию
                    type_name, catalog_name, uuid_value, field_name,
                    None, processed_db  # source_data не нужен, так как ищем по полному наименованию
                )
            elif ref_uuid and ref_uuid != "00000000-0000-0000-0000-000000000000" and ref_type:
                # Для обычных ссылочных полей ищем по UUID
                if ref_uuid == uuid_value and ref_type == type_name:
                    try:
                        item_ref = getattr(item, "Ссылка", None)
                        if item_ref:
                            target_field = field_name_mapping.get(field_name, field_name)
                            setattr(item, target_field, item_ref)
                            continue
                    except Exception:
                        pass
                
                # Специальная обработка для полей типа ПланСчетов.* - поиск по коду из маппинга
                if ref_type and (ref_type.startswith("ПланСчетов.") or ref_type.startswith("ChartOfAccountsRef.")):
                    from tools.chart_of_accounts_mapper import extract_account_code, get_mapped_account_code, load_mapping
                    from tools.logger import verbose_print
                    
                    # Извлекаем код счета из представления
                    source_code = extract_account_code(ref_presentation)
                    if source_code:
                        # Загружаем маппинг плана счетов
                        mapping_path = "CONF/chart_of_accounts_mapping.json"
                        mapping, _ = load_mapping(mapping_path)
                        
                        # Получаем маппированный код
                        mapped_code = get_mapped_account_code(source_code, mapping)
                        if mapped_code:
                            # Ищем счет по коду в приемнике
                            ref_obj = _find_account_by_code_in_receiver(com_object, mapped_code, ref_type)
                            if ref_obj:
                                verbose_print(f"  → Найден счет по коду из маппинга: {source_code} -> {mapped_code}")
                            else:
                                verbose_print(f"  ⚠ Счет с кодом '{mapped_code}' (из маппинга {source_code}) не найден в приемнике")
                
                # Если не нашли через маппинг, пробуем стандартный поиск по UUID
                if not ref_obj:
                    ref_obj = get_reference_by_uuid(com_object, ref_uuid, ref_type)
                
                if not ref_obj:
                    from tools.writer_utils import get_predefined_element_by_name
                    if ref_type and ref_presentation:
                        try:
                            predefined_obj = get_predefined_element_by_name(com_object, ref_type, ref_presentation)
                            if predefined_obj:
                                ref_obj = predefined_obj
                        except Exception:
                            pass
                    
                    if not ref_obj:
                        # Для плана счетов НЕ создаем новый элемент, только поиск
                        if ref_type and (ref_type.startswith("ПланСчетов.") or ref_type.startswith("ChartOfAccountsRef.")):
                            from tools.logger import verbose_print
                            verbose_print(f"  ⚠ Счет плана счетов не найден и не будет создан: {ref_presentation} (UUID: {ref_uuid[:30]}...)")
                        else:
                            # Подготавливаем source_data с информацией о is_group из JSON
                            source_data_for_ref = None
                            if 'is_group' in ref_data:
                                # Если is_group есть в JSON, создаем source_data с этой информацией
                                source_data_for_ref = {'is_group': ref_data['is_group']}
                            
                            ref_obj = create_reference_by_uuid(
                                com_object, ref_uuid, ref_type, ref_presentation, source_data_for_ref, processed_db
                            )
            
            if ref_obj:
                try:
                    target_field = field_name_mapping.get(field_name, field_name)
                    setattr(item, target_field, ref_obj)
                    verbose_print(f"  → Установлено ссылочное поле {target_field}: {ref_presentation}")
                except Exception as e:
                    verbose_print(f"  ⚠ Не удалось установить ссылочное поле {field_name}: {e}")
        
        # ЭТАП 7-8: УСТАНОВКА ПЕРЕЧИСЛЕНИЙ И ОБЫЧНЫХ ПОЛЕЙ
        verbose_print(f"  [ЭТАП 7-8] Установка перечислений и обычных полей")
        for field_name, field_value in item_data.items():
            if field_name in ('uuid', 'Ссылка', 'ЭтоГруппа') or field_name in standard_fields:
                continue
            if field_name.endswith('_UUID') or field_name.endswith('_Представление') or field_name.endswith('_Тип'):
                continue
            
            # Пропускаем ссылочные поля (уже обработаны)
            ref_data = parse_reference_field(field_value)
            if ref_data:
                continue
            
            if field_value is not None and field_value != '':
                if isinstance(field_value, str) and field_value.startswith("Перечисление."):
                    enum_obj = _get_enum_from_string(com_object, field_value)
                    if enum_obj:
                        try:
                            setattr(item, field_name, enum_obj)
                            verbose_print(f"    ✓ Установлено поле перечисления '{field_name}': {field_value}")
                        except Exception as e:
                            verbose_print(f"    ⚠ Ошибка при установке поля перечисления '{field_name}': {e}")
                elif isinstance(field_value, str) and field_value.startswith("Справочник."):
                    # Обрабатываем предопределенные значения справочников
                    predefined_ref = _get_predefined_reference_from_string(com_object, field_value)
                    if predefined_ref:
                        try:
                            setattr(item, field_name, predefined_ref)
                            verbose_print(f"    ✓ Установлено поле предопределенного справочника '{field_name}': {field_value}")
                        except Exception as e:
                            verbose_print(f"    ⚠ Ошибка при установке поля предопределенного справочника '{field_name}': {e}")
                else:
                    if isinstance(field_value, str):
                        if field_value.lower() == 'true':
                            field_value = True
                        elif field_value.lower() == 'false':
                            field_value = False
                        # Проверяем, является ли это датой (формат: "YYYY-MM-DD HH:MM:SS+00:00" или "YYYY-MM-DD")
                        elif _is_date_string(field_value):
                            date_obj = _convert_string_to_date(com_object, field_value)
                            if date_obj:
                                field_value = date_obj
                    try:
                        setattr(item, field_name, field_value)
                    except Exception:
                        pass
        
        return item
        
    except Exception as e:
        verbose_print(f"  ✗ Ошибка при подготовке {catalog_name} '{item_name}': {e}")
        import traceback
        traceback.print_exc()
        return None


def finalize_catalog_item(
    com_object,
    item,
    item_data: Dict,
    catalog_name: str,
    type_name: str,
    processed_db: Optional[str] = None,
) -> bool:
    """
    Завершает запись элемента справочника: выполняет этапы 10-12 (режим обмена, запись, сохранение).
    
    Этапы:
    10. Установка режима обмена данными
    11. Запись элемента
    12. Сохранение в БД reference_objects
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item: COM-объект элемента справочника
        item_data: Словарь с данными элемента
        catalog_name: Имя справочника в 1С
        type_name: Полное имя типа
        processed_db: Путь к обработанной БД
        
    Returns:
        True если успешно, False если ошибка
    """
    from tools.logger import verbose_print
    
    item_name = item_data.get('Наименование', 'Без наименования')
    uuid_value = item_data.get('uuid', '')
    
    try:
        # ЭТАП 10: УСТАНОВКА РЕЖИМА ОБМЕНА ДАННЫМИ
        verbose_print(f"  [ЭТАП 10] Установка режима обмена данными перед записью")
        try:
            item_exchange = safe_getattr(item, "ОбменДанными", None)
            if item_exchange:
                item_exchange = call_if_callable(item_exchange)
                if item_exchange:
                    item_exchange.Загрузка = True
                    verbose_print(f"  → Режим обмена данными установлен")
        except Exception as e:
            verbose_print(f"  ⚠ Ошибка при установке режима обмена данными: {e}")
        
        # ЭТАП 11: ЗАПИСЬ ЭЛЕМЕНТА
        verbose_print(f"  [ЭТАП 11] Запись элемента через item.Записать()")
        item.Записать()
        verbose_print(f"  [ЭТАП 11] ✓ Элемент записан")
        
        # ЭТАП 12: СОХРАНЕНИЕ В БД reference_objects
        verbose_print(f"  [ЭТАП 12] Сохранение в БД reference_objects")
        if uuid_value and uuid_value != "00000000-0000-0000-0000-000000000000":
            try:
                import sqlite3
                import os
                refs_db_path = get_reference_objects_db_path()
                
                source_data = None
                if processed_db:
                    try:
                        source_conn = sqlite3.connect(processed_db)
                        source_cursor = source_conn.cursor()
                        if type_name.startswith("Справочник."):
                            table_name = type_name.replace("Справочник.", "").lower()
                            source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?", (f"%{table_name}%",))
                            tables = [row[0] for row in source_cursor.fetchall()]
                            if tables:
                                table_name = tables[0]
                                source_cursor.execute(f"SELECT * FROM {table_name} WHERE uuid = ?", (uuid_value,))
                                row = source_cursor.fetchone()
                                if row:
                                    column_names = [desc[0] for desc in source_cursor.description]
                                    source_data = {}
                                    for i, col_name in enumerate(column_names):
                                        source_data[col_name] = row[i]
                        source_conn.close()
                    except Exception:
                        pass
                
                if not source_data:
                    source_data = item_data
                
                conn = sqlite3.connect(refs_db_path)
                save_reference_object(
                    conn, uuid_value, type_name, item_name, source_data,
                    filled=True, parent_type="catalog", parent_name=catalog_name,
                    parent_uuid="", field_name=""
                )
                conn.close()
                verbose_print(f"  → Сохранено в БД reference_objects")
            except Exception as e:
                verbose_print(f"  ⚠ Ошибка при сохранении в reference_objects: {e}")
        
        verbose_print(f"  ✓ {catalog_name} '{item_name}': записан")
        return True
        
    except Exception as e:
        verbose_print(f"  ✗ Ошибка при записи {catalog_name} '{item_name}': {e}")
        import traceback
        traceback.print_exc()
        return False
