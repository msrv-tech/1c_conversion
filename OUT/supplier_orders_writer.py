# -*- coding: utf-8 -*-
"""
Модуль выгрузки документов «Заказ поставщику» (преобразованных в справочник «ДоговорыКонтрагентов»)
из обработанной БД в 1С приемник.

Данные в БД уже в формате приемника, запись происходит с сохранением UUID.
Все договоры создаются с видом соглашения "Спецификация".
"""

import os
import sys

# Добавляем родительскую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, get_default_organization_json, parse_reference_field
from tools.base_writer import (
    _get_enum_from_string,
    write_catalog_item,
    setup_exchange_mode,
    create_reference_by_uuid,
)
from tools.onec_connector import find_object_by_uuid
from tools.logger import verbose_print  # noqa: E402

fix_encoding()

_RAMOCHNYJ_VID_SOGLASHENIYA = "Перечисление.ВидыСоглашений.РамочныйДоговор"
_EMPTY_UUID = "00000000-0000-0000-0000-000000000000"


def _ensure_bazovyj_dogovor_ramochyj_vid_soglasheniya(com_object, item_data: dict) -> None:
    """
    После записи договора по заказу: у договора из БазовыйДоговор выставить ВидСоглашения = РамочныйДоговор,
    если он ещё не рамочный. Не трогает случай, когда базовый совпадает с записываемым договором.
    """
    spec_uuid = str(item_data.get("uuid") or "").strip().lower()

    base_uuid = ""
    raw_bd = item_data.get("БазовыйДоговор")
    if isinstance(raw_bd, str) and raw_bd.strip().startswith("{"):
        try:
            d = json.loads(raw_bd)
            base_uuid = str(d.get("uuid") or "").strip()
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass
    if not base_uuid:
        base_uuid = str(item_data.get("БазовыйДоговор_UUID") or "").strip()

    if not base_uuid or base_uuid == _EMPTY_UUID or base_uuid.lower() == spec_uuid:
        return

    enum_ram = _get_enum_from_string(com_object, _RAMOCHNYJ_VID_SOGLASHENIYA)
    if not enum_ram:
        return

    try:
        base_ref = find_object_by_uuid(com_object, base_uuid, "Справочник.ДоговорыКонтрагентов")
        if not base_ref:
            verbose_print(
                f"    ℹ️ БазовыйДоговор {base_uuid[:8]}… не найден — пропуск корректировки ВидСоглашения"
            )
            return
        obj = base_ref.ПолучитьОбъект()
        if not hasattr(obj, "ВидСоглашения"):
            verbose_print("    ⚠ У договора БазовыйДоговор нет реквизита ВидСоглашения")
            return
        try:
            if obj.ВидСоглашения == enum_ram:
                return
        except Exception:
            pass
        obj.ВидСоглашения = enum_ram
        obj.ОбменДанными.Загрузка = True
        obj.Записать()
        verbose_print("    ✓ БазовыйДоговор: ВидСоглашения = РамочныйДоговор")
    except Exception as e:
        verbose_print(f"    ⚠ Ошибка при установке ВидСоглашения у БазовыйДоговор: {e}")


def _write_item(com_object, item_data, processed_db=None):
    """
    Записывает элемент справочника ДоговорыКонтрагентов в 1С с сохранением UUID.
    
    Args:
        com_object: COM-объект подключения к 1С (приемник)
        item_data: Словарь с данными элемента
        processed_db: Путь к обработанной БД для получения данных ссылочных объектов
        
    Returns:
        True если успешно, False если ошибка
    """
    # Убеждаемся, что ВидСоглашения установлен в "Спецификация"
    vid_soglasheniya = item_data.get("ВидСоглашения", "")
    if not vid_soglasheniya or vid_soglasheniya != "Перечисление.ВидыСоглашений.Спецификация":
        item_data["ВидСоглашения"] = "Перечисление.ВидыСоглашений.Спецификация"
        verbose_print(f"    ✓ Установлен ВидСоглашения = Спецификация")
    
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
                org_data = json.loads(org_json_str)
                verbose_print(f"    ✓ Заполнено поле Организация: {org_data.get('presentation', '')}")
            except Exception:
                pass

    # Извлекаем НоменклатурнаяГруппа_UUID для догрузки (customДоговор в НоменклатурныеГруппы)
    ng_uuid = None
    ng_presentation = item_data.get("НоменклатурнаяГруппа_Представление", "") or ""
    ng_json = item_data.get("НоменклатурнаяГруппа")
    if ng_json and isinstance(ng_json, str) and ng_json.strip().startswith("{"):
        try:
            ng_parsed = parse_reference_field(ng_json)
            if ng_parsed:
                ng_uuid = ng_parsed.get("uuid", "")
                if not ng_presentation and ng_parsed.get("presentation"):
                    ng_presentation = ng_parsed.get("presentation", "")
        except Exception:
            pass
    if not ng_uuid:
        ng_uuid = item_data.get("НоменклатурнаяГруппа_UUID", "")
    if ng_uuid:
        ng_uuid = str(ng_uuid).strip()
    if ng_uuid == "00000000-0000-0000-0000-000000000000":
        ng_uuid = None

    # Извлекаем Номенклатура_UUID для догрузки (custom_ДоговорСпецификация в Номенклатура)
    nom_uuid = None
    nom_presentation = item_data.get("Номенклатура_Представление", "") or ""
    nom_json = item_data.get("Номенклатура")
    if nom_json and isinstance(nom_json, str) and nom_json.strip().startswith("{"):
        try:
            nom_parsed = parse_reference_field(nom_json)
            if nom_parsed:
                nom_uuid = nom_parsed.get("uuid", "")
                if not nom_presentation and nom_parsed.get("presentation"):
                    nom_presentation = nom_parsed.get("presentation", "")
        except Exception:
            pass
    if not nom_uuid:
        nom_uuid = item_data.get("Номенклатура_UUID", "")
    if nom_uuid:
        nom_uuid = str(nom_uuid).strip()
    if nom_uuid == "00000000-0000-0000-0000-000000000000":
        nom_uuid = None

    # Номенклатура и НоменклатурнаяГруппа не являются реквизитами ДоговорыКонтрагентов — не записываем
    for key in ("Номенклатура", "Номенклатура_UUID", "Номенклатура_Представление", "Номенклатура_Тип",
                "НоменклатурнаяГруппа", "НоменклатурнаяГруппа_UUID", "НоменклатурнаяГруппа_Представление", "НоменклатурнаяГруппа_Тип"):
        item_data.pop(key, None)

    # Записываем элемент в справочник ДоговорыКонтрагентов
    # Указываем source_object_name и source_object_type для правильной загрузки маппинга
    success = write_catalog_item(
        com_object,
        item_data,
        "ДоговорыКонтрагентов",
        "Справочник.ДоговорыКонтрагентов",
        ["Код", "Наименование", "ПометкаУдаления", "Комментарий"],
        processed_db=processed_db,
        source_object_name="ЗаказПоставщику",
        source_object_type="document"
    )

    if success:
        _ensure_bazovyj_dogovor_ramochyj_vid_soglasheniya(com_object, item_data)

    # Догрузка: обновляем НоменклатурныеГруппы.customДоговор = записанный договор
    if success and ng_uuid and item_data.get("uuid"):
        contract_uuid = str(item_data["uuid"]).strip()
        if contract_uuid and contract_uuid != "00000000-0000-0000-0000-000000000000":
            try:
                ng_ref = find_object_by_uuid(com_object, ng_uuid, "Справочник.НоменклатурныеГруппы")
                if not ng_ref:
                    ng_ref = create_reference_by_uuid(
                        com_object,
                        ng_uuid,
                        "Справочник.НоменклатурныеГруппы",
                        ng_presentation or f"НоменклатурнаяГруппа {ng_uuid[:8]}",
                        source_data=None,
                        processed_db=processed_db,
                    )
                    if ng_ref:
                        verbose_print(f"    ✓ НоменклатурнаяГруппа: создана неполная запись для догрузки")
                contract_ref = find_object_by_uuid(com_object, contract_uuid, "Справочник.ДоговорыКонтрагентов")
                if ng_ref and contract_ref:
                    ng_obj = ng_ref.ПолучитьОбъект()
                    ng_obj.customДоговор = contract_ref
                    ng_obj.ОбменДанными.Загрузка = True
                    ng_obj.Записать()
                    verbose_print(f"    ✓ НоменклатурнаяГруппа: customДоговор обновлён")
                elif not contract_ref:
                    verbose_print(f"    ⚠ Договор {contract_uuid[:8]}... не найден для догрузки")
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка догрузки customДоговор в НоменклатурныеГруппы: {e}")

    # Догрузка: обновляем Номенклатура.custom_ДоговорСпецификация = записанный договор (если Номенклатура приходит с Номенклатурной группой)
    if success and nom_uuid and ng_uuid and item_data.get("uuid"):
        contract_uuid = str(item_data["uuid"]).strip()
        if contract_uuid and contract_uuid != "00000000-0000-0000-0000-000000000000":
            try:
                nom_ref = find_object_by_uuid(com_object, nom_uuid, "Справочник.Номенклатура")
                if nom_ref:
                    contract_ref = find_object_by_uuid(com_object, contract_uuid, "Справочник.ДоговорыКонтрагентов")
                    if contract_ref:
                        nom_obj = nom_ref.ПолучитьОбъект()
                        if hasattr(nom_obj, "custom_ДоговорСпецификация"):
                            nom_obj.custom_ДоговорСпецификация = contract_ref
                            nom_obj.ОбменДанными.Загрузка = True
                            nom_obj.Записать()
                            verbose_print(f"    ✓ Номенклатура: custom_ДоговорСпецификация обновлён")
                        else:
                            verbose_print(f"    ⚠ Номенклатура: реквизит custom_ДоговорСпецификация не найден")
                    else:
                        verbose_print(f"    ⚠ Договор {contract_uuid[:8]}... не найден для догрузки в Номенклатуру")
                else:
                    verbose_print(f"    ℹ️ Номенклатура {nom_uuid[:8]}... не найдена в приёмнике (пропуск догрузки)")
            except Exception as e:
                verbose_print(f"    ⚠ Ошибка догрузки custom_ДоговорСпецификация в Номенклатура: {e}")

    return success


def write_supplier_orders_to_1c(sqlite_db_file, com_object, process_func=None):
    """
    Выгружает документы ЗаказПоставщику (преобразованные в ДоговорыКонтрагентов) 
    из обработанной БД в 1С приемник.
    
    Запись происходит в режиме ОбменДанными.Загрузка = Истина с сохранением UUID.
    Все договоры создаются с видом соглашения "Спецификация".
    
    Args:
        sqlite_db_file: Путь к файлу обработанной базы данных SQLite
        com_object: COM-объект подключения к 1С (приемник) - обязательный параметр
        process_func: Не используется (данные уже обработаны)
    
    Returns:
        True если успешно, False если ошибка
    """
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ДОКУМЕНТОВ ЗАКАЗПОСТАВЩИКУ (→ ДОГОВОРЫКОНТРАГЕНТОВ) ИЗ ОБРАБОТАННОЙ БД В 1С ПРИЕМНИК")
    verbose_print("=" * 80)
    
    if com_object is None:
        print("Ошибка: com_object обязателен")
        return False
    
    # Устанавливаем режим обмена данными
    setup_exchange_mode(com_object)
    
    # Шаг 1: Подключение к БД
    verbose_print("\n[1/3] Подключение к обработанной базе данных SQLite...")
    db_connection = connect_to_sqlite(sqlite_db_file)
    
    if not db_connection:
        print("Ошибка: Не удалось подключиться к базе данных SQLite")
        return False
    
    # Шаг 2: Чтение элементов из БД
    verbose_print("\n[2/3] Чтение документов из обработанной БД...")
    items = get_from_db(db_connection, "supplier_orders_processed")
    db_connection.close()
    
    if not items:
        verbose_print("Документы не найдены в обработанной базе данных")
        return False
    
    verbose_print(f"Найдено документов для записи: {len(items)}")
    
    # Шаг 3: Запись элементов в 1С
    verbose_print("\n[3/3] Запись документов в 1С приемник...")
    success_count = 0
    error_count = 0
    
    for i, item in enumerate(items, 1):
        try:
            uuid = item.get("uuid") or ""
            number = item.get("Номер", "")
            date = item.get("Дата", "")
            uuid_display = uuid[:8] if uuid else "N/A"
            
            verbose_print(f"\n[{i}/{len(items)}] Запись договора: Номер={number}, Дата={date}, UUID={uuid_display}...")
            
            result = _write_item(com_object, item, processed_db=sqlite_db_file)
            
            if result:
                success_count += 1
                verbose_print(f"    ✓ Договор записан успешно")
            else:
                error_count += 1
                verbose_print(f"    ✗ Ошибка при записи договора")
                
        except Exception as e:
            error_count += 1
            verbose_print(f"    ✗ Исключение при записи договора: {e}")
            import traceback
            verbose_print(traceback.format_exc())
            continue
    
    verbose_print("\n" + "=" * 80)
    verbose_print(f"ИТОГИ ЗАПИСИ:")
    verbose_print(f"  Успешно записано: {success_count}")
    verbose_print(f"  Ошибок: {error_count}")
    verbose_print("=" * 80)
    
    return success_count > 0

