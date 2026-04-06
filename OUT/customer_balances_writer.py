# -*- coding: utf-8 -*-
"""
Модуль выгрузки остатков взаиморасчетов с покупателями в документ «Ввод начальных остатков».
"""

import os
import sys
import json
from typing import Dict, List, Optional

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field
from tools.base_writer import create_reference_by_uuid, _find_account_by_code_in_receiver
from tools.onec_connector import connect_to_1c, safe_getattr, call_if_callable, find_object_by_uuid
from tools.logger import verbose_print
from tools.chart_of_accounts_mapper import load_mapping, extract_account_code, get_mapped_account_code

DOCUMENT_NAME = "ВводНачальныхОстатков"
TABLE_NAME = "РасчетыСКонтрагентами"
ACCOUNTING_SECTION = "РасчетыСПокупателямиИЗаказчиками"
COMMENT_MARKER = "### Загрузка остатков взаиморасчетов с покупателями (31.12.2025) ###"

def get_or_create_settlement_doc(com_object, item, org_ref, kontr_ref, dog_ref, val_ref, acc_ref=None):
    """Поиск или создание Документа расчетов с контрагентом с использованием UUID исходного документа."""
    doc_rasch_info = parse_reference_field(item.get("Документ"))
    if not doc_rasch_info or doc_rasch_info["uuid"] == "00000000-0000-0000-0000-000000000000":
        return None
        
    doc_uuid = doc_rasch_info["uuid"]
    doc_number = item.get("Документ_Номер", "")
    doc_date_str = item.get("Документ_Дата", "")
    doc_sum = item.get("Документ_Сумма", 0)
    
    # Валюта документа
    val_doc_json = item.get("Документ_Валюта")
    val_doc_ref = val_ref # Fallback
    if val_doc_json:
        val_doc_info = parse_reference_field(val_doc_json)
        if val_doc_info and val_doc_info["uuid"] != "00000000-0000-0000-0000-000000000000":
            val_doc_ref = create_reference_by_uuid(com_object, val_doc_info["uuid"], "Справочник.Валюты", ref_presentation=val_doc_info.get("presentation", ""))

    # Конвертируем дату
    doc_date = None
    if doc_date_str:
        try:
            d_str = doc_date_str.replace("-", "").replace(":", "").replace(" ", "")
            year = int(d_str[0:4])
            month = int(d_str[4:6])
            day = int(d_str[6:8])
            hour = int(d_str[8:10]) if len(d_str) >= 10 else 0
            minute = int(d_str[10:12]) if len(d_str) >= 12 else 0
            second = int(d_str[12:14]) if len(d_str) >= 14 else 0
            doc_date = com_object.NewObject("Дата", year, month, day, hour, minute, second)
        except:
            pass

    # Поиск по UUID
    try:
        uuid_obj = com_object.NewObject("УникальныйИдентификатор", doc_uuid)
        doc_ref = com_object.Документы.ДокументРасчетовСКонтрагентом.ПолучитьСсылку(uuid_obj)
        doc_obj = doc_ref.ПолучитьОбъект()
        
        if doc_obj is None:
            doc_obj = com_object.Документы.ДокументРасчетовСКонтрагентом.СоздатьДокумент()
            doc_obj.УстановитьСсылкуНового(doc_ref)
        
        # Дата обязательна. Если конвертация не удалась, используем текущую или фиксированную.
        actual_date = doc_date if doc_date else "20251231235959"
        
        try:
            # Пытаемся заполнить все поля
            doc_obj.Дата = actual_date
            doc_obj.Номер = doc_number
            doc_obj.Организация = org_ref
            doc_obj.Контрагент = kontr_ref
            doc_obj.ДоговорКонтрагента = dog_ref
            doc_obj.ВалютаДокумента = val_doc_ref
            doc_obj.СуммаДокумента = doc_sum
            
            # Счет учета (пробуем разные варианты имен реквизитов)
            if acc_ref:
                for attr_name in ["СчетУчетаРасчетовСКомитентом", "СчетУчетаРасчетов", "СчетРасчетов", "СчетУчета"]:
                    if hasattr(doc_obj, attr_name):
                        setattr(doc_obj, attr_name, acc_ref)
                        break
            
            # Заполняем входящие данные
            doc_obj.НомерВходящегоДокумента = doc_number
            doc_obj.ДатаВходящегоДокумента = actual_date
            
            # Тип документа источника для комментария
            src_type = doc_rasch_info.get("type", "Неизвестный тип")
            doc_obj.Комментарий = f"Загружено из УПП. Тип: {src_type}. UUID: {doc_uuid}."
            
            doc_obj.ОбменДанными.Загрузка = True
            doc_obj.Записать()
        except Exception as e_inner:
            verbose_print(f"  ⚠ Не удалось заполнить все поля Документа расчетов: {e_inner}")
            # Резервный вариант: только дата
            doc_obj.Дата = actual_date
            doc_obj.ОбменДанными.Загрузка = True
            doc_obj.Записать()
            
        return doc_obj.Ссылка
        
    except Exception as e:
        verbose_print(f"  ⚠ Критическая ошибка при работе с Документом расчетов (UUID: {doc_uuid}): {e}")
        return None


def get_or_create_settlement_doc_by_refs(com_object, org_ref, kontr_ref, dog_ref, val_ref, acc_ref=None):
    """Ищет или создаёт документ расчетов с контрагентом по организации, контрагенту и договору.
    Используется для строк без источника (76.А, 76.АА → 62.01.2, 62.02.2)."""
    if not org_ref or not kontr_ref:
        return None
    try:
        query = com_object.NewObject("Запрос")
        query.Текст = """ВЫБРАТЬ ПЕРВЫЕ 1 Док.Ссылка КАК Ссылка ИЗ Документ.ДокументРасчетовСКонтрагентом КАК Док ГДЕ Док.Организация = &Организация И Док.Контрагент = &Контрагент И Док.ДоговорКонтрагента = &ДоговорКонтрагента"""
        query.УстановитьПараметр("Организация", org_ref)
        query.УстановитьПараметр("Контрагент", kontr_ref)
        query.УстановитьПараметр("ДоговорКонтрагента", dog_ref if dog_ref else com_object.Справочники.ДоговорыКонтрагентов.ПустаяСсылка)
        result = query.Выполнить()
        selection = result.Выбрать()
        if selection.Следующий():
            return selection.Ссылка

        doc_obj = com_object.Документы.ДокументРасчетовСКонтрагентом.СоздатьДокумент()
        doc_obj.Дата = "20251231235959"
        doc_obj.Организация = org_ref
        doc_obj.Контрагент = kontr_ref
        doc_obj.ДоговорКонтрагента = dog_ref if dog_ref else com_object.Справочники.ДоговорыКонтрагентов.ПустаяСсылка
        if val_ref:
            doc_obj.ВалютаДокумента = val_ref
        if acc_ref:
            for attr_name in ["СчетУчетаРасчетовСКомитентом", "СчетУчетаРасчетов", "СчетРасчетов", "СчетУчета"]:
                if hasattr(doc_obj, attr_name):
                    setattr(doc_obj, attr_name, acc_ref)
                    break
        if hasattr(doc_obj, "НомерВходящегоДокумента"):
            doc_obj.НомерВходящегоДокумента = "Остатки 62 (аренда)"
        if hasattr(doc_obj, "ДатаВходящегоДокумента"):
            doc_obj.ДатаВходящегоДокумента = "20251231235959"
        doc_obj.Комментарий = "Ввод остатков 62.01.2/62.02.2 (из 76.А/76.АА). Создан автоматически."
        doc_obj.ОбменДанными.Загрузка = True
        doc_obj.Записать()
        return doc_obj.Ссылка
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка поиска/создания документа расчетов по реквизитам: {e}")
        return None


def write_customer_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    """Записывает остатки взаиморасчетов с покупателями в документ Ввод начальных остатков."""
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ ВЗАИМОРАСЧЕТОВ С ПОКУПАТЕЛЯМИ В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    # Шаг 1: Чтение из БД
    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False
    
    items = get_from_db(db_connection, "customer_balances")
    db_connection.close()

    if not items:
        verbose_print("Нет данных для записи.")
        return True

    # Загружаем маппинг плана счетов
    mapping_path = "CONF/chart_of_accounts_mapping.json"
    coa_mapping, _ = load_mapping(mapping_path)

    verbose_print(f"Прочитано строк для записи: {len(items)}")

    try:
        # Шаг 2: Поиск или создание документа
        documents_manager = safe_getattr(com_object, "Документы", None)
        doc_manager = safe_getattr(documents_manager, DOCUMENT_NAME, None)
        if not doc_manager:
            verbose_print(f"Ошибка: Менеджер документа {DOCUMENT_NAME} не найден.")
            return False

        # Группируем по Организации, так как в УХ РазделУчета и Организация в шапке
        # Для простоты в данном скрипте создаем один документ на каждую организацию
        org_items = {}
        for item in items:
            org_json = item.get("Организация")
            if not org_json: continue
            org_info = parse_reference_field(org_json)
            org_uuid = org_info.get("uuid")
            if not org_uuid: continue
            
            if org_uuid not in org_items:
                org_items[org_uuid] = {"info": org_info, "items": []}
            org_items[org_uuid]["items"].append(item)

        for org_uuid, data in org_items.items():
            org_info = data["info"]
            org_items_list = data["items"]
            
            verbose_print(f"Обработка организации: {org_info.get('presentation')} ({org_uuid})")

            # Пытаемся найти существующий документ
            query_text = f"""ВЫБРАТЬ ПЕРВЫЕ 1
                Док.Ссылка КАК Ссылка
            ИЗ
                Документ.{DOCUMENT_NAME} КАК Док
            ГДЕ
                Док.Комментарий ПОДОБНО "%{COMMENT_MARKER}%"
                И Док.Организация = &Организация
                И Док.РазделУчета = ЗНАЧЕНИЕ(Перечисление.РазделыУчетаДляВводаОстатков.{ACCOUNTING_SECTION})
                И Док.Дата МЕЖДУ ДАТАВРЕМЯ(2025, 12, 31, 0, 0, 0) И ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59)
            """
            
            org_ref = create_reference_by_uuid(com_object, org_uuid, "Справочник.Организации", ref_presentation=org_info.get("presentation", ""))
            
            query = com_object.NewObject("Запрос")
            query.Текст = query_text
            query.УстановитьПараметр("Организация", org_ref)
            
            result = query.Выполнить()
            selection = result.Выбрать()
            
            doc_obj = None
            if selection.Следующий():
                verbose_print(f"  Найден существующий документ. Обновляем.")
                doc_obj = selection.Ссылка.ПолучитьОбъект()
            else:
                verbose_print(f"  Создаем новый документ.")
                doc_obj = doc_manager.СоздатьДокумент()
                doc_obj.Дата = "20251231235959"
                doc_obj.Организация = org_ref
                doc_obj.РазделУчета = getattr(com_object.Перечисления.РазделыУчетаДляВводаОстатков, ACCOUNTING_SECTION)
                doc_obj.ОтражатьВБухгалтерскомУчете = True
                doc_obj.ОтражатьВНалоговомУчете = True
                doc_obj.Комментарий = f"{COMMENT_MARKER}\nЗагружено автоматически."

            # Шаг 3: Заполнение табличной части
            tabular_section = safe_getattr(doc_obj, TABLE_NAME, None)
            if tabular_section:
                tabular_section.Очистить()
                
                for item in org_items_list:
                    new_row = tabular_section.Добавить()
                    
                    # 1. Сначала определяем все ссылки, которые понадобятся и для строки, и для документа расчетов
                    
                    # Контрагент
                    kontr_ref = None
                    kontr_info = parse_reference_field(item.get("Контрагент"))
                    if kontr_info:
                        kontr_ref = create_reference_by_uuid(com_object, kontr_info["uuid"], "Справочник.Контрагенты", ref_presentation=kontr_info.get("presentation", ""), processed_db=sqlite_db_file)
                        new_row.Контрагент = kontr_ref
                    
                    # Договор или Сделка (в УХ Сделка из УПП часто переходит в ДоговорКонтрагента)
                    dog_ref = None
                    sdelka_info = parse_reference_field(item.get("Сделка"))
                    dog_info = parse_reference_field(item.get("Договор"))
                    
                    if sdelka_info and sdelka_info.get("uuid") and sdelka_info["uuid"] != "00000000-0000-0000-0000-000000000000":
                        # Если есть сделка, используем её как ДоговорКонтрагента
                        dog_ref = create_reference_by_uuid(
                            com_object, 
                            sdelka_info["uuid"], 
                            "Справочник.ДоговорыКонтрагентов", 
                            ref_presentation=sdelka_info.get("presentation", ""), 
                            processed_db=sqlite_db_file
                        )
                    elif dog_info:
                        # Иначе используем обычный договор
                        dog_ref = create_reference_by_uuid(
                            com_object, 
                            dog_info["uuid"], 
                            "Справочник.ДоговорыКонтрагентов", 
                            ref_presentation=dog_info.get("presentation", ""), 
                            processed_db=sqlite_db_file
                        )
                    new_row.ДоговорКонтрагента = dog_ref
                    
                    # Валюта
                    val_ref = None
                    val_info = parse_reference_field(item.get("Валюта"))
                    if val_info:
                        val_ref = create_reference_by_uuid(com_object, val_info["uuid"], "Справочник.Валюты", ref_presentation=val_info.get("presentation", ""), processed_db=sqlite_db_file)
                        new_row.Валюта = val_ref
                    
                    # Счет учета
                    acc_ref = None
                    mapped_code = None
                    acc_info = parse_reference_field(item.get("Счет"))
                    if acc_info:
                        # Используем маппинг счетов
                        source_code = extract_account_code(acc_info.get("presentation", ""))
                        if source_code:
                            mapped_code = get_mapped_account_code(source_code, coa_mapping)
                            if mapped_code:
                                acc_ref = _find_account_by_code_in_receiver(com_object, mapped_code, "ПланСчетов.Хозрасчетный")
                        
                        if not acc_ref:
                            # Запасной вариант: поиск по UUID
                            acc_ref = find_object_by_uuid(com_object, acc_info["uuid"], "ПланСчетов.Хозрасчетный")
                        
                        if not acc_ref and source_code:
                            # Последний шанс: поиск по исходному коду
                            acc_ref = _find_account_by_code_in_receiver(com_object, source_code, "ПланСчетов.Хозрасчетный")
                        
                        if not mapped_code and source_code:
                            mapped_code = source_code
                            
                        new_row.СчетУчета = acc_ref

                    # 2. Создаем/ищем документ расчетов. Если в источнике нет (76.А/76.АА) — создаём по реквизитам
                    doc_rasch_ref = get_or_create_settlement_doc(
                        com_object, 
                        item, 
                        org_ref, 
                        kontr_ref, 
                        dog_ref, 
                        val_ref,
                        acc_ref=acc_ref
                    )
                    if not doc_rasch_ref:
                        doc_rasch_ref = get_or_create_settlement_doc_by_refs(
                            com_object, org_ref, kontr_ref, dog_ref, val_ref, acc_ref
                        )
                    if doc_rasch_ref:
                        new_row.Документ = doc_rasch_ref
                    
                    # Суммы
                    try:
                        amount = item.get("СуммаВзаиморасчетовОстаток")
                        amount = float(amount) if amount is not None and amount != "" else 0
                    except:
                        amount = 0

                    # Для счёта 62.02.x (авансы полученные, включая 62.02.2 из 76.АА) — СуммаКт
                    acc_code = mapped_code or (extract_account_code(acc_info.get("presentation", "")) if acc_info else "") or ""

                    if acc_code.startswith("62.02"):
                        if hasattr(new_row, "СуммаКт"):
                            new_row.СуммаКт = amount
                        else:
                            new_row.Сумма = amount
                    else:
                        new_row.Сумма = amount
                    # Если в УХ есть СуммаВзаиморасчетов и СуммаРегл, заполняем обе
                    if hasattr(new_row, "СуммаВзаиморасчетов"):
                        new_row.СуммаВзаиморасчетов = amount
                    
                    # НУ = БУ: устанавливаем сумму по налоговому учёту равной бухгалтерской (как на счёте 60)
                    if hasattr(new_row, "СуммаРегл"):
                        new_row.СуммаРегл = amount
                    if hasattr(new_row, "СуммаНУ"):
                        new_row.СуммаНУ = amount
                    
                    # Курс и кратность
                    new_row.КурсВзаиморасчетов = 1
                    new_row.КратностьВзаиморасчетов = 1

            # Шаг 4: Запись и проведение
            doc_obj.ОбменДанными.Загрузка = True
            try:
                doc_obj.Записать()
                verbose_print(f"  Документ успешно записан: {doc_obj.Ссылка}")
                
                # Пробуем провести
                try:
                    doc_obj.ОбменДанными.Загрузка = False
                    doc_obj.Записать(com_object.РежимЗаписиДокумента.Проведение)
                    verbose_print(f"  Документ успешно проведен: {doc_obj.Ссылка}")
                except Exception as post_err:
                    verbose_print(f"  ⚠ Не удалось провести документ (сохранен как черновик): {post_err}")
            except Exception as write_err:
                verbose_print(f"  Ошибка при записи документа: {write_err}")
                return False

        return True

    except Exception as e:
        verbose_print(f"Критическая ошибка при записи в 1С: {e}")
        import traceback
        verbose_print(traceback.format_exc())
        return False

if __name__ == "__main__":
    target = os.getenv("TARGET_1C", "target")
    com = connect_to_1c(target)
    if com:
        write_customer_balances_to_1c("BD/customer_balances_processed.db", com)

