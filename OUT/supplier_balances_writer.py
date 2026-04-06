# -*- coding: utf-8 -*-
"""
Модуль выгрузки остатков взаиморасчетов с поставщиками в документ «Ввод начальных остатков».
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
ACCOUNTING_SECTION = "РасчетыСПоставщикамиИПодрядчиками"
COMMENT_MARKER = "### Загрузка остатков взаиморасчетов с поставщиками (31.12.2025) ###"

def _parse_date_to_1c(com_object, date_str):
    """Конвертирует строку даты в объект 1С Дата."""
    if not date_str:
        return None
    try:
        d_str = str(date_str).replace("-", "").replace(":", "").replace(" ", "").replace(".", "")[:14]
        if len(d_str) < 8:
            return None
        year = int(d_str[0:4])
        month = int(d_str[4:6])
        day = int(d_str[6:8])
        hour = int(d_str[8:10]) if len(d_str) >= 10 else 0
        minute = int(d_str[10:12]) if len(d_str) >= 12 else 0
        second = int(d_str[12:14]) if len(d_str) >= 14 else 0
        return com_object.NewObject("Дата", year, month, day, hour, minute, second)
    except Exception:
        return None


def _date_to_1c_string(date_str):
    """Преобразует дату в строку формата 1С YYYYMMDDHHMISS для присвоения через COM."""
    if not date_str:
        return None
    try:
        # Поддержка YYYY-MM-DD HH:MM:SS, YYYY-MM-DD, ISO с T
        d_str = str(date_str).replace("-", "").replace(":", "").replace(" ", "").replace(".", "").replace("T", "")[:14]
        if len(d_str) < 8:
            return None
        # Дополняем до 14 символов (YYYYMMDDHHMISS)
        return d_str.ljust(14, "0")
    except Exception:
        return None


def get_or_create_settlement_doc(com_object, item, org_ref, kontr_ref, dog_ref, val_ref, acc_ref=None):
    """Поиск или создание Документа расчетов с контрагентом с использованием UUID исходного документа."""
    doc_rasch_info = parse_reference_field(item.get("Документ"))
    if not doc_rasch_info or doc_rasch_info["uuid"] == "00000000-0000-0000-0000-000000000000":
        return None
        
    doc_uuid = doc_rasch_info["uuid"]
    doc_number = item.get("Документ_Номер", "")
    doc_date_str = item.get("Документ_Дата", "")
    doc_num_vhod = item.get("Документ_НомерВходящий")
    doc_date_vhod_str = item.get("Документ_ДатаВходящая")
    doc_sum = item.get("Документ_Сумма", 0)
    
    # Валюта документа
    val_doc_json = item.get("Документ_Валюта")
    val_doc_ref = val_ref # Fallback
    if val_doc_json:
        val_doc_info = parse_reference_field(val_doc_json)
        if val_doc_info and val_doc_info["uuid"] != "00000000-0000-0000-0000-000000000000":
            val_doc_ref = create_reference_by_uuid(com_object, val_doc_info["uuid"], "Справочник.Валюты", ref_presentation=val_doc_info.get("presentation", ""))

    # Конвертируем дату
    doc_date = _parse_date_to_1c(com_object, doc_date_str)
    doc_date_vhod = _parse_date_to_1c(com_object, doc_date_vhod_str)

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
            
            # Заполняем входящие данные: номер — строка, дата — строка YYYYMMDDHHMISS (1С COM так принимает)
            doc_obj.НомерВходящегоДокумента = doc_num_vhod if doc_num_vhod is not None else ""
            date_1c_str = _date_to_1c_string(doc_date_vhod_str)
            if date_1c_str:
                doc_obj.ДатаВходящегоДокумента = date_1c_str
            
            # П.3: custom_* из СчетФактураПолученный
            if hasattr(doc_obj, "custom_НомерВходящегоДокументаСчетФактура"):
                val_sf = item.get("Документ_НомерВходящийСФ", "")
                if val_sf is not None:
                    doc_obj.custom_НомерВходящегоДокументаСчетФактура = str(val_sf)
            if hasattr(doc_obj, "custom_ДатаВходящегоДокументаСчетФактура"):
                date_sf_str = _date_to_1c_string(item.get("Документ_ДатаВходящаяСФ"))
                if date_sf_str:
                    doc_obj.custom_ДатаВходящегоДокументаСчетФактура = date_sf_str
            
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

def write_supplier_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    """Записывает остатки взаиморасчетов с поставщиками в документ Ввод начальных остатков."""
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ ВЗАИМОРАСЧЕТОВ С ПОСТАВЩИКАМИ В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    # Шаг 1: Чтение из БД
    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False
    
    items = get_from_db(db_connection, "supplier_balances")
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
                            
                        new_row.СчетУчета = acc_ref

                    # 2. Теперь создаем/ищем документ расчетов, передавая все найденные ссылки
                    doc_rasch_ref = get_or_create_settlement_doc(
                        com_object, 
                        item, 
                        org_ref, 
                        kontr_ref, 
                        dog_ref, 
                        val_ref,
                        acc_ref=acc_ref
                    )
                    if doc_rasch_ref:
                        new_row.Документ = doc_rasch_ref
                    
                    # Суммы
                    try:
                        amount_raw = item.get("СуммаВзаиморасчетовОстаток")
                        amount = float(amount_raw) if amount_raw is not None and amount_raw != "" else 0
                    except:
                        amount = 0
                    
                    # В УХ для поставщиков (60.01) часто используется СуммаКт
                    # А для авансов (60.02) - СуммаДт или просто Сумма
                    # Для 60.04 (расчеты по субподряду) - СуммаКт (кредит)
                    acc_code = ""
                    if acc_info:
                        acc_code = extract_account_code(acc_info.get("presentation", "")) or ""
                    
                    if acc_code.startswith("60.01") or acc_code.startswith("60.04"):
                        # Расчеты с поставщиками и расчеты по субподряду - кредит
                        if hasattr(new_row, "СуммаКт"):
                            new_row.СуммаКт = amount
                        else:
                            new_row.Сумма = amount
                    elif acc_code.startswith("60.02") or acc_code.startswith("60.05"):
                        # Авансы выданные и авансы по субподряду - дебет
                        if hasattr(new_row, "СуммаДт"):
                            new_row.СуммаДт = amount
                        else:
                            new_row.Сумма = amount
                    else:
                        new_row.Сумма = amount
                        
                    if hasattr(new_row, "СуммаВзаиморасчетов"):
                        new_row.СуммаВзаиморасчетов = amount
                    
                    # НУ = БУ: устанавливаем сумму по налоговому учёту равной бухгалтерской
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
        write_supplier_balances_to_1c("BD/supplier_balances_processed.db", com)

