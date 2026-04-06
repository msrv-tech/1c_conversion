# -*- coding: utf-8 -*-
"""
Модуль выгрузки остатков по характеристикам в документ «Ввод начальных остатков».
"""

import os
import sys
import json
from typing import Dict, List, Optional

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field
from tools.base_writer import create_reference_by_uuid, find_object_by_uuid, _find_account_by_code_in_receiver
from tools.onec_connector import connect_to_1c, safe_getattr
from tools.logger import verbose_print
from tools.chart_of_accounts_mapper import load_mapping, extract_account_code, get_mapped_account_code

DOCUMENT_NAME = "ВводНачальныхОстатков"
TABLE_NAME = "НоменклатураНаСкладе"
COMMENT_MARKER = "### Загрузка остатков МПЗ по характеристикам (31.12.2025) ###"

def get_or_create_service_settlement_doc(com_object, org_ref):
    """Находит или создает служебный документ расчетов для использования в качестве партии."""
    comment = "### СЛУЖЕБНЫЙ: Для ввода остатков (Партия) ###"
    
    query_text = """ВЫБРАТЬ ПЕРВЫЕ 1
        Док.Ссылка КАК Ссылка
    ИЗ
        Документ.ДокументРасчетовСКонтрагентом КАК Док
    ГДЕ
        Док.Организация = &Организация
        И Док.Комментарий ПОДОБНО &Комментарий
    """
    
    query = com_object.NewObject("Запрос")
    query.Текст = query_text
    query.УстановитьПараметр("Организация", org_ref)
    query.УстановитьПараметр("Комментарий", f"%{comment}%")
    
    result = query.Выполнить()
    selection = result.Выбрать()
    
    if selection.Следующий():
        return selection.Ссылка
    
    # Создаем новый
    try:
        doc_manager = com_object.Документы.ДокументРасчетовСКонтрагентом
        doc_obj = doc_manager.СоздатьДокумент()
        doc_obj.Дата = "20251231235959"
        doc_obj.Организация = org_ref
        doc_obj.Комментарий = f"{comment}\nСоздан автоматически для заполнения поля Партия."
        
        # Заполняем минимально необходимые поля для УХ
        if hasattr(doc_obj, "НомерВходящегоДокумента"):
            doc_obj.НомерВходящегоДокумента = "СЛУЖЕБНЫЙ"
        if hasattr(doc_obj, "ДатаВходящегоДокумента"):
            doc_obj.ДатаВходящегоДокумента = "20251231235959"
            
        doc_obj.ОбменДанными.Загрузка = True
        doc_obj.Записать()
        return doc_obj.Ссылка
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка при создании служебного документа расчетов: {e}")
        return None

def write_characteristics_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    """Записывает остатки по характеристикам в документ Ввод начальных остатков."""
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ ПО ХАРАКТЕРИСТИКАМ В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    # Шаг 1: Чтение из БД
    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False
    
    items = get_from_db(db_connection, "characteristics_balances")
    db_connection.close()

    if not items:
        verbose_print("Нет данных для записи.")
        return True

    # Загружаем маппинг плана счетов
    mapping_path = "CONF/chart_of_accounts_mapping.json"
    coa_mapping, _ = load_mapping(mapping_path)

    verbose_print(f"Прочитано строк остатков для записи: {len(items)}")

    # Группируем по Организации и Разделу учета
    doc_groups = {}
    for item in items:
        org_json = item.get("Организация")
        if not org_json: continue
        org_info = parse_reference_field(org_json)
        org_uuid = org_info.get("uuid")
        if not org_uuid: continue
        
        # Определяем раздел учета для текущей строки
        acc_json = item.get("СчетУчета", "")
        acc_info = parse_reference_field(acc_json)
        acc_repr = acc_info.get("presentation", "")
        
        section = "Товары" # По умолчанию
        if acc_repr.startswith("10"):
            section = "Материалы"
        elif acc_repr.startswith("41"):
            section = "Товары"
        
        group_key = (org_uuid, section)
        if group_key not in doc_groups:
            doc_groups[group_key] = []
        doc_groups[group_key].append(item)

    try:
        documents_manager = safe_getattr(com_object, "Документы", None)
        doc_manager = safe_getattr(documents_manager, DOCUMENT_NAME, None)
        
        for (org_uuid, section), group_items in doc_groups.items():
            org_json = group_items[0].get("Организация")
            org_info = parse_reference_field(org_json)
            
            verbose_print(f"Обработка: Организация {org_info.get('presentation')}, Раздел: {section}")
            
            # Находим или создаем документ для этой организации и раздела
            query_text = f"""ВЫБРАТЬ ПЕРВЫЕ 1
                Док.Ссылка КАК Ссылка
            ИЗ
                Документ.{DOCUMENT_NAME} КАК Док
            ГДЕ
                Док.Комментарий ПОДОБНО "%{COMMENT_MARKER}%"
                И Док.Организация = &Организация
                И Док.РазделУчета = ЗНАЧЕНИЕ(Перечисление.РазделыУчетаДляВводаОстатков.{section})
                И Док.Дата МЕЖДУ ДАТАВРЕМЯ(2025, 12, 31, 0, 0, 0) И ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59)
            """
            
            query = com_object.NewObject("Запрос")
            query.Текст = query_text
            query.УстановитьПараметр("Организация", create_reference_by_uuid(com_object, org_uuid, "Справочник.Организации"))
            result = query.Выполнить()
            selection = result.Выбрать()
            
            doc_obj = None
            if selection.Следующий():
                verbose_print(f"  Обновляем существующий документ ({section})")
                doc_obj = selection.Ссылка.ПолучитьОбъект()
            else:
                verbose_print(f"  Создаем новый документ ({section})")
                doc_obj = doc_manager.СоздатьДокумент()
                doc_obj.Дата = "20251231235959"
                doc_obj.Организация = create_reference_by_uuid(com_object, org_uuid, "Справочник.Организации")
                doc_obj.Комментарий = f"{COMMENT_MARKER}\nЗагружено автоматически."
                
                try:
                    doc_obj.РазделУчета = getattr(com_object.Перечисления.РазделыУчетаДляВводаОстатков, section)
                except:
                    verbose_print(f"  ⚠ Не удалось установить РазделУчета {section}")

            # Эти поля заполняем всегда (и для новых, и для существующих)
            doc_obj.ОтражатьВБухгалтерскомУчете = True
            doc_obj.ОтражатьВНалоговомУчете = True

            # Получаем служебный документ расчетов для партии
            service_settlement_doc = get_or_create_service_settlement_doc(com_object, doc_obj.Организация)

            # Шаг 3: Заполнение табличной части
            tabular_section = safe_getattr(doc_obj, TABLE_NAME, None)
            if tabular_section:
                tabular_section.Очистить()
                
                for item in group_items:
                    new_row = tabular_section.Добавить()
                    
                    # Партия
                    if service_settlement_doc:
                        new_row.Партия = service_settlement_doc

                    # Номенклатура (UUID характеристики)
                    nom_json = item.get("Номенклатура")
                    nom_info = parse_reference_field(nom_json)
                    if nom_info and nom_info.get("uuid"):
                        new_row.Номенклатура = create_reference_by_uuid(
                            com_object, nom_info["uuid"], "Справочник.Номенклатура", 
                            ref_presentation=nom_info.get("presentation"), processed_db=sqlite_db_file
                        )
                    
                    # Склад
                    wh_json = item.get("Склад")
                    wh_info = parse_reference_field(wh_json)
                    if wh_info and wh_info.get("uuid"):
                        new_row.Склад = create_reference_by_uuid(com_object, wh_info["uuid"], "Справочник.Склады")
                    
                    # Счет учета
                    acc_json = item.get("СчетУчета")
                    acc_info = parse_reference_field(acc_json)
                    if acc_info:
                        # Используем маппинг счетов
                        source_code = extract_account_code(acc_info.get("presentation", ""))
                        if source_code:
                            mapped_code = get_mapped_account_code(source_code, coa_mapping)
                            if mapped_code:
                                # Ищем счет в приемнике по маппированному коду
                                acc_ref = _find_account_by_code_in_receiver(com_object, mapped_code, "ПланСчетов.Хозрасчетный")
                                if acc_ref:
                                    new_row.СчетУчета = acc_ref
                                else:
                                    verbose_print(f"  ⚠ Счет {mapped_code} (маппинг {source_code}) не найден в приемнике")
                            else:
                                # Если маппинга нет, пробуем исходный код
                                acc_ref = _find_account_by_code_in_receiver(com_object, source_code, "ПланСчетов.Хозрасчетный")
                                if acc_ref:
                                    new_row.СчетУчета = acc_ref
                                else:
                                    verbose_print(f"  ⚠ Счет {source_code} не найден в приемнике и маппинг отсутствует")
                        else:
                            # Если не удалось извлечь код, пробуем по UUID (как запасной вариант, если счета синхронизированы по UUID)
                            acc_ref = find_object_by_uuid(com_object, acc_info["uuid"], "ПланСчетов.Хозрасчетный")
                            if acc_ref:
                                new_row.СчетУчета = acc_ref
                            else:
                                verbose_print(f"  ⚠ Не удалось найти счет по UUID или извлечь код: {acc_info.get('presentation')}")
                    else:
                        verbose_print(f"  ⚠ Нет данных о счете для строки")
                    
                    # Количественные и стоимостные показатели
                    try:
                        qty = item.get("Quantity", 0)
                        qty = float(qty) if qty is not None and qty != "" else 0
                    except:
                        qty = 0
                        
                    try:
                        sum_val = item.get("Amount", 0)
                        sum_val = float(sum_val) if sum_val is not None and sum_val != "" else 0
                    except:
                        sum_val = 0
                    
                    new_row.Количество = qty
                    new_row.Сумма = sum_val
                    
                    if hasattr(new_row, "СуммаНУ"):
                        # По запросу: СуммаНУ = Сумма
                        new_row.СуммаНУ = sum_val

            # Шаг 4: Запись
            doc_obj.ОбменДанными.Загрузка = True
            try:
                doc_obj.Записать()
                verbose_print(f"  Документ успешно записан: {doc_obj.Ссылка}")
                
                # Попытка проведения
                try:
                    doc_obj.ОбменДанными.Загрузка = False
                    doc_obj.Записать(com_object.РежимЗаписиДокумента.Проведение)
                    verbose_print(f"  Документ успешно проведен: {doc_obj.Ссылка}")
                except Exception as post_err:
                    verbose_print(f"  ⚠ Не удалось провести документ (сохранен как черновик): {post_err}")
                    
            except Exception as write_err:
                verbose_print(f"  Ошибка при записи документа: {write_err}")

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
        write_characteristics_balances_to_1c("BD/characteristics_balances_processed.db", com)
