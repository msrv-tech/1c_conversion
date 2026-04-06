# -*- coding: utf-8 -*-
"""
Модуль выгрузки остатков по счёту 20 (Основное производство) в документ «Ввод начальных остатков»,
раздел НезавершенноеПроизводство, табличная часть БухСправка.
Документы разбиваются по подразделениям — отдельный документ на каждую пару (Организация, ПодразделениеОрганизации).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field
from tools.base_writer import create_reference_by_uuid, find_object_by_uuid, _find_account_by_code_in_receiver
from tools.chart_of_accounts_mapper import load_mapping, extract_account_code, get_mapped_account_code
from tools.onec_connector import connect_to_1c, safe_getattr
from tools.logger import verbose_print

DOCUMENT_NAME = "ВводНачальныхОстатков"
TABLE_NAME = "БухСправка"
ACCOUNTING_SECTION = "НезавершенноеПроизводство"
COMMENT_MARKER = "### Загрузка остатков счёта 20 (31.12.2025) ###"


def _ref_by_uuid_or_type(com_object, ref_json, default_type: str, processed_db: str = None):
    """Возвращает ссылку по UUID; тип берётся из ref_json или default_type."""
    info = parse_reference_field(ref_json)
    if not info or not info.get("uuid") or info.get("uuid") == "00000000-0000-0000-0000-000000000000":
        return None
    ref_type = (info.get("type") or default_type).strip()
    if not ref_type:
        ref_type = default_type
    try:
        ref = find_object_by_uuid(com_object, info["uuid"], ref_type)
        if ref:
            return ref
        ref = create_reference_by_uuid(
            com_object,
            info["uuid"],
            ref_type,
            ref_presentation=info.get("presentation", ""),
            processed_db=processed_db,
        )
        return ref
    except Exception:
        return None


def write_account_20_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    """Записывает остатки счёта 20 в документ Ввод начальных остатков, ТЧ БухСправка, раздел НезавершенноеПроизводство."""
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ СЧЁТА 20 В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False

    items = get_from_db(db_connection, "account_20_balances")
    db_connection.close()

    if not items:
        verbose_print("Нет данных для записи.")
        return True

    # Загружаем маппинг плана счетов
    mapping_path = "CONF/chart_of_accounts_mapping.json"
    coa_mapping, _ = load_mapping(mapping_path)

    verbose_print(f"Прочитано строк остатков для записи: {len(items)}")

    # Группируем по (Организация, ПодразделениеОрганизации)
    doc_groups = {}
    for item in items:
        org_json = item.get("Организация")
        if not org_json:
            continue
        org_info = parse_reference_field(org_json)
        org_uuid = org_info.get("uuid") if org_info else None
        if not org_uuid:
            continue

        subdiv_json = item.get("ПодразделениеОрганизации")
        subdiv_info = parse_reference_field(subdiv_json)
        subdiv_uuid = (subdiv_info.get("uuid") or "").strip() if subdiv_info else ""
        if subdiv_uuid == "00000000-0000-0000-0000-000000000000":
            subdiv_uuid = ""

        group_key = (org_uuid, subdiv_uuid or "")
        if group_key not in doc_groups:
            doc_groups[group_key] = []
        doc_groups[group_key].append(item)

    try:
        documents_manager = safe_getattr(com_object, "Документы", None)
        doc_manager = safe_getattr(documents_manager, DOCUMENT_NAME, None)
        if not doc_manager:
            verbose_print("Ошибка: документ ВводНачальныхОстатков не найден в приемнике.")
            return False

        for (org_uuid, subdiv_uuid), group_items in doc_groups.items():
            org_info = parse_reference_field(group_items[0].get("Организация"))
            subdiv_info = parse_reference_field(group_items[0].get("ПодразделениеОрганизации"))
            subdiv_present = subdiv_info.get("presentation", "") if subdiv_info else ""
            verbose_print(f"Обработка: Организация {org_info.get('presentation') if org_info else org_uuid}, "
                         f"Подразделение: {subdiv_present or '(пусто)'}")

            # Поиск документа: по Организации, Подразделению, Разделу, Комментарию, Дате
            if subdiv_uuid:
                query_text = f"""ВЫБРАТЬ ПЕРВЫЕ 1
                    Док.Ссылка КАК Ссылка
                ИЗ
                    Документ.{DOCUMENT_NAME} КАК Док
                ГДЕ
                    Док.Комментарий ПОДОБНО "%{COMMENT_MARKER}%"
                    И Док.Организация = &Организация
                    И Док.ПодразделениеОрганизации = &ПодразделениеОрганизации
                    И Док.РазделУчета = ЗНАЧЕНИЕ(Перечисление.РазделыУчетаДляВводаОстатков.{ACCOUNTING_SECTION})
                    И Док.Дата МЕЖДУ ДАТАВРЕМЯ(2025, 12, 31, 0, 0, 0) И ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59)
                """
                query = com_object.NewObject("Запрос")
                query.Текст = query_text
                query.УстановитьПараметр("Организация", create_reference_by_uuid(com_object, org_uuid, "Справочник.Организации"))
                query.УстановитьПараметр("ПодразделениеОрганизации",
                    create_reference_by_uuid(com_object, subdiv_uuid, "Справочник.ПодразделенияОрганизаций",
                        ref_presentation=subdiv_info.get("presentation", ""), processed_db=sqlite_db_file))
            else:
                query_text = f"""ВЫБРАТЬ ПЕРВЫЕ 1
                    Док.Ссылка КАК Ссылка
                ИЗ
                    Документ.{DOCUMENT_NAME} КАК Док
                ГДЕ
                    Док.Комментарий ПОДОБНО "%{COMMENT_MARKER}%"
                    И Док.Организация = &Организация
                    И Док.ПодразделениеОрганизации = ЗНАЧЕНИЕ(Справочник.ПодразделенияОрганизаций.ПустаяСсылка)
                    И Док.РазделУчета = ЗНАЧЕНИЕ(Перечисление.РазделыУчетаДляВводаОстатков.{ACCOUNTING_SECTION})
                    И Док.Дата МЕЖДУ ДАТАВРЕМЯ(2025, 12, 31, 0, 0, 0) И ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59)
                """
                query = com_object.NewObject("Запрос")
                query.Текст = query_text
                query.УстановитьПараметр("Организация", create_reference_by_uuid(com_object, org_uuid, "Справочник.Организации"))

            result = query.Выполнить()
            selection = result.Выбрать()

            if selection.Следующий():
                verbose_print(f"  Обновляем существующий документ ({ACCOUNTING_SECTION})")
                doc_obj = selection.Ссылка.ПолучитьОбъект()
            else:
                verbose_print(f"  Создаем новый документ ({ACCOUNTING_SECTION})")
                doc_obj = doc_manager.СоздатьДокумент()
                doc_obj.Дата = "20251231235959"
                doc_obj.Организация = create_reference_by_uuid(com_object, org_uuid, "Справочник.Организации")
                doc_obj.Комментарий = f"{COMMENT_MARKER}\nЗагружено автоматически."
                try:
                    doc_obj.РазделУчета = getattr(com_object.Перечисления.РазделыУчетаДляВводаОстатков, ACCOUNTING_SECTION)
                except AttributeError:
                    verbose_print(f"  ⚠ Не удалось установить РазделУчета {ACCOUNTING_SECTION}")

                if subdiv_uuid and hasattr(doc_obj, "ПодразделениеОрганизации"):
                    subdiv_ref = create_reference_by_uuid(
                        com_object, subdiv_uuid, "Справочник.ПодразделенияОрганизаций",
                        ref_presentation=subdiv_info.get("presentation", "") if subdiv_info else "",
                        processed_db=sqlite_db_file)
                    doc_obj.ПодразделениеОрганизации = subdiv_ref

            doc_obj.ОтражатьВБухгалтерскомУчете = True
            doc_obj.ОтражатьВНалоговомУчете = True

            tabular_section = safe_getattr(doc_obj, TABLE_NAME, None)
            if not tabular_section:
                verbose_print(f"  ⚠ Табличная часть {TABLE_NAME} не найдена в документе.")
                continue

            tabular_section.Очистить()

            for item in group_items:
                new_row = tabular_section.Добавить()

                # СчетУчета — с маппингом
                acc_json = item.get("СчетУчета")
                acc_info = parse_reference_field(acc_json)
                if acc_info:
                    source_code = extract_account_code(acc_info.get("presentation", ""))
                    if source_code:
                        mapped_code = get_mapped_account_code(source_code, coa_mapping)
                        if mapped_code:
                            acc_ref = _find_account_by_code_in_receiver(com_object, mapped_code, "ПланСчетов.Хозрасчетный")
                        else:
                            acc_ref = _find_account_by_code_in_receiver(com_object, source_code, "ПланСчетов.Хозрасчетный")
                        if acc_ref and hasattr(new_row, "СчетУчета"):
                            new_row.СчетУчета = acc_ref
                    else:
                        if acc_info.get("uuid"):
                            acc_ref = find_object_by_uuid(com_object, acc_info["uuid"], "ПланСчетов.Хозрасчетный")
                            if acc_ref:
                                new_row.СчетУчета = acc_ref

                # В приёмнике: Субконто1 = Номенклатурная группа, Субконто2 = Статья затрат.
                # Подразделение — только в шапке документа.
                if hasattr(new_row, "Субконто1"):
                    ref_nom = _ref_by_uuid_or_type(com_object, item.get("Субконто1"), "Справочник.НоменклатурныеГруппы",
                        processed_db=sqlite_db_file)
                    if ref_nom:
                        new_row.Субконто1 = ref_nom

                if hasattr(new_row, "Субконто2"):
                    ref_st = _ref_by_uuid_or_type(com_object, item.get("Субконто2"), "Справочник.СтатьиЗатрат",
                        processed_db=sqlite_db_file)
                    if ref_st:
                        new_row.Субконто2 = ref_st

                # Валюта
                if hasattr(new_row, "Валюта"):
                    curr_ref = _ref_by_uuid_or_type(com_object, item.get("Валюта"), "Справочник.Валюты",
                        processed_db=sqlite_db_file)
                    if curr_ref:
                        new_row.Валюта = curr_ref

                # ВалютнаяСумма, СуммаКт, Сумма, Количество
                try:
                    curr_sum = item.get("CurrencyAmount", 0)
                    curr_sum = float(curr_sum) if curr_sum is not None and curr_sum != "" else 0.0
                except (TypeError, ValueError):
                    curr_sum = 0.0
                if hasattr(new_row, "ВалютнаяСумма"):
                    new_row.ВалютнаяСумма = curr_sum

                try:
                    sum_kt = item.get("AmountCredit", 0)
                    sum_kt = float(sum_kt) if sum_kt is not None and sum_kt != "" else 0.0
                except (TypeError, ValueError):
                    sum_kt = 0.0
                if hasattr(new_row, "СуммаКт"):
                    new_row.СуммаКт = sum_kt

                try:
                    sum_val = item.get("Amount", 0)
                    sum_val = float(sum_val) if sum_val is not None and sum_val != "" else 0.0
                except (TypeError, ValueError):
                    sum_val = 0.0
                if hasattr(new_row, "Сумма"):
                    new_row.Сумма = sum_val

                try:
                    amount_nu = item.get("AmountNU", 0)
                    amount_nu = float(amount_nu) if amount_nu is not None and amount_nu != "" else 0.0
                except (TypeError, ValueError):
                    amount_nu = 0.0
                try:
                    amount_pr = item.get("AmountPR", 0)
                    amount_pr = float(amount_pr) if amount_pr is not None and amount_pr != "" else 0.0
                except (TypeError, ValueError):
                    amount_pr = 0.0
                try:
                    amount_vr = item.get("AmountVR", 0)
                    amount_vr = float(amount_vr) if amount_vr is not None and amount_vr != "" else 0.0
                except (TypeError, ValueError):
                    amount_vr = 0.0
                if hasattr(new_row, "СуммаНУ"):
                    new_row.СуммаНУ = amount_nu
                if hasattr(new_row, "СуммаПР"):
                    new_row.СуммаПР = amount_pr
                if hasattr(new_row, "СуммаВР"):
                    new_row.СуммаВР = amount_vr

                try:
                    qty = item.get("Quantity", 0)
                    qty = float(qty) if qty is not None and qty != "" else 0.0
                except (TypeError, ValueError):
                    qty = 0.0
                if hasattr(new_row, "Количество"):
                    new_row.Количество = qty

            doc_obj.ОбменДанными.Загрузка = True
            try:
                doc_obj.Записать()
                verbose_print(f"  Документ успешно записан: {doc_obj.Ссылка}")
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
        write_account_20_balances_to_1c("BD/account_20_balances_processed.db", com)
