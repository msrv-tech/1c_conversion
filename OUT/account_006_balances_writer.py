# -*- coding: utf-8 -*-
"""
Модуль выгрузки остатков по счёту 006 (Бланки строгой отчётности) в документ «Ввод начальных остатков»,
раздел ПрочиеСчетаБухгалтерскогоУчета, табличная часть БухСправка.
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
ACCOUNTING_SECTION = "ПрочиеСчетаБухгалтерскогоУчета"
COMMENT_MARKER = "### Загрузка остатков счёта 006 бланки строгой отчётности (31.12.2025) ###"

SOURCE_ACCOUNT_CODE = "006"
MAPPING_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "CONF", "chart_of_accounts_mapping.json")


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


def _receiver_account_code(item: dict, coa_mapping: dict) -> str:
    """Код счёта в приёмнике по маппингу; по умолчанию 006."""
    acc_json = item.get("СчетУчета")
    acc_info = parse_reference_field(acc_json)
    source_code = None
    if acc_info:
        source_code = extract_account_code(acc_info.get("presentation", ""))
    if not source_code:
        source_code = SOURCE_ACCOUNT_CODE
    mapped = get_mapped_account_code(source_code, coa_mapping)
    if mapped:
        return mapped
    return source_code


def write_account_006_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    """Записывает остатки счёта 006 в документ Ввод начальных остатков, ТЧ БухСправка, раздел ПрочиеСчетаБухгалтерскогоУчета."""
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ СЧЁТА 006 (БЛАНКИ СТРОГОЙ ОТЧЁТНОСТИ) В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    coa_mapping, _ = load_mapping(MAPPING_PATH)

    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False

    items = get_from_db(db_connection, "account_006_balances")
    db_connection.close()

    if not items:
        verbose_print("Нет данных для записи.")
        return True

    verbose_print(f"Прочитано строк остатков для записи: {len(items)}")

    doc_groups = {}
    for item in items:
        org_json = item.get("Организация")
        if not org_json:
            continue
        org_info = parse_reference_field(org_json)
        org_uuid = org_info.get("uuid") if org_info else None
        if not org_uuid:
            continue
        if org_uuid not in doc_groups:
            doc_groups[org_uuid] = []
        doc_groups[org_uuid].append(item)

    try:
        documents_manager = safe_getattr(com_object, "Документы", None)
        doc_manager = safe_getattr(documents_manager, DOCUMENT_NAME, None)
        if not doc_manager:
            verbose_print("Ошибка: документ ВводНачальныхОстатков не найден в приемнике.")
            return False

        for org_uuid, group_items in doc_groups.items():
            org_info = parse_reference_field(group_items[0].get("Организация"))
            verbose_print(f"Обработка: Организация {org_info.get('presentation') if org_info else org_uuid}, Раздел: {ACCOUNTING_SECTION}")

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

            doc_obj.ОтражатьВБухгалтерскомУчете = True
            doc_obj.ОтражатьВНалоговомУчете = True

            tabular_section = safe_getattr(doc_obj, TABLE_NAME, None)
            if not tabular_section:
                verbose_print(f"  ⚠ Табличная часть {TABLE_NAME} не найдена в документе.")
                continue

            tabular_section.Очистить()

            for item in group_items:
                new_row = tabular_section.Добавить()

                target_code = _receiver_account_code(item, coa_mapping)
                acc_ref = _find_account_by_code_in_receiver(com_object, target_code, "ПланСчетов.Хозрасчетный")
                if acc_ref and hasattr(new_row, "СчетУчета"):
                    new_row.СчетУчета = acc_ref

                if hasattr(new_row, "Субконто1"):
                    ref1 = _ref_by_uuid_or_type(
                        com_object,
                        item.get("БланкиСтрогойОтчетности"),
                        "Справочник.БланкиСтрогойОтчетности",
                        processed_db=sqlite_db_file,
                    )
                    if ref1:
                        new_row.Субконто1 = ref1

                if hasattr(new_row, "Субконто2"):
                    ref2 = _ref_by_uuid_or_type(
                        com_object,
                        item.get("Склад"),
                        "Справочник.Склады",
                        processed_db=sqlite_db_file,
                    )
                    if ref2:
                        new_row.Субконто2 = ref2

                try:
                    qty = item.get("Quantity", 0)
                    qty = float(qty) if qty is not None and qty != "" else 0.0
                except (TypeError, ValueError):
                    qty = 0.0
                if hasattr(new_row, "Количество"):
                    new_row.Количество = qty

                try:
                    sum_val = item.get("Amount", 0)
                    sum_val = float(sum_val) if sum_val is not None and sum_val != "" else 0.0
                except (TypeError, ValueError):
                    sum_val = 0.0

                if hasattr(new_row, "Сумма"):
                    new_row.Сумма = sum_val
                if hasattr(new_row, "СуммаНУ"):
                    new_row.СуммаНУ = sum_val
                if hasattr(new_row, "СуммаПР"):
                    new_row.СуммаПР = sum_val
                if hasattr(new_row, "СуммаВР"):
                    new_row.СуммаВР = sum_val

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
        write_account_006_balances_to_1c("BD/account_006_balances_processed.db", com)
