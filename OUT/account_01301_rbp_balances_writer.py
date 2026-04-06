# -*- coding: utf-8 -*-
"""
Выгрузка остатков по счёту 013.01 (Субконто1 = РБП) в документ «Ввод начальных остатков»,
раздел ПрочиеСчетаБухгалтерскогоУчета, ТЧ БухСправка. Только суммы БУ (СуммаДт/СуммаКт → Сумма).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field
from tools.base_writer import create_reference_by_uuid, find_object_by_uuid, _find_account_by_code_in_receiver
from tools.onec_connector import connect_to_1c, safe_getattr
from tools.logger import verbose_print

DOCUMENT_NAME = "ВводНачальныхОстатков"
TABLE_NAME = "БухСправка"
ACCOUNTING_SECTION = "ПрочиеСчетаБухгалтерскогоУчета"
COMMENT_MARKER = "### Загрузка остатков сч.013.01 РБП (31.12.2025) ###"


def _ref_by_uuid_or_type(com_object, ref_json, default_type: str, processed_db: str = None):
    info = parse_reference_field(ref_json)
    if not info or not info.get("uuid") or info.get("uuid") == "00000000-0000-0000-0000-000000000000":
        return None
    ref_type = (info.get("type") or default_type).strip()
    if not ref_type:
        return None
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


def write_account_01301_rbp_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ СЧЁТА 013.01 (РБП) В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False

    items = get_from_db(db_connection, "account_01301_rbp_balances", limit=0)
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
        doc_groups.setdefault(org_uuid, []).append(item)

    try:
        documents_manager = safe_getattr(com_object, "Документы", None)
        doc_manager = safe_getattr(documents_manager, DOCUMENT_NAME, None)
        if not doc_manager:
            verbose_print("Ошибка: документ ВводНачальныхОстатков не найден в приемнике.")
            return False

        for org_uuid, group_items in doc_groups.items():
            org_info = parse_reference_field(group_items[0].get("Организация"))
            verbose_print(
                f"Обработка: Организация {org_info.get('presentation') if org_info else org_uuid}, "
                f"Раздел: {ACCOUNTING_SECTION}"
            )

            org_ref = create_reference_by_uuid(
                com_object,
                org_uuid,
                "Справочник.Организации",
                ref_presentation=org_info.get("presentation", "") if org_info else "",
                processed_db=sqlite_db_file,
            )

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
            query.УстановитьПараметр("Организация", org_ref)
            result = query.Выполнить()
            selection = result.Выбрать()

            if selection.Следующий():
                verbose_print(f"  Обновляем существующий документ ({ACCOUNTING_SECTION})")
                doc_obj = selection.Ссылка.ПолучитьОбъект()
            else:
                verbose_print(f"  Создаем новый документ ({ACCOUNTING_SECTION})")
                doc_obj = doc_manager.СоздатьДокумент()
                doc_obj.Дата = "20251231235959"
                doc_obj.Организация = org_ref
                doc_obj.Комментарий = f"{COMMENT_MARKER}\nЗагружено автоматически."
                try:
                    doc_obj.РазделУчета = getattr(
                        com_object.Перечисления.РазделыУчетаДляВводаОстатков, ACCOUNTING_SECTION
                    )
                except AttributeError:
                    verbose_print(f"  ⚠ Не удалось установить РазделУчета {ACCOUNTING_SECTION}")

            doc_obj.ОтражатьВБухгалтерскомУчете = True
            doc_obj.ОтражатьВНалоговомУчете = False

            tabular_section = safe_getattr(doc_obj, TABLE_NAME, None)
            if not tabular_section:
                verbose_print(f"  ⚠ Табличная часть {TABLE_NAME} не найдена в документе.")
                continue

            tabular_section.Очистить()

            for item in group_items:
                rbp_json = item.get("РБП_ДляЗаписи")
                if not rbp_json:
                    verbose_print("  ⚠ Пропуск строки без РБП_ДляЗаписи")
                    continue

                target_code = (item.get("TargetAccountCode") or "").strip()
                if not target_code:
                    verbose_print("  ⚠ Пропуск строки без TargetAccountCode")
                    continue

                try:
                    sum_dt = float(item.get("СуммаДт")) if item.get("СуммаДт") not in (None, "") else 0.0
                except (TypeError, ValueError):
                    sum_dt = 0.0
                try:
                    sum_kt = float(item.get("СуммаКт")) if item.get("СуммаКт") not in (None, "") else 0.0
                except (TypeError, ValueError):
                    sum_kt = 0.0
                net = sum_dt - sum_kt
                if abs(net) < 0.01:
                    continue

                new_row = tabular_section.Добавить()

                acc_ref = _find_account_by_code_in_receiver(
                    com_object, target_code, "ПланСчетов.Хозрасчетный"
                )
                if acc_ref and hasattr(new_row, "СчетУчета"):
                    new_row.СчетУчета = acc_ref

                if hasattr(new_row, "Субконто1"):
                    rbp_ref = _ref_by_uuid_or_type(
                        com_object,
                        rbp_json,
                        "Справочник.РасходыБудущихПериодов",
                        processed_db=sqlite_db_file,
                    )
                    if rbp_ref:
                        new_row.Субконто1 = rbp_ref

                if hasattr(new_row, "Субконто2") and item.get("Субконто2"):
                    ref2 = _ref_by_uuid_or_type(
                        com_object,
                        item.get("Субконто2"),
                        "",
                        processed_db=sqlite_db_file,
                    )
                    if ref2:
                        new_row.Субконто2 = ref2

                if hasattr(new_row, "Субконто3") and item.get("Субконто3"):
                    ref3 = _ref_by_uuid_or_type(
                        com_object,
                        item.get("Субконто3"),
                        "",
                        processed_db=sqlite_db_file,
                    )
                    if ref3:
                        new_row.Субконто3 = ref3

                if hasattr(new_row, "Сумма"):
                    new_row.Сумма = net

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
        write_account_01301_rbp_balances_to_1c("BD/account_01301_rbp_balances_processed.db", com)
