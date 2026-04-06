# -*- coding: utf-8 -*-
"""
Модуль выгрузки остатков РБП в документ «Ввод начальных остатков».
97 → Раздел РасходыБудущихПериодов, ТЧ РасходыБудущихПериодов.
76.19→76.01.9, 76.09→76.09.1 → Раздел РасчетыСПрочимиДебиторамиИКредиторами.
76.01.9 с РБП: ТЧ РасходыБудущихПериодов (Субконто2=РБП). 76.09.1: ТЧ РасчетыСКонтрагентами.
97.21: документы разбиваются по подразделениям — отдельный документ на каждую пару (Организация, ПодразделениеОрганизации).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field
from tools.base_writer import create_reference_by_uuid, _find_account_by_code_in_receiver
from tools.onec_connector import connect_to_1c, safe_getattr
from tools.logger import verbose_print

DOCUMENT_NAME = "ВводНачальныхОстатков"
TABLE_NAME_RBP = "РасходыБудущихПериодов"
TABLE_NAME_CONTR = "РасчетыСКонтрагентами"
ACCOUNTING_SECTION_RBP = "РасходыБудущихПериодов"
ACCOUNTING_SECTION_CONTR = "РасчетыСПрочимиДебиторамиИКредиторами"
COMMENT_MARKER_97 = "### Загрузка остатков РБП сч.97 (31.12.2025) ###"
COMMENT_MARKER_76019 = "### Загрузка остатков РБП сч.76.01.9 (31.12.2025) ###"
COMMENT_MARKER_76091 = "### Загрузка остатков сч.76.09.1 (31.12.2025) ###"


def get_or_create_settlement_doc_by_refs(com_object, org_ref, kontr_ref, dog_ref, val_ref, acc_ref=None):
    """Ищет или создаёт документ расчетов с контрагентом."""
    if not org_ref or not kontr_ref:
        return None
    try:
        query = com_object.NewObject("Запрос")
        query.Текст = """ВЫБРАТЬ ПЕРВЫЕ 1 Док.Ссылка КАК Ссылка ИЗ Документ.ДокументРасчетовСКонтрагентом КАК Док
            ГДЕ Док.Организация = &Организация И Док.Контрагент = &Контрагент И Док.ДоговорКонтрагента = &ДоговорКонтрагента"""
        query.УстановитьПараметр("Организация", org_ref)
        query.УстановитьПараметр("Контрагент", kontr_ref)
        query.УстановитьПараметр("ДоговорКонтрагента", dog_ref if dog_ref else com_object.Справочники.ДоговорыКонтрагентов.ПустаяСсылка())
        result = query.Выполнить()
        selection = result.Выбрать()
        if selection.Следующий():
            return selection.Ссылка

        doc_obj = com_object.Документы.ДокументРасчетовСКонтрагентом.СоздатьДокумент()
        doc_obj.Дата = "20251231235959"
        doc_obj.Организация = org_ref
        doc_obj.Контрагент = kontr_ref
        doc_obj.ДоговорКонтрагента = dog_ref if dog_ref else com_object.Справочники.ДоговорыКонтрагентов.ПустаяСсылка()
        if val_ref:
            doc_obj.ВалютаДокумента = val_ref
        if acc_ref:
            for attr_name in ["СчетУчетаРасчетовСКомитентом", "СчетУчетаРасчетов", "СчетРасчетов", "СчетУчета"]:
                if hasattr(doc_obj, attr_name):
                    setattr(doc_obj, attr_name, acc_ref)
                    break
        doc_obj.Комментарий = "Ввод остатков РБП 76.09.1. Создан автоматически."
        doc_obj.ОбменДанными.Загрузка = True
        doc_obj.Записать()
        return doc_obj.Ссылка
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка создания документа расчетов: {e}")
        return None


def write_rbp_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ РБП В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False

    # Документ остатков должен быть полным — читаем все строки (limit=0 = без ограничения)
    items = get_from_db(db_connection, "rbp_balances", limit=0)
    db_connection.close()

    if not items:
        verbose_print("Нет данных для записи.")
        return True

    verbose_print(f"Прочитано строк: {len(items)}")

    # Группировка: для 97.21 — по (Организация, ПодразделениеОрганизации, balance_type), для остальных — по (Организация, balance_type)
    groups = {}
    for item in items:
        org_json = item.get("Организация")
        if not org_json:
            continue
        org_info = parse_reference_field(org_json)
        org_uuid = org_info.get("uuid") if org_info else None
        if not org_uuid:
            continue
        bt = item.get("balance_type") or ""
        subdiv_uuid = ""
        if bt == "97.21":
            subdiv_json = item.get("ПодразделениеОрганизации")
            subdiv_info = parse_reference_field(subdiv_json)
            subdiv_uuid = (subdiv_info.get("uuid") or "").strip() if subdiv_info else ""
            if subdiv_uuid == "00000000-0000-0000-0000-000000000000":
                subdiv_uuid = ""
        key = (org_uuid, subdiv_uuid, bt)
        if key not in groups:
            groups[key] = []
        groups[key].append(item)

    try:
        doc_manager = safe_getattr(safe_getattr(com_object, "Документы", None), DOCUMENT_NAME, None)
        if not doc_manager:
            verbose_print("Ошибка: документ ВводНачальныхОстатков не найден.")
            return False

        for (org_uuid, subdiv_uuid, balance_type), group_items in groups.items():
            org_info = parse_reference_field(group_items[0].get("Организация"))
            org_ref = create_reference_by_uuid(
                com_object, org_uuid, "Справочник.Организации",
                ref_presentation=org_info.get("presentation", ""),
                processed_db=sqlite_db_file,
            )
            subdiv_info = parse_reference_field(group_items[0].get("ПодразделениеОрганизации")) if balance_type == "97.21" else None

            if balance_type.startswith("97"):
                comment = COMMENT_MARKER_97
                section = ACCOUNTING_SECTION_RBP
                table_name = TABLE_NAME_RBP
            elif balance_type == "76.01.9":
                comment = COMMENT_MARKER_76019
                section = ACCOUNTING_SECTION_RBP
                table_name = TABLE_NAME_RBP
            elif balance_type == "76.09.1":
                comment = COMMENT_MARKER_76091
                section = ACCOUNTING_SECTION_CONTR
                table_name = TABLE_NAME_CONTR
            else:
                continue

            verbose_print(f"Организация: {org_info.get('presentation')}, тип: {balance_type}, подразделение: {subdiv_info.get('presentation', '') if subdiv_info and subdiv_uuid else '(пусто)'}")

            # Для 97.21 ищем/создаём документ с учётом ПодразделениеОрганизации
            if balance_type == "97.21" and subdiv_uuid:
                query_text = f"""ВЫБРАТЬ ПЕРВЫЕ 1 Док.Ссылка КАК Ссылка ИЗ Документ.{DOCUMENT_NAME} КАК Док
                    ГДЕ Док.Комментарий ПОДОБНО "%{comment}%"
                    И Док.Организация = &Организация
                    И Док.ПодразделениеОрганизации = &ПодразделениеОрганизации
                    И Док.РазделУчета = ЗНАЧЕНИЕ(Перечисление.РазделыУчетаДляВводаОстатков.{section})
                    И Док.Дата МЕЖДУ ДАТАВРЕМЯ(2025, 12, 31, 0, 0, 0) И ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59)"""
                query = com_object.NewObject("Запрос")
                query.Текст = query_text
                query.УстановитьПараметр("Организация", org_ref)
                subdiv_ref = create_reference_by_uuid(
                    com_object, subdiv_uuid, "Справочник.ПодразделенияОрганизаций",
                    ref_presentation=subdiv_info.get("presentation", "") if subdiv_info else "",
                    processed_db=sqlite_db_file,
                )
                query.УстановитьПараметр("ПодразделениеОрганизации", subdiv_ref)
            elif balance_type == "97.21" and not subdiv_uuid:
                query_text = f"""ВЫБРАТЬ ПЕРВЫЕ 1 Док.Ссылка КАК Ссылка ИЗ Документ.{DOCUMENT_NAME} КАК Док
                    ГДЕ Док.Комментарий ПОДОБНО "%{comment}%"
                    И Док.Организация = &Организация
                    И Док.ПодразделениеОрганизации = ЗНАЧЕНИЕ(Справочник.ПодразделенияОрганизаций.ПустаяСсылка)
                    И Док.РазделУчета = ЗНАЧЕНИЕ(Перечисление.РазделыУчетаДляВводаОстатков.{section})
                    И Док.Дата МЕЖДУ ДАТАВРЕМЯ(2025, 12, 31, 0, 0, 0) И ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59)"""
                query = com_object.NewObject("Запрос")
                query.Текст = query_text
                query.УстановитьПараметр("Организация", org_ref)
            else:
                query_text = f"""ВЫБРАТЬ ПЕРВЫЕ 1 Док.Ссылка КАК Ссылка ИЗ Документ.{DOCUMENT_NAME} КАК Док
                    ГДЕ Док.Комментарий ПОДОБНО "%{comment}%"
                    И Док.Организация = &Организация
                    И Док.РазделУчета = ЗНАЧЕНИЕ(Перечисление.РазделыУчетаДляВводаОстатков.{section})
                    И Док.Дата МЕЖДУ ДАТАВРЕМЯ(2025, 12, 31, 0, 0, 0) И ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59)"""
                query = com_object.NewObject("Запрос")
                query.Текст = query_text
                query.УстановитьПараметр("Организация", org_ref)
            result = query.Выполнить()
            selection = result.Выбрать()

            if selection.Следующий():
                doc_obj = selection.Ссылка.ПолучитьОбъект()
            else:
                doc_obj = doc_manager.СоздатьДокумент()
                doc_obj.Дата = "20251231235959"
                doc_obj.Организация = org_ref
                doc_obj.РазделУчета = getattr(com_object.Перечисления.РазделыУчетаДляВводаОстатков, section)
                doc_obj.ОтражатьВБухгалтерскомУчете = True
                doc_obj.ОтражатьВНалоговомУчете = True
                doc_obj.Комментарий = f"{comment}\nЗагружено автоматически."
                if balance_type == "97.21" and subdiv_uuid and hasattr(doc_obj, "ПодразделениеОрганизации"):
                    subdiv_ref = create_reference_by_uuid(
                        com_object, subdiv_uuid, "Справочник.ПодразделенияОрганизаций",
                        ref_presentation=subdiv_info.get("presentation", "") if subdiv_info else "",
                        processed_db=sqlite_db_file,
                    )
                    doc_obj.ПодразделениеОрганизации = subdiv_ref

            tabular_section = safe_getattr(doc_obj, table_name, None)
            if not tabular_section:
                verbose_print(f"  ⚠ ТЧ {table_name} не найдена.")
                continue

            tabular_section.Очистить()

            acc_code = balance_type
            acc_ref = _find_account_by_code_in_receiver(com_object, acc_code, "ПланСчетов.Хозрасчетный")

            for item in group_items:
                try:
                    sum_dt = float(item.get("СуммаДт")) if item.get("СуммаДт") not in (None, "") else 0
                except (TypeError, ValueError):
                    sum_dt = 0
                try:
                    sum_kt = float(item.get("СуммаКт")) if item.get("СуммаКт") not in (None, "") else 0
                except (TypeError, ValueError):
                    sum_kt = 0
                try:
                    sum_nu = float(item.get("СуммаНУ")) if item.get("СуммаНУ") not in (None, "") else 0
                except (TypeError, ValueError):
                    sum_nu = 0

                net = sum_dt - sum_kt
                # Для строк только из НУ: СуммаДт/СуммаКт=0, но СуммаНУ может быть ненулевой
                if abs(net) < 0.01 and abs(sum_nu) < 0.01:
                    continue
                nu_only = abs(net) < 0.01
                if abs(net) >= 0.01:
                    if net > 0:
                        sum_dt, sum_kt = net, 0
                    else:
                        sum_dt, sum_kt = 0, abs(net)
                else:
                    # НУ-only (БУ=0): Сумма=0, ВР=-НУ (чтобы БУ=НУ+ПР+ВР)
                    sum_dt, sum_kt = 0, 0

                new_row = tabular_section.Добавить()

                if acc_ref and hasattr(new_row, "СчетУчета"):
                    new_row.СчетУчета = acc_ref

                if balance_type.startswith("97"):
                    # ТЧ РасходыБудущихПериодов: Субконто1=РБП, Субконто2=Работники (для 97.01, 97.71)
                    rbp_json = item.get("РБП_ДляЗаписи")
                    if not rbp_json:
                        continue  # Пропускаем строки без РБП
                    rbp_info = parse_reference_field(rbp_json)
                    if rbp_info and rbp_info.get("uuid"):
                        rbp_ref = create_reference_by_uuid(
                            com_object, rbp_info["uuid"], "Справочник.РасходыБудущихПериодов",
                            ref_presentation=rbp_info.get("presentation", ""),
                            processed_db=sqlite_db_file,
                        )
                        if rbp_ref:
                            if hasattr(new_row, "Субконто1"):
                                new_row.Субконто1 = rbp_ref
                            elif hasattr(new_row, "РасходыБудущихПериодов"):
                                new_row.РасходыБудущихПериодов = rbp_ref

                    workers_json = item.get("Работники_ДляЗаписи")
                    if workers_json and hasattr(new_row, "Субконто2"):
                        workers_info = parse_reference_field(workers_json)
                        if workers_info and workers_info.get("uuid"):
                            workers_ref = create_reference_by_uuid(
                                com_object, workers_info["uuid"],
                                workers_info.get("type", "Справочник.ФизическиеЛица"),
                                ref_presentation=workers_info.get("presentation", ""),
                                processed_db=sqlite_db_file,
                            )
                            if workers_ref:
                                new_row.Субконто2 = workers_ref

                    val_json = item.get("Валюта")
                    if val_json and hasattr(new_row, "Валюта"):
                        val_info = parse_reference_field(val_json)
                        if val_info and val_info.get("uuid"):
                            val_ref = create_reference_by_uuid(
                                com_object, val_info["uuid"], "Справочник.Валюты",
                                ref_presentation=val_info.get("presentation", ""),
                                processed_db=sqlite_db_file,
                            )
                            if val_ref:
                                new_row.Валюта = val_ref

                    sum_val = 0 if nu_only else (sum_dt if sum_dt != 0 else -sum_kt)
                    if hasattr(new_row, "Сумма"):
                        new_row.Сумма = sum_val
                    try:
                        pr = float(item.get("СуммаПР")) if item.get("СуммаПР") not in (None, "") else 0
                    except (TypeError, ValueError):
                        pr = 0
                    if hasattr(new_row, "СуммаНУ"):
                        try:
                            new_row.СуммаНУ = float(item.get("СуммаНУ")) if item.get("СуммаНУ") not in (None, "") else sum_val
                        except (TypeError, ValueError):
                            new_row.СуммаНУ = sum_val
                    if hasattr(new_row, "СуммаПР"):
                        new_row.СуммаПР = pr
                    if hasattr(new_row, "СуммаВР"):
                        if nu_only:
                            # БУ=0: ВР = -НУ - ПР (чтобы БУ = НУ + ПР + ВР)
                            new_row.СуммаВР = -sum_nu - pr
                        else:
                            try:
                                new_row.СуммаВР = float(item.get("СуммаВР")) if item.get("СуммаВР") not in (None, "") else 0
                            except (TypeError, ValueError):
                                new_row.СуммаВР = 0
                    if hasattr(new_row, "СуммаДт") and sum_dt != 0:
                        new_row.СуммаДт = sum_dt
                    if hasattr(new_row, "СуммаКт") and sum_kt != 0:
                        new_row.СуммаКт = sum_kt

                elif balance_type == "76.01.9":
                    # 76.19→76.01.9: ТЧ РасходыБудущихПериодов, Субконто1=Контрагент, Субконто2=РБП
                    rbp_json = item.get("РБП_ДляЗаписи")
                    kontr_json = item.get("Контрагент_ДляЗаписи")
                    val_json = item.get("Валюта")

                    if not rbp_json:
                        continue  # Пропускаем строки без РБП
                    rbp_info = parse_reference_field(rbp_json)
                    if not rbp_info or not rbp_info.get("uuid"):
                        continue
                    rbp_ref = create_reference_by_uuid(
                        com_object, rbp_info["uuid"], "Справочник.РасходыБудущихПериодов",
                        ref_presentation=rbp_info.get("presentation", ""),
                        processed_db=sqlite_db_file,
                    )
                    if not rbp_ref:
                        continue

                    if hasattr(new_row, "Субконто2"):
                        new_row.Субконто2 = rbp_ref
                    if kontr_json and hasattr(new_row, "Субконто1"):
                        kontr_info = parse_reference_field(kontr_json)
                        if kontr_info and kontr_info.get("uuid"):
                            kontr_ref = create_reference_by_uuid(
                                com_object, kontr_info["uuid"], "Справочник.Контрагенты",
                                ref_presentation=kontr_info.get("presentation", ""),
                                processed_db=sqlite_db_file,
                            )
                            if kontr_ref:
                                new_row.Субконто1 = kontr_ref

                    if val_json and hasattr(new_row, "Валюта"):
                        val_info = parse_reference_field(val_json)
                        if val_info and val_info.get("uuid"):
                            val_ref = create_reference_by_uuid(
                                com_object, val_info["uuid"], "Справочник.Валюты",
                                ref_presentation=val_info.get("presentation", ""),
                                processed_db=sqlite_db_file,
                            )
                            if val_ref:
                                new_row.Валюта = val_ref

                    sum_val = sum_dt if sum_dt != 0 else -sum_kt
                    if hasattr(new_row, "Сумма"):
                        new_row.Сумма = sum_val
                    if hasattr(new_row, "СуммаНУ"):
                        new_row.СуммаНУ = sum_val
                    if hasattr(new_row, "СуммаДт") and sum_dt != 0:
                        new_row.СуммаДт = sum_dt
                    if hasattr(new_row, "СуммаКт") and sum_kt != 0:
                        new_row.СуммаКт = sum_kt

                elif balance_type == "76.09.1":
                    # ТЧ РасчетыСКонтрагентами
                    kontr_json = item.get("Контрагент_ДляЗаписи")
                    dog_json = item.get("Договор_ДляЗаписи")
                    val_json = item.get("Валюта")

                    kontr_ref = None
                    if kontr_json:
                        kontr_info = parse_reference_field(kontr_json)
                        if kontr_info and kontr_info.get("uuid"):
                            kontr_ref = create_reference_by_uuid(
                                com_object, kontr_info["uuid"], "Справочник.Контрагенты",
                                ref_presentation=kontr_info.get("presentation", ""),
                                processed_db=sqlite_db_file,
                            )

                    if not kontr_ref:
                        continue  # Пропускаем строки без контрагента

                    dog_ref = None
                    if dog_json:
                        dog_info = parse_reference_field(dog_json)
                        if dog_info and dog_info.get("uuid"):
                            dog_ref = create_reference_by_uuid(
                                com_object, dog_info["uuid"], "Справочник.ДоговорыКонтрагентов",
                                ref_presentation=dog_info.get("presentation", ""),
                                processed_db=sqlite_db_file,
                            )

                    val_ref = None
                    if val_json:
                        val_info = parse_reference_field(val_json)
                        if val_info and val_info.get("uuid"):
                            val_ref = create_reference_by_uuid(
                                com_object, val_info["uuid"], "Справочник.Валюты",
                                ref_presentation=val_info.get("presentation", ""),
                                processed_db=sqlite_db_file,
                            )

                    doc_rasch_ref = get_or_create_settlement_doc_by_refs(
                        com_object, org_ref, kontr_ref or com_object.Справочники.Контрагенты.ПустаяСсылка(),
                        dog_ref, val_ref, acc_ref,
                    )

                    new_row.Контрагент = kontr_ref if kontr_ref else com_object.Справочники.Контрагенты.ПустаяСсылка()
                    new_row.ДоговорКонтрагента = dog_ref if dog_ref else com_object.Справочники.ДоговорыКонтрагентов.ПустаяСсылка()
                    if val_ref:
                        new_row.Валюта = val_ref
                    if doc_rasch_ref:
                        new_row.Документ = doc_rasch_ref
                    if hasattr(new_row, "СуммаДт") and sum_dt != 0:
                        new_row.СуммаДт = sum_dt
                    if hasattr(new_row, "СуммаКт") and sum_kt != 0:
                        new_row.СуммаКт = sum_kt
                    sum_val = sum_dt if sum_dt != 0 else -sum_kt
                    if hasattr(new_row, "Сумма"):
                        new_row.Сумма = sum_val
                    if hasattr(new_row, "КурсВзаиморасчетов"):
                        new_row.КурсВзаиморасчетов = 1
                    if hasattr(new_row, "КратностьВзаиморасчетов"):
                        new_row.КратностьВзаиморасчетов = 1

            doc_obj.ОбменДанными.Загрузка = True
            try:
                doc_obj.Записать()
                verbose_print(f"  Документ записан: {doc_obj.Ссылка}")
                try:
                    doc_obj.ОбменДанными.Загрузка = False
                    doc_obj.Записать(com_object.РежимЗаписиДокумента.Проведение)
                    verbose_print(f"  Документ проведен.")
                except Exception as post_err:
                    verbose_print(f"  ⚠ Не удалось провести: {post_err}")
            except Exception as write_err:
                verbose_print(f"  Ошибка записи: {write_err}")
                return False

        return True

    except Exception as e:
        verbose_print(f"Критическая ошибка: {e}")
        import traceback
        verbose_print(traceback.format_exc())
        return False


if __name__ == "__main__":
    target = os.getenv("TARGET_1C", "target")
    com = connect_to_1c(target)
    if com:
        write_rbp_balances_to_1c("BD/rbp_balances_processed.db", com)
