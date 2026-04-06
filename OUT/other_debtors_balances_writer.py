# -*- coding: utf-8 -*-
"""
Модуль выгрузки остатков по счёту 76 (расчёты с прочими дебиторами и кредиторами)
в документ «Ввод начальных остатков».
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field
from tools.base_writer import create_reference_by_uuid, _find_account_by_code_in_receiver
from tools.onec_connector import connect_to_1c, safe_getattr, find_object_by_uuid
from tools.logger import verbose_print
from tools.chart_of_accounts_mapper import load_mapping, extract_account_code, get_mapped_account_code

DOCUMENT_NAME = "ВводНачальныхОстатков"
TABLE_NAME = "РасчетыСКонтрагентами"
TABLE_NAME_RBP = "РасходыБудущихПериодов"  # для 76.01.2 и 76.01.9 (Субконто1, Субконто2)
ACCOUNTING_SECTION = "РасчетыСПрочимиДебиторамиИКредиторами"
ACCOUNTING_SECTION_RBP = "РасходыБудущихПериодов"  # для 76.01.2 и 76.01.9
COMMENT_MARKER_BASE = "### Загрузка остатков по счёту"  # + " 76.01 (31.12.2025) ###" — счёт добавляется при разбиении по счетам

# Счета 76.01.2 и 76.01.9: ТЧ РасходыБудущихПериодов (Субконто1, Субконто2), РазделУчета РасходыБудущихПериодов; остальные — РасчетыСКонтрагентами
ACCOUNTS_WITH_RBP_SUBCONTO2 = ("76.01.2", "76.01.9")

# Тип в УПП для субконто Договор: только Справочник.ДоговорыКонтрагентов — валидный для записи в УХ
VALID_CONTRACT_TYPES = ("ДоговорыКонтрагентов", "Справочник.ДоговорыКонтрагентов", "СправочникСсылка.ДоговорыКонтрагентов")
VALID_RBP_TYPES = ("РасходыБудущихПериодов", "Справочник.РасходыБудущихПериодов", "СправочникСсылка.РасходыБудущихПериодов")


def _is_valid_contract_type(ref_type: str) -> bool:
    """Проверяет, является ли тип ссылкой на ДоговорыКонтрагентов (в остатках 76 может быть РБП, документы)."""
    if not ref_type:
        return False
    return any(v in (ref_type or "") for v in VALID_CONTRACT_TYPES)


def _is_rbp_type(ref_type: str) -> bool:
    """Проверяет, является ли тип ссылкой на РасходыБудущихПериодов (для 76.01.2, 76.01.9)."""
    if not ref_type:
        return False
    return any(v in (ref_type or "") for v in VALID_RBP_TYPES)


def get_or_create_settlement_doc(com_object, item, org_ref, kontr_ref, dog_ref, val_ref, acc_ref=None):
    """Поиск или создание Документа расчетов с контрагентом с использованием UUID исходного документа."""
    doc_rasch_info = parse_reference_field(item.get("Документ"))
    if not doc_rasch_info or doc_rasch_info["uuid"] == "00000000-0000-0000-0000-000000000000":
        return None

    doc_uuid = doc_rasch_info["uuid"]
    doc_number = item.get("Документ_Номер", "")
    doc_date_str = item.get("Документ_Дата", "")
    doc_sum = item.get("Документ_Сумма", 0)

    val_doc_json = item.get("Документ_Валюта")
    val_doc_ref = val_ref
    if val_doc_json:
        val_doc_info = parse_reference_field(val_doc_json)
        if val_doc_info and val_doc_info["uuid"] != "00000000-0000-0000-0000-000000000000":
            val_doc_ref = create_reference_by_uuid(com_object, val_doc_info["uuid"], "Справочник.Валюты", ref_presentation=val_doc_info.get("presentation", ""))

    doc_date = None
    if doc_date_str:
        try:
            d_str = str(doc_date_str).replace("-", "").replace(":", "").replace(" ", "")
            year = int(d_str[0:4])
            month = int(d_str[4:6])
            day = int(d_str[6:8])
            hour = int(d_str[8:10]) if len(d_str) >= 10 else 0
            minute = int(d_str[10:12]) if len(d_str) >= 12 else 0
            second = int(d_str[12:14]) if len(d_str) >= 14 else 0
            doc_date = com_object.NewObject("Дата", year, month, day, hour, minute, second)
        except Exception:
            pass

    try:
        uuid_obj = com_object.NewObject("УникальныйИдентификатор", doc_uuid)
        doc_ref = com_object.Документы.ДокументРасчетовСКонтрагентом.ПолучитьСсылку(uuid_obj)
        doc_obj = doc_ref.ПолучитьОбъект()

        if doc_obj is None:
            doc_obj = com_object.Документы.ДокументРасчетовСКонтрагентом.СоздатьДокумент()
            doc_obj.УстановитьСсылкуНового(doc_ref)

        actual_date = doc_date if doc_date else "20251231235959"

        try:
            doc_obj.Дата = actual_date
            doc_obj.Номер = doc_number
            doc_obj.Организация = org_ref
            doc_obj.Контрагент = kontr_ref
            doc_obj.ДоговорКонтрагента = dog_ref
            doc_obj.ВалютаДокумента = val_doc_ref
            doc_obj.СуммаДокумента = doc_sum

            if acc_ref:
                for attr_name in ["СчетУчетаРасчетовСКомитентом", "СчетУчетаРасчетов", "СчетРасчетов", "СчетУчета"]:
                    if hasattr(doc_obj, attr_name):
                        setattr(doc_obj, attr_name, acc_ref)
                        break

            doc_obj.НомерВходящегоДокумента = doc_number
            doc_obj.ДатаВходящегоДокумента = actual_date
            src_type = doc_rasch_info.get("type", "Неизвестный тип")
            doc_obj.Комментарий = f"Загружено из УПП. Тип: {src_type}. UUID: {doc_uuid}."

            doc_obj.ОбменДанными.Загрузка = True
            doc_obj.Записать()
        except Exception as e_inner:
            verbose_print(f"  ⚠ Не удалось заполнить все поля Документа расчетов: {e_inner}")
            doc_obj.Дата = actual_date
            doc_obj.ОбменДанными.Загрузка = True
            doc_obj.Записать()

        return doc_obj.Ссылка

    except Exception as e:
        verbose_print(f"  ⚠ Критическая ошибка при работе с Документом расчетов (UUID: {doc_uuid}): {e}")
        return None


def get_or_create_settlement_doc_by_refs(com_object, org_ref, kontr_ref, dog_ref, val_ref, acc_ref=None):
    """Ищет или создаёт документ расчетов с контрагентом по организации, контрагенту и договору."""
    if not org_ref or not kontr_ref:
        return None
    try:
        query = com_object.NewObject("Запрос")
        query.Текст = """ВЫБРАТЬ ПЕРВЫЕ 1 Док.Ссылка КАК Ссылка ИЗ Документ.ДокументРасчетовСКонтрагентом КАК Док ГДЕ Док.Организация = &Организация И Док.Контрагент = &Контрагент И Док.ДоговорКонтрагента = &ДоговорКонтрагента"""
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
        if hasattr(doc_obj, "НомерВходящегоДокумента"):
            doc_obj.НомерВходящегоДокумента = "Остатки 76"
        if hasattr(doc_obj, "ДатаВходящегоДокумента"):
            doc_obj.ДатаВходящегоДокумента = "20251231235959"
        doc_obj.Комментарий = "Ввод остатков по сч. 76. Создан автоматически."
        doc_obj.ОбменДанными.Загрузка = True
        doc_obj.Записать()
        return doc_obj.Ссылка
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка поиска/создания документа расчетов по реквизитам: {e}")
        return None


def write_other_debtors_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    """Записывает остатки по счёту 76 в документ Ввод начальных остатков."""
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ ПО СЧЁТУ 76 В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False

    items = get_from_db(db_connection, "other_debtors_balances")
    db_connection.close()

    if not items:
        verbose_print("Нет данных для записи.")
        return True

    mapping_path = "CONF/chart_of_accounts_mapping.json"
    coa_mapping, _ = load_mapping(mapping_path)

    verbose_print(f"Прочитано строк для записи: {len(items)}")

    try:
        documents_manager = safe_getattr(com_object, "Документы", None)
        doc_manager = safe_getattr(documents_manager, DOCUMENT_NAME, None)
        if not doc_manager:
            verbose_print(f"Ошибка: Менеджер документа {DOCUMENT_NAME} не найден.")
            return False

        # Группировка по организации И по счёту — отдельный документ «Ввод остатков» на каждую пару (орг, счёт)
        org_account_items = {}
        for item in items:
            org_json = item.get("Организация")
            if not org_json:
                continue
            org_info = parse_reference_field(org_json)
            org_uuid = org_info.get("uuid")
            if not org_uuid:
                continue

            acc_info = parse_reference_field(item.get("Счет"))
            source_code = extract_account_code(acc_info.get("presentation", "")) if acc_info else ""
            mapped_code = get_mapped_account_code(source_code, coa_mapping) if source_code else None
            account_key = mapped_code or source_code or (acc_info.get("uuid", "") if acc_info else "unknown")

            key = (org_uuid, account_key)
            if key not in org_account_items:
                org_account_items[key] = {"org_info": org_info, "account_key": account_key, "items": []}
            org_account_items[key]["items"].append(item)

        for (org_uuid, account_key), data in org_account_items.items():
            org_info = data["org_info"]
            org_items_list = data["items"]
            comment_marker = f"{COMMENT_MARKER_BASE} {account_key} (31.12.2025) ###"
            is_rbp_account = account_key in ACCOUNTS_WITH_RBP_SUBCONTO2
            accounting_section = ACCOUNTING_SECTION_RBP if is_rbp_account else ACCOUNTING_SECTION

            verbose_print(f"Обработка организации: {org_info.get('presentation')} ({org_uuid}), счёт {account_key}, раздел {accounting_section}")

            query_text = f"""ВЫБРАТЬ ПЕРВЫЕ 1
                Док.Ссылка КАК Ссылка
            ИЗ
                Документ.{DOCUMENT_NAME} КАК Док
            ГДЕ
                Док.Комментарий ПОДОБНО "%{comment_marker}%"
                И Док.Организация = &Организация
                И Док.РазделУчета = ЗНАЧЕНИЕ(Перечисление.РазделыУчетаДляВводаОстатков.{accounting_section})
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
                verbose_print(f"  Найден существующий документ по сч. {account_key}. Обновляем.")
                doc_obj = selection.Ссылка.ПолучитьОбъект()
            else:
                verbose_print(f"  Создаем новый документ по сч. {account_key}.")
                doc_obj = doc_manager.СоздатьДокумент()
                doc_obj.Дата = "20251231235959"
                doc_obj.Организация = org_ref
                doc_obj.РазделУчета = getattr(com_object.Перечисления.РазделыУчетаДляВводаОстатков, accounting_section)
                doc_obj.ОтражатьВБухгалтерскомУчете = True
                doc_obj.ОтражатьВНалоговомУчете = True
                doc_obj.Комментарий = f"{comment_marker}\nЗагружено автоматически."

            table_name = TABLE_NAME_RBP if is_rbp_account else TABLE_NAME
            tabular_section = safe_getattr(doc_obj, table_name, None)
            if tabular_section:
                tabular_section.Очистить()

                for item in org_items_list:
                    try:
                        sum_dt = float(item.get("СуммаДт")) if item.get("СуммаДт") not in (None, "") else 0
                    except (TypeError, ValueError):
                        sum_dt = 0
                    try:
                        sum_kt = float(item.get("СуммаКт")) if item.get("СуммаКт") not in (None, "") else 0
                    except (TypeError, ValueError):
                        sum_kt = 0

                    net = sum_dt - sum_kt
                    if abs(net) < 0.01:
                        continue
                    if net > 0:
                        sum_dt, sum_kt = net, 0
                    else:
                        sum_dt, sum_kt = 0, abs(net)

                    new_row = tabular_section.Добавить()

                    kontr_ref = None
                    kontr_info = parse_reference_field(item.get("Контрагент"))
                    if kontr_info:
                        kontr_ref = create_reference_by_uuid(com_object, kontr_info["uuid"], "Справочник.Контрагенты", ref_presentation=kontr_info.get("presentation", ""), processed_db=sqlite_db_file)

                    dog_ref = None
                    dog_info = parse_reference_field(item.get("Договор"))
                    if dog_info and is_rbp_account and _is_rbp_type(dog_info.get("type", "")):
                        dog_ref = create_reference_by_uuid(
                            com_object,
                            dog_info["uuid"],
                            "Справочник.РасходыБудущихПериодов",
                            ref_presentation=dog_info.get("presentation", ""),
                            processed_db=sqlite_db_file
                        )
                    elif dog_info and _is_valid_contract_type(dog_info.get("type", "")):
                        dog_ref = create_reference_by_uuid(
                            com_object,
                            dog_info["uuid"],
                            "Справочник.ДоговорыКонтрагентов",
                            ref_presentation=dog_info.get("presentation", ""),
                            processed_db=sqlite_db_file
                        )

                    val_ref = None
                    val_info = parse_reference_field(item.get("Валюта"))
                    if val_info:
                        val_ref = create_reference_by_uuid(com_object, val_info["uuid"], "Справочник.Валюты", ref_presentation=val_info.get("presentation", ""), processed_db=sqlite_db_file)

                    acc_ref = None
                    acc_info = parse_reference_field(item.get("Счет"))
                    if acc_info:
                        source_code = extract_account_code(acc_info.get("presentation", ""))
                        if source_code:
                            mapped_code = get_mapped_account_code(source_code, coa_mapping)
                            if mapped_code:
                                acc_ref = _find_account_by_code_in_receiver(com_object, mapped_code, "ПланСчетов.Хозрасчетный")

                        if not acc_ref:
                            acc_ref = find_object_by_uuid(com_object, acc_info["uuid"], "ПланСчетов.Хозрасчетный")

                        if not acc_ref and source_code:
                            acc_ref = _find_account_by_code_in_receiver(com_object, source_code, "ПланСчетов.Хозрасчетный")

                    dog_ref_safe = dog_ref if (dog_ref and dog_info and _is_valid_contract_type(dog_info.get("type", ""))) else com_object.Справочники.ДоговорыКонтрагентов.ПустаяСсылка()
                    doc_rasch_ref = get_or_create_settlement_doc(
                        com_object, item, org_ref, kontr_ref, dog_ref_safe, val_ref, acc_ref=acc_ref
                    )
                    if not doc_rasch_ref:
                        doc_rasch_ref = get_or_create_settlement_doc_by_refs(
                            com_object, org_ref, kontr_ref, dog_ref_safe, val_ref, acc_ref
                        )

                    if is_rbp_account:
                        # ТЧ РасходыБудущихПериодов: Субконто1=Контрагент, Субконто2=РБП
                        if hasattr(new_row, "Субконто1"):
                            new_row.Субконто1 = kontr_ref if kontr_ref else com_object.Справочники.Контрагенты.ПустаяСсылка()
                        if hasattr(new_row, "Субконто2"):
                            new_row.Субконто2 = dog_ref if dog_ref else com_object.Справочники.РасходыБудущихПериодов.ПустаяСсылка()
                        if hasattr(new_row, "Валюта") and val_ref:
                            new_row.Валюта = val_ref
                        if hasattr(new_row, "СчетУчета") and acc_ref:
                            new_row.СчетУчета = acc_ref
                        sum_val = sum_dt if sum_dt != 0 else -sum_kt
                        if hasattr(new_row, "Сумма"):
                            new_row.Сумма = sum_val
                        if sum_dt != 0 and hasattr(new_row, "СуммаДт"):
                            new_row.СуммаДт = sum_dt
                        if sum_kt != 0 and hasattr(new_row, "СуммаКт"):
                            new_row.СуммаКт = sum_kt
                    else:
                        # РасчетыСКонтрагентами: Контрагент, ДоговорКонтрагента, Документ, Валюта, СчетУчета, Сумма/СуммаДт/СуммаКт
                        new_row.Контрагент = kontr_ref if kontr_ref else com_object.Справочники.Контрагенты.ПустаяСсылка()
                        new_row.ДоговорКонтрагента = dog_ref if dog_ref else com_object.Справочники.ДоговорыКонтрагентов.ПустаяСсылка()
                        if val_ref:
                            new_row.Валюта = val_ref
                        if acc_ref:
                            new_row.СчетУчета = acc_ref
                        if doc_rasch_ref:
                            new_row.Документ = doc_rasch_ref
                        if hasattr(new_row, "СуммаДт") and sum_dt != 0:
                            new_row.СуммаДт = sum_dt
                        if hasattr(new_row, "СуммаКт") and sum_kt != 0:
                            new_row.СуммаКт = sum_kt
                        if hasattr(new_row, "Сумма"):
                            new_row.Сумма = sum_dt if sum_dt != 0 else -sum_kt
                        if hasattr(new_row, "КурсВзаиморасчетов"):
                            new_row.КурсВзаиморасчетов = 1
                        if hasattr(new_row, "КратностьВзаиморасчетов"):
                            new_row.КратностьВзаиморасчетов = 1

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
        write_other_debtors_balances_to_1c("BD/other_debtors_balances_processed.db", com)
