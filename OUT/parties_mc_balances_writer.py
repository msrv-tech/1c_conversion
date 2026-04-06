# -*- coding: utf-8 -*-
"""
Модуль выгрузки остатков МЦ.02, МЦ.04 (партии материалов в эксплуатации) в документ «Ввод начальных остатков»,
раздел ПрочиеСчетаБухгалтерскогоУчета, табличная часть БухСправка.
Источник: ПартииМатериаловВЭксплуатацииБухгалтерскийУчет (счета 10.11, 10.09).

Документы ввода остатков разбиваются по шапке ПодразделениеОрганизации (как счёт 20 / ОС): ключ группировки
(организация, подразделение измерения РН, счёт МЦ.02 или МЦ.04).

В строке БухСправка (проверьте план счетов приёмника): Субконто1 — номенклатура, Субконто2 — партия
(документ ПартияМатериаловВЭксплуатации создаётся/ищется по реквизитам, без UUID документа из УПП),
Субконто3 — физлицо/работник, Субконто4 — подразделение. Если в конфигурации нет Субконто4, измерение
пропускается через hasattr.
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field
from tools.base_writer import create_reference_by_uuid, find_object_by_uuid, _find_account_by_code_in_receiver, setup_exchange_mode
from tools.onec_connector import connect_to_1c, safe_getattr
from tools.logger import verbose_print

DOCUMENT_NAME = "ВводНачальныхОстатков"
TABLE_NAME = "БухСправка"
ACCOUNTING_SECTION = "ПрочиеСчетаБухгалтерскогоУчета"
COMMENT_MARKERS = {
    "МЦ.02": "### Загрузка остатков МЦ.02 партии в эксплуатации (31.12.2025) ###",
    "МЦ.04": "### Загрузка остатков МЦ.04 партии в эксплуатации (31.12.2025) ###",
}

# Дата документа партии при вводе остатков на 31.12.2025 (как раньше при переносе по UUID УПП)
PARTY_DOC_DATETIME = "20251231235959"
# Префикс в комментарии партии для поиска при повторной выгрузке (без привязки к UUID документа УПП)
PARTY_COMMENT_TAG = "МЦ_ввод_остатков_партия"


def _transfer_doc_number_from_item(item: dict) -> str:
    """Номер документа передачи в УПП: из поля загрузчика или из представления ссылки."""
    if not item:
        return ""
    raw = (item.get("ДокументПередачиНомер") or "").strip()
    if raw:
        return raw
    batch_json = item.get("ДокументПередачи")
    batch_info = parse_reference_field(batch_json) if batch_json else None
    pr = (batch_info.get("presentation") or "").strip() if batch_info else ""
    if not pr:
        return ""
    m = re.search(r"\b(\d{4,})\s+от\s", pr, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m2 = re.search(r"\b(\d{5,})\b", pr)
    return m2.group(1).strip() if m2 else ""


def _ensure_nomenclature_ref(com_object, nom_info: dict, processed_db: str = None):
    """
    Находит или создаёт номенклатуру по UUID. Если найдена по UUID — обновляет Наименование
    (могла быть создана без характеристики при простом переносе серий).
    """
    uuid_val = (nom_info.get("uuid") or "").strip()
    presentation = (nom_info.get("presentation") or "").strip()
    if not uuid_val or uuid_val == "00000000-0000-0000-0000-000000000000":
        return None
    ref = find_object_by_uuid(com_object, uuid_val, "Справочник.Номенклатура")
    if ref:
        try:
            obj = ref.ПолучитьОбъект()
            if obj and presentation and hasattr(obj, "Наименование"):
                curr = (obj.Наименование or "").strip()
                if curr != presentation:
                    obj.Наименование = presentation
                    obj.ОбменДанными.Загрузка = True
                    obj.Записать()
        except Exception as e:
            verbose_print(f"    ⚠ Не удалось обновить наименование номенклатуры {uuid_val}: {e}")
        return ref
    return create_reference_by_uuid(
        com_object,
        uuid_val,
        "Справочник.Номенклатура",
        ref_presentation=presentation,
        processed_db=processed_db,
    )


def _ref_by_uuid_or_type(com_object, ref_json, default_type: str, processed_db: str = None, fallback_types: list = None):
    """Возвращает ссылку по UUID; fallback_types — альтернативные типы для поиска в приёмнике."""
    info = parse_reference_field(ref_json)
    if not info or not info.get("uuid") or info.get("uuid") == "00000000-0000-0000-0000-000000000000":
        return None
    ref_type = (info.get("type") or default_type).strip() or default_type
    types_to_try = [ref_type] + (fallback_types or [])
    for try_type in types_to_try:
        if not try_type:
            continue
        try:
            ref = find_object_by_uuid(com_object, info["uuid"], try_type)
            if ref:
                return ref
            ref = create_reference_by_uuid(
                com_object,
                info["uuid"],
                try_type,
                ref_presentation=info.get("presentation", ""),
                processed_db=processed_db,
            )
            if ref:
                return ref
        except Exception:
            continue
    return None


def _uuid_key_for_marker(raw: str | None) -> str:
    if not raw:
        return "-"
    s = str(raw).strip().lower()
    if len(s) >= 2 and s[0] == "{" and s[-1] == "}":
        s = s[1:-1].strip()
    if s == "00000000-0000-0000-0000-000000000000":
        return "-"
    return s or "-"


def _party_comment_body(nom_uuid_key: str, nazn_uuid_key: str, upp_party_presentation: str) -> str:
    """Текст комментария: стабильный ключ поиска + справочно партия в УПП."""
    hint = (upp_party_presentation or "").strip()
    if len(hint) > 200:
        hint = hint[:197] + "..."
    upp_part = f" УПП_партия:{hint}" if hint else ""
    return f"{PARTY_COMMENT_TAG}|Ном={nom_uuid_key}|Назн={nazn_uuid_key}{upp_part}"


def _find_or_create_party_doc_by_requisites(
    com_object,
    org_ref,
    item: dict,
    processed_db: str = None,
):
    """
    Ищет или создаёт Документ.ПартияМатериаловВЭксплуатации по организации, номенклатуре строки,
    назначению и дате — без УстановитьСсылкуНового (UUID не из УПП).
    """
    if not item or not org_ref:
        return None
    try:
        doc_manager = safe_getattr(com_object, "Документы", None)
        if not doc_manager:
            return None
        pm_doc_manager = safe_getattr(doc_manager, "ПартияМатериаловВЭксплуатации", None)
        if not pm_doc_manager:
            verbose_print("  ⚠ Документ ПартияМатериаловВЭксплуатации не найден в приёмнике.")
            return None

        nom_json = item.get("Номенклатура_ДляЗаписи") or item.get("Номенклатура")
        nom_info = parse_reference_field(nom_json) if nom_json else None
        if not nom_info or not nom_info.get("uuid"):
            return None
        nom_ref = _ensure_nomenclature_ref(com_object, nom_info, processed_db=processed_db)
        if not nom_ref:
            return None

        nazn_info = parse_reference_field(item.get("НазначениеИспользования")) if item.get("НазначениеИспользования") else None
        nazn_uuid_key = _uuid_key_for_marker(nazn_info.get("uuid") if nazn_info else None)
        nom_uuid_key = _uuid_key_for_marker(nom_info.get("uuid"))

        batch_json = item.get("ДокументПередачи")
        batch_info = parse_reference_field(batch_json) if batch_json else None
        upp_party_pr = (batch_info.get("presentation") or "") if batch_info else ""

        marker_substr = f"{PARTY_COMMENT_TAG}|Ном={nom_uuid_key}|Назн={nazn_uuid_key}"
        comment_full = _party_comment_body(nom_uuid_key, nazn_uuid_key, upp_party_pr)

        query_text = """ВЫБРАТЬ ПЕРВЫЕ 1
            Док.Ссылка КАК Ссылка
        ИЗ
            Документ.ПартияМатериаловВЭксплуатации КАК Док
        ГДЕ
            Док.Организация = &Организация
            И Док.Номенклатура = &Номенклатура
            И Док.Дата = &ДатаПартии
            И Док.Комментарий ПОДОБНО &МаркерПодобно
        """
        query = com_object.NewObject("Запрос")
        query.Текст = query_text
        query.УстановитьПараметр("Организация", org_ref)
        query.УстановитьПараметр("Номенклатура", nom_ref)
        query.УстановитьПараметр("ДатаПартии", PARTY_DOC_DATETIME)
        query.УстановитьПараметр("МаркерПодобно", f"%{marker_substr}%")

        try:
            selection = query.Выполнить().Выбрать()
            if selection.Следующий():
                found_ref = selection.Ссылка
                num = _transfer_doc_number_from_item(item)
                if num:
                    try:
                        ex_obj = found_ref.ПолучитьОбъект()
                        if ex_obj is not None and hasattr(ex_obj, "Номер"):
                            cur = (str(ex_obj.Номер) if ex_obj.Номер is not None else "").strip()
                            if not cur:
                                ex_obj.Номер = num
                                ex_obj.ОбменДанными.Загрузка = True
                                ex_obj.Записать()
                    except Exception as num_err:
                        verbose_print(f"  ⚠ Не удалось записать номер партии УПП в существующий документ: {num_err}")
                return found_ref
        except Exception as q_err:
            verbose_print(f"  ⚠ Запрос поиска партии по реквизитам: {q_err}")

        doc_obj = pm_doc_manager.СоздатьДокумент()
        doc_obj.Дата = PARTY_DOC_DATETIME
        doc_obj.Организация = org_ref
        doc_obj.Номенклатура = nom_ref
        doc_obj.Комментарий = f"Загружено из УПП. {comment_full}"
        upp_num = _transfer_doc_number_from_item(item)
        if upp_num and hasattr(doc_obj, "Номер"):
            doc_obj.Номер = upp_num

        nazn_ref = _ref_by_uuid_or_type(
            com_object,
            item.get("НазначениеИспользования"),
            "Справочник.НазначенияИспользования",
            processed_db=processed_db,
            fallback_types=["Справочник.НазначенияИспользованияНоменклатуры"],
        )
        if nazn_ref:
            for attr in ("НазначениеИспользования", "НазначениеИспользованияНоменклатуры"):
                if hasattr(doc_obj, attr):
                    setattr(doc_obj, attr, nazn_ref)
                    break

        doc_obj.ОбменДанными.Загрузка = True
        doc_obj.Записать()
        return doc_obj.Ссылка
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка при создании/поиске документа ПартияМатериаловВЭксплуатации: {e}")
        return None


def _get_target_account_code(item: dict) -> str:
    code = (item.get("TargetAccountCode") or "").strip()
    return code if code in ("МЦ.02", "МЦ.04") else "МЦ.02"


def write_parties_mc_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    """Записывает остатки МЦ.02, МЦ.04 в документ Ввод начальных остатков, ТЧ БухСправка, раздел ПрочиеСчетаБухгалтерскогоУчета."""
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ МЦ.02, МЦ.04 (ПАРТИИ В ЭКСПЛУАТАЦИИ) В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    setup_exchange_mode(com_object)

    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False

    items = get_from_db(db_connection, "parties_mc_balances")
    db_connection.close()

    if not items:
        verbose_print("Нет данных для записи.")
        return True

    verbose_print(f"Прочитано строк остатков для записи: {len(items)}")

    # Группировка: (организация, подразделение из РН, счёт МЦ.02/МЦ.04) — шапка ПодразделениеОрганизации
    doc_groups = {}
    for item in items:
        org_json = item.get("Организация")
        if not org_json:
            continue
        org_info = parse_reference_field(org_json)
        org_uuid = org_info.get("uuid") if org_info else None
        if not org_uuid:
            continue

        subdiv_json = item.get("Подразделение")
        subdiv_info = parse_reference_field(subdiv_json) if subdiv_json else None
        subdiv_uuid = (subdiv_info.get("uuid") or "").strip() if subdiv_info else ""
        if subdiv_uuid == "00000000-0000-0000-0000-000000000000":
            subdiv_uuid = ""

        acc_code = _get_target_account_code(item)
        key = (org_uuid, subdiv_uuid or "", acc_code)
        if key not in doc_groups:
            doc_groups[key] = []
        doc_groups[key].append(item)

    try:
        documents_manager = safe_getattr(com_object, "Документы", None)
        doc_manager = safe_getattr(documents_manager, DOCUMENT_NAME, None)
        if not doc_manager:
            verbose_print("Ошибка: документ ВводНачальныхОстатков не найден в приемнике.")
            return False

        for (org_uuid, subdiv_uuid, acc_code), group_items in doc_groups.items():
            comment_marker = COMMENT_MARKERS.get(acc_code, COMMENT_MARKERS["МЦ.02"])
            org_info = parse_reference_field(group_items[0].get("Организация"))
            subdiv_info = parse_reference_field(group_items[0].get("Подразделение"))
            subdiv_present = subdiv_info.get("presentation", "") if subdiv_info else ""
            verbose_print(
                f"Обработка: Организация {org_info.get('presentation') if org_info else org_uuid}, "
                f"Подразделение: {subdiv_present or '(пусто)'}, Счёт {acc_code}, Раздел: {ACCOUNTING_SECTION}"
            )

            if subdiv_uuid:
                query_text = f"""ВЫБРАТЬ ПЕРВЫЕ 1
                    Док.Ссылка КАК Ссылка
                ИЗ
                    Документ.{DOCUMENT_NAME} КАК Док
                ГДЕ
                    Док.Комментарий ПОДОБНО "%{comment_marker}%"
                    И Док.Организация = &Организация
                    И Док.ПодразделениеОрганизации = &ПодразделениеОрганизации
                    И Док.РазделУчета = ЗНАЧЕНИЕ(Перечисление.РазделыУчетаДляВводаОстатков.{ACCOUNTING_SECTION})
                    И Док.Дата МЕЖДУ ДАТАВРЕМЯ(2025, 12, 31, 0, 0, 0) И ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59)
                """
                query = com_object.NewObject("Запрос")
                query.Текст = query_text
                query.УстановитьПараметр("Организация", create_reference_by_uuid(com_object, org_uuid, "Справочник.Организации"))
                query.УстановитьПараметр(
                    "ПодразделениеОрганизации",
                    create_reference_by_uuid(
                        com_object,
                        subdiv_uuid,
                        "Справочник.ПодразделенияОрганизаций",
                        ref_presentation=subdiv_info.get("presentation", "") if subdiv_info else "",
                        processed_db=sqlite_db_file,
                    ),
                )
            else:
                query_text = f"""ВЫБРАТЬ ПЕРВЫЕ 1
                    Док.Ссылка КАК Ссылка
                ИЗ
                    Документ.{DOCUMENT_NAME} КАК Док
                ГДЕ
                    Док.Комментарий ПОДОБНО "%{comment_marker}%"
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
                verbose_print(f"  Обновляем существующий документ ({acc_code})")
                doc_obj = selection.Ссылка.ПолучитьОбъект()
            else:
                verbose_print(f"  Создаем новый документ ({acc_code})")
                doc_obj = doc_manager.СоздатьДокумент()
                doc_obj.Дата = "20251231235959"
                doc_obj.Организация = create_reference_by_uuid(com_object, org_uuid, "Справочник.Организации")
                doc_obj.Комментарий = f"{comment_marker}\nЗагружено автоматически."
                try:
                    doc_obj.РазделУчета = getattr(com_object.Перечисления.РазделыУчетаДляВводаОстатков, ACCOUNTING_SECTION)
                except AttributeError:
                    verbose_print(f"  ⚠ Не удалось установить РазделУчета {ACCOUNTING_SECTION}")

            if subdiv_uuid and hasattr(doc_obj, "ПодразделениеОрганизации"):
                subdiv_ref = create_reference_by_uuid(
                    com_object,
                    subdiv_uuid,
                    "Справочник.ПодразделенияОрганизаций",
                    ref_presentation=subdiv_info.get("presentation", "") if subdiv_info else "",
                    processed_db=sqlite_db_file,
                )
                try:
                    doc_obj.ПодразделениеОрганизации = subdiv_ref
                except Exception as e:
                    verbose_print(f"  ⚠ Не удалось установить ПодразделениеОрганизации в шапке: {e}")

            doc_obj.ОтражатьВБухгалтерскомУчете = True
            doc_obj.ОтражатьВНалоговомУчете = True

            tabular_section = safe_getattr(doc_obj, TABLE_NAME, None)
            if not tabular_section:
                verbose_print(f"  ⚠ Табличная часть {TABLE_NAME} не найдена в документе.")
                continue

            tabular_section.Очистить()

            for item in group_items:
                new_row = tabular_section.Добавить()

                target_code = _get_target_account_code(item)
                acc_ref = _find_account_by_code_in_receiver(com_object, target_code, "ПланСчетов.Хозрасчетный")
                if acc_ref and hasattr(new_row, "СчетУчета"):
                    new_row.СчетУчета = acc_ref

                # Субконто1 = Номенклатура (Наименование_Характеристика_Серия; при нахождении по UUID — обновляем наименование)
                if hasattr(new_row, "Субконто1"):
                    nom_json = item.get("Номенклатура_ДляЗаписи")
                    nom_info = parse_reference_field(nom_json) if nom_json else None
                    if nom_info and nom_info.get("uuid"):
                        nom_ref = _ensure_nomenclature_ref(
                            com_object,
                            nom_info,
                            processed_db=sqlite_db_file,
                        )
                        if nom_ref:
                            new_row.Субконто1 = nom_ref

                # Субконто2 = партия в эксплуатации (ищем/создаём по орг + номенклатура + назначение + дата, не по UUID УПП)
                if hasattr(new_row, "Субконто2"):
                    ref2 = _find_or_create_party_doc_by_requisites(
                        com_object,
                        doc_obj.Организация,
                        item,
                        processed_db=sqlite_db_file,
                    )
                    if ref2:
                        new_row.Субконто2 = ref2

                # Субконто3 = сотрудник/физлицо; Субконто4 = подразделение (типовой план МЦ: номенклатура,
                # партия в Субконто1–2; работник и подразделение раздельно, без «ИЛИ»).
                if hasattr(new_row, "Субконто3"):
                    fiz_json = item.get("ФизЛицо")
                    ref3 = _ref_by_uuid_or_type(
                        com_object,
                        fiz_json,
                        "Справочник.ФизическиеЛица",
                        processed_db=sqlite_db_file,
                        fallback_types=[
                            "Справочник.РаботникиОрганизаций",
                            "Справочник.СотрудникиОрганизаций",
                            "Справочник.ПодразделенияОрганизаций",
                            "Справочник.Подразделения",
                        ],
                    )
                    if ref3:
                        new_row.Субконто3 = ref3

                if hasattr(new_row, "Субконто4"):
                    subdiv_row_json = item.get("Подразделение")
                    ref4 = _ref_by_uuid_or_type(
                        com_object,
                        subdiv_row_json,
                        "Справочник.ПодразделенияОрганизаций",
                        processed_db=sqlite_db_file,
                        fallback_types=["Справочник.Подразделения"],
                    )
                    if ref4:
                        new_row.Субконто4 = ref4

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
        write_parties_mc_balances_to_1c("BD/parties_mc_balances_processed.db", com)
