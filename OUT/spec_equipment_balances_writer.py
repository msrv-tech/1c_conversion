# -*- coding: utf-8 -*-
"""
Модуль выгрузки остатков по счёту 10.11 (спецодежда и спецоснастка в эксплуатации)
в документ «Ввод начальных остатков», табличная часть СпецодеждаИСпецоснасткаВЭксплуатации.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field
from tools.base_writer import create_reference_by_uuid, find_object_by_uuid, _find_account_by_code_in_receiver
from tools.onec_connector import connect_to_1c, safe_getattr
from tools.logger import verbose_print
from tools.chart_of_accounts_mapper import load_mapping, extract_account_code, get_mapped_account_code

DOCUMENT_NAME = "ВводНачальныхОстатков"
TABLE_NAME = "СпецодеждаИСпецоснасткаВЭксплуатации"
ACCOUNTING_SECTION = "Материалы"
COMMENT_MARKER = "### Загрузка остатков по счёту 10.11 спецодежда и спецоснастка (31.12.2025) ###"


def get_or_create_service_settlement_doc_spec(com_object, org_ref):
    """Находит или создает служебный документ расчетов для партии спецодежды/спецоснастки."""
    comment = "### СЛУЖЕБНЫЙ: Для ввода остатков спецодежда/спецоснастка (Партия) ###"

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

    try:
        doc_manager = com_object.Документы.ДокументРасчетовСКонтрагентом
        doc_obj = doc_manager.СоздатьДокумент()
        doc_obj.Дата = "20251231235959"
        doc_obj.Организация = org_ref
        doc_obj.Комментарий = f"{comment}\nСоздан автоматически для заполнения поля Партия (спецодежда/спецоснастка)."

        if hasattr(doc_obj, "НомерВходящегоДокумента"):
            doc_obj.НомерВходящегоДокумента = "СЛУЖЕБНЫЙ_СПЕЦ"
        if hasattr(doc_obj, "ДатаВходящегоДокумента"):
            doc_obj.ДатаВходящегоДокумента = "20251231235959"

        doc_obj.ОбменДанными.Загрузка = True
        doc_obj.Записать()
        return doc_obj.Ссылка
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка при создании служебного документа расчетов для спецодежды: {e}")
        return None


def _ref_by_uuid_or_type(com_object, ref_json, default_type: str, processed_db: str = None, fallback_types: list = None):
    """Возвращает ссылку по UUID; тип берётся из ref_json или default_type. fallback_types — альтернативные типы для поиска в приёмнике."""
    info = parse_reference_field(ref_json)
    if not info or not info.get("uuid") or info.get("uuid") == "00000000-0000-0000-0000-000000000000":
        return None
    ref_type = (info.get("type") or default_type).strip()
    if not ref_type:
        ref_type = default_type
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


def _create_or_find_spec_transfer_doc(
    com_object,
    doc_uuid: str,
    org_ref,
    ref_presentation: str = "",
    item: dict = None,
    processed_db: str = None,
):
    """Создаёт или находит документ ПартияМатериаловВЭксплуатации по UUID. Заполняет Номенклатура и Назначение использования."""
    if not doc_uuid or doc_uuid == "00000000-0000-0000-0000-000000000000":
        return None
    try:
        doc_manager = safe_getattr(com_object, "Документы", None)
        if not doc_manager:
            return None
        pm_doc_manager = safe_getattr(doc_manager, "ПартияМатериаловВЭксплуатации", None)
        if not pm_doc_manager:
            verbose_print("  ⚠ Документ ПартияМатериаловВЭксплуатации не найден в приёмнике.")
            return None

        uuid_obj = com_object.NewObject("УникальныйИдентификатор", doc_uuid)
        doc_ref = pm_doc_manager.ПолучитьСсылку(uuid_obj)
        doc_obj = doc_ref.ПолучитьОбъект()

        is_new = doc_obj is None
        if is_new:
            doc_obj = pm_doc_manager.СоздатьДокумент()
            doc_obj.УстановитьСсылкуНового(doc_ref)
            doc_obj.Дата = "20251231235959"
            if org_ref:
                doc_obj.Организация = org_ref
            doc_obj.Комментарий = f"Загружено из УПП. Партия материалов в эксплуатации. UUID: {doc_uuid}."
            verbose_print(f"  → Создан документ ПартияМатериаловВЭксплуатации: {ref_presentation or doc_uuid[:30]}...")

        # Заполняем реквизиты партии: Организация, Номенклатура, НазначениеИспользования
        needs_write = is_new
        if org_ref and hasattr(doc_obj, "Организация"):
            try:
                is_empty = doc_obj.Организация is None
                if not is_empty and hasattr(doc_obj.Организация, "Пустая"):
                    is_empty = bool(doc_obj.Организация.Пустая())
                if is_empty:
                    doc_obj.Организация = org_ref
                    needs_write = True
            except Exception:
                doc_obj.Организация = org_ref
                needs_write = True

        if item:
            nom_json = item.get("Номенклатура_ДляЗаписи") or item.get("Номенклатура")
            nom_info = parse_reference_field(nom_json) if nom_json else None
            if nom_info and nom_info.get("uuid"):
                nom_ref = create_reference_by_uuid(
                    com_object,
                    nom_info["uuid"],
                    "Справочник.Номенклатура",
                    ref_presentation=nom_info.get("presentation", ""),
                    processed_db=processed_db,
                )
                if nom_ref and hasattr(doc_obj, "Номенклатура"):
                    doc_obj.Номенклатура = nom_ref
                    needs_write = True

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
                        needs_write = True
                        break

        if needs_write:
            doc_obj.ОбменДанными.Загрузка = True
            doc_obj.Записать()

        return doc_obj.Ссылка
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка при создании/поиске документа ПартияМатериаловВЭксплуатации: {e}")
        return None


def write_spec_equipment_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    """Записывает остатки по счёту 10.11 в документ Ввод начальных остатков, ТЧ СпецодеждаИСпецоснасткаВЭксплуатации."""
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ ПО СЧЁТУ 10.11 (СПЕЦОДЕЖДА И СПЕЦОСНАСТКА) В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False

    items = get_from_db(db_connection, "spec_equipment_balances")
    db_connection.close()

    if not items:
        verbose_print("Нет данных для записи.")
        return True

    coa_mapping, _ = load_mapping("CONF/chart_of_accounts_mapping.json")
    verbose_print(f"Прочитано строк остатков для записи: {len(items)}")

    # Группируем по организации (один документ на организацию, раздел СпецоснасткаВЭксплуатации)
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

            service_settlement_doc = get_or_create_service_settlement_doc_spec(com_object, doc_obj.Организация)

            tabular_section = safe_getattr(doc_obj, TABLE_NAME, None)
            if not tabular_section:
                verbose_print(f"  ⚠ Табличная часть {TABLE_NAME} не найдена в документе.")
                continue

            tabular_section.Очистить()

            # Счёт 10.11 маппится в 10.11.1 в приёмнике
            acc_code = get_mapped_account_code("10.11", coa_mapping) or "10.11"
            acc_ref = _find_account_by_code_in_receiver(com_object, acc_code, "ПланСчетов.Хозрасчетный")
            if not acc_ref:
                verbose_print(f"  ⚠ Счёт {acc_code} (маппинг 10.11) не найден в приемнике.")

            for item in group_items:
                new_row = tabular_section.Добавить()

                if acc_ref:
                    new_row.СчетУчета = acc_ref

                # Партия (служебный документ расчетов)
                if service_settlement_doc and hasattr(new_row, "Партия"):
                    new_row.Партия = service_settlement_doc

                # Номенклатура_ДляЗаписи: процессор объединил Номенклатура+Характеристика (как в characteristics_balances)
                nom_json = item.get("Номенклатура_ДляЗаписи") or item.get("Номенклатура")
                nom_info = parse_reference_field(nom_json) if nom_json else None
                if nom_info and nom_info.get("uuid"):
                    nom_ref = create_reference_by_uuid(
                        com_object,
                        nom_info["uuid"],
                        "Справочник.Номенклатура",
                        ref_presentation=nom_info.get("presentation", ""),
                        processed_db=sqlite_db_file,
                    )
                    if nom_ref and hasattr(new_row, "Номенклатура"):
                        new_row.Номенклатура = nom_ref

                # СпецМатериалПартияМатериалаВЭксплуатации (ДокументПередачи) — создаём/обновляем и заполняем реквизиты
                if hasattr(new_row, "СпецМатериалПартияМатериалаВЭксплуатации"):
                    batch_json = item.get("СпецМатериалПартияМатериалаВЭксплуатации")
                    batch_info = parse_reference_field(batch_json) if batch_json else None
                    if batch_info and batch_info.get("uuid"):
                        ref2 = _create_or_find_spec_transfer_doc(
                            com_object,
                            batch_info["uuid"],
                            doc_obj.Организация,
                            batch_info.get("presentation", ""),
                            item=item,
                            processed_db=sqlite_db_file,
                        )
                        if ref2:
                            new_row.СпецМатериалПартияМатериалаВЭксплуатации = ref2

                # СпецМатериалПодразделениеФизЛицо (Подразделение или ФизЛицо/работник)
                podr_fiz_json = item.get("СпецМатериалПодразделениеФизЛицо")
                if hasattr(new_row, "СпецМатериалПодразделениеФизЛицо") and podr_fiz_json:
                    ref3 = _ref_by_uuid_or_type(
                        com_object,
                        podr_fiz_json,
                        "Справочник.ПодразделенияОрганизаций",
                        processed_db=sqlite_db_file,
                        fallback_types=[
                            "Справочник.Подразделения",
                            "Справочник.ФизическиеЛица",
                            "Справочник.СотрудникиОрганизаций",
                            "Справочник.РаботникиОрганизаций",
                        ],
                    )
                    if ref3:
                        new_row.СпецМатериалПодразделениеФизЛицо = ref3

                # Назначение использования — в приёмнике УХ: НазначенияИспользованияНоменклатуры
                nazn_ref = _ref_by_uuid_or_type(
                    com_object,
                    item.get("НазначениеИспользования"),
                    "Справочник.НазначенияИспользования",
                    processed_db=sqlite_db_file,
                    fallback_types=["Справочник.НазначенияИспользованияНоменклатуры"],
                )
                if nazn_ref:
                    if hasattr(new_row, "СпецМатериалНазначениеИспользования"):
                        new_row.СпецМатериалНазначениеИспользования = nazn_ref
                    elif hasattr(new_row, "НазначениеИспользования"):
                        new_row.НазначениеИспользования = nazn_ref

                # Количество
                try:
                    qty = item.get("Quantity", 0)
                    qty = float(qty) if qty is not None and qty != "" else 0.0
                except (TypeError, ValueError):
                    qty = 0.0
                if hasattr(new_row, "Количество"):
                    new_row.Количество = qty

                # Сумма / СпецМатериалПервоначальнаяСтоимость
                try:
                    sum_val = item.get("Amount", 0)
                    sum_val = float(sum_val) if sum_val is not None and sum_val != "" else 0.0
                except (TypeError, ValueError):
                    sum_val = 0.0

                if hasattr(new_row, "Сумма"):
                    new_row.Сумма = sum_val
                if hasattr(new_row, "СпецМатериалПервоначальнаяСтоимость"):
                    new_row.СпецМатериалПервоначальнаяСтоимость = sum_val
                if hasattr(new_row, "СуммаНУ"):
                    new_row.СуммаНУ = sum_val
                if hasattr(new_row, "СпецМатериалПервоначальнаяСтоимостьНУ"):
                    new_row.СпецМатериалПервоначальнаяСтоимостьНУ = sum_val

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
        write_spec_equipment_balances_to_1c("BD/spec_equipment_balances_processed.db", com)
