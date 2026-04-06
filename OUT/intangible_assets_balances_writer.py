# -*- coding: utf-8 -*-
"""
Модуль выгрузки остатков НМА в документ «Ввод начальных остатков» в 1С УХ.
Раздел учета: НематериальныеАктивы.
Табличная часть: НМА.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field, get_predefined_element_by_name
from tools.base_writer import create_reference_by_uuid, find_object_by_uuid, _find_account_by_code_in_receiver, _get_enum_from_string
from tools.onec_connector import connect_to_1c
from tools.logger import verbose_print

DOCUMENT_NAME = "ВводНачальныхОстатков"
TABLE_NAME = "НМА"
ACCOUNTING_SECTION = "НематериальныеАктивыИНИОКР"
COMMENT_MARKER = "### Загрузка остатков НМА (31.12.2025) ###"

def _ref_by_uuid_or_type(com_object, ref_json, default_type: str, processed_db: str = None):
    """Возвращает ссылку по UUID; тип берётся из ref_json или default_type."""
    info = parse_reference_field(ref_json)
    if not info or not info.get("uuid") or info.get("uuid") == "00000000-0000-0000-0000-000000000000":
        return None
    ref_type = (info.get("type") or default_type).strip()
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

def write_intangible_assets_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    """Записывает остатки НМА в документ Ввод начальных остатков."""
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ НМА В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False

    items = get_from_db(db_connection, "intangible_assets_balances")
    db_connection.close()

    if not items:
        verbose_print("Нет данных для записи.")
        return True

    verbose_print(f"Прочитано строк остатков: {len(items)}")

    # Группируем по (Организация, ПодразделениеОрганизации)
    doc_groups = {}
    for item in items:
        org_json = item.get("Организация")
        org_info = parse_reference_field(org_json)
        org_uuid = org_info.get("uuid") if org_info else None
        if not org_uuid:
            continue

        subdiv_json = item.get("ПодразделениеОрганизации")
        subdiv_info = parse_reference_field(subdiv_json)
        subdiv_uuid = (subdiv_info.get("uuid") or "").strip() if subdiv_info else ""
        if subdiv_uuid == "00000000-0000-0000-0000-000000000000":
            subdiv_uuid = ""

        group_key = (org_uuid, subdiv_uuid)
        if group_key not in doc_groups:
            doc_groups[group_key] = []
        doc_groups[group_key].append(item)

    try:
        doc_manager = getattr(com_object.Документы, DOCUMENT_NAME)

        for (org_uuid, subdiv_uuid), group_items in doc_groups.items():
            org_info = parse_reference_field(group_items[0].get("Организация"))
            subdiv_info = parse_reference_field(group_items[0].get("ПодразделениеОрганизации"))

            org_name = org_info.get('presentation', org_uuid)
            subdiv_name = subdiv_info.get('presentation', '(пусто)') if subdiv_info else '(пусто)'

            group_unique_marker = f"{COMMENT_MARKER} {subdiv_name}"

            verbose_print(f"Обработка: Организация {org_name}, Подразделение: {subdiv_name}")

            # Событие принятия к учету НМА (если есть справочник СобытияНМА)
            event_ref = None
            try:
                if hasattr(com_object.Справочники, "СобытияНМА"):
                    event_ref = com_object.Справочники.СобытияНМА.НайтиПоНаименованию("Принятие к учету")
                    if event_ref:
                        try:
                            if event_ref.Пустая():
                                event_ref = None
                        except Exception:
                            pass
            except Exception as e:
                verbose_print(f"  ⚠ СобытияНМА не найдены: {e}")

            query = com_object.NewObject("Запрос")
            query.Текст = f"""ВЫБРАТЬ ПЕРВЫЕ 1 Ссылка ИЗ Документ.{DOCUMENT_NAME}
                            ГДЕ Комментарий ПОДОБНО &Маркер
                            И Организация = &Организация
                            И РазделУчета = ЗНАЧЕНИЕ(Перечисление.РазделыУчетаДляВводаОстатков.{ACCOUNTING_SECTION})
                            И Дата МЕЖДУ ДАТАВРЕМЯ(2025, 12, 31, 0, 0, 0) И ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59)"""

            query.УстановитьПараметр("Маркер", f"%{group_unique_marker}%")
            query.УстановитьПараметр("Организация", create_reference_by_uuid(com_object, org_uuid, "Справочник.Организации"))

            res = query.Выполнить().Выбрать()
            if res.Следующий():
                doc_obj = res.Ссылка.ПолучитьОбъект()
                verbose_print(f"  Обновление существующего документа: {res.Ссылка}")
            else:
                doc_obj = doc_manager.СоздатьДокумент()
                doc_obj.Дата = "20251231235959"
                doc_obj.Организация = create_reference_by_uuid(com_object, org_uuid, "Справочник.Организации")
                doc_obj.РазделУчета = getattr(com_object.Перечисления.РазделыУчетаДляВводаОстатков, ACCOUNTING_SECTION)
                if subdiv_uuid:
                    doc_obj.ПодразделениеОрганизации = create_reference_by_uuid(com_object, subdiv_uuid, "Справочник.ПодразделенияОрганизаций")
                doc_obj.Комментарий = f"{group_unique_marker}\nЗагружено автоматически."
                verbose_print("  Создание нового документа")

            doc_obj.ОтражатьВБухгалтерскомУчете = True
            doc_obj.ОтражатьВНалоговомУчете = True
            doc_obj.ОтражатьПоСпециальнымРегистрам = True

            tabular_section = getattr(doc_obj, TABLE_NAME)
            tabular_section.Очистить()

            for item in group_items:
                new_row = tabular_section.Добавить()

                # Нематериальный актив
                nma_ref = _ref_by_uuid_or_type(com_object, item.get("НематериальныйАктив"), "Справочник.НематериальныеАктивы", sqlite_db_file)
                if nma_ref:
                    new_row.НематериальныйАктив = nma_ref

                if event_ref and hasattr(new_row, "СостояниеПринятияКУчетуРегл"):
                    new_row.СостояниеПринятияКУчетуРегл = event_ref

                # Перечисления
                for enum_field in ["СпособНачисленияАмортизацииБУ", "СпособПоступленияРегл", "МетодНачисленияАмортизацииНУ", "ПорядокВключенияСтоимостиВСоставРасходовНУ", "ПорядокВключенияСтоимостиВСоставРасходовУСН", "ПорядокПогашенияСтоимостиБУ"]:
                    val = item.get(enum_field)
                    if val and isinstance(val, str) and val.startswith("Перечисление.") and hasattr(new_row, enum_field):
                        enum_obj = _get_enum_from_string(com_object, val)
                        if not enum_obj:
                            parts = val.split(".", 2)
                            if len(parts) == 3:
                                enum_obj = get_predefined_element_by_name(com_object, f"{parts[0]}.{parts[1]}", parts[2])
                        if enum_obj:
                            try:
                                setattr(new_row, enum_field, enum_obj)
                            except Exception as e:
                                verbose_print(f"  ⚠ Ошибка установки перечисления {enum_field}: {e}")

                fields_to_fill = [
                    "ДатаПринятияКУчетуРегл", "НачислятьАмортизациюБУ", "НачислятьАмортизациюНУ",
                    "ТекущаяСтоимостьБУ", "ТекущаяСтоимостьНУ", "ТекущаяСтоимостьПР", "ТекущаяСтоимостьВР",
                    "НакопленнаяАмортизацияБУ", "НакопленнаяАмортизацияНУ", "НакопленнаяАмортизацияПР", "НакопленнаяАмортизацияВР",
                    "СчетУчетаБУ", "СчетАмортизацииБУ", "СпособНачисленияАмортизацииБУ", "МетодНачисленияАмортизацииНУ",
                    "СпособПоступленияРегл",
                    "СрокПолезногоИспользованияБУ", "СрокПолезногоИспользованияНУ", "СрокПолезногоИспользованияУСН",
                    "ПервоначальнаяСтоимостьБУ", "ПервоначальнаяСтоимостьНУ", "ПервоначальнаяСтоимостьУСН",
                    "ПорядокВключенияСтоимостиВСоставРасходовНУ", "ПорядокВключенияСтоимостиВСоставРасходовУСН",
                    "ПорядокПогашенияСтоимостиБУ",
                    "СпециальныйКоэффициентНУ", "КоэффициентАмортизацииБУ",
                    "СуммаНачисленнойАмортизацииУСН", "ДатаПриобретенияУСН",
                    "СпособОтраженияРасходовПоАмортизации"
                ]

                for field in fields_to_fill:
                    if field in item and hasattr(new_row, field):
                        val = item[field]
                        if field == "ДатаПринятияКУчетуРегл":
                            if val:
                                try:
                                    clean_date = str(val).replace("-", "").replace(":", "").replace(" ", "")[:14]
                                    try:
                                        date_obj = com_object.Дата(clean_date)
                                    except Exception:
                                        date_obj = clean_date
                                    setattr(new_row, "ДатаПринятияКУчетуРегл", date_obj)
                                    if hasattr(new_row, "ДатаПринятияКУчету"):
                                        setattr(new_row, "ДатаПринятияКУчету", date_obj)
                                except Exception as e:
                                    verbose_print(f"  ⚠ Ошибка установки даты {field}: {e}")
                        elif field in ["СчетУчетаБУ", "СчетАмортизацииБУ"]:
                            acc_info = parse_reference_field(val)
                            if acc_info:
                                code = acc_info.get("presentation", "").split(" ")[0]
                                acc_ref = _find_account_by_code_in_receiver(com_object, code, "ПланСчетов.Хозрасчетный")
                                if acc_ref:
                                    setattr(new_row, field, acc_ref)
                        elif field == "СпособОтраженияРасходовПоАмортизации":
                            ref = _ref_by_uuid_or_type(com_object, val, "", sqlite_db_file)
                            if ref:
                                setattr(new_row, field, ref)
                        elif field in ["ПорядокВключенияСтоимостиВСоставРасходовНУ", "СпособНачисленияАмортизацииБУ", "МетодНачисленияАмортизацииНУ", "СпособПоступленияРегл", "ПорядокВключенияСтоимостиВСоставРасходовУСН", "ПорядокПогашенияСтоимостиБУ"]:
                            if val and isinstance(val, str) and val.startswith("Перечисление."):
                                enum_obj = _get_enum_from_string(com_object, val)
                                if not enum_obj:
                                    parts = val.split(".", 2)
                                    if len(parts) == 3:
                                        enum_obj = get_predefined_element_by_name(com_object, f"{parts[0]}.{parts[1]}", parts[2])
                                if enum_obj:
                                    try:
                                        setattr(new_row, field, enum_obj)
                                    except Exception as e:
                                        verbose_print(f"  ⚠ Ошибка установки перечисления {field}: {e}")
                        else:
                            try:
                                setattr(new_row, field, val)
                            except Exception:
                                pass

                if "СпособОтраженияРасходовПоАмортизации" in item:
                    ref = _ref_by_uuid_or_type(com_object, item["СпособОтраженияРасходовПоАмортизации"], "", sqlite_db_file)
                    if ref and hasattr(new_row, "СпособОтраженияРасходовПоАмортизации"):
                        setattr(new_row, "СпособОтраженияРасходовПоАмортизации", ref)

            doc_obj.ОбменДанными.Загрузка = True
            try:
                doc_obj.Записать()
                verbose_print(f"  Документ записан: {doc_obj.Ссылка}")

                try:
                    doc_obj.ОбменДанными.Загрузка = False
                    doc_obj.Записать(com_object.РежимЗаписиДокумента.Проведение)
                    verbose_print(f"  Документ успешно проведен: {doc_obj.Ссылка}")
                except Exception as e_post:
                    verbose_print(f"  ⚠ Не удалось провести документ: {e_post}")
            except Exception as e:
                verbose_print(f"  Ошибка записи документа: {e}")

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
        write_intangible_assets_balances_to_1c("BD/intangible_assets_balances_processed.db", com)
