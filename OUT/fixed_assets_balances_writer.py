# -*- coding: utf-8 -*-
"""
Модуль выгрузки остатков ОС в документ «Ввод начальных остатков» в 1С УХ.
Раздел учета: ОсновныеСредства.
Табличная часть: ОС.
"""

import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field, get_predefined_element_by_name
from tools.base_writer import create_reference_by_uuid, find_object_by_uuid, _find_account_by_code_in_receiver, _get_enum_from_string
from tools.onec_connector import connect_to_1c, safe_getattr
from tools.logger import verbose_print

DOCUMENT_NAME = "ВводНачальныхОстатков"
TABLE_NAME = "ОС"
ACCOUNTING_SECTION = "ОсновныеСредства"
COMMENT_MARKER = "### Загрузка остатков ОС (31.12.2025) ###"

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

def write_fixed_assets_balances_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    """Записывает остатки ОС в документ Ввод начальных остатков."""
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА ОСТАТКОВ ОС В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False

    items = get_from_db(db_connection, "fixed_assets_balances")
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
            
            # Создаем уникальный маркер для этой группы (Организация + Подразделение)
            group_unique_marker = f"{COMMENT_MARKER} {subdiv_name}"
            
            verbose_print(f"Обработка: Организация {org_name}, Подразделение: {subdiv_name}")

            # Получаем ссылку на событие для заполнения СостояниеПринятияКУчетуРегл
            event_ref = None
            try:
                event_ref = com_object.Справочники.СобытияОС.НайтиПоНаименованию("Принятие к учету с вводом в эксплуатацию")
                
                # В 1С COM проверка на пустоту: event_ref.Пустая()
                is_empty = True
                try:
                    is_empty = event_ref.Пустая()
                except:
                    is_empty = not event_ref

                if is_empty:
                    verbose_print("  ⚠ Событие 'Принятие к учету с вводом в эксплуатацию' не найдено. Пробую поиск по части имени...")
                    query_event = com_object.NewObject("Запрос")
                    query_event.Текст = "ВЫБРАТЬ ПЕРВЫЕ 1 Ссылка ИЗ Справочник.СобытияОС ГДЕ Наименование ПОДОБНО ""Принятие к учету%"""
                    res_event = query_event.Выполнить().Выбрать()
                    if res_event.Следующий():
                        event_ref = res_event.Ссылка
                        verbose_print(f"  Найдено альтернативное событие: {event_ref.Наименование}")
                    else:
                        verbose_print("  ⚠ Событие не найдено даже по маске.")
                        event_ref = None
                else:
                    verbose_print(f"  Найдено событие: {event_ref.Наименование}")
            except Exception as e:
                verbose_print(f"  ⚠ Не удалось найти событие ОС: {e}")

            # Поиск существующего документа по уникальному маркеру в комментарии
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
                
                # Основное средство
                os_ref = _ref_by_uuid_or_type(com_object, item.get("ОсновноеСредство"), "Справочник.ОсновныеСредства", sqlite_db_file)
                if os_ref:
                    new_row.ОсновноеСредство = os_ref
                
                # Состояние принятия к учету
                if event_ref:
                    new_row.СостояниеПринятияКУчетуРегл = event_ref

                # Перечисления — устанавливаем явно до цикла (формат Перечисление.Имя.Значение)
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
                        else:
                            verbose_print(f"  ⚠ Не удалось получить перечисление {enum_field}={val}")

                # Заполнение реквизитов ТЧ по именам (совпадают в запросе и ТЧ)
                fields_to_fill = [
                    "ИнвентарныйНомерРегл", "ДатаПринятияКУчетуРегл", "НачислятьАмортизациюБУ", "НачислятьАмортизациюНУ",
                    "ТекущаяСтоимостьБУ", "ТекущаяСтоимостьНУ", "ТекущаяСтоимостьПР", "ТекущаяСтоимостьВР",
                    "НакопленнаяАмортизацияБУ", "НакопленнаяАмортизацияНУ", "НакопленнаяАмортизацияПР", "НакопленнаяАмортизацияВР",
                    "СчетУчетаБУ", "СчетАмортизацииБУ", "СпособНачисленияАмортизацииБУ", "МетодНачисленияАмортизацииНУ",
                    "СпособПоступленияРегл",
                    "СрокПолезногоИспользованияБУ", "СрокПолезногоИспользованияНУ", "СрокПолезногоИспользованияУСН",
                    "ПервоначальнаяСтоимостьБУ", "ПервоначальнаяСтоимостьНУ", "ПервоначальнаяСтоимостьУСН",
                    "ПорядокВключенияСтоимостиВСоставРасходовНУ", "ПорядокВключенияСтоимостиВСоставРасходовУСН",
                    "ПорядокПогашенияСтоимостиБУ",
                    "СпециальныйКоэффициентНУ", "КоэффициентАмортизацииБУ", "КоэффициентУскоренияБУ",
                    "АмортизацияДо2002НУ", "СтоимостьДо2002НУ", "ПараметрВыработкиБУ",
                    "СуммаНачисленнойАмортизацииУСН", "ДатаПриобретенияУСН",
                    "ДатаПоследнейМодернизацииРегл", "НомерДокументаМодернизацииРегл", "СостояниеМодернизацииРегл",
                    "СуммаПоследнейМодернизацииБУ", "СуммаПоследнейМодернизацииНУ",
                    "СрокИспользованияДляВычисленияАмортизацииБУ", "СтоимостьДляВычисленияАмортизацииБУ",
                    "ОбъемПродукцииРаботБУ", "ОбъемПродукцииРаботДляВычисленияАмортизацииБУ",
                    "ВыработкаКоличествоБУ", "РЦ_ЛиквидационнаяСтоимость",
                    "НазваниеДокументаПринятияКУчетуРегл", "НомерДокументаПринятияКУчетуРегл",
                    "НазваниеДокументаМодернизацииРегл"
                ]

                for field in fields_to_fill:
                    if field in item and hasattr(new_row, field):
                        val = item[field]
                        if field == "ДатаПринятияКУчетуРегл":
                            # ДатаПринятияКУчетуРегл в ТЧ ОС может ожидать объект Дата
                            if val:
                                try:
                                    # Формат в SQLite обычно YYYY-MM-DD HH:MM:SS
                                    # 1С COM ожидает формат YYYYMMDDHHMMSS для метода Дата()
                                    clean_date = str(val).replace("-", "").replace(":", "").replace(" ", "")[:14]
                                    
                                    # ПРЯМАЯ УСТАНОВКА ЧЕРЕЗ СВОЙСТВО, ЕСЛИ МЕТОД Дата() НЕ ДОСТУПЕН
                                    # В некоторых версиях COMConnector метод Дата() может быть недоступен напрямую у com_object
                                    # Попробуем использовать универсальный способ через XML или строку
                                    try:
                                        date_obj = com_object.Дата(clean_date)
                                    except:
                                        # Если com_object.Дата() не работает, пробуем через XDTO или просто строку
                                        # (1С часто понимает строку в формате YYYYMMDDHHMMSS при присваивании дате)
                                        date_obj = clean_date
                                    
                                    setattr(new_row, "ДатаПринятияКУчетуРегл", date_obj)
                                    # Попробуем также установить в ДатаПринятияКУчету, если оно есть
                                    if hasattr(new_row, "ДатаПринятияКУчету"):
                                        setattr(new_row, "ДатаПринятияКУчету", date_obj)
                                except Exception as e:
                                    verbose_print(f"  ⚠ Ошибка установки даты {field}: {e}")
                                    setattr(new_row, field, val)
                        elif field in ["СчетУчетаБУ", "СчетАмортизацииБУ"]:
                            # Счета обрабатываем отдельно (по коду или UUID)
                            acc_info = parse_reference_field(val)
                            if acc_info:
                                code = acc_info.get("presentation", "").split(" ")[0]
                                acc_ref = _find_account_by_code_in_receiver(com_object, code, "ПланСчетов.Хозрасчетный")
                                if acc_ref:
                                    setattr(new_row, field, acc_ref)
                        elif field in ["МОЛРегл", "СпособОтраженияРасходовПоАмортизации", "ГрафикАмортизацииБУ"]:
                            ref = _ref_by_uuid_or_type(com_object, val, "", sqlite_db_file)
                            if ref:
                                setattr(new_row, field, ref)
                        elif field in ["ПорядокВключенияСтоимостиВСоставРасходовНУ", "СпособНачисленияАмортизацииБУ", "МетодНачисленияАмортизацииНУ", "СпособПоступленияРегл", "ПорядокВключенияСтоимостиВСоставРасходовУСН", "ПорядокПогашенияСтоимостиБУ"]:
                            # Значение в формате Перечисление.Имя.Значение — преобразуем в COM-объект (уже установлено выше, не перезаписываем строкой)
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
                            except:
                                pass

                # Дополнительные ссылки
                for extra_ref_field in ["МОЛРегл", "СпособОтраженияРасходовПоАмортизации", "ГрафикАмортизацииБУ"]:
                    if extra_ref_field in item:
                        ref = _ref_by_uuid_or_type(com_object, item[extra_ref_field], "", sqlite_db_file)
                        if ref:
                            setattr(new_row, extra_ref_field, ref)

            doc_obj.ОбменДанными.Загрузка = True
            try:
                doc_obj.Записать()
                verbose_print(f"  Документ записан: {doc_obj.Ссылка}")
                
                # Проведение документа
                try:
                    doc_obj.ОбменДанными.Загрузка = False
                    # РежимЗаписиДокумента.Проведение = 1
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
        write_fixed_assets_balances_to_1c("BD/fixed_assets_balances_processed.db", com)
