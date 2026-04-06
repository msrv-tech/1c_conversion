# -*- coding: utf-8 -*-
"""
Модуль выгрузки дополнительных регистров ОС в 1С УХ.
Записывает в РегистрСведений РегистрацияТранспортныхСредств.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field, get_predefined_element_by_name
from tools.base_writer import create_reference_by_uuid, find_object_by_uuid, _get_enum_from_string
from tools.onec_connector import connect_to_1c, safe_getattr
from tools.logger import verbose_print

# Дата по умолчанию для Периода периодических регистров (01.01.2026)
DEFAULT_PERIOD_STR = "20260101000000"


def _parse_date_to_com(com_object, date_val):
    """Конвертирует дату в объект 1С Дата (как в supplier_balances_writer)."""
    if not date_val:
        return None
    try:
        d_str = str(date_val).replace("-", "").replace(":", "").replace(" ", "").replace(".", "")[:14]
        if len(d_str) < 8:
            return None
        year = int(d_str[0:4])
        month = int(d_str[4:6])
        day = int(d_str[6:8])
        hour = int(d_str[8:10]) if len(d_str) >= 10 else 0
        minute = int(d_str[10:12]) if len(d_str) >= 12 else 0
        second = int(d_str[12:14]) if len(d_str) >= 14 else 0
        return com_object.NewObject("Дата", year, month, day, hour, minute, second)
    except Exception:
        return None


def _set_period(mgr, com_object, period_str):
    """Устанавливает Период записи регистра (периодические регистры)."""
    if not period_str:
        return
    period_date = _parse_date_to_com(com_object, period_str)
    if period_date:
        try:
            mgr.Период = period_date
            return
        except Exception:
            pass
    # Fallback: строка YYYYMMDDHHMMSS (как в balances_nomenclature_groups)
    s = str(period_str).replace("-", "").replace(":", "").replace(" ", "")[:14]
    if len(s) >= 8:
        try:
            mgr.Период = s.ljust(14, "0")
        except Exception:
            pass


def _ref_from_item(com_object, item: dict, field: str, default_type: str, processed_db: str = None):
    """Получает ссылку из JSON-поля item[field]."""
    val = item.get(field)
    info = parse_reference_field(val)
    if not info or not info.get("uuid") or info["uuid"] == "00000000-0000-0000-0000-000000000000":
        return None
    ref_type = (info.get("type") or default_type).strip()
    ref = find_object_by_uuid(com_object, info["uuid"], ref_type)
    if ref:
        return ref
    return create_reference_by_uuid(
        com_object,
        info["uuid"],
        ref_type,
        ref_presentation=info.get("presentation", ""),
        processed_db=processed_db,
    )


def _set_register_record_field(manager, field: str, value, com_object):
    """Устанавливает поле записи регистра сведений."""
    if value is None or value == "":
        return
    if not hasattr(manager, field):
        return
    try:
        if isinstance(value, str) and value.startswith("Перечисление."):
            enum_obj = _get_enum_from_string(com_object, value)
            if not enum_obj:
                parts = value.split(".", 2)
                if len(parts) >= 3:
                    enum_obj = get_predefined_element_by_name(com_object, f"{parts[0]}.{parts[1]}", parts[2])
            if enum_obj:
                setattr(manager, field, enum_obj)
            return
        if isinstance(value, bool):
            setattr(manager, field, value)
            return
        if isinstance(value, (int, float)):
            setattr(manager, field, value)
            return
        setattr(manager, field, value)
    except Exception as e:
        verbose_print(f"  ⚠ Ошибка установки {field}: {e}")


def _set_exchange_mode(mgr):
    """Устанавливает ОбменДанными.Загрузка = True, если свойство доступно."""
    try:
        od = safe_getattr(mgr, "ОбменДанными", None)
        if od is not None:
            od.Загрузка = True
    except Exception:
        pass


def _write_vehicles_records(com_object, items: list, sqlite_db_file: str) -> int:
    """Записывает записи в РегистрацияТранспортныхСредств."""
    reg = safe_getattr(safe_getattr(com_object, "РегистрыСведений", None), "РегистрацияТранспортныхСредств", None)
    if not reg:
        verbose_print("  Регистр РегистрацияТранспортныхСредств не найден")
        return 0
    saved = 0
    for item in items:
        org_ref = _ref_from_item(com_object, item, "Организация", "Справочник.Организации", sqlite_db_file)
        os_ref = _ref_from_item(com_object, item, "ОсновноеСредство", "Справочник.ОсновныеСредства", sqlite_db_file)
        if not org_ref or not os_ref:
            continue
        try:
            mgr = reg.СоздатьМенеджерЗаписи()
            _set_period(mgr, com_object, item.get("Период") or DEFAULT_PERIOD_STR)
            mgr.Организация = org_ref
            mgr.ОсновноеСредство = os_ref
            po_ref = _ref_from_item(
                com_object, item, "ПостановкаНаУчетВНалоговомОргане",
                "Справочник.ПостановкиНаУчетВНалоговыхОрганах", sqlite_db_file
            )
            if po_ref:
                mgr.ПостановкаНаУчетВНалоговомОргане = po_ref
            no_ref = _ref_from_item(
                com_object, item, "НалоговыйОрган",
                "РегистрСведений.РегистрацияВНалоговомОрганеФизЛиц", sqlite_db_file
            )
            if no_ref:
                mgr.НалоговыйОрган = no_ref
            for f in ("ВключатьВНалоговуюБазу", "РегистрационныйЗнак", "Марка", "ИдентификационныйНомер",
                      "КодПоОКТМО", "КодПоОКАТО", "КодВидаТранспортногоСредства",
                      "НалоговаяБаза", "ЕдиницаИзмеренияНалоговойБазы", "НалоговаяСтавка",
                      "НалоговаяЛьгота", "КодНалоговойЛьготы", "ЛьготнаяСтавка",
                      "ПроцентУменьшения", "СуммаУменьшения", "ЭкологическийКласс",
                      "ОбщаяСобственность", "ДоляВПравеОбщейСобственностиЧислитель",
                      "ДоляВПравеОбщейСобственностиЗнаменатель", "ПовышающийКоэффициент",
                      "ВидЗаписи", "Комментарий"):
                _set_register_record_field(mgr, f, item.get(f), com_object)
            _set_exchange_mode(mgr)
            mgr.Записать()
            saved += 1
        except Exception as e:
            verbose_print(f"  Ошибка записи регистрации ТС: {e}")
    return saved


def write_fixed_assets_additional_registers_to_1c(
    sqlite_db_file: str,
    com_object: object,
    process_func=None,
) -> bool:
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА РЕГИСТРАЦИИ ТРАНСПОРТНЫХ СРЕДСТВ В 1С")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False

    total_saved = 0

    try:
        items = get_from_db(db_connection, "vehicles_registration")
        if not items:
            verbose_print("  vehicles_registration: нет данных")
        else:
            verbose_print(f"  vehicles_registration: {len(items)} записей")
            total_saved = _write_vehicles_records(com_object, items, sqlite_db_file)
            verbose_print(f"    Записано: {total_saved}")
        verbose_print(f"Всего записано: {total_saved}")
    finally:
        db_connection.close()

    return True


if __name__ == "__main__":
    target = os.getenv("TARGET_1C", "target")
    com = connect_to_1c(target)
    if com:
        write_fixed_assets_additional_registers_to_1c(
            "BD/fixed_assets_additional_registers_processed.db",
            com,
        )
