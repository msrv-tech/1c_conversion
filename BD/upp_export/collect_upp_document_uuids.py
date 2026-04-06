# -*- coding: utf-8 -*-
"""
Скрипт для сбора UUID документов УПП за период проведения для дальнейшего экспорта
справочников по кодам/UUID через export_by_code.py.

Период по умолчанию: 01.01.2026 - 13.03.2026.
Подключается к UPP (SOURCE_CONNECTION_STRING / source), выполняет запросы к документам,
извлекает UUID документов и связанных справочников, сохраняет в BD/upp_export/.

Использование:
    python BD/upp_export/collect_upp_document_uuids.py
    python BD/upp_export/collect_upp_document_uuids.py --output-dir BD/upp_export --date-from 2026-01-01 --date-to 2026-03-13 --verbose
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(os.path.dirname(_SCRIPT_DIR))
sys.path.insert(0, _ROOT_DIR)

from tools.encoding_fix import fix_encoding
from tools.logger import verbose_print, set_verbose
from tools.onec_connector import connect_to_1c, execute_query
from tools.reference_objects import get_reference_objects_db_path, set_prod_mode

fix_encoding()

# Маппинг: поле в запросе (суффикс _UUID) -> catalog_name для export_by_code
# Имена полей из temp/ссылки (метаданные УПП)
FIELD_TO_CATALOG: Dict[str, str] = {
    "ДоговорКонтрагента_UUID": "contractor_contracts",
    "ЗаказПоставщику_UUID": "supplier_orders",
    "ЗаказПокупателя_UUID": "customer_orders",
    "Контрагент_UUID": "contractors",
    "СчетУчета_UUID": "bank_accounts",
    "СчетОрганизации_UUID": "bank_accounts",
    "СчетКонтрагента_UUID": "bank_accounts",
    "БанковскийСчет_UUID": "bank_accounts",
    "БанковскийСчетКонтрагента_UUID": "bank_accounts",
    "БанковскийСчетОрганизации_UUID": "bank_accounts",
    "СчетПолучателя_UUID": "bank_accounts",
    "СчетПлательщика_UUID": "bank_accounts",
    "Сделка_UUID": "supplier_orders",  # fallback; реальная маршрутизация — по ТИПЗНАЧЕНИЯ в DEAL_FIELD_CONFIGS
    "ДокументОснование_UUID": "supplier_orders",
    "Проект_UUID": "projects",
    "customПроект_UUID": "projects",
    "Номенклатура_UUID": "nomenclature",
    "ХарактеристикаНоменклатуры_UUID": "nomenclature_characteristics",
    "СерияНоменклатуры_UUID": "nomenclature_series",
    "НоменклатурнаяГруппа_UUID": "nomenclature_groups",
    "СтатьяЗатрат_UUID": "cost_items",
    "СтатьиЗатрат_UUID": "cost_items",
    "РасходыБудущихПериодов_UUID": "prepaid_expenses",
    "ПрочиеДоходыИРасходы_UUID": "other_income_and_expenses",
    "СпособОтраженияРасходовПоАмортизации_UUID": "amortization_expense_methods",
    "НазначениеИспользования_UUID": "ppe_usage_purposes",
    "НазначениеИспользованияНоменклатуры_UUID": "ppe_usage_purposes",
}

# Конфигурация документов: (имя_документа, запрос_полей, доп_условие)
# Поля — список кортежей (поле_в_запросе, алиас_UUID)
DOCUMENT_CONFIGS: List[Tuple[str, List[Tuple[str, str]], Optional[str]]] = [
    # Платёжные документы — шапка (имена полей из temp/ссылки)
    (
        "ЗаявкаНаРасходованиеСредств",
        [
            ("Док.ДокументОснование", "ЗаказПоставщику_UUID"),
            ("Док.Контрагент", "Контрагент_UUID"),
            ("Док.БанковскийСчетКасса", "БанковскийСчет_UUID"),
        ],
        None,
    ),
    (
        "ПлатежноеПоручениеИсходящее",
        [
            ("Док.ДоговорКонтрагента", "ДоговорКонтрагента_UUID"),
            ("Док.Контрагент", "Контрагент_UUID"),
            ("Док.СчетОрганизации", "СчетОрганизации_UUID"),
            ("Док.СчетКонтрагента", "СчетКонтрагента_UUID"),
        ],
        None,
    ),
    (
        "ПлатежноеПоручениеВходящее",
        [
            ("Док.ДоговорКонтрагента", "ДоговорКонтрагента_UUID"),
            ("Док.Контрагент", "Контрагент_UUID"),
            ("Док.СчетОрганизации", "СчетОрганизации_UUID"),
            ("Док.СчетКонтрагента", "СчетКонтрагента_UUID"),
        ],
        "И Док.Оплачено = ИСТИНА",
    ),
    (
        "ПлатежныйОрдерПоступлениеДенежныхСредств",
        [
            ("Док.ДоговорКонтрагента", "ДоговорКонтрагента_UUID"),
            ("Док.Контрагент", "Контрагент_UUID"),
            ("Док.СчетОрганизации", "СчетОрганизации_UUID"),
            ("Док.СчетКонтрагента", "СчетКонтрагента_UUID"),
        ],
        None,
    ),
    (
        "ПлатежныйОрдерСписаниеДенежныхСредств",
        [
            ("Док.ДоговорКонтрагента", "ДоговорКонтрагента_UUID"),
            ("Док.Контрагент", "Контрагент_UUID"),
            ("Док.СчетОрганизации", "СчетОрганизации_UUID"),
            ("Док.СчетКонтрагента", "СчетКонтрагента_UUID"),
        ],
        None,
    ),
    # Поступление товаров и услуг — шапка (Сделка вынесена в DEAL_FIELD_CONFIGS — маршрутизация по типу)
    (
        "ПоступлениеТоваровУслуг",
        [
            ("Док.ДоговорКонтрагента", "ДоговорКонтрагента_UUID"),
            ("Док.Контрагент", "Контрагент_UUID"),
            ("Док.БанковскийСчетКонтрагента", "БанковскийСчетКонтрагента_UUID"),
        ],
        None,
    ),
    # Реализация и Акт — шапка (Сделка вынесена в DEAL_FIELD_CONFIGS — маршрутизация по типу)
    (
        "РеализацияТоваровУслуг",
        [
            ("Док.ДоговорКонтрагента", "ДоговорКонтрагента_UUID"),
            ("Док.Контрагент", "Контрагент_UUID"),
            ("Док.БанковскийСчетОрганизации", "БанковскийСчетОрганизации_UUID"),
        ],
        None,
    ),
    (
        "АктОбОказанииПроизводственныхУслуг",
        [
            ("Док.ДоговорКонтрагента", "ДоговорКонтрагента_UUID"),
            ("Док.Контрагент", "Контрагент_UUID"),
            ("Док.БанковскийСчетОрганизации", "БанковскийСчетОрганизации_UUID"),
        ],
        None,
    ),
    # Требование-накладная — только ТЧ
    (
        "ТребованиеНакладная",
        [
            ("ТЧ.СтатьяЗатрат", "СтатьяЗатрат_UUID"),
            ("ТЧ.Номенклатура", "Номенклатура_UUID"),
            ("ТЧ.ХарактеристикаНоменклатуры", "ХарактеристикаНоменклатуры_UUID"),
            ("ТЧ.СерияНоменклатуры", "СерияНоменклатуры_UUID"),
        ],
        None,
    ),
    # Передача материалов в эксплуатацию — ТЧ Материалы (НазначениеИспользования → ppe_usage_purposes)
    (
        "ПередачаМатериаловВЭксплуатацию",
        [
            ("ТЧ.НазначениеИспользования", "НазначениеИспользования_UUID"),
            ("ТЧ.Номенклатура", "Номенклатура_UUID"),
            ("ТЧ.ХарактеристикаНоменклатуры", "ХарактеристикаНоменклатуры_UUID"),
            ("ТЧ.СерияНоменклатуры", "СерияНоменклатуры_UUID"),
        ],
        None,
    ),
]

# Документы с табличной частью (поля из temp/ссылки)
TABULAR_QUERIES: List[Tuple[str, str, List[Tuple[str, str]]]] = [
    # Заявка: ДоговорКонтрагента в ТЧ РасшифровкаПлатежа (Сделка вынесена в DEAL_FIELD_CONFIGS)
    (
        "ЗаявкаНаРасходованиеСредств",
        "РасшифровкаПлатежа",
        [
            ("ТЧ.ДоговорКонтрагента", "ДоговорКонтрагента_UUID"),
        ],
    ),
    # Поступление.Товары: ЗаказПоставщику, Номенклатура, Характеристика, Серия
    (
        "ПоступлениеТоваровУслуг",
        "Товары",
        [
            ("ТЧ.ЗаказПоставщику", "ЗаказПоставщику_UUID"),
            ("ТЧ.Номенклатура", "Номенклатура_UUID"),
            ("ТЧ.ХарактеристикаНоменклатуры", "ХарактеристикаНоменклатуры_UUID"),
            ("ТЧ.СерияНоменклатуры", "СерияНоменклатуры_UUID"),
        ],
    ),
    # Поступление.Услуги: НоменклатурнаяГруппа, СтатьяЗатрат
    (
        "ПоступлениеТоваровУслуг",
        "Услуги",
        [
            ("ТЧ.Номенклатура", "Номенклатура_UUID"),
            ("ТЧ.НоменклатурнаяГруппа", "НоменклатурнаяГруппа_UUID"),
            ("ТЧ.СтатьяЗатрат", "СтатьяЗатрат_UUID"),
        ],
    ),
    # Реализация.Товары, Услуги, ВозвратнаяТара — ЗаказПокупателя (customer_orders)
    (
        "РеализацияТоваровУслуг",
        "Товары",
        [
            ("ТЧ.ЗаказПокупателя", "ЗаказПокупателя_UUID"),
            ("ТЧ.Номенклатура", "Номенклатура_UUID"),
            ("ТЧ.ХарактеристикаНоменклатуры", "ХарактеристикаНоменклатуры_UUID"),
            ("ТЧ.СерияНоменклатуры", "СерияНоменклатуры_UUID"),
        ],
    ),
    (
        "РеализацияТоваровУслуг",
        "Услуги",
        [
            ("ТЧ.ЗаказПокупателя", "ЗаказПокупателя_UUID"),
        ],
    ),
    (
        "РеализацияТоваровУслуг",
        "ВозвратнаяТара",
        [
            ("ТЧ.ЗаказПокупателя", "ЗаказПокупателя_UUID"),
        ],
    ),
    # Акт.Услуги (СтатьяЗатрат в Услуги нет по temp/ссылки)
    (
        "АктОбОказанииПроизводственныхУслуг",
        "Услуги",
        [
            ("ТЧ.Номенклатура", "Номенклатура_UUID"),
            ("ТЧ.НоменклатурнаяГруппа", "НоменклатурнаяГруппа_UUID"),
        ],
    ),
]

# Поля «Сделка» — маршрутизация по ТИПЗНАЧЕНИЯ: ЗаказПокупателя → customer_orders, ЗаказПоставщику → supplier_orders
# (doc_name, field_expr, tabular_name=None для шапки, extra_condition)
DEAL_FIELD_CONFIGS: List[Tuple[str, str, Optional[str], Optional[str]]] = [
    ("ПоступлениеТоваровУслуг", "Док.Сделка", None, None),
    ("РеализацияТоваровУслуг", "Док.Сделка", None, None),
    ("АктОбОказанииПроизводственныхУслуг", "Док.Сделка", None, None),
    ("ЗаявкаНаРасходованиеСредств", "ТЧ.Сделка", "РасшифровкаПлатежа", None),
]


def _build_header_query_single_field(
    doc_name: str,
    field: str,
    col_alias: str,
    extra_condition: Optional[str],
    date_from: str,
    date_to: str,
) -> str:
    """Строит запрос по шапке документа для ОДНОГО поля. ПРЕДСТАВЛЕНИЕ нужен для строкового UUID (без него 1С возвращает COM-объект)."""
    alias = doc_name
    field_expr = field.replace("Док.", alias + ".")
    where_parts = [
        f"{alias}.Дата МЕЖДУ ДАТАВРЕМЯ({date_from}) И ДАТАВРЕМЯ({date_to})",
        f"{alias}.Проведен = ИСТИНА",
    ]
    if extra_condition:
        where_parts.append(extra_condition.strip().lstrip("И ").replace("Док.", alias + "."))
    return f"""ВЫБРАТЬ
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР({field_expr})) КАК {col_alias}
ИЗ
    Документ.{doc_name} КАК {alias}
ГДЕ
    {" И ".join(where_parts)}
"""


def _build_tabular_query_single_field(
    doc_name: str,
    tabular_name: str,
    field: str,
    col_alias: str,
    date_from: str,
    date_to: str,
) -> str:
    """Строит запрос по табличной части для ОДНОГО поля. Без РАЗЛИЧНЫЕ — ПРЕДСТАВЛЕНИЕ(строка) не допускается в РАЗЛИЧНЫЕ; уникализация в Python."""
    return f"""ВЫБРАТЬ
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР({field})) КАК {col_alias}
ИЗ
    Документ.{doc_name}.{tabular_name} КАК ТЧ
        ВНУТРЕННЕЕ СОЕДИНЕНИЕ Документ.{doc_name} КАК Док
        ПО ТЧ.Ссылка = Док.Ссылка
ГДЕ
    Док.Дата МЕЖДУ ДАТАВРЕМЯ({date_from}) И ДАТАВРЕМЯ({date_to})
    И Док.Проведен = ИСТИНА
"""


def _build_doc_tabular_query_single_field(
    doc_name: str,
    tabular_name: str,
    field: str,
    col_alias: str,
    date_from: str,
    date_to: str,
) -> str:
    """Запрос для документов с ТЧ — одно поле. Без РАЗЛИЧНЫЕ (строка не допускается); уникализация в Python."""
    return f"""ВЫБРАТЬ
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР({field})) КАК {col_alias}
ИЗ
    Документ.{doc_name}.{tabular_name} КАК ТЧ
        ВНУТРЕННЕЕ СОЕДИНЕНИЕ Документ.{doc_name} КАК Док
        ПО ТЧ.Ссылка = Док.Ссылка
ГДЕ
    Док.Дата МЕЖДУ ДАТАВРЕМЯ({date_from}) И ДАТАВРЕМЯ({date_to})
    И Док.Проведен = ИСТИНА
"""


def _build_header_query_typed_deal(
    doc_name: str,
    field_expr: str,
    extra_condition: Optional[str],
    date_from: str,
    date_to: str,
) -> str:
    """Запрос по шапке: UUID и ТИПЗНАЧЕНИЯ для маршрутизации Сделки."""
    alias = doc_name
    field = field_expr.replace("Док.", alias + ".")
    where_parts = [
        f"{alias}.Дата МЕЖДУ ДАТАВРЕМЯ({date_from}) И ДАТАВРЕМЯ({date_to})",
        f"{alias}.Проведен = ИСТИНА",
    ]
    if extra_condition:
        where_parts.append(extra_condition.strip().lstrip("И ").replace("Док.", alias + "."))
    return f"""ВЫБРАТЬ
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР({field})) КАК Сделка_UUID,
    ТИПЗНАЧЕНИЯ({field}) КАК Сделка_Тип
ИЗ
    Документ.{doc_name} КАК {alias}
ГДЕ
    {" И ".join(where_parts)}
"""


def _build_tabular_query_typed_deal(
    doc_name: str,
    tabular_name: str,
    field_expr: str,
    date_from: str,
    date_to: str,
) -> str:
    """Запрос по ТЧ: UUID и ТИПЗНАЧЕНИЯ для маршрутизации Сделки."""
    return f"""ВЫБРАТЬ
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР({field_expr})) КАК Сделка_UUID,
    ТИПЗНАЧЕНИЯ({field_expr}) КАК Сделка_Тип
ИЗ
    Документ.{doc_name}.{tabular_name} КАК ТЧ
        ВНУТРЕННЕЕ СОЕДИНЕНИЕ Документ.{doc_name} КАК Док
        ПО ТЧ.Ссылка = Док.Ссылка
ГДЕ
    Док.Дата МЕЖДУ ДАТАВРЕМЯ({date_from}) И ДАТАВРЕМЯ({date_to})
    И Док.Проведен = ИСТИНА
"""


def _extract_deal_uuids_from_rows(rows: List[Dict[str, Any]]) -> Dict[str, set]:
    """Извлекает UUID сделок и направляет в customer_orders или supplier_orders по типу."""
    result: Dict[str, set] = defaultdict(set)
    for row in rows:
        uuid_val = row.get("Сделка_UUID")
        type_val = row.get("Сделка_Тип") or ""
        if not uuid_val or not isinstance(uuid_val, str):
            continue
        u = uuid_val.strip().lower()
        if not u or u == "00000000-0000-0000-0000-000000000000":
            continue
        type_str = str(type_val).strip()
        if "ЗаказПокупателя" in type_str:
            result["customer_orders"].add(u)
        elif "ЗаказПоставщику" in type_str:
            result["supplier_orders"].add(u)
    return dict(result)


def _parse_date_param(d: str) -> str:
    """Преобразует '2026-01-01' в '2026,1,1' для 1С (без пробелов, как в рабочих запросах)."""
    dt = datetime.strptime(d, "%Y-%m-%d")
    return f"{dt.year},{dt.month},{dt.day}"


def _parse_datetime_param(d: str, end_of_day: bool = False) -> str:
    """Преобразует '2026-03-13' в '2026,3,13' или '2026,3,13,23,59,59'."""
    dt = datetime.strptime(d, "%Y-%m-%d")
    if end_of_day:
        return f"{dt.year},{dt.month},{dt.day},23,59,59"
    return f"{dt.year},{dt.month},{dt.day}"


def _extract_uuids_from_rows(
    rows: List[Dict[str, Any]],
    field_aliases: List[str],
) -> Dict[str, set]:
    """Извлекает UUID из строк и группирует по каталогам."""
    result: Dict[str, set] = defaultdict(set)
    for row in rows:
        for alias in field_aliases:
            val = row.get(alias)
            if val is not None and val != "":
                u = (str(val) if not isinstance(val, str) else val).strip().lower()
                if u and u != "00000000-0000-0000-0000-000000000000":
                    catalog = FIELD_TO_CATALOG.get(alias)
                    if catalog:
                        result[catalog].add(u)
    return dict(result)


def _run_query(
    com_object,
    query_text: str,
    columns: List[str],
) -> List[Dict[str, Any]]:
    """Выполняет запрос и возвращает строки."""
    try:
        rows = execute_query(com_object, query_text, columns, params=None)
        return rows or []
    except Exception as e:
        verbose_print(f"  Ошибка запроса: {e}")
        return []


def collect_uuids(
    com_object,
    date_from: str,
    date_to: str,
) -> Dict[str, set]:
    """Собирает UUID из всех документов УПП за период."""
    date_from_1c = _parse_date_param(date_from)
    date_to_1c = _parse_datetime_param(date_to, end_of_day=True)
    all_uuids: Dict[str, set] = defaultdict(set)

    for doc_name, fields, extra_condition in DOCUMENT_CONFIGS:
        if doc_name in ("ТребованиеНакладная", "ПередачаМатериаловВЭксплуатацию"):
            tabular = "Материалы"  # оба документа используют ТЧ Материалы (temp/ссылки)
            for field, col_alias in fields:
                query = _build_doc_tabular_query_single_field(
                    doc_name, tabular, field, col_alias, date_from_1c, date_to_1c
                )
                verbose_print(f"  Запрос: {doc_name}.{col_alias}...")
                try:
                    rows = _run_query(com_object, query, [col_alias])
                    if rows:
                        extracted = _extract_uuids_from_rows(rows, [col_alias])
                        for cat, uuids in extracted.items():
                            all_uuids[cat].update(uuids)
                        verbose_print(f"    Строк: {len(rows)}")
                except Exception as e:
                    verbose_print(f"    Ошибка: {e}")
        else:
            for field, col_alias in fields:
                query = _build_header_query_single_field(
                    doc_name, field, col_alias, extra_condition, date_from_1c, date_to_1c
                )
                verbose_print(f"  Запрос: {doc_name}.{col_alias}...")
                try:
                    rows = _run_query(com_object, query, [col_alias])
                    if rows:
                        extracted = _extract_uuids_from_rows(rows, [col_alias])
                        for cat, uuids in extracted.items():
                            all_uuids[cat].update(uuids)
                        verbose_print(f"    Строк: {len(rows)}")
                except Exception as e:
                    verbose_print(f"    Ошибка: {e}")

    for doc_name, tabular_name, fields in TABULAR_QUERIES:
        for field, col_alias in fields:
            query = _build_tabular_query_single_field(
                doc_name, tabular_name, field, col_alias, date_from_1c, date_to_1c
            )
            verbose_print(f"  Запрос ТЧ: {doc_name}.{tabular_name}.{col_alias}...")
            try:
                rows = _run_query(com_object, query, [col_alias])
                if rows:
                    extracted = _extract_uuids_from_rows(rows, [col_alias])
                    for cat, uuids in extracted.items():
                        all_uuids[cat].update(uuids)
                    verbose_print(f"    Строк: {len(rows)}")
            except Exception as e:
                verbose_print(f"    Ошибка: {e}")

    # Сделка — маршрутизация по ТИПЗНАЧЕНИЯ (ЗаказПокупателя → customer_orders, ЗаказПоставщику → supplier_orders)
    for doc_name, field_expr, tabular_name, extra_condition in DEAL_FIELD_CONFIGS:
        cols = ["Сделка_UUID", "Сделка_Тип"]
        if tabular_name:
            query = _build_tabular_query_typed_deal(
                doc_name, tabular_name, field_expr, date_from_1c, date_to_1c
            )
            verbose_print(f"  Запрос Сделка (ТЧ): {doc_name}.{tabular_name}...")
        else:
            query = _build_header_query_typed_deal(
                doc_name, field_expr, extra_condition, date_from_1c, date_to_1c
            )
            verbose_print(f"  Запрос Сделка (шапка): {doc_name}...")
        try:
            rows = _run_query(com_object, query, cols)
            if rows:
                extracted = _extract_deal_uuids_from_rows(rows)
                for cat, uuids in extracted.items():
                    all_uuids[cat].update(uuids)
                verbose_print(f"    Строк: {len(rows)}")
        except Exception as e:
            verbose_print(f"    Ошибка: {e}")

    return dict(all_uuids)


def _load_filled_uuids_from_reference_objects_prod(base_dir: Optional[str] = None) -> Set[str]:
    """Загружает UUID из reference_objects_prod с filled=1 (уже экспортированы)."""
    set_prod_mode(True)
    refs_path = get_reference_objects_db_path(base_dir)
    if not os.path.exists(refs_path):
        verbose_print(f"  reference_objects_prod не найден: {refs_path}, исключение не применяется")
        return set()
    conn = sqlite3.connect(refs_path)
    cursor = conn.cursor()
    cursor.execute("SELECT ref_uuid FROM reference_objects WHERE filled = 1")
    uuids = {
        (row[0] or "").strip().lower()
        for row in cursor.fetchall()
        if row[0] and (row[0] or "").strip() != "00000000-0000-0000-0000-000000000000"
    }
    conn.close()
    return uuids


def _exclude_filled_uuids(
    uuids_by_catalog: Dict[str, set],
    filled_uuids: Set[str],
) -> Dict[str, set]:
    """Исключает UUID, которые уже есть в reference_objects_prod с filled=1."""
    if not filled_uuids:
        return uuids_by_catalog
    result: Dict[str, set] = {}
    for catalog, uuids in uuids_by_catalog.items():
        filtered = {u for u in uuids if u.lower() not in filled_uuids}
        result[catalog] = filtered
    return result


def write_uuids_to_files(
    uuids_by_catalog: Dict[str, set],
    output_dir: str,
) -> None:
    """Записывает UUID в файлы по каталогам."""
    os.makedirs(output_dir, exist_ok=True)
    for catalog, uuids in sorted(uuids_by_catalog.items()):
        if not uuids:
            continue
        path = os.path.join(output_dir, f"{catalog}_uuids.txt")
        with open(path, "w", encoding="utf-8") as f:
            for u in sorted(uuids):
                f.write(u + "\n")
        verbose_print(f"  Записано {len(uuids)} UUID в {path}")


def _resolve_output_path(output_dir: str) -> str:
    if os.path.isabs(output_dir):
        return output_dir
    return os.path.join(_ROOT_DIR, output_dir)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Сбор UUID документов УПП для экспорта справочников по кодам/UUID"
    )
    parser.add_argument(
        "--output-dir",
        default="BD/upp_export",
        help="Директория для файлов UUID (относительно корня проекта, если не абсолютный путь)",
    )
    parser.add_argument(
        "--date-from",
        default="2026-01-01",
        help="Начало периода (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--date-to",
        default="2026-03-13",
        help="Конец периода (YYYY-MM-DD)",
    )
    parser.add_argument("--verbose", action="store_true", help="Подробный вывод")
    parser.add_argument(
        "--no-exclude-filled",
        action="store_true",
        help="Не исключать UUID из reference_objects_prod с filled=1",
    )
    args = parser.parse_args()

    if args.verbose:
        set_verbose(True)

    output_dir = _resolve_output_path(args.output_dir)

    verbose_print("=" * 80)
    verbose_print("СБОР UUID ДОКУМЕНТОВ УПП")
    verbose_print("=" * 80)
    verbose_print(f"Период: {args.date_from} — {args.date_to}")
    verbose_print(f"Выходная директория: {output_dir}")

    com_object = connect_to_1c("source")
    if not com_object:
        print("Ошибка: не удалось подключиться к UPP (source)")
        return 1

    verbose_print("\n[1/3] Сбор UUID из документов...")
    uuids_by_catalog = collect_uuids(com_object, args.date_from, args.date_to)

    if not args.no_exclude_filled:
        verbose_print("\n[2/3] Исключение UUID из reference_objects_prod (filled=1)...")
        bd_dir = os.path.join(_ROOT_DIR, "BD")
        filled_uuids = _load_filled_uuids_from_reference_objects_prod(bd_dir)
        if filled_uuids:
            before_total = sum(len(u) for u in uuids_by_catalog.values())
            uuids_by_catalog = _exclude_filled_uuids(uuids_by_catalog, filled_uuids)
            after_total = sum(len(u) for u in uuids_by_catalog.values())
            excluded = before_total - after_total
            verbose_print(f"  Исключено {excluded} UUID (всего filled в prod: {len(filled_uuids)})")
            if excluded > 0:
                print(f"Исключено {excluded} UUID (уже в reference_objects_prod с filled=1)")
        else:
            verbose_print("  Нет заполненных объектов в prod, исключение не применяется")
    else:
        verbose_print("\n[2/3] Пропуск исключения (--no-exclude-filled)")

    verbose_print("\n[3/3] Запись в файлы...")
    write_uuids_to_files(uuids_by_catalog, output_dir)

    total = sum(len(u) for u in uuids_by_catalog.values())
    verbose_print("\n" + "=" * 80)
    verbose_print("СТАТИСТИКА")
    verbose_print("=" * 80)
    for catalog in sorted(uuids_by_catalog.keys()):
        count = len(uuids_by_catalog[catalog])
        verbose_print(f"  {catalog}: {count} UUID")
    verbose_print(f"  Всего: {total} UUID")
    verbose_print("\nДля экспорта используйте:")
    verbose_print("  python BD/upp_export/run_upp_exports.py")

    return 0


if __name__ == "__main__":
    sys.exit(main())
