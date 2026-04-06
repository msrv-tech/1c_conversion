# -*- coding: utf-8 -*-
"""
Сверка остатков МЦ: УПП (РН «Партии материалов в эксплуатации БУ») vs УХ приёмник (БУ МЦ.02, МЦ.04).

Запуск (из корня проекта C:\\1c):
    pip install python-dotenv  # если ещё не стоит
    # тестовый приёмник (по умолчанию target из .env):
    python tools/reconcile_parties_mc_uh.py --output BD/parties_mc_mc_reconcile.csv --as-of 2026-01-01
    # prod приёмник:
    python tools/reconcile_parties_mc_uh.py --prod --output BD/parties_mc_mc_reconcile.csv --as-of 2026-01-01
    # открытие в Excel (RU) без искажения сумм:
    python tools/reconcile_parties_mc_uh.py -o BD/parties_mc_mc_reconcile.csv --as-of 2026-01-01 --csv-excel-ru
    # УХ — только строки документов ввода остатков (как после выгрузки writer):
    # python tools/reconcile_parties_mc_uh.py -o BD/by_docs.csv --as-of 2026-01-01 --uh-from-doc-numbers "00УХ-000119,00УХ-000120"

Требования:
    - .env: SOURCE_CONNECTION_STRING, TARGET_CONNECTION_STRING (для УПП и тестового УХ)
    - при --prod: TARGET_CONNECTION_STRING_PROD (без фолбэка)

Промежуточная БД (--sqlite-db): агрегаты УПП/УХ и строки сверки для повторного анализа.

Сопоставление УПП↔УХ по умолчанию: счёт МЦ + UUID (УПП: серия → характеристика → номенклатура; УХ: UUID Субконто1).
Запрос УПП совпадает с загрузчиком: свёртка ВТ_Сгруппировано (агрегат по измерениям без характеристики/серии).
Флаг --agg-by-nom-code-only: свёртка только по коду номенклатуры (старое поведение).

Сумма УПП: СтоимостьОстаток (как в импорте parties_mc_balances_loader), без погашенной стоимости.
Запрос УПП: все строки остатков по счетам 10.11/10.09 (как СУММА в виртуальной таблице Остатки), без отбора КоличествоОстаток <> 0.

УПП и УХ: количество и сумма — суммы по всем строкам, попавшим в ключ сверки (без порога по стоимости/сумме).
Числа из 1С нормализуются (запятая, пробелы). См. УПП_СтрокРегистра / УХ_СтрокИсточника.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, DefaultDict, Dict, List, Tuple

# Корень проекта
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

try:
    from dotenv import load_dotenv
except ImportError:
    print("Ошибка: установите python-dotenv: pip install python-dotenv")
    sys.exit(1)

load_dotenv(os.path.join(_ROOT, ".env"))
load_dotenv()

from IN.parties_mc_balances_loader import (  # noqa: E402
    ACCOUNT_TO_TARGET,
    COLUMNS as UPP_COLUMNS,
    QUERY_TEXT as UPP_QUERY_TEXT,
    REFERENCE_COLUMNS as UPP_REFERENCE_COLUMNS,
)
from tools.db_manager import process_reference_fields  # noqa: E402
from tools.logger import set_verbose, verbose_print  # noqa: E402
from tools.onec_connector import connect_to_1c, execute_batch_query  # noqa: E402
from tools.writer_utils import parse_reference_field  # noqa: E402
# В запрос УПП вставляется Номенклатура.Код (см. _build_upp_query)
_UPP_QUERY_INSERT_MARK = (
    "    ТИПЗНАЧЕНИЯ(ВТ_Сгруппировано.Номенклатура) КАК Номенклатура_Тип,\n\n"
    "    ВТ_Сгруппировано.ХарактеристикаНоменклатуры КАК ХарактеристикаНоменклатуры,"
)
_UPP_QUERY_INSERT_REPL = (
    "    ТИПЗНАЧЕНИЯ(ВТ_Сгруппировано.Номенклатура) КАК Номенклатура_Тип,\n"
    "    ВТ_Сгруппировано.Номенклатура.Код КАК НоменклатураКод,\n\n"
    "    ВТ_Сгруппировано.ХарактеристикаНоменклатуры КАК ХарактеристикаНоменклатуры,"
)

# Пустой код в ключе группировки (без фолбэка на наименование)
_PLACEHOLDER_NO_NOM_CODE = "__БЕЗ_КОДА__"
# Нет UUID для ключа сопоставления (ни серия/хар/ном не дали uuid)
_PLACEHOLDER_NO_MATCH_UUID = "__БЕЗ_UUID__"
# Как в PROCESS/parties_mc_balances_processor: пустая ссылка в 1С часто приходит нулевым UUID
_ZERO_UUID = "00000000-0000-0000-0000-000000000000"

# Колонки УПП как в загрузчике + НоменклатураКод после Номенклатура_Тип
_ix_nom_type = UPP_COLUMNS.index("Номенклатура_Тип")
UPP_COLUMNS_RECONCILE = (
    UPP_COLUMNS[: _ix_nom_type + 1]
    + ["НоменклатураКод"]
    + UPP_COLUMNS[_ix_nom_type + 1 :]
)

# Запрос УХ: остатки БУ по МЦ.02 / МЦ.04, агрегация по организации, счёту, субконто1 (номенклатура)
UH_QUERY_TEMPLATE = """
ВЫБРАТЬ
    ХозрасчетныйОстатки.Организация КАК Организация,
    ХозрасчетныйОстатки.Счет КАК Счет,
    ХозрасчетныйОстатки.Субконто1 КАК Субконто1,
    СУММА(ХозрасчетныйОстатки.КоличествоОстаток) КАК КоличествоОстаток,
    СУММА(ХозрасчетныйОстатки.СуммаОстаток) КАК СуммаОстаток
ПОМЕСТИТЬ ВТ_Остатки
ИЗ
    РегистрБухгалтерии.Хозрасчетный.Остатки(ДАТАВРЕМЯ(__Y__, __M__, __D__), Счет В (&Счета), , ) КАК ХозрасчетныйОстатки
СГРУППИРОВАТЬ ПО
    ХозрасчетныйОстатки.Организация,
    ХозрасчетныйОстатки.Счет,
    ХозрасчетныйОстатки.Субконто1
;

////////////////////////////////////////////////////////////////////////////////
ВЫБРАТЬ
    ПРЕДСТАВЛЕНИЕ(ВТ_Остатки.Организация) КАК Организация_Представление,
    ВТ_Остатки.Счет.Код КАК СчетКод,
    ПРЕДСТАВЛЕНИЕ(ВТ_Остатки.Счет) КАК Счет_Представление,
    ПРЕДСТАВЛЕНИЕ(ВТ_Остатки.Субконто1) КАК Номенклатура_Представление,
    ВТ_Остатки.Субконто1.Код КАК НоменклатураКод,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ВТ_Остатки.Субконто1)) КАК Субконто1_UUID,
    ВТ_Остатки.КоличествоОстаток КАК КоличествоОстаток,
    ВТ_Остатки.СуммаОстаток КАК СуммаОстаток
ИЗ
    ВТ_Остатки КАК ВТ_Остатки
"""

UH_COLUMNS = [
    "Организация_Представление",
    "СчетКод",
    "Счет_Представление",
    "Номенклатура_Представление",
    "НоменклатураКод",
    "Субконто1_UUID",
    "КоличествоОстаток",
    "СуммаОстаток",
]

MC_ACCOUNTS_UH = ("МЦ.02", "МЦ.04")

# Имя типа документа для --uh-from-doc-numbers (защита от подстановки в текст запроса)
_UH_DOC_TYPE_RE = re.compile(r"^[A-Za-zА-Яа-яЁё0-9_]+$")


def _parse_as_of(s: str) -> Tuple[int, int, int]:
    try:
        dt = datetime.strptime(s.strip(), "%Y-%m-%d")
        return dt.year, dt.month, dt.day
    except ValueError as e:
        raise SystemExit(f"Ошибка: неверный формат --as-of «{s}», ожидается ГГГГ-ММ-ДД") from e


def _build_upp_query(y: int, m: int, d: int) -> str:
    q = UPP_QUERY_TEXT.replace(
        "ДАТАВРЕМЯ(2026, 1, 1)", f"ДАТАВРЕМЯ({y}, {m}, {d})"
    )
    if _UPP_QUERY_INSERT_MARK not in q:
        print(
            "Ошибка: шаблон запроса УПП не содержит маркер для вставки НоменклатураКод "
            "(обновите IN/parties_mc_balances_loader.py или маркер в reconcile_parties_mc_uh)."
        )
        sys.exit(1)
    return q.replace(_UPP_QUERY_INSERT_MARK, _UPP_QUERY_INSERT_REPL)


def _build_uh_query(y: int, m: int, d: int) -> str:
    return (
        UH_QUERY_TEMPLATE.replace("__Y__", str(y))
        .replace("__M__", str(m))
        .replace("__D__", str(d))
    )


def _normalize_nomenclature_code(raw: Any) -> str:
    """Ключ группировки: код номенклатуры; пустой код — отдельный bucket."""
    if raw is None:
        return _PLACEHOLDER_NO_NOM_CODE
    s = str(raw).strip()
    return s if s else _PLACEHOLDER_NO_NOM_CODE


def _display_nomenclature_code(code_key: str) -> str:
    return "" if code_key == _PLACEHOLDER_NO_NOM_CODE else code_key


def _display_match_uuid(uuid_key: str) -> str:
    return "" if uuid_key == _PLACEHOLDER_NO_MATCH_UUID else uuid_key


def _canonical_uuid_string(s: str) -> str:
    """Единый вид UUID для сравнения УПП/УХ (регистр, без фигурных скобок)."""
    t = (s or "").strip().lower()
    if len(t) >= 2 and t[0] == "{" and t[-1] == "}":
        t = t[1:-1].strip()
    return t


def _normalize_uuid_key(raw: Any) -> str:
    """Нормализация UUID из 1С для ключа словаря."""
    if raw is None:
        return _PLACEHOLDER_NO_MATCH_UUID
    s = _canonical_uuid_string(str(raw))
    if not s:
        return _PLACEHOLDER_NO_MATCH_UUID
    return s


def _parse_1c_number(val: Any) -> float:
    """Число из 1С/COM: int/float или строка с запятой/пробелами (как в представлении)."""
    if val is None or val == "":
        return 0.0
    if isinstance(val, bool):
        return float(val)
    if isinstance(val, int):
        return float(val)
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return 0.0
        return float(val)
    s = str(val).strip().replace("\u00a0", "").replace(" ", "")
    if not s or s.lower() in ("none", "nan"):
        return 0.0
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _to_float(val: Any) -> float:
    """Алиас для совместимости (диагностика и т.п.)."""
    return _parse_1c_number(val)


def _presentation_from_ref_cell(value: Any) -> str:
    """Представление из поля-ссылки после process_reference_fields (JSON) или обычная строка."""
    if value is None or value == "":
        return ""
    if isinstance(value, str):
        info = parse_reference_field(value)
        if info and (info.get("presentation") or "").strip():
            return str(info["presentation"]).strip()
        if not value.lstrip().startswith("{"):
            return value.strip()
        return ""
    return str(value).strip()


def _uuid_from_ref_cell(value: Any) -> str:
    """UUID из поля-ссылки после process_reference_fields (JSON)."""
    if value is None or value == "":
        return ""
    if isinstance(value, str):
        info = parse_reference_field(value)
        if info:
            u = _canonical_uuid_string(str(info.get("uuid") or ""))
            # Нулевой UUID — не заполненная серия/характеристика; иначе ломается цепочка серия→хар→ном
            if u and u != _canonical_uuid_string(_ZERO_UUID):
                return u
        return ""
    return ""


def _upp_match_uuid(row: dict) -> str:
    """
    UUID для сопоставления с УХ (Субконто1): серия → характеристика → номенклатура.
    """
    su = _uuid_from_ref_cell(row.get("СерияНоменклатуры"))
    if su:
        return su
    cu = _uuid_from_ref_cell(row.get("ХарактеристикаНоменклатуры"))
    if cu:
        return cu
    nu = _uuid_from_ref_cell(row.get("Номенклатура"))
    if nu:
        return nu
    return _PLACEHOLDER_NO_MATCH_UUID


def _round_money(x: float) -> float:
    """Округление суммы для отчёта (без хвостов float в CSV)."""
    return round(float(x), 2)


def _round_qty_out(x: float) -> float:
    """Округление количества для отчёта."""
    return round(float(x), 6)


def _format_cell_ru_excel(val: Any, *, money: bool) -> str:
    """Строка для Excel (RU): десятичная запятая (поле не должно содержать «,» как разделитель CSV)."""
    if val is None or val == "":
        return ""
    x = float(val)
    if money:
        s = f"{_round_money(x):.2f}"
    else:
        xq = _round_qty_out(x)
        if abs(xq - round(xq)) < 1e-9:
            s = str(int(round(xq)))
        else:
            s = f"{xq:.6f}".rstrip("0").rstrip(".")
    return s.replace(".", ",")


def _fetch_upp_rows(com_object, y: int, m: int, d: int) -> List[dict]:
    accounts_list = com_object.NewObject("Массив")
    for code in ACCOUNT_TO_TARGET:
        account_ref = com_object.ПланыСчетов.Хозрасчетный.НайтиПоКоду(code)
        if account_ref.Пустая():
            print(f"Ошибка: счёт источника {code} не найден в плане счетов УПП.")
            sys.exit(1)
        accounts_list.Add(account_ref)

    query_text = _build_upp_query(y, m, d)
    params = {"Счета": accounts_list}
    # При reference_columns execute_batch_query возвращает (rows, reference_rows), иначе — только rows
    raw = execute_batch_query(
        com_object,
        query_text,
        UPP_COLUMNS_RECONCILE,
        params=params,
        reference_columns=UPP_REFERENCE_COLUMNS,
    )
    if isinstance(raw, tuple):
        rows, _ref_rows = raw
    else:
        rows = raw
    if not rows:
        verbose_print("УПП: строк остатков не получено (пустой результат запроса).")
        return []
    rows = process_reference_fields(rows, UPP_REFERENCE_COLUMNS)
    return rows


def _new_upp_bucket() -> Dict[str, Any]:
    return {
        "qty": 0.0,
        "amt": 0.0,
        "upp_lines": 0,
        "upp_nom": "",
        "upp_char": "",
        "upp_series": "",
        "nom_code": _PLACEHOLDER_NO_NOM_CODE,
    }


def _aggregate_upp(
    rows: List[dict],
    *,
    by_nom_code_only: bool,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Свертка УПП: по умолчанию ключ (TargetAccountCode, match_uuid);
    при by_nom_code_only — ключ (TargetAccountCode, код_номенклатуры).

    Количество и стоимость суммируются по всем строкам ключа.
    """
    acc: DefaultDict[Tuple[str, str], Dict[str, Any]] = defaultdict(_new_upp_bucket)
    for row in rows:
        acc_code = str(row.get("СчетУчетаКод") or "").strip()
        if acc_code not in ACCOUNT_TO_TARGET:
            print(
                f"Ошибка: код счёта УПП «{acc_code}» не входит в маппинг МЦ "
                f"(ожидаются: {', '.join(sorted(ACCOUNT_TO_TARGET.keys()))})."
            )
            sys.exit(1)
        target_mc = ACCOUNT_TO_TARGET[acc_code]
        nom_k = _normalize_nomenclature_code(row.get("НоменклатураКод"))

        if by_nom_code_only:
            key: Tuple[str, str] = (target_mc, nom_k)
        else:
            mu = _upp_match_uuid(row)
            key = (target_mc, mu)

        qty = _parse_1c_number(row.get("КоличествоОстаток"))
        amt = _parse_1c_number(row.get("СтоимостьОстаток"))

        b = acc[key]
        b["qty"] += qty
        b["amt"] += amt
        b["upp_lines"] += 1
        b["nom_code"] = nom_k

        nom_p = _presentation_from_ref_cell(row.get("Номенклатура"))
        if nom_p and len(nom_p) > len(str(b["upp_nom"] or "")):
            b["upp_nom"] = nom_p
        char_p = _presentation_from_ref_cell(row.get("ХарактеристикаНоменклатуры"))
        if char_p and len(char_p) > len(str(b["upp_char"] or "")):
            b["upp_char"] = char_p
        ser_p = _presentation_from_ref_cell(row.get("СерияНоменклатуры"))
        if ser_p and len(ser_p) > len(str(b["upp_series"] or "")):
            b["upp_series"] = ser_p
    return dict(acc)


def _fetch_uh_rows(com_object, y: int, m: int, d: int) -> List[dict]:
    accounts_list = com_object.NewObject("Массив")
    for code in MC_ACCOUNTS_UH:
        account_ref = com_object.ПланыСчетов.Хозрасчетный.НайтиПоКоду(code)
        if account_ref.Пустая():
            print(f"Ошибка: счёт приёмника {code} не найден в плане счетов УХ.")
            sys.exit(1)
        accounts_list.Add(account_ref)

    query_text = _build_uh_query(y, m, d)
    params = {"Счета": accounts_list}
    return execute_batch_query(
        com_object, query_text, UH_COLUMNS, params=params
    ) or []


def _validate_uh_document_type(name: str) -> str:
    t = (name or "").strip()
    if not t or not _UH_DOC_TYPE_RE.fullmatch(t):
        print(
            "Ошибка: --uh-document-type должно быть непустым именем метаданных документа "
            "(буквы, цифры, подчёркивание), без пробелов и кавычек."
        )
        sys.exit(1)
    return t


def _parse_doc_numbers(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _fetch_uh_rows_from_entry_documents(
    com_object, doc_numbers: List[str], document_type: str
) -> List[dict]:
    """
    Строки УХ из ТЧ БухСправка документов ввода остатков (как выгрузил writer),
    а не срез РегистрБухгалтерии.Хозрасчетный.Остатки по всей базе.
    """
    dt = _validate_uh_document_type(document_type)
    nums = com_object.NewObject("Массив")
    for n in doc_numbers:
        nums.Add(str(n).strip())

    # Синтаксис: Документ.<Имя>.<ТЧ>
    query_text = f"""
ВЫБРАТЬ
    Док.Номер КАК ДокументНомер,
    ПРЕДСТАВЛЕНИЕ(Док.Организация) КАК ОрганизацияДок_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Док.Организация)) КАК ОрганизацияДок_UUID,
    ПРЕДСТАВЛЕНИЕ(Док.Организация) КАК Организация_Представление,
    ТЧ.СчетУчета.Код КАК СчетКод,
    ПРЕДСТАВЛЕНИЕ(ТЧ.СчетУчета) КАК Счет_Представление,
    ПРЕДСТАВЛЕНИЕ(ТЧ.Субконто1) КАК Номенклатура_Представление,
    ТЧ.Субконто1.Код КАК НоменклатураКод,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(ТЧ.Субконто1)) КАК Субконто1_UUID,
    ТЧ.Количество КАК КоличествоОстаток,
    ТЧ.Сумма КАК СуммаОстаток
ИЗ
    Документ.{dt} КАК Док
        ВНУТРЕННЕЕ СОЕДИНЕНИЕ Документ.{dt}.БухСправка КАК ТЧ
        ПО Док.Ссылка = ТЧ.Ссылка
ГДЕ
    Док.Номер В (&НомераДокументов)
    И Док.РазделУчета = ЗНАЧЕНИЕ(Перечисление.РазделыУчетаДляВводаОстатков.ПрочиеСчетаБухгалтерскогоУчета)
"""

    doc_columns = [
        "ДокументНомер",
        "ОрганизацияДок_Представление",
        "ОрганизацияДок_UUID",
        "Организация_Представление",
        "СчетКод",
        "Счет_Представление",
        "Номенклатура_Представление",
        "НоменклатураКод",
        "Субконто1_UUID",
        "КоличествоОстаток",
        "СуммаОстаток",
    ]
    return (
        execute_batch_query(
            com_object,
            query_text,
            doc_columns,
            params={"НомераДокументов": nums},
        )
        or []
    )


def _organization_uuid_from_upp_row(row: dict) -> str:
    return _uuid_from_ref_cell(row.get("Организация"))


def _filter_upp_by_organization_uuids(
    upp_rows: List[dict], org_uuids: set[str]
) -> Tuple[List[dict], List[str]]:
    """Фильтр УПП по UUID организаций из выбранных документов УХ."""
    if not org_uuids:
        return [], []
    out: List[dict] = []
    missing = 0
    for r in upp_rows:
        ou = _organization_uuid_from_upp_row(r)
        if not ou:
            missing += 1
            continue
        if ou in org_uuids:
            out.append(r)
    return out, (
        [f"строк УПП без UUID организации (пропущено): {missing}"] if missing else []
    )


def _organization_uuids_from_uh_doc_rows(uh_doc_rows: List[dict]) -> set[str]:
    s: set[str] = set()
    for r in uh_doc_rows:
        u = _canonical_uuid_string(str(r.get("ОрганизацияДок_UUID") or ""))
        if u:
            s.add(u)
    return s


def _new_uh_bucket() -> Dict[str, Any]:
    return {
        "qty": 0.0,
        "amt": 0.0,
        "uh_lines": 0,
        "presentation": "",
        "nom_code": _PLACEHOLDER_NO_NOM_CODE,
    }


def _aggregate_uh(
    rows: List[dict],
    *,
    by_nom_code_only: bool,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    УХ: по умолчанию ключ (СчетКод, Субконто1_UUID); при by_nom_code_only — (СчетКод, код_номенклатуры).

    Количество и сумма суммируются по всем строкам источника. Отбор по ненулевой сумме не делаем (в отличие от УПП):
    в РБ при одном Субконто1 возможны строки с нулевой суммой и ненулевым количеством; иначе занижается qty.
    """
    acc: DefaultDict[Tuple[str, str], Dict[str, Any]] = defaultdict(_new_uh_bucket)
    for row in rows:
        code = (row.get("СчетКод") or "").strip()
        nom_k = _normalize_nomenclature_code(row.get("НоменклатураКод"))
        pres = (row.get("Номенклатура_Представление") or "").strip()

        if by_nom_code_only:
            key = (code, nom_k)
        else:
            su = _normalize_uuid_key(row.get("Субконто1_UUID"))
            key = (code, su)

        qv = _parse_1c_number(row.get("КоличествоОстаток"))
        av = _parse_1c_number(row.get("СуммаОстаток"))
        b = acc[key]
        b["qty"] += qv
        b["amt"] += av
        b["uh_lines"] += 1
        b["nom_code"] = nom_k
        if pres and len(pres) > len(str(b["presentation"] or "")):
            b["presentation"] = pres
    return dict(acc)


_UPP_AGG_FALLBACK: Dict[str, Any] = {
    "qty": 0.0,
    "amt": 0.0,
    "upp_lines": 0,
    "upp_nom": "",
    "upp_char": "",
    "upp_series": "",
    "nom_code": _PLACEHOLDER_NO_NOM_CODE,
}

_UH_AGG_FALLBACK: Dict[str, Any] = {
    "qty": 0.0,
    "amt": 0.0,
    "uh_lines": 0,
    "presentation": "",
    "nom_code": _PLACEHOLDER_NO_NOM_CODE,
}


def _merge_and_rows(
    upp_agg: Dict[Tuple[str, str], Dict[str, Any]],
    uh_agg: Dict[Tuple[str, str], Dict[str, Any]],
    *,
    by_nom_code_only: bool,
) -> List[Dict[str, Any]]:
    """Full outer join: (СчетМЦ, uuid) или (СчетМЦ, код_номенклатуры) в зависимости от режима."""
    all_keys = set(upp_agg.keys()) | set(uh_agg.keys())
    out: List[Dict[str, Any]] = []

    for key in sorted(all_keys, key=lambda x: (x[0], x[1])):
        mc, second = key
        u = upp_agg.get(key, _UPP_AGG_FALLBACK)
        h = uh_agg.get(key, _UH_AGG_FALLBACK)
        in_u = key in upp_agg
        in_h = key in uh_agg

        if by_nom_code_only:
            nom_internal = second
            dc = _display_nomenclature_code(nom_internal)
            key_uuid_display = ""
        else:
            dc_u = _display_nomenclature_code(u.get("nom_code") or _PLACEHOLDER_NO_NOM_CODE)
            dc_h = _display_nomenclature_code(h.get("nom_code") or _PLACEHOLDER_NO_NOM_CODE)
            dc = dc_u or dc_h
            key_uuid_display = _display_match_uuid(second)

        if in_u and in_h:
            src = "оба"
            d_q = _round_qty_out(float(u["qty"]) - float(h["qty"]))
            d_a = _round_money(float(u["amt"]) - float(h["amt"]))
        elif in_u:
            src = "только_УПП"
            d_q = _round_qty_out(float(u["qty"]))
            d_a = _round_money(float(u["amt"]))
        else:
            src = "только_УХ"
            d_q = _round_qty_out(-float(h["qty"]))
            d_a = _round_money(-float(h["amt"]))

        row_out: Dict[str, Any] = {
            "СчетМЦ": mc,
            "НоменклатураКод": dc,
            "СопоставлениеUUID": key_uuid_display if not by_nom_code_only else "",
            "УПП_Номенклатура": (u.get("upp_nom") or "") if in_u else "",
            "УПП_Характеристика": (u.get("upp_char") or "") if in_u else "",
            "УПП_Серия": (u.get("upp_series") or "") if in_u else "",
            "УПП_Количество": _round_qty_out(u["qty"]) if in_u else "",
            "УПП_Сумма": _round_money(u["amt"]) if in_u else "",
            "УПП_СтрокРегистра": int(u.get("upp_lines") or 0) if in_u else "",
            "УХ_Номенклатура": (h.get("presentation") or "") if in_h else "",
            "УХ_Количество": _round_qty_out(h["qty"]) if in_h else "",
            "УХ_Сумма": _round_money(h["amt"]) if in_h else "",
            "УХ_СтрокИсточника": int(h.get("uh_lines") or 0) if in_h else "",
            "РазницаКоличество": d_q,
            "РазницаСумма": d_a,
            "ИсточникСтроки": src,
        }
        out.append(row_out)

    return out


def _save_intermediate_sqlite(
    db_path: str,
    *,
    as_of: str,
    uh_mode: str,
    by_nom_code_only: bool,
    upp_agg: Dict[Tuple[str, str], Dict[str, Any]],
    uh_agg: Dict[Tuple[str, str], Dict[str, Any]],
    result_rows: List[Dict[str, Any]],
) -> None:
    """Перезаписывает промежуточную БД: метаданные, агрегаты УПП/УХ, результат сверки."""
    abs_db = os.path.abspath(db_path)
    db_dir = os.path.dirname(abs_db)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = sqlite3.connect(abs_db)
    try:
        cur = conn.cursor()
        for tbl in (
            "reconcile_result",
            "uh_agg",
            "upp_agg",
            "reconcile_meta",
        ):
            cur.execute(f"DROP TABLE IF EXISTS {tbl}")

        cur.execute(
            """
            CREATE TABLE reconcile_meta (
                started_at TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                uh_mode TEXT NOT NULL
            )
            """
        )
        cur.execute(
            "INSERT INTO reconcile_meta (started_at, as_of_date, uh_mode) VALUES (?, ?, ?)",
            (started, as_of, uh_mode),
        )

        cur.execute(
            """
            CREATE TABLE upp_agg (
                target_account TEXT,
                match_uuid TEXT,
                nomenclature_code TEXT,
                nomenclature_presentation TEXT,
                characteristic_presentation TEXT,
                series_presentation TEXT,
                line_count INTEGER,
                quantity REAL,
                amount REAL
            )
            """
        )
        for (mc, second), vals in upp_agg.items():
            if by_nom_code_only:
                muid = ""
                nom_disp = _display_nomenclature_code(second)
            else:
                muid = _display_match_uuid(second)
                nom_disp = _display_nomenclature_code(
                    vals.get("nom_code") or _PLACEHOLDER_NO_NOM_CODE
                )
            cur.execute(
                "INSERT INTO upp_agg VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    mc,
                    muid,
                    nom_disp,
                    vals.get("upp_nom") or "",
                    vals.get("upp_char") or "",
                    vals.get("upp_series") or "",
                    int(vals.get("upp_lines") or 0),
                    vals["qty"],
                    vals["amt"],
                ),
            )

        cur.execute(
            """
            CREATE TABLE uh_agg (
                account_code TEXT,
                subkonto_uuid TEXT,
                nomenclature_code TEXT,
                nomenclature_presentation TEXT,
                line_count INTEGER,
                quantity REAL,
                amount REAL
            )
            """
        )
        for (code, second), vals in uh_agg.items():
            if by_nom_code_only:
                suid = ""
                nom_disp = _display_nomenclature_code(second)
            else:
                suid = _display_match_uuid(second)
                nom_disp = _display_nomenclature_code(
                    vals.get("nom_code") or _PLACEHOLDER_NO_NOM_CODE
                )
            cur.execute(
                "INSERT INTO uh_agg VALUES (?,?,?,?,?,?,?)",
                (
                    code,
                    suid,
                    nom_disp,
                    vals.get("presentation") or "",
                    int(vals.get("uh_lines") or 0),
                    vals["qty"],
                    vals["amt"],
                ),
            )

        cur.execute(
            """
            CREATE TABLE reconcile_result (
                account_mc TEXT,
                nomenclature_code TEXT,
                match_uuid TEXT,
                upp_nom TEXT,
                upp_char TEXT,
                upp_series TEXT,
                upp_qty TEXT,
                upp_amt TEXT,
                upp_line_count TEXT,
                uh_nom TEXT,
                uh_qty TEXT,
                uh_amt TEXT,
                uh_line_count TEXT,
                diff_qty TEXT,
                diff_amt TEXT,
                row_source TEXT
            )
            """
        )
        for r in result_rows:
            cur.execute(
                """
                INSERT INTO reconcile_result VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    r.get("СчетМЦ"),
                    r.get("НоменклатураКод"),
                    r.get("СопоставлениеUUID", ""),
                    r.get("УПП_Номенклатура"),
                    r.get("УПП_Характеристика"),
                    r.get("УПП_Серия"),
                    r.get("УПП_Количество"),
                    r.get("УПП_Сумма"),
                    r.get("УПП_СтрокРегистра", ""),
                    r.get("УХ_Номенклатура"),
                    r.get("УХ_Количество"),
                    r.get("УХ_Сумма"),
                    r.get("УХ_СтрокИсточника", ""),
                    r.get("РазницаКоличество"),
                    r.get("РазницаСумма"),
                    r.get("ИсточникСтроки"),
                ),
            )

        conn.commit()
    finally:
        conn.close()
    verbose_print(f"Промежуточная БД: {abs_db}")


def main() -> int:
    default_sqlite = os.path.join(_ROOT, "BD", "parties_mc_reconcile_work.db")
    parser = argparse.ArgumentParser(
        description="Сверка остатков МЦ: УПП (РН партий) vs УХ приёмник (БУ МЦ.02/МЦ.04)"
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Путь к выходному CSV (UTF-8 с BOM)",
    )
    parser.add_argument(
        "--as-of",
        default="2026-01-01",
        help="Дата остатков (ГГГГ-ММ-ДД), по умолчанию 2026-01-01",
    )
    parser.add_argument(
        "--target-1c",
        default="target",
        help='Приёмник УХ: имя из конфигурации (по умолчанию target — тестовый) или строка подключения. С --prod не используется.',
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Сверка с УХ prod (TARGET_CONNECTION_STRING_PROD); игнорирует --target-1c",
    )
    parser.add_argument(
        "--sqlite-db",
        default=default_sqlite,
        help=f"Путь к промежуточной SQLite (агрегаты и результат). По умолчанию: {default_sqlite}",
    )
    parser.add_argument("--verbose", action="store_true", help="Подробный лог")
    parser.add_argument(
        "--csv-excel-ru",
        action="store_true",
        help=(
            "CSV для Excel (Россия): разделитель «;», в числах десятичная запятая. "
            "Иначе при открытии двойным щелчком Excel с русской локалью часто путает точку в суммах с разделителем тысяч."
        ),
    )
    parser.add_argument(
        "--agg-by-nom-code-only",
        action="store_true",
        help=(
            "Свертка только по коду номенклатуры (без UUID): как раньше одна строка на код на счёте. "
            "По умолчанию — детализация по UUID (серия/характеристика/номенклатура УПП ↔ Субконто1 УХ)."
        ),
    )
    parser.add_argument(
        "--uh-from-doc-numbers",
        default="",
        metavar="НОМЕРА",
        help=(
            "Сверять не с остатками РБ по всей базе, а со строками ТЧ «БухСправка» документов "
            "с указанными номерами (через запятую), например: 00УХ-000119,00УХ-000120. "
            "УПП фильтруется по UUID организации этих документов."
        ),
    )
    parser.add_argument(
        "--uh-document-type",
        default="ВводНачальныхОстатков",
        help="Имя документа в метаданных для --uh-from-doc-numbers (как в OUT/parties_mc_balances_writer).",
    )
    args = parser.parse_args()

    set_verbose(args.verbose)

    if args.prod:
        prod_conn = os.getenv("TARGET_CONNECTION_STRING_PROD")
        if not prod_conn or not str(prod_conn).strip():
            print(
                "Ошибка: при --prod необходимо указать TARGET_CONNECTION_STRING_PROD в .env"
            )
            return 1
        uh_spec = prod_conn.strip()
        uh_mode = "prod"
    else:
        uh_spec = (args.target_1c or "target").strip()
        # В БД не пишем строку подключения (пароль)
        uh_mode = (
            "target"
            if uh_spec == "target"
            else "source"
            if uh_spec == "source"
            else "other"
        )

    y, m, d = _parse_as_of(args.as_of)

    com_upp = connect_to_1c("source")
    if not com_upp:
        print("Ошибка: не удалось подключиться к УПП (source).")
        return 1

    com_uh = connect_to_1c(uh_spec)
    if not com_uh:
        label = "УХ prod" if args.prod else f"УХ ({uh_spec!r})"
        print(f"Ошибка: не удалось подключиться к {label}.")
        return 1

    try:
        verbose_print("Загрузка остатков УПП...")
        upp_raw = _fetch_upp_rows(com_upp, y, m, d)

        doc_nums = _parse_doc_numbers(args.uh_from_doc_numbers or "")
        if doc_nums:
            verbose_print(
                f"УХ: строки ТЧ БухСправка документов {doc_nums} "
                f"(метаданные: {args.uh_document_type!r})..."
            )
            uh_raw = _fetch_uh_rows_from_entry_documents(
                com_uh, doc_nums, args.uh_document_type
            )
            if not uh_raw:
                print(
                    "Ошибка: по указанным номерам документов не получено строк ТЧ БухСправка "
                    "(номера, --uh-document-type, проведение документа, права)."
                )
                return 1
            org_uuids = _organization_uuids_from_uh_doc_rows(uh_raw)
            if not org_uuids:
                print(
                    "Ошибка: из документов УХ не удалось прочитать UUID организации "
                    "(колонка ОрганизацияДок_UUID пуста)."
                )
                return 1
            upp_before = len(upp_raw)
            upp_raw, upp_warns = _filter_upp_by_organization_uuids(upp_raw, org_uuids)
            for w in upp_warns:
                print(w)
            if not upp_raw:
                print(
                    "Ошибка: после фильтра УПП по организации документов не осталось строк УПП. "
                    f"UUID организаций из документов: {sorted(org_uuids)}; "
                    f"строк УПП до фильтра: {upp_before}."
                )
                return 1
            verbose_print(
                f"УПП: для сверки оставлены строки организаций документов ({len(org_uuids)} шт.): "
                f"{len(upp_raw)} из {upp_before}"
            )
            uh_mode = f"{uh_mode}|uh_docs={','.join(doc_nums)}"
        else:
            verbose_print("Загрузка остатков УХ (БУ, регистр Хозрасчетный.Остатки)...")
            uh_raw = _fetch_uh_rows(com_uh, y, m, d)

        upp_agg = _aggregate_upp(upp_raw, by_nom_code_only=args.agg_by_nom_code_only)
        if args.agg_by_nom_code_only:
            verbose_print(f"УПП: агрегатов по коду номенклатуры {len(upp_agg)}")
        else:
            verbose_print(f"УПП: агрегатов по ключу (счёт + UUID) {len(upp_agg)}")

        uh_agg = _aggregate_uh(uh_raw, by_nom_code_only=args.agg_by_nom_code_only)
        if args.agg_by_nom_code_only:
            verbose_print(f"УХ: агрегатов по коду номенклатуры {len(uh_agg)}")
        else:
            verbose_print(f"УХ: агрегатов по ключу (счёт + UUID Субконто1) {len(uh_agg)}")

        rows = _merge_and_rows(
            upp_agg, uh_agg, by_nom_code_only=args.agg_by_nom_code_only
        )
    finally:
        try:
            com_upp = None
            com_uh = None
        except Exception:
            pass

    sqlite_path = (args.sqlite_db or "").strip()
    if sqlite_path:
        _save_intermediate_sqlite(
            sqlite_path,
            as_of=args.as_of,
            uh_mode=uh_mode,
            by_nom_code_only=args.agg_by_nom_code_only,
            upp_agg=upp_agg,
            uh_agg=uh_agg,
            result_rows=rows,
        )

    fieldnames = [
        "СчетМЦ",
        "НоменклатураКод",
        "СопоставлениеUUID",
        "УПП_Номенклатура",
        "УПП_Характеристика",
        "УПП_Серия",
        "УПП_Количество",
        "УПП_Сумма",
        "УПП_СтрокРегистра",
        "УХ_Номенклатура",
        "УХ_Количество",
        "УХ_Сумма",
        "УХ_СтрокИсточника",
        "РазницаКоличество",
        "РазницаСумма",
        "ИсточникСтроки",
    ]

    out_path = os.path.abspath(args.output)
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    numeric_cols = (
        "УПП_Количество",
        "УПП_Сумма",
        "УХ_Количество",
        "УХ_Сумма",
        "РазницаКоличество",
        "РазницаСумма",
    )
    money_cols = {"УПП_Сумма", "УХ_Сумма", "РазницаСумма"}

    def _row_for_csv(r: Dict[str, Any]) -> Dict[str, Any]:
        if not args.csv_excel_ru:
            return r
        out = dict(r)
        for k in numeric_cols:
            if k not in out:
                continue
            out[k] = _format_cell_ru_excel(out[k], money=(k in money_cols))
        return out

    delim = ";" if args.csv_excel_ru else ","
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
            delimiter=delim,
        )
        w.writeheader()
        for r in rows:
            w.writerow(_row_for_csv(r))

    msg = f"Записано строк: {len(rows)} → {out_path}"
    if sqlite_path:
        msg += f"; промежуточная БД: {os.path.abspath(sqlite_path)}"
    print(msg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
