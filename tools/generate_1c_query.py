# -*- coding: utf-8 -*-
"""
Утилита для формирования статического текста запросов 1С по справочникам.

Использование:
    python tools/generate_1c_query.py --catalog "НоменклатурныеГруппы"
    python tools/generate_1c_query.py --catalog "НоменклатурныеГруппы" --limit 20

По умолчанию используется конфигурация подключения 'source' из config.py.

Лучше сначала выгрузить конфигурацию и использовать ее для генерации запроса.
"""

import argparse
import os

from tools.encoding_fix import fix_encoding

fix_encoding()
import sys
from typing import Dict, Iterable, List, Tuple

# Добавляем корень проекта в sys.path, чтобы можно было импортировать onec_connector
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.onec_connector import (
    call_if_callable,
    connect_to_1c,
    find_catalog_metadata,
    safe_getattr,
)


def _is_reference_type(type_description) -> bool:
    """Определяет, является ли тип чисто ссылочным (без примитивов)."""
    if not type_description:
        return False

    type_description = call_if_callable(type_description)
    if type_description is None:
        return False

    types = safe_getattr(type_description, "Types", None)
    types = call_if_callable(types)
    if not types:
        return False

    count = call_if_callable(safe_getattr(types, "Count", None))
    if count is None:
        return False

    has_reference = False
    for idx in range(int(count)):
        type_item = types.Get(idx)
        name = safe_getattr(type_item, "Name", "")
        if not name:
            name = str(type_item)
        if "Ссылка" in name:
            has_reference = True
        else:
            lowered = name.lower()
            if lowered in {"неопределено", "null", "неопред"}:
                continue
            # Как только встречаем примитив или нессылочный тип — считаем реквизит смешанным
            return False

    return has_reference


def _collect_requisites(meta) -> List[Dict[str, object]]:
    result: List[Dict[str, object]] = []
    requisites = safe_getattr(meta, "Реквизиты", None)
    requisites = call_if_callable(requisites)
    if not requisites:
        return result

    count = call_if_callable(safe_getattr(requisites, "Count", None))
    if not isinstance(count, int):
        return result

    for idx in range(int(count)):
        requisite = requisites.Get(idx)
        name = safe_getattr(requisite, "Name", "")
        if not name:
            continue
        result.append(
            {
                "name": name,
                "is_reference": _is_reference_type(
                    safe_getattr(requisite, "Type", None)
                ),
            }
        )
    return result


def _can_emit_reference_field(
    com_object, catalog_path: str, alias: str, field_name: str
) -> bool:
    """
    Проверяет, что реквизит действительно возвращает ссылочный тип по данным.

    Выбираем первое ненулевое значение и смотрим, является ли оно COM-объектом.
    """
    query = com_object.NewObject("Запрос")
    query.Текст = f"""ВЫБРАТЬ ПЕРВЫЕ 100
    {alias}.{field_name} КАК Значение
ИЗ
    {catalog_path} КАК {alias}
"""
    try:
        result = query.Выполнить()
        selection = result.Выбрать()
        while selection.Следующий():
            value = None
            if hasattr(selection, "Get"):
                try:
                    value = selection.Get("Значение")
                except Exception:
                    value = None
            if value is None:
                value = safe_getattr(selection, "Значение", None)
            if value is not None and hasattr(value, "_oleobj_"):
                return True
    except Exception:
        pass
    return False


def _append_reference_select(
    select_parts: List[str],
    column_names: List[str],
    alias: str,
    field_name: str,
    include_base: bool = False,
) -> None:
    if include_base:
        select_parts.append(f"{alias}.{field_name} КАК {field_name}")
        column_names.append(field_name)
    select_parts.append(
        f"ПРЕДСТАВЛЕНИЕ({alias}.{field_name}) КАК {field_name}_Представление"
    )
    select_parts.append(
        f"ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР({alias}.{field_name})) "
        f"КАК {field_name}_UUID"
    )
    select_parts.append(
        f"ВЫБОР\n"
        f"        КОГДА {alias}.{field_name} = НЕОПРЕДЕЛЕНО\n"
        f'            ТОГДА ""\n'
        f"        ИНАЧЕ ТИПЗНАЧЕНИЯ({alias}.{field_name})\n"
        f"    КОНЕЦ КАК {field_name}_Тип"
    )
    column_names.append(f"{field_name}_Представление")
    column_names.append(f"{field_name}_UUID")
    column_names.append(f"{field_name}_Тип")


def _catalog_has_owner(meta) -> bool:
    owners_attr = safe_getattr(meta, "Owners", None)
    owners = call_if_callable(owners_attr)
    if owners is None:
        owners = owners_attr
    count = call_if_callable(safe_getattr(owners, "Count", None))
    if isinstance(count, int) and count > 0:
        return True

    owner_attr = safe_getattr(meta, "Owner", None)
    owner = call_if_callable(owner_attr)
    if owner:
        return True

    use_standard_owner = safe_getattr(meta, "UseStandardOwner", None)
    use_standard_owner = call_if_callable(use_standard_owner)
    if isinstance(use_standard_owner, bool):
        return use_standard_owner
    return bool(use_standard_owner)


def _catalog_is_hierarchical(meta) -> bool:
    """Определяет, является ли справочник иерархическим."""
    use_hierarchy = safe_getattr(meta, "UseHierarchy", None)
    use_hierarchy = call_if_callable(use_hierarchy)
    if use_hierarchy is not None:
        return bool(use_hierarchy)
    
    hierarchical_flag = safe_getattr(meta, "Hierarchical", None)
    hierarchical_flag = call_if_callable(hierarchical_flag)
    if hierarchical_flag is not None:
        return bool(hierarchical_flag)
    
    return False


def _build_main_query(
    com_object, catalog_path: str, requisites, limit: int, has_owner: bool, is_hierarchical: bool = False
) -> Tuple[str, List[str]]:
    alias = "Каталог"
    select_parts = [
        f"{alias}.Ссылка КАК Ссылка",
        f"ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР({alias}.Ссылка)) КАК uuid",
        f"{alias}.Код КАК Код",
        f"{alias}.Наименование КАК Наименование",
    ]

    column_names = ["Ссылка", "uuid", "Код", "Наименование"]

    if has_owner:
        _append_reference_select(
            select_parts, column_names, alias, "Владелец", include_base=True
        )

    # Для иерархических справочников добавляем выгрузку родителя
    if is_hierarchical:
        _append_reference_select(
            select_parts, column_names, alias, "Родитель", include_base=True
        )

    for requisite in requisites:
        name = requisite["name"]
        # Пропускаем Родитель, так как он уже добавлен для иерархических справочников
        if is_hierarchical and name == "Родитель":
            continue
        select_parts.append(f"{alias}.{name} КАК {name}")
        column_names.append(name)
        should_emit = (
            requisite.get("is_reference")
            or _can_emit_reference_field(com_object, catalog_path, alias, name)
        )
        if should_emit:
            _append_reference_select(select_parts, column_names, alias, name)

    select_body = ",\n    ".join(select_parts)
    limit_clause = f"ПЕРВЫЕ {limit}" if limit > 0 else ""

    query_text = f"""ВЫБРАТЬ {limit_clause}
    {select_body}
ИЗ
    {catalog_path} КАК {alias}
"""
    return query_text.strip(), column_names


def _collect_tabular_sections(meta) -> List[Dict[str, object]]:
    sections: List[Dict[str, object]] = []
    tabular = safe_getattr(meta, "ТабличныеЧасти", None)
    tabular = call_if_callable(tabular)
    if not tabular:
        return sections

    count = call_if_callable(safe_getattr(tabular, "Count", None))
    if not isinstance(count, int):
        return sections

    for idx in range(int(count)):
        part = tabular.Get(idx)
        name = safe_getattr(part, "Name", "")
        if not name:
            continue
        sections.append(
            {"name": name, "requisites": _collect_requisites(part)}
        )
    return sections


def _build_tabular_query(
    com_object,
    catalog_path: str,
    section_name: str,
    requisites: Iterable[Dict[str, object]],
) -> Tuple[str, List[str]]:
    alias = "ТЧ"
    table_path = f"{catalog_path}.{section_name}"
    column_names = ["parent_link", "parent_uuid", "НомерСтроки"]
    select_parts = [
        f"{alias}.Ссылка КАК parent_link",
        f"ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР({alias}.Ссылка)) КАК parent_uuid",
        f"{alias}.НомерСтроки КАК НомерСтроки",
    ]

    for requisite in requisites:
        name = requisite["name"]
        select_parts.append(f"{alias}.{name} КАК {name}")
        column_names.append(name)
        should_emit = (
            requisite.get("is_reference")
            or _can_emit_reference_field(com_object, table_path, alias, name)
        )
        if should_emit:
            select_parts.append(f"ПРЕДСТАВЛЕНИЕ({alias}.{name}) КАК {name}_Представление")
            select_parts.append(
                f"ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР({alias}.{name})) КАК {name}_UUID"
            )
            select_parts.append(
                f"ВЫБОР\n"
                f"        КОГДА {alias}.{name} = НЕОПРЕДЕЛЕНО\n"
                f'            ТОГДА ""\n'
                f"        ИНАЧЕ ТИПЗНАЧЕНИЯ({alias}.{name})\n"
                f"    КОНЕЦ КАК {name}_Тип"
            )
            column_names.append(f"{name}_Представление")
            column_names.append(f"{name}_UUID")
            column_names.append(f"{name}_Тип")

    select_body = ",\n    ".join(select_parts)

    query_text = f"""ВЫБРАТЬ
    {select_body}
ИЗ
    {table_path} КАК {alias}
ГДЕ
    {alias}.Ссылка В (&Ссылки)
УПОРЯДОЧИТЬ ПО
    {alias}.Ссылка,
    {alias}.НомерСтроки
"""
    return query_text.strip(), column_names


def main():
    parser = argparse.ArgumentParser(
        description="Генерация текста запроса 1С для справочника"
    )
    parser.add_argument(
        "--source",
        default="source",
        help="Имя конфигурации или строка подключения (по умолчанию: source)",
    )
    parser.add_argument(
        "--catalog",
        required=True,
        help="Имя метаданных справочника (например, 'НоменклатурныеГруппы')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Ограничение количества строк (0 — без ограничения)",
    )

    args = parser.parse_args()

    com_object = connect_to_1c(args.source)
    if not com_object:
        raise SystemExit("Не удалось подключиться к базе 1С")

    catalog_metadata, metadata_name = find_catalog_metadata(
        com_object, [args.catalog]
    )
    catalog_path = f"Справочник.{metadata_name}"

    requisites = _collect_requisites(catalog_metadata)
    has_owner = _catalog_has_owner(catalog_metadata)
    is_hierarchical = _catalog_is_hierarchical(catalog_metadata)
    query_text, columns = _build_main_query(
        com_object, catalog_path, requisites, args.limit, has_owner, is_hierarchical
    )

    print("=== Основной запрос ===")
    print(query_text)
    print("\nКолонки:")
    print(columns)

    sections = _collect_tabular_sections(catalog_metadata)
    if not sections:
        print("\nТабличные части отсутствуют.")
        return

    print("\n=== Табличные части ===")
    for section in sections:
        section_query, section_columns = _build_tabular_query(
            com_object, catalog_path, section["name"], section["requisites"]
        )
        print(f"\n-- Табличная часть: {section['name']}")
        print(section_query)
        print("Колонки:")
        print(section_columns)


if __name__ == "__main__":
    main()

