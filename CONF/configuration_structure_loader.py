# -*- coding: utf-8 -*-
"""
Загрузка структуры конфигурации 1С (справочники, документы, перечисления)
в отдельную базу данных SQLite.

Для работы используется COM-коннектор 1С и общие функции проекта.
"""

import json
import os
import sys
from contextlib import closing
from typing import Any, Dict, List, Optional, Tuple, Set
from itertools import chain

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding  # noqa: E402

fix_encoding()

from tools.db_manager import connect_to_sqlite, ensure_database_exists  # noqa: E402
from tools.onec_connector import (  # noqa: E402
    call_if_callable,
    connect_to_1c,
    safe_getattr,
)

CATALOGS_TABLE = "metadata_catalogs"
DOCUMENTS_TABLE = "metadata_documents"
ENUMERATIONS_TABLE = "metadata_enumerations"
LEGACY_ENUMERATION_VALUES_TABLE = "metadata_enumeration_values"
UNAVAILABLE_TYPE = "<Недоступно по COM>"
EXCLUDED_SUFFIXES = ("ПрисоединенныеФайлы",)
PROPERTY_ALIASES = {
    "Catalogs": ("Catalogs", "Справочники"),
    "Documents": ("Documents", "Документы"),
    "Enumerations": ("Enumerations", "Перечисления"),
    "Attributes": ("Attributes", "Requisites", "Реквизиты"),
    "Requisites": ("Requisites", "Attributes", "Реквизиты"),
    "TabularSections": ("TabularSections", "ТабличныеЧасти"),
    "PredefinedItems": ("PredefinedItems", "PredefinedValues", "ПредопределенныеЭлементы"),
    "PredefinedValues": ("PredefinedValues", "ПредопределенныеЗначения"),
    "Forms": ("Forms", "Формы"),
    "Resources": ("Resources", "Ресурсы"),
    "Values": ("Values", "Значения"),
    "EnumerationValues": ("EnumerationValues", "ЗначенияПеречисления"),
    "Name": ("Name", "Имя"),
    "FullName": ("FullName", "ПолноеИмя"),
    "Synonym": ("Synonym", "Синоним"),
    "Comment": ("Comment", "Комментарий"),
    "Type": ("Type", "Тип"),
    "Types": ("Types", "Типы"),
    "Count": ("Count", "Количество"),
}


def _string_value(connection, value) -> str:
    if value is None:
        return ""
    string_method = safe_getattr(connection, "String", None)
    if callable(string_method):
        try:
            text = string_method(value)
            if text:
                return str(text)
        except Exception:
            return ""
    return _to_str(value)


def _get_internal_name(value_obj: Any) -> str:
    metadata = safe_getattr(value_obj, "Metadata", None)
    metadata = call_if_callable(metadata)
    if metadata:
        name = _to_str(_get_property(metadata, "Name"))
        if name:
            return name
    name = _to_str(_get_property(value_obj, "Name"))
    if name:
        return name
    return ""


def _to_str(value: Any) -> str:
    if value is None:
        return ""

    try:
        value = call_if_callable(value)
    except Exception:
        pass

    if value is None:
        return ""

    if isinstance(value, (str, int, float, bool)):
        return str(value)

    if hasattr(value, "_oleobj_"):
        for attr_name in (
            "FullName",
            "Name",
            "QualifiedName",
            "TypeName",
            "Presentation",
            "Представление",
            "Synonym",
            "Title",
        ):
            attr = safe_getattr(value, attr_name, None)
            attr = call_if_callable(attr)
            if attr:
                text = _to_str(attr)
                if text:
                    return text

    try:
        text = str(value)
        if "<COMObject" in text:
            return ""
        return text
    except Exception:
        return ""


def _string_type(connection, type_obj) -> str:
    to_string = safe_getattr(connection, "String", None)
    if not callable(to_string):
        return ""
    try:
        value = to_string(type_obj)
        if value is None:
            return ""
        text = str(value).strip()
        return text
    except Exception:
        return ""


def _normalize_type_representation(value: str) -> str:
    if not value:
        return ""
    items = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if part.count(" ") == 1 and "." not in part:
            prefix, suffix = part.split(" ", 1)
            if prefix and suffix:
                part = f"{prefix}.{suffix}"
        items.append(part)
    return ", ".join(items) if items else value


def _metadata_name(meta_obj: Any) -> str:
    meta_obj = call_if_callable(meta_obj)
    if meta_obj is None:
        return ""
    name = _to_str(safe_getattr(meta_obj, "FullName", None))
    if not name:
        name = _to_str(safe_getattr(meta_obj, "Name", None))
    return name


def _find_metadata_name(metadata_root: Any, type_value: Any) -> str:
    if metadata_root is None or type_value is None:
        return ""

    for method_name in ("НайтиПоТипу", "FindByType"):
        method = safe_getattr(metadata_root, method_name, None)
        if not method:
            continue
        try:
            result = method(type_value)
        except Exception:
            continue
        name = _metadata_name(result)
        if name:
            return name
    return ""


def _collect_type_candidates(type_obj: Any) -> List[Any]:
    candidates: List[Any] = []
    for method_name in ("ПолучитьТип", "ТИП", "Тип"):
        method = safe_getattr(type_obj, method_name, None)
        if callable(method):
            try:
                candidate = method()
            except Exception:
                continue
            if candidate is not None:
                candidates.append(candidate)
    for attr_name in ("ValueType", "BaseType", "Тип", "Base", "Значение", "Value"):
        candidate = safe_getattr(type_obj, attr_name, None)
        candidate = call_if_callable(candidate)
        if candidate is not None:
            candidates.append(candidate)
    return candidates


def _resolve_single_type(
    connection,
    metadata_root: Any,
    type_obj: Any,
    visited: Optional[set] = None,
) -> str:
    if type_obj is None:
        return ""

    if visited is None:
        visited = set()
    obj_id = id(type_obj)
    if obj_id in visited:
        return ""
    visited.add(obj_id)

    meta_candidate = safe_getattr(type_obj, "Метаданные", None)
    meta_candidate = call_if_callable(meta_candidate)
    name = _metadata_name(meta_candidate)
    if name:
        return name

    name = _find_metadata_name(metadata_root, type_obj)
    if name:
        return name

    str_name = _normalize_type_representation(_string_type(connection, type_obj))
    if str_name:
        return str_name

    for collection_name in ("Types", "Типы", "BaseTypes", "ValueTypes", "ТипыЗначений"):
        collection = _get_property(type_obj, collection_name, None)
        for item in _iter_collection(collection):
            candidate_name = _resolve_single_type(connection, metadata_root, item, visited)
            if candidate_name:
                return candidate_name

    for candidate in _collect_type_candidates(type_obj):
        candidate_name = _resolve_single_type(connection, metadata_root, candidate, visited)
        if candidate_name:
            return candidate_name

    return ""


def _resolve_type_names(connection, metadata_root: Any, type_obj: Any) -> List[str]:
    names: List[str] = []

    types_method = safe_getattr(type_obj, "Типы", None)
    if not callable(types_method):
        types_method = safe_getattr(type_obj, "Types", None)

    if callable(types_method):
        collection = types_method()
        for item in _iter_collection(collection):
            name = _resolve_single_type(connection, metadata_root, item)
            if not name:
                name = _normalize_type_representation(_string_type(connection, item))
            if not name:
                name = UNAVAILABLE_TYPE
            if name not in names:
                names.append(name)

    if not names:
        name = _resolve_single_type(connection, metadata_root, type_obj)
        if not name:
            name = _normalize_type_representation(_string_type(connection, type_obj))
        if not name:
            name = UNAVAILABLE_TYPE
        names.append(name)

    return names


def _iter_type_collection(collection: Any) -> List[Any]:
    items: List[Any] = []
    for item in _iter_collection(collection):
        if item is not None:
            items.append(item)
    return items


def _type_object_to_name(type_obj: Any, metadata_root: Any, visited: Optional[set] = None) -> str:
    if type_obj is None:
        return ""

    if visited is None:
        visited = set()
    obj_id = id(type_obj)
    if obj_id in visited:
        return ""
    visited.add(obj_id)

    name = _metadata_name(safe_getattr(type_obj, "Метаданные", None))
    if name:
        return name

    name = _metadata_name(type_obj)
    if name:
        return name

    metadata_name = _find_metadata_name(metadata_root, type_obj)
    if metadata_name:
        return metadata_name

    aggregated_names: List[str] = []
    for collection_name in ("Types", "BaseTypes", "Типы", "ValueTypes", "ТипыЗначений"):
        collection = _get_property(type_obj, collection_name, None)
        for item in _iter_type_collection(collection):
            item_name = _type_object_to_name(item, metadata_root, visited)
            if not item_name:
                continue
            if item_name not in aggregated_names:
                aggregated_names.append(item_name)
    if aggregated_names:
        return ", ".join(aggregated_names)

    for candidate in _collect_type_candidates(type_obj):
        candidate_name = _type_object_to_name(candidate, metadata_root, visited)
        if candidate_name:
            return candidate_name

    primitive_name = _to_str(type_obj)
    if primitive_name and "<COMObject" not in primitive_name:
        return primitive_name
    return ""


def _normalize_item_name(name: str) -> str:
    return name.strip().lower()


def _collect_requisites(meta_obj: Any, metadata_root: Any, connection) -> List[Dict[str, str]]:
    requisites: List[Dict[str, str]] = []
    seen_names: Set[str] = set()
    for collection_name in ("Attributes", "Requisites"):
        for item in _iter_collection(_get_property(meta_obj, collection_name, None)):
            name = _to_str(_get_property(item, "Name"))
            if not name:
                continue
            norm_name = _normalize_item_name(name)
            if norm_name in seen_names:
                continue
            seen_names.add(norm_name)
            type_obj = _get_property(item, "Type", None)
            resolved_names = _resolve_type_names(connection, metadata_root, type_obj)
            type_name = ", ".join(resolved_names) if resolved_names else ""
            requisites.append({"name": name, "type": type_name})
    return requisites


def _collect_items_for_outputs(
    meta_obj: Any,
    collection_name: str,
    metadata_root: Any,
    connection,
    seen_names: Optional[Set[str]] = None,
) -> Tuple[List[str], List[Dict[str, str]]]:
    json_items: List[str] = []
    db_items: List[Dict[str, str]] = []
    local_seen: Set[str] = seen_names if seen_names is not None else set()

    for item in _iter_collection(_get_property(meta_obj, collection_name, None)):
        name = _to_str(_get_property(item, "Name"))
        if not name:
            continue
        norm_name = _normalize_item_name(name)
        if norm_name in local_seen:
            continue
        local_seen.add(norm_name)
        type_obj = _get_property(item, "Type", None)
        resolved_names = _resolve_type_names(connection, metadata_root, type_obj)
        full_type = ", ".join(resolved_names)
        short_type = ", ".join(resolved_names[:3]) if resolved_names else ""

        db_items.append({"name": name, "type": full_type})
        json_items.append(f"{name}: {short_type}" if short_type else name)

    return json_items, db_items


def _collect_requisites_for_outputs(
    meta_obj: Any,
    metadata_root: Any,
    connection,
) -> Tuple[List[str], List[Dict[str, str]]]:
    json_requisites: List[str] = []
    db_requisites: List[Dict[str, str]] = []
    seen_names: Set[str] = set()

    for collection_name in ("Attributes", "Requisites"):
        json_items, db_items = _collect_items_for_outputs(
            meta_obj,
            collection_name,
            metadata_root,
            connection,
            seen_names=seen_names,
        )
        json_requisites.extend(json_items)
        db_requisites.extend(db_items)

    if _is_hierarchical_object(meta_obj):
        folder_name = "ЭтоГруппа"
        norm_name = _normalize_item_name(folder_name)
        if norm_name not in seen_names:
            seen_names.add(norm_name)
            db_requisites.append({"name": folder_name, "type": "Булево"})
            json_requisites.append(f"{folder_name}: Булево")
        
        # Добавляем родителя для иерархических справочников
        parent_name = "Родитель"
        norm_parent_name = _normalize_item_name(parent_name)
        if norm_parent_name not in seen_names:
            seen_names.add(norm_parent_name)
            # Получаем полное имя справочника для типа родителя
            catalog_full_name = _to_str(_get_property(meta_obj, "FullName"))
            if catalog_full_name and catalog_full_name.startswith("Справочник."):
                parent_type = catalog_full_name.replace("Справочник.", "СправочникСсылка.")
            else:
                catalog_name = _to_str(_get_property(meta_obj, "Name"))
                parent_type = f"СправочникСсылка.{catalog_name}" if catalog_name else "СправочникСсылка"
            db_requisites.append({"name": parent_name, "type": parent_type})
            json_requisites.append(f"{parent_name}: {parent_type}")

    return json_requisites, db_requisites


def _collect_sections_for_outputs(
    meta_obj: Any,
    metadata_root: Any,
    connection,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    json_sections: List[Dict[str, Any]] = []
    db_sections: List[Dict[str, Any]] = []

    for section in _iter_collection(_get_property(meta_obj, "TabularSections", None)):
        section_name = _to_str(_get_property(section, "Name"))
        if not section_name:
            continue
        section_synonym = _to_str(_get_property(section, "Synonym"))
        section_comment = _to_str(_get_property(section, "Comment"))

        json_section: Dict[str, Any] = {"name": section_name}
        db_section: Dict[str, Any] = {"name": section_name}
        if section_synonym:
            json_section["synonym"] = section_synonym
            db_section["synonym"] = section_synonym
        if section_comment:
            json_section["comment"] = section_comment
            db_section["comment"] = section_comment

        section_seen: Set[str] = set()
        for property_name, key in (
            ("Attributes", "attributes"),
            ("Requisites", "requisites"),
            ("Resources", "resources"),
        ):
            json_items, db_items = _collect_items_for_outputs(
                section,
                property_name,
                metadata_root,
                connection,
                seen_names=section_seen,
            )
            if json_items:
                json_section[key] = json_items
            if db_items:
                db_section[key] = db_items

        json_sections.append(json_section)
        db_sections.append(db_section)

    return json_sections, db_sections


def _ensure_tables(cursor) -> None:
    cursor.execute(f"DROP TABLE IF EXISTS {LEGACY_ENUMERATION_VALUES_TABLE}")
    cursor.execute(f"DROP TABLE IF EXISTS {CATALOGS_TABLE}")
    cursor.execute(f"DROP TABLE IF EXISTS {DOCUMENTS_TABLE}")
    cursor.execute(f"DROP TABLE IF EXISTS {ENUMERATIONS_TABLE}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOGS_TABLE} (
            name TEXT PRIMARY KEY,
            synonym TEXT,
            full_name TEXT,
            is_hierarchical INTEGER,
            hierarchy_type TEXT,
            max_hierarchy_levels INTEGER,
            requisites_json TEXT,
            tabular_sections_json TEXT,
            predefined_json TEXT
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DOCUMENTS_TABLE} (
            name TEXT PRIMARY KEY,
            synonym TEXT,
            full_name TEXT,
            requisites_json TEXT,
            tabular_sections_json TEXT
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ENUMERATIONS_TABLE} (
            name TEXT PRIMARY KEY,
            synonym TEXT,
            full_name TEXT,
            values_json TEXT
        )
        """
    )


def _save_catalogs(
    metadata_root: Any,
    cursor,
    connection,
) -> Tuple[int, List[Dict[str, Any]]]:
    catalogs_collection = _get_property(metadata_root, "Catalogs", None)
    entries: List[tuple] = []
    json_entries: List[Dict[str, Any]] = []

    for catalog in _iter_collection(catalogs_collection):
        basic = _extract_basic_info(catalog)
        if not basic["name"]:
            continue
        if basic["name"].endswith(EXCLUDED_SUFFIXES):
            continue
        json_reqs, db_reqs = _collect_requisites_for_outputs(
            catalog,
            metadata_root,
            connection,
        )
        json_sections, db_sections = _collect_sections_for_outputs(
            catalog,
            metadata_root,
            connection,
        )
        predefined_items = _collect_predefined_items(catalog)

        entries.append(
            (
                basic["name"],
                basic["synonym"],
                basic["full_name"],
                1 if basic.get("is_hierarchical") else 0,
                basic.get("hierarchy_type", ""),
                basic.get("max_hierarchy_levels"),
                json.dumps(db_reqs, ensure_ascii=False) if db_reqs else "[]",
                json.dumps(db_sections, ensure_ascii=False) if db_sections else "[]",
                json.dumps(predefined_items, ensure_ascii=False) if predefined_items else "[]",
            )
        )

        json_entry = {
            "name": basic["name"],
            "synonym": basic["synonym"],
            "full_name": basic["full_name"],
            "requisites": json_reqs,
            "tabular_sections": json_sections,
            "predefined_items": predefined_items[:],
            "is_hierarchical": bool(basic.get("is_hierarchical")),
        }
        if basic.get("hierarchy_type"):
            json_entry["hierarchy_type"] = basic["hierarchy_type"]
        if basic.get("max_hierarchy_levels") is not None:
            json_entry["max_hierarchy_levels"] = basic["max_hierarchy_levels"]

        json_entries.append(json_entry)

    cursor.execute(f"DELETE FROM {CATALOGS_TABLE}")
    if entries:
        cursor.executemany(
            f"""
            INSERT INTO {CATALOGS_TABLE}
            (name, synonym, full_name, is_hierarchical, hierarchy_type, max_hierarchy_levels,
             requisites_json, tabular_sections_json, predefined_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            entries,
        )
    return len(entries), json_entries


def _save_documents(
    metadata_root: Any,
    cursor,
    connection,
) -> Tuple[int, List[Dict[str, Any]]]:
    documents_collection = _get_property(metadata_root, "Documents", None)
    entries: List[tuple] = []
    json_entries: List[Dict[str, Any]] = []

    for document in _iter_collection(documents_collection):
        basic = _extract_basic_info(document)
        if not basic["name"]:
            continue
        if basic["name"].endswith(EXCLUDED_SUFFIXES):
            continue
        json_reqs, db_reqs = _collect_requisites_for_outputs(
            document,
            metadata_root,
            connection,
        )
        json_sections, db_sections = _collect_sections_for_outputs(
            document,
            metadata_root,
            connection,
        )

        entries.append(
            (
                basic["name"],
                basic["synonym"],
                basic["full_name"],
                json.dumps(db_reqs, ensure_ascii=False) if db_reqs else "[]",
                json.dumps(db_sections, ensure_ascii=False) if db_sections else "[]",
            )
        )

        json_entries.append(
            {
                "name": basic["name"],
                "synonym": basic["synonym"],
                "full_name": basic["full_name"],
                "requisites": json_reqs,
                "tabular_sections": json_sections,
            }
        )

    cursor.execute(f"DELETE FROM {DOCUMENTS_TABLE}")
    if entries:
        cursor.executemany(
            f"INSERT INTO {DOCUMENTS_TABLE} (name, synonym, full_name, requisites_json, tabular_sections_json) VALUES (?, ?, ?, ?, ?)",
            entries,
        )
    return len(entries), json_entries


def _collect_predefined_values(
    enum_obj: Any, metadata_root: Any, connection
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    json_values: List[Dict[str, str]] = []
    db_values: List[Dict[str, str]] = []
    names: List[str] = []
    seen_names: set[str] = set()

    raw_values = _get_property(enum_obj, "PredefinedValues", None)
    if not raw_values:
        raw_values = _get_property(enum_obj, "Values", None)
    if not raw_values:
        raw_values = _get_property(enum_obj, "EnumerationValues", None)

    if raw_values and not isinstance(raw_values, list):
        try:
            iterator = raw_values if hasattr(raw_values, "__iter__") else []
        except TypeError:
            iterator = []
        items = list(iterator)
        if not items:
            get_method = safe_getattr(raw_values, "Get", None)
            get_method_alt = safe_getattr(raw_values, "Получить", None)
            count_method = safe_getattr(raw_values, "Count", None)
            count_method_alt = safe_getattr(raw_values, "Количество", None)
            count = None
            if callable(count_method):
                try:
                    count = count_method()
                except Exception:
                    count = None
            if count is None and callable(count_method_alt):
                try:
                    count = count_method_alt()
                except Exception:
                    count = None
            if isinstance(count, (int, float)) and count > 0:
                extractors = [m for m in (get_method, get_method_alt) if callable(m)]
                for idx in range(int(count)):
                    for extractor in extractors:
                        try:
                            items.append(extractor(idx))
                            break
                        except Exception:
                            continue
        raw_values = items

    if raw_values is None:
        raw_values = []

    def _register_value(internal_name: str, synonym_value: str) -> None:
        if not internal_name:
            return
        if internal_name not in seen_names:
            names.append(internal_name)
            seen_names.add(internal_name)
        db_values.append({"name": internal_name, "synonym": synonym_value or ""})

    for value_meta in raw_values:
        name = _to_str(_get_property(value_meta, "Name")) or _to_str(
            _get_property(value_meta, "Наименование")
        )
        synonym_value = _to_str(_get_property(value_meta, "Synonym")) or _to_str(
            _get_property(value_meta, "Синоним")
        )

        raw_value = _get_property(value_meta, "Value", None)
        if raw_value is None:
            raw_value = safe_getattr(value_meta, "ПолучитьЗначение", None)
            if callable(raw_value):
                try:
                    raw_value = raw_value()
                except Exception:
                    raw_value = None

        internal_name = name
        if raw_value is not None:
            derived_name = _get_enum_internal_name(connection, enum_obj, raw_value)
            if derived_name:
                internal_name = derived_name
            if not synonym_value:
                synonym_value = (
                    _to_str(_get_property(raw_value, "Synonym"))
                    or _to_str(_get_property(raw_value, "Синоним"))
                    or _to_str(_get_property(raw_value, "Presentation"))
                    or _to_str(_get_property(raw_value, "Представление"))
                )
            if not synonym_value:
                synonym_value = _string_value(connection, raw_value)

        if not internal_name:
            internal_name = _string_value(connection, raw_value) or name

        if not internal_name:
            continue

        _register_value(internal_name, synonym_value)

    if not names:
        enum_name = _to_str(_get_property(enum_obj, "Name"))
        meta_values = _get_enum_meta_values(enum_obj)
        if enum_name and meta_values:
            try:
                manager = _get_enum_manager(connection, enum_name)
                for index, meta_value in enumerate(meta_values):
                    value_obj = None
                    if manager is not None:
                        get_value_method = safe_getattr(manager, "Get", None) or safe_getattr(manager, "Получить", None)
                        if callable(get_value_method):
                            try:
                                value_obj = get_value_method(index)
                            except Exception:
                                value_obj = None
                    if value_obj is None:
                        value_obj = safe_getattr(meta_value, "Value", None)
                    internal_name = (
                        _to_str(_get_property(meta_value, "Name"))
                        or _to_str(_get_property(meta_value, "Имя"))
                        or _get_enum_internal_name(connection, enum_obj, value_obj)
                    )
                    synonym_value = _to_str(_get_property(meta_value, "Synonym")) or _to_str(
                        _get_property(meta_value, "Синоним")
                    )
                    if value_obj is not None and not synonym_value:
                        synonym_value = (
                            _to_str(_get_property(value_obj, "Synonym"))
                            or _to_str(_get_property(value_obj, "Синоним"))
                            or _to_str(_get_property(value_obj, "Presentation"))
                            or _to_str(_get_property(value_obj, "Представление"))
                        )
                    if not synonym_value and value_obj is not None:
                        synonym_value = _string_value(connection, value_obj)
                    if internal_name:
                        _register_value(internal_name, synonym_value)
            except Exception:
                pass

    if names:
        json_values = [{"name": ", ".join(names)}]

    return json_values, db_values


def _save_enumerations(metadata_root: Any, cursor, connection) -> Tuple[int, List[Dict[str, Any]]]:
    enumerations_collection = _get_property(metadata_root, "Enumerations", None)
    enumeration_entries: List[tuple] = []
    data_entries: List[Dict[str, Any]] = []

    for enumeration in _iter_collection(enumerations_collection):
        basic = _extract_basic_info(enumeration)
        if not basic["name"]:
            continue

        json_values, db_values = _collect_predefined_values(
            enumeration, metadata_root, connection
        )

        data_entries.append(
            {
                "name": basic["name"],
                "synonym": basic["synonym"],
                "full_name": basic["full_name"],
                "comment": _to_str(_get_property(enumeration, "Comment")),
                "values": json_values,
            }
        )

        enumeration_entries.append(
            (
                basic["name"],
                basic["synonym"],
                basic["full_name"],
                json.dumps(db_values, ensure_ascii=False) if db_values else "[]",
            )
        )

    cursor.execute(f"DELETE FROM {ENUMERATIONS_TABLE}")

    if enumeration_entries:
        cursor.executemany(
            f"INSERT INTO {ENUMERATIONS_TABLE} (name, synonym, full_name, values_json) VALUES (?, ?, ?, ?)",
            enumeration_entries,
        )

    return len(enumeration_entries), data_entries


def load_configuration_structure(
    source_db_path: str,
    sqlite_db_file: str,
    json_output: Optional[str] = None,
    **_kwargs,
) -> bool:
    """Основная точка входа для загрузки метаданных конфигурации."""
    if not source_db_path:
        print("Ошибка: не указан путь или конфигурация базы 1С-источника.")
        return False

    if not ensure_database_exists(sqlite_db_file):
        print("Ошибка: не удалось подготовить файл SQLite для сохранения метаданных.")
        return False

    connection_1c = connect_to_1c(source_db_path)
    if connection_1c is None:
        print("Ошибка: подключение к 1С не выполнено, загрузка метаданных невозможна.")
        return False

    metadata_root = _get_property(connection_1c, "Metadata", None)
    metadata_root = call_if_callable(metadata_root)
    if metadata_root is None:
        print("Ошибка: объект Metadata недоступен в соединении 1С.")
        return False

    with closing(connect_to_sqlite(sqlite_db_file)) as sqlite_conn:
        if sqlite_conn is None:
            print("Ошибка: не удалось открыть SQLite-базу для сохранения метаданных.")
            return False

        catalogs_saved = documents_saved = enumerations_saved = 0
        catalogs_json: List[Dict[str, Any]] = []
        documents_json: List[Dict[str, Any]] = []
        enumerations_json: List[Dict[str, Any]] = []
        cursor = sqlite_conn.cursor()
        try:
            _ensure_tables(cursor)

            catalogs_saved, catalogs_json = _save_catalogs(metadata_root, cursor, connection_1c)
            documents_saved, documents_json = _save_documents(metadata_root, cursor, connection_1c)
            enumerations_saved, enumerations_json = _save_enumerations(metadata_root, cursor, connection_1c)

            sqlite_conn.commit()
        except Exception as error:
            sqlite_conn.rollback()
            print(f"Ошибка при сохранении метаданных конфигурации: {error}")
            return False
        finally:
            cursor.close()

    if json_output:
        try:
            json_output = os.path.abspath(json_output)
            directory = os.path.dirname(json_output)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
            with open(json_output, "w", encoding="utf-8") as json_file:
                json.dump(
                    {
                        "catalogs": catalogs_json,
                        "enumerations": enumerations_json,
                    },
                    json_file,
                    ensure_ascii=False,
                    indent=2,
                )
            print(f"JSON-структура метаданных сохранена: {json_output}")
        except Exception as error:
            print(f"Ошибка записи JSON-файла '{json_output}': {error}")
            return False

    print(
        "Метаданные конфигурации сохранены.",
        f"Справочники: {catalogs_saved}",
        f"Перечисления: {enumerations_saved}",
        sep="\n",
    )
    return True


def _get_property(obj: Any, name: str, default: Optional[str] = "") -> Any:
    value = safe_getattr(obj, name, None)
    value = call_if_callable(value)
    if value is None:
        alt_name = name
        if name.endswith("s"):
            alt_name = name[:-1]
        value = safe_getattr(obj, alt_name, None)
        value = call_if_callable(value)
    if value is None:
        for alias in PROPERTY_ALIASES.get(name, ()):  # noqa: PERF203 - небольшое множество
            if alias == name:
                continue
            value = safe_getattr(obj, alias, None)
            value = call_if_callable(value)
            if value is not None:
                break
    return default if value is None else value


def _iter_collection(collection: Any) -> List[Any]:
    items: List[Any] = []
    if collection is None:
        return items
    collection = call_if_callable(collection)
    if collection is None:
        return items

    count = _get_property(collection, "Count", None)
    if isinstance(count, str):
        try:
            count = int(count)
        except ValueError:
            count = None
    if count is None:
        count = _get_property(collection, "Количество", None)
    if isinstance(count, str):
        try:
            count = int(count)
        except ValueError:
            count = None

    if isinstance(count, (int, float)):
        for index in range(int(count)):
            item = None
            try:
                item = collection.Get(index)
            except Exception:
                try:
                    item = collection.Получить(index)
                except Exception:
                    item = None
            if item is not None:
                items.append(item)
        return items

    index = 0
    while True:
        item = None
        try:
            item = collection.Get(index)
        except Exception:
            try:
                item = collection.Получить(index)
            except Exception:
                item = None
        if item is None:
            break
        items.append(item)
        index += 1
    return items


def _is_hierarchical_object(meta_obj: Any) -> bool:
    use_hierarchy = _get_property(meta_obj, "UseHierarchy", None)
    if use_hierarchy is not None:
        return bool(use_hierarchy)
    hierarchical_flag = _get_property(meta_obj, "Hierarchical", None)
    if hierarchical_flag is not None:
        return bool(hierarchical_flag)
    return False


def _extract_basic_info(meta_obj: Any) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "name": _to_str(_get_property(meta_obj, "Name")),
        "full_name": _to_str(_get_property(meta_obj, "FullName")),
        "synonym": _to_str(_get_property(meta_obj, "Synonym")),
    }

    is_hierarchical = _is_hierarchical_object(meta_obj)
    info["is_hierarchical"] = is_hierarchical

    hierarchy_obj = _get_property(meta_obj, "HierarchyType", None)
    hierarchy_type = ""
    if hierarchy_obj is not None:
        name_value = safe_getattr(hierarchy_obj, "Name", None)
        hierarchy_type = _to_str(name_value)
        if not hierarchy_type:
            hierarchy_type = _metadata_name(hierarchy_obj)
        if not hierarchy_type:
            hierarchy_type = _to_str(hierarchy_obj)
    if hierarchy_type:
        info["hierarchy_type"] = hierarchy_type

    max_levels = _get_property(meta_obj, "MaxHierarchyLevels", None)
    if max_levels is not None:
        try:
            info["max_hierarchy_levels"] = int(max_levels)
        except (TypeError, ValueError):
            pass

    return info


def _collect_predefined_items(meta_obj: Any) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for predefined in _iter_collection(_get_property(meta_obj, "PredefinedItems", None)):
        name = _to_str(_get_property(predefined, "Name"))
        if not name:
            continue
        entry = {"name": name}
        code = _to_str(_get_property(predefined, "Code"))
        if code:
            entry["code"] = code
        description = _to_str(_get_property(predefined, "Description"))
        if description:
            entry["description"] = description
        presentation = _to_str(_get_property(predefined, "Presentation"))
        if presentation and presentation != name:
            entry["presentation"] = presentation
        items.append(entry)
    return items


def _get_enum_manager(connection, enum_name: str):
    if not enum_name:
        return None
    managers = safe_getattr(connection, "Enumerations", None)
    managers = call_if_callable(managers)
    if managers is None:
        managers = safe_getattr(connection, "Перечисления", None)
        managers = call_if_callable(managers)
    if managers is None:
        return None

    possible_names = [enum_name]
    short_name = enum_name.split(".")[-1]
    if short_name != enum_name:
        possible_names.append(short_name)

    for name_candidate in possible_names:
        manager = safe_getattr(managers, name_candidate, None)
        if manager is not None:
            return manager

    return None


def _get_enum_meta_values(metadata_enum: Any) -> List[Any]:
    containers = [
        _get_property(metadata_enum, "EnumerationValues", None),
        _get_property(metadata_enum, "Values", None),
        _get_property(metadata_enum, "PredefinedValues", None),
    ]

    for container in containers:
        container = call_if_callable(container)
        if container is None:
            continue

        items = _iter_collection(container)
        if items:
            return items

        count_attr = safe_getattr(container, "Count", None)
        count_attr_alt = safe_getattr(container, "Количество", None)
        count = None
        for candidate in (count_attr, count_attr_alt):
            if callable(candidate):
                try:
                    count = candidate()
                    break
                except Exception:
                    count = None
        if not isinstance(count, (int, float)):
            continue
        count = int(count)

        items = []
        for method_name in ("__getitem__", "Get", "Получить"):
            getter = safe_getattr(container, method_name, None)
            if callable(getter):
                try:
                    items = [getter(index) for index in range(count)]
                    if items:
                        return items
                except Exception:
                    items = []
            elif method_name == "__getitem__" and hasattr(container, "__getitem__"):
                try:
                    items = [container[index] for index in range(count)]
                    if items:
                        return items
                except Exception:
                    items = []
    return []


def _get_enum_internal_name(
    connection, metadata_enum: Any, value_obj: Any
) -> str:
    enum_name = _to_str(_get_property(metadata_enum, "Name"))
    manager = _get_enum_manager(connection, enum_name)
    if manager is None:
        return ""

    index_method = safe_getattr(manager, "Index", None)
    if not callable(index_method):
        index_method = safe_getattr(manager, "Индекс", None)
    if not callable(index_method):
        return ""

    try:
        index = index_method(value_obj)
    except Exception:
        return ""

    if not isinstance(index, (int, float)):
        return ""

    index = int(index)
    values_meta = _get_enum_meta_values(metadata_enum)
    if index < 0 or index >= len(values_meta):
        return ""

    meta_value = values_meta[index]
    internal_name = (
        _to_str(_get_property(meta_value, "Name"))
        or _to_str(_get_property(meta_value, "Имя"))
    )
    return internal_name
