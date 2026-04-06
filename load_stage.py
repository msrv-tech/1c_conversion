from __future__ import annotations

import importlib
import inspect
import os
from typing import Callable, Dict, Optional, Tuple

from tools.update_reference_types import (
    build_enumeration_index,
    update_reference_types,
)
from tools.onec_connector import connect_to_1c

from stage_utils import load_module_from_file


MetadataBundle = Tuple[Dict[str, str], Dict[str, Dict[str, str]]]
MetadataCache = Dict[str, MetadataBundle]
_METADATA_CACHE: MetadataCache = {}


def get_available_loaders(base_dir: str) -> list[str]:
    in_dir = os.path.join(base_dir, "IN")
    loaders: list[str] = []
    if os.path.isdir(in_dir):
        for file_name in os.listdir(in_dir):
            if file_name.endswith("_loader.py"):
                loaders.append(file_name[:-3])

    conf_loader = os.path.join(base_dir, "CONF", "configuration_structure_loader.py")
    if os.path.exists(conf_loader):
        loaders.append("configuration_structure_loader")

    return sorted(set(loaders))


def _get_metadata_bundle(
    metadata_db: Optional[str],
) -> Optional[MetadataBundle]:
    if not metadata_db:
        return None
    if metadata_db in _METADATA_CACHE:
        return _METADATA_CACHE[metadata_db]
    if not os.path.exists(metadata_db):
        from tools.logger import verbose_print
        verbose_print(f"Предупреждение: база метаданных не найдена: {metadata_db}")
        return None
    metadata_map = {}  # Пустой словарь, так как build_metadata_map больше не используется
    enumeration_index = build_enumeration_index(metadata_db)
    bundle = (metadata_map, enumeration_index)
    _METADATA_CACHE[metadata_db] = bundle
    return bundle


# Глобальная переменная для хранения COM-подключения к источнику
_source_com_object: Optional[object] = None
_source_db_path: Optional[str] = None


def _get_source_com_object(source_db_path: str):
    """
    Получает или создает COM-подключение к источнику.
    Подключение создается один раз и переиспользуется для всех справочников.
    """
    global _source_com_object, _source_db_path
    
    # Если подключение уже создано для этого источника, возвращаем его
    if _source_com_object is not None and _source_db_path == source_db_path:
        return _source_com_object
    
    # Если источник изменился, закрываем старое подключение
    if _source_com_object is not None and _source_db_path != source_db_path:
        try:
            _source_com_object = None
        except:
            pass
    
    # Создаем новое подключение
    from tools.logger import verbose_print
    verbose_print("\n[Подключение к источнику 1С]")
    _source_com_object = connect_to_1c(source_db_path)
    if not _source_com_object:
        print("Ошибка: не удалось подключиться к источнику 1С")
        return None
    
    _source_db_path = source_db_path
    return _source_com_object


def load_catalog_to_db(
    base_dir: str,
    catalog_name: str,
    source_db_path: str,
    sqlite_db_file: str,
    mode: str,
    process_func: Optional[Callable] = None,
    filters_db: Optional[str] = None,
    json_output_path: Optional[str] = None,
    metadata_db: Optional[str] = None,
    skip_type_update: bool = False,
) -> bool:
    # Получаем или создаем COM-подключение к источнику
    com_object = _get_source_com_object(source_db_path)
    if com_object is None:
        return False
    
    if catalog_name == "configuration_structure":
        try:
            module = importlib.import_module("CONF.configuration_structure_loader")
        except ModuleNotFoundError as error:
            print("Ошибка: модуль CONF.configuration_structure_loader не найден")
            print(error)
            return False

        load_function = getattr(module, "load_configuration_structure", None)
        if load_function is None:
            print("Ошибка: в CONF.configuration_structure_loader отсутствует load_configuration_structure")
            return False

        json_kwargs: Dict[str, object] = {}
        if json_output_path:
            json_kwargs["json_output"] = json_output_path

        # Передаем com_object
        signature = inspect.signature(load_function)
        if "com_object" in signature.parameters:
            result = load_function(com_object, sqlite_db_file, **json_kwargs)
        else:
            # Для обратной совместимости (если функция еще не обновлена)
            result = load_function(source_db_path, sqlite_db_file, **json_kwargs)
        return bool(result)

    loader_name = f"{catalog_name}_loader"
    loader_path = os.path.join(base_dir, "IN", f"{loader_name}.py")

    if not os.path.exists(loader_path):
        print(f"Ошибка: Модуль загрузки '{loader_name}' не найден")
        print(f"Ожидаемый путь: {loader_path}")
        return False

    loader_module = load_module_from_file(loader_path, loader_name)
    if loader_module is None:
        return False

    load_function_name = f"load_{catalog_name}"
    if not hasattr(loader_module, load_function_name):
        print(f"Ошибка: Функция '{load_function_name}' не найдена в модуле '{loader_name}'")
        return False

    load_function = getattr(loader_module, load_function_name)
    signature = inspect.signature(load_function)
    kwargs: Dict[str, object] = {}
    
    # Если функция принимает com_object, передаем его как позиционный аргумент
    if "com_object" in signature.parameters:
        # Передаем только остальные параметры через kwargs, com_object - позиционно
        if "mode" in signature.parameters:
            kwargs["mode"] = mode
        if "process_func" in signature.parameters:
            kwargs["process_func"] = process_func
        if "filters_db" in signature.parameters:
            kwargs["filters_db"] = filters_db
        if "json_output" in signature.parameters:
            kwargs["json_output"] = json_output_path
        
        # Вызываем функцию: sqlite_db_file первым, com_object вторым, остальное через kwargs
        result = load_function(sqlite_db_file, com_object, **kwargs)
    else:
        # Для обратной совместимости (если функция еще не обновлена)
        if "source_db_path" in signature.parameters:
            kwargs["source_db_path"] = source_db_path
        if "mode" in signature.parameters:
            kwargs["mode"] = mode
        if "process_func" in signature.parameters:
            kwargs["process_func"] = process_func
        if "filters_db" in signature.parameters:
            kwargs["filters_db"] = filters_db
        if "json_output" in signature.parameters:
            kwargs["json_output"] = json_output_path
        
        result = load_function(source_db_path, sqlite_db_file, **kwargs)

    if result and not skip_type_update:
        table_name = getattr(loader_module, "TABLE_NAME", None)
        key_column = getattr(loader_module, "TYPE_UPDATE_KEY_COLUMN", "uuid")
        metadata_bundle = _get_metadata_bundle(metadata_db)

        if metadata_bundle and table_name:
            metadata_map, enumeration_index = metadata_bundle
            try:
                updated = update_reference_types(
                    target_db=sqlite_db_file,
                    table_name=table_name,
                    metadata_map=metadata_map,
                    enumeration_index=enumeration_index,
                    key_column=key_column,
                )
                print(
                    f"Обновление типов: {updated} строк изменено (таблица {table_name})."
                )
            except Exception as error:
                print(
                    f"Предупреждение: не удалось обновить типы для таблицы {table_name}: {error}"
                )
        elif table_name is None:
            print("Предупреждение: в модуле отсутствует TABLE_NAME, пропускаем обновление типов.")

    return bool(result)

