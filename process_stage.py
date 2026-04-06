from __future__ import annotations

import os
import shutil
from typing import Optional

from stage_utils import load_module_from_file


def get_available_processors(base_dir: str) -> list[str]:
    process_dir = os.path.join(base_dir, "PROCESS")
    if not os.path.isdir(process_dir):
        return []

    processors: list[str] = []
    for file_name in os.listdir(process_dir):
        if file_name.endswith("_processor.py"):
            processors.append(file_name[:-3])
    return sorted(processors)


def process_catalog_to_db(
    base_dir: str,
    catalog_name: str,
    source_db_file: str,
    processed_db_file: str,
) -> bool:
    if not source_db_file:
        print("Ошибка: не указан путь к исходной базе данных для обработки.")
        return False
    source_db_file = os.path.abspath(source_db_file)
    if not os.path.exists(source_db_file):
        print(f"Ошибка: исходная база данных не найдена: {source_db_file}")
        return False

    if not processed_db_file:
        print("Ошибка: не указан путь для сохранения обработанной базы данных.")
        return False
    processed_db_file = os.path.abspath(processed_db_file)
    processed_dir = os.path.dirname(processed_db_file)
    if processed_dir and not os.path.exists(processed_dir):
        os.makedirs(processed_dir, exist_ok=True)

    processor_name = f"{catalog_name}_processor"
    processor_path = os.path.join(base_dir, "PROCESS", f"{processor_name}.py")

    if not os.path.exists(processor_path):
        print(f"Предупреждение: модуль обработки '{processor_name}' не найден.")
        print("Будет выполнено копирование базы данных без изменений.")
        try:
            shutil.copy2(source_db_file, processed_db_file)
            return True
        except OSError as error:
            print(f"Ошибка копирования файла базы данных: {error}")
            return False

    processor_module = load_module_from_file(processor_path, processor_name)
    if processor_module is None:
        return False

    process_function_name = f"process_{catalog_name}"
    if not hasattr(processor_module, process_function_name):
        print(f"Ошибка: функция '{process_function_name}' не найдена в модуле '{processor_name}'")
        return False

    process_function = getattr(processor_module, process_function_name)
    try:
        return bool(process_function(source_db_file, processed_db_file))
    except Exception as error:
        print(f"Ошибка выполнения обработки справочника '{catalog_name}': {error}")
        return False

