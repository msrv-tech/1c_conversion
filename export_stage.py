from __future__ import annotations

import os
from typing import Callable, Optional

from tools.onec_connector import connect_to_1c
from tools.writer_utils import set_default_fetch_limit
from stage_utils import load_module_from_file

# Глобальная переменная для хранения COM-подключения к приемнику
_target_com_object: Optional[object] = None
_target_db_path: Optional[str] = None


def _get_target_com_object(target_db_path: str):
    """
    Получает или создает COM-подключение к приемнику.
    Подключение создается один раз и переиспользуется для всех справочников.
    """
    global _target_com_object, _target_db_path
    
    # Если подключение уже создано для этого приемника, возвращаем его
    if _target_com_object is not None and _target_db_path == target_db_path:
        return _target_com_object
    
    # Если приемник изменился, закрываем старое подключение
    if _target_com_object is not None and _target_db_path != target_db_path:
        try:
            _target_com_object = None
        except:
            pass
    
    # Создаем новое подключение
    from tools.logger import verbose_print
    verbose_print("\n[Подключение к приемнику 1С]")
    _target_com_object = connect_to_1c(target_db_path)
    if not _target_com_object:
        print("Ошибка: не удалось подключиться к приемнику 1С")
        return None
    
    _target_db_path = target_db_path
    return _target_com_object


def get_available_writers(base_dir: str) -> list[str]:
    out_dir = os.path.join(base_dir, "OUT")
    if not os.path.isdir(out_dir):
        return []

    writers: list[str] = []
    for file_name in os.listdir(out_dir):
        if file_name.endswith("_writer.py"):
            writers.append(file_name[:-3])
    return sorted(writers)


def load_from_db_to_1c(
    base_dir: str,
    catalog_name: str,
    sqlite_db_file: str,
    target_db_path: str,
    process_func: Optional[Callable] = None,
    mode: str = "test",
) -> bool:
    from tools.logger import verbose_print
    verbose_print(f"  [1/2] Подключение к 1С приемнику...")
    # Получаем или создаем COM-подключение к приемнику
    com_object = _get_target_com_object(target_db_path)
    if com_object is None:
        print(f"  ✗ Ошибка подключения к 1С приемнику")
        return False
    verbose_print(f"  ✓ Подключение к 1С приемнику установлено")
    
    verbose_print(f"  [2/2] Загрузка модуля экспорта для '{catalog_name}'...")
    
    writer_name = f"{catalog_name}_writer"
    writer_path = os.path.join(base_dir, "OUT", f"{writer_name}.py")

    if not os.path.exists(writer_path):
        print(f"  ✗ Ошибка: Модуль выгрузки '{writer_name}' не найден")
        print(f"  Ожидаемый путь: {writer_path}")
        return False

    writer_module = load_module_from_file(writer_path, writer_name)
    verbose_print(f"  ✓ Модуль экспорта загружен")
    if writer_module is None:
        return False

    write_function_name = f"write_{catalog_name}_to_1c"
    if not hasattr(writer_module, write_function_name):
        print(f"Ошибка: Функция '{write_function_name}' не найдена в модуле '{writer_name}'")
        return False

    write_function = getattr(writer_module, write_function_name)
    
    # Всегда передаем com_object
    from tools.logger import verbose_print
    verbose_print(f"  Начинаем экспорт данных...")
    export_limit = 50 if mode == "test" else None
    set_default_fetch_limit(export_limit)
    try:
        result = write_function(sqlite_db_file, com_object, process_func)
    finally:
        set_default_fetch_limit(None)
    if result:
        verbose_print(f"  ✓ Экспорт завершен успешно")
    else:
        verbose_print(f"  ✗ Ошибка при экспорте")
    return bool(result)

