import importlib.util
from types import ModuleType
from typing import Optional


def load_module_from_file(module_path: str, module_name: str) -> Optional[ModuleType]:
    """
    Динамически загружает модуль из файла.
    """
    try:
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None:
            return None
        module = importlib.util.module_from_spec(spec)
        if spec.loader is None:
            return None
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
        return module
    except Exception as error:
        print(f"Ошибка загрузки модуля {module_name}: {error}")
        return None

