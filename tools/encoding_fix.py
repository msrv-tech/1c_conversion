"""Модуль для исправления кодировки вывода в Windows консоли."""
import sys
import io


def fix_encoding() -> None:
    """Настраивает кодировку UTF-8 для stdout и stderr в Windows."""
    if sys.platform == "win32":
        # Для Python 3.7+ используем reconfigure
        if hasattr(sys.stdout, "reconfigure"):
            try:
                sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, ValueError):
                pass
        if hasattr(sys.stderr, "reconfigure"):
            try:
                sys.stderr.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, ValueError):
                pass
        
        # Альтернативный способ для старых версий Python
        if sys.stdout.encoding != "utf-8":
            try:
                sys.stdout = io.TextIOWrapper(
                    sys.stdout.buffer, encoding="utf-8", errors="replace"
                )
            except (AttributeError, ValueError):
                pass
        if sys.stderr.encoding != "utf-8":
            try:
                sys.stderr = io.TextIOWrapper(
                    sys.stderr.buffer, encoding="utf-8", errors="replace"
                )
            except (AttributeError, ValueError):
                pass

