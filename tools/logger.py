# -*- coding: utf-8 -*-
"""
Модуль для управления выводом логов.
Поддерживает краткий и подробный режимы вывода.
Может записывать логи в файл.
"""

import datetime
import os
from typing import Optional

_VERBOSE = False
_LOG_FILE: Optional[str] = None
_LOG_FILE_HANDLE = None


def set_verbose(verbose: bool):
    """Устанавливает режим подробного вывода."""
    global _VERBOSE
    _VERBOSE = verbose


def is_verbose() -> bool:
    """Возвращает True, если включен режим подробного вывода."""
    return _VERBOSE


def set_log_file(log_file: Optional[str]):
    """
    Устанавливает файл для записи логов.
    
    Args:
        log_file: Путь к файлу для записи логов. Если None, логирование в файл отключается.
    """
    global _LOG_FILE, _LOG_FILE_HANDLE
    
    # Закрываем предыдущий файл, если он был открыт
    if _LOG_FILE_HANDLE:
        try:
            _LOG_FILE_HANDLE.close()
        except Exception:
            pass
        _LOG_FILE_HANDLE = None
    
    _LOG_FILE = log_file
    
    # Открываем новый файл для записи (append mode)
    if _LOG_FILE:
        try:
            # Создаем директорию, если её нет
            log_dir = os.path.dirname(os.path.abspath(_LOG_FILE))
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            # Используем режим 'a' (append) с line buffering для немедленной записи
            # Это помогает избежать проблем с блокировкой на Windows и обеспечивает реальное время
            _LOG_FILE_HANDLE = open(_LOG_FILE, 'a', encoding='utf-8', buffering=1)  # Line buffering для немедленной записи
        except Exception as e:
            print(f"Ошибка при открытии файла логов {_LOG_FILE}: {e}")
            _LOG_FILE = None
            _LOG_FILE_HANDLE = None


def close_log_file():
    """Закрывает файл логов, если он был открыт."""
    global _LOG_FILE_HANDLE
    if _LOG_FILE_HANDLE:
        try:
            _LOG_FILE_HANDLE.close()
        except Exception:
            pass
        _LOG_FILE_HANDLE = None


def _get_timestamp() -> str:
    """Возвращает текущую временную метку в формате [HH:MM:SS]."""
    now = datetime.datetime.now()
    return now.strftime("[%H:%M:%S]")


def _format_with_timestamp(*args, **kwargs) -> tuple:
    """Форматирует аргументы с добавлением временной метки."""
    # Получаем временную метку
    timestamp = _get_timestamp()
    
    # Если есть только один аргумент-строка и нет sep/end, добавляем метку к нему
    if len(args) == 1 and isinstance(args[0], str) and 'sep' not in kwargs and 'end' not in kwargs:
        # Для многострочных сообщений добавляем метку к каждой непустой строке
        message = args[0]
        if '\n' in message:
            lines = message.split('\n')
            formatted_lines = []
            for line in lines:
                if line.strip():  # Добавляем метку только к непустым строкам
                    formatted_lines.append(f"{timestamp} {line}")
                else:
                    formatted_lines.append(line)  # Пустые строки оставляем как есть
            return ('\n'.join(formatted_lines),), kwargs
    
    # Для обычных сообщений добавляем метку в начало
    if args:
        # Если это f-string или обычная строка, добавляем метку
        formatted_message = f"{timestamp} {' '.join(str(arg) for arg in args)}"
        return (formatted_message,), kwargs
    
    return args, kwargs


def _write_to_file(message: str):
    """Записывает сообщение в файл логов, если файл установлен."""
    global _LOG_FILE_HANDLE
    if _LOG_FILE_HANDLE:
        try:
            _LOG_FILE_HANDLE.write(message + '\n')
            _LOG_FILE_HANDLE.flush()  # Сбрасываем буфер для немедленной записи
            # Дополнительно синхронизируем с диском (если поддерживается)
            try:
                os.fsync(_LOG_FILE_HANDLE.fileno())
            except:
                pass
        except Exception as e:
            # Если ошибка записи, выводим предупреждение один раз
            print(f"Ошибка записи в файл логов: {e}", flush=True)


def verbose_print(*args, **kwargs):
    """Выводит сообщение только если включен режим подробного вывода."""
    if _VERBOSE:
        formatted_args, formatted_kwargs = _format_with_timestamp(*args, **kwargs)
        message = ' '.join(str(arg) for arg in formatted_args)
        # Принудительно сбрасываем буфер для вывода в реальном времени
        if 'flush' not in kwargs:
            kwargs['flush'] = True
        print(*formatted_args, **formatted_kwargs)
        _write_to_file(message)


def info_print(*args, **kwargs):
    """Выводит информационное сообщение (всегда)."""
    formatted_args, formatted_kwargs = _format_with_timestamp(*args, **kwargs)
    message = ' '.join(str(arg) for arg in formatted_args)
    # Принудительно сбрасываем буфер для вывода в реальном времени
    if 'flush' not in kwargs:
        kwargs['flush'] = True
    print(*formatted_args, **formatted_kwargs)
    _write_to_file(message)

