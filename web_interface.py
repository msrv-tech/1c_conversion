# -*- coding: utf-8 -*-
"""
Веб-интерфейс для запуска обмена справочников
"""

import json
import os
import subprocess
import sys
import threading
import queue
import sqlite3
from flask import Flask, render_template, jsonify, Response, request
from flask_cors import CORS

# Импорт менеджера версий договоров
try:
    from tools.contract_version_manager import update_contract_versions
except ImportError:
    update_contract_versions = None

app = Flask(__name__)
CORS(app)

# Глобальная очередь для логов
log_queues = {}
# Словарь для отслеживания потоков выполнения
running_threads = {}
# Словарь для хранения путей к файлам логов для каждого справочника
log_file_paths = {}

# Базовый путь проекта
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CATALOG_MAPPING_PATH = os.path.join(BASE_DIR, 'CONF', 'catalog_mapping.json')

TARGET_OPTIONS = [
    {
        "key": "default",
        "label": "MODEL1",
        "env": "TARGET_CONNECTION_STRING",
        "fallback_env": "TARGET_1C",
    },
    {
        "key": "preprod",
        "label": "PREPROD",
        "env": "TARGET_CONNECTION_STRING_PREPROD",
        "fallback_env": None,
    },
    {
        "key": "matveev",
        "label": "MATVEEV",
        "env": "TARGET_CONNECTION_STRING_MATVEEV",
        "fallback_env": None,
    },
    {
        "key": "prod",
        "label": "PROD",
        "env": "TARGET_CONNECTION_STRING_PROD",
        "fallback_env": None,
    },
]


def load_catalog_mapping():
    """Загружает mapping справочников из JSON"""
    try:
        with open(CATALOG_MAPPING_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки catalog_mapping.json: {e}")
        return {}


def _load_env():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


def _get_target_connection(target_key: str) -> str:
    if not target_key:
        raise ValueError("Не указан ключ приемника.")
    config = next((item for item in TARGET_OPTIONS if item["key"] == target_key), None)
    if not config:
        raise ValueError(f"Неизвестный приемник: {target_key}")
    connection = os.getenv(config["env"])
    if not connection and config.get("fallback_env"):
        connection = os.getenv(config["fallback_env"])
    if not connection:
        raise ValueError(f"Не задана переменная окружения {config['env']}")
    return connection


def _format_connection_display(connection: str) -> str:
    if not connection:
        return "Недоступно"
    try:
        from tools.onec_connector import resolve_connection_string
        try:
            display, _ = resolve_connection_string(connection)
        except Exception:
            display = connection
    except ImportError:
        display = connection
    return display


def _build_target_options():
    options = []
    for item in TARGET_OPTIONS:
        try:
            connection = _get_target_connection(item["key"])
            available = True
            error = None
        except ValueError as exc:
            connection = None
            available = False
            error = str(exc)
        display = _format_connection_display(connection)
        if item["key"] == "prod" and connection:
            display = f"[PROD] {display}"
        options.append({
            "key": item["key"],
            "label": item["label"],
            "display": display,
            "available": available,
            "error": error,
        })
    return options


def get_db_path(catalog_name: str, processed: bool = False) -> str:
    """
    Формирует путь к базе данных справочника
    
    Args:
        catalog_name: Имя справочника (например, 'contractors')
        processed: True для обработанной БД, False для сырой
    
    Returns:
        Путь к файлу БД
    """
    db_dir = os.path.join(BASE_DIR, 'BD')
    suffix = "_processed" if processed else ""
    file_name = f"{catalog_name}{suffix}.db"
    return os.path.join(db_dir, file_name)


def run_export_by_codes(catalog_name, codes, uuids, log_queue, target_key, target_1c):
    """
    Запускает экспорт справочника по кодам или UUID в приемник
    Логи записываются в файл, который читается через polling API
    
    Args:
        catalog_name: Имя справочника (например, 'contractors')
        codes: Список кодов для фильтрации
        uuids: Список UUID для фильтрации
        log_queue: Очередь для передачи логов (используется для совместимости, но не критична)
    """
    log_file_path = None
    try:
        # Формируем команду для запуска export_by_code.py
        export_script = os.path.join(BASE_DIR, 'export_by_code.py')
        
        # Проверяем существование export_by_code.py
        if not os.path.exists(export_script):
            log_queue.put(f"[ОШИБКА] Файл export_by_code.py не найден: {export_script}\n")
            log_queue.put(None)
            return
        
        # Используем ТОЛЬКО обработанную БД - без fallback на сырую
        processed_db = get_db_path(catalog_name, processed=True)
        
        if not os.path.exists(processed_db):
            error_msg = f"[ОШИБКА] Обработанная база данных не найдена для справочника '{catalog_name}'\n"
            error_msg += f"[ОШИБКА] Ожидаемый путь: {processed_db}\n"
            error_msg += f"[ОШИБКА] Экспорт возможен только из обработанной БД. Сначала выполните обработку данных.\n"
            log_queue.put(error_msg)
            log_queue.put(None)
            return
        
        sqlite_db = processed_db
        log_queue.put(f"[ИНФО] Используется обработанная БД: {sqlite_db}\n")
        
        if not target_1c:
            log_queue.put("[ОШИБКА] Не указан приемник для экспорта.\n")
            log_queue.put(None)
            return
        
        # Создаем временный файл для логов
        logs_dir = os.path.join(BASE_DIR, 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        log_file_path = os.path.join(logs_dir, f'{catalog_name}_export_by_code_{threading.current_thread().ident}.log')
        
        # Сохраняем путь к файлу логов для polling API
        log_file_paths[catalog_name] = log_file_path
        
        # Команда для экспорта по кодам
        cmd = [
            sys.executable,
            '-u',  # Unbuffered mode
            export_script,
            '--catalog', catalog_name,
            '--sqlite-db', sqlite_db,
            '--mode', 'full',
            '--verbose',  # Всегда используем verbose режим для веб-интерфейса
        ]
        
        if target_key == "prod":
            cmd.append('--prod')
        else:
            cmd.extend(['--target-1c', target_1c])
        
        # Добавляем коды
        if codes:
            cmd.append('--codes')
            cmd.extend(codes)
        
        # Добавляем UUID
        if uuids:
            cmd.append('--uuids')
            cmd.extend(uuids)
        
        # Подготавливаем переменные окружения
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'
        env['PYTHONIOENCODING'] = 'utf-8'
        
        # Запускаем процесс с выводом в файл логов
        log_file_handle = open(log_file_path, 'w', encoding='utf-8')
        
        process = subprocess.Popen(
            cmd,
            stdout=log_file_handle,
            stderr=subprocess.STDOUT,  # Перенаправляем stderr в stdout
            cwd=BASE_DIR,
            env=env
        )
        
        # Ждем завершения процесса
        return_code = process.wait()
        
        # Закрываем файл логов
        log_file_handle.close()
        
    except Exception as e:
        log_queue.put(f"\n[КРИТИЧЕСКАЯ ОШИБКА] {str(e)}\n")
        import traceback
        log_queue.put(traceback.format_exc() + "\n")
        if 'log_file_handle' in locals():
            try:
                log_file_handle.close()
            except:
                pass
    finally:
        # Сигнал завершения
        log_queue.put(None)
        # Удаляем временный файл логов и путь из словаря после задержки
        if log_file_path and os.path.exists(log_file_path):
            def cleanup_log_file():
                import time
                time.sleep(30)
                try:
                    if catalog_name in log_file_paths:
                        del log_file_paths[catalog_name]
                    if os.path.exists(log_file_path):
                        os.remove(log_file_path)
                except:
                    pass
            threading.Thread(target=cleanup_log_file, daemon=True).start()


def run_exchange(catalog_name, log_queue, target_key, target_1c):
    """
    Запускает полный цикл обмена для справочника в режиме full (prod)
    Логи записываются в файл, который читается через polling API
    
    Args:
        catalog_name: Имя справочника (например, 'contractors')
        log_queue: Очередь для передачи логов (используется для совместимости, но не критична)
    """
    log_file_path = None
    try:
        # Создаем временный файл для логов
        logs_dir = os.path.join(BASE_DIR, 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        log_file_path = os.path.join(logs_dir, f'{catalog_name}_web_{threading.current_thread().ident}.log')
        
        # Сохраняем путь к файлу логов для polling API
        log_file_paths[catalog_name] = log_file_path
        
        if target_key == "prod":
            with open(log_file_path, 'w', encoding='utf-8') as log_file:
                log_file.write("[ЗАГЛУШКА] Полный экспорт в PROD отключен в веб-интерфейсе.\n")
                log_file.write("[ИНФО] Выберите другой приемник или используйте CLI.\n")
            return
        
        # Формируем команду для запуска main.py
        main_script = os.path.join(BASE_DIR, 'main.py')
        
        # Проверяем существование main.py
        if not os.path.exists(main_script):
            log_queue.put(f"[ОШИБКА] Файл main.py не найден: {main_script}\n")
            with open(log_file_path, 'a', encoding='utf-8') as log_file:
                log_file.write(f"[ОШИБКА] Файл main.py не найден: {main_script}\n")
            log_queue.put(None)
            return
        
        if not target_1c:
            log_queue.put("[ОШИБКА] Не указан приемник для экспорта.\n")
            with open(log_file_path, 'a', encoding='utf-8') as log_file:
                log_file.write("[ОШИБКА] Не указан приемник для экспорта.\n")
            log_queue.put(None)
            return
        
        # Определяем пути к БД
        sqlite_db = os.path.join(BASE_DIR, 'BD')
        
        # Команда для полного цикла обмена в режиме full
        cmd = [
            sys.executable,
            '-u',  # Unbuffered mode
            main_script,
            '--catalog', catalog_name,
            '--import',
            '--process',
            '--export',
            '--mode', 'full',
            '--verbose',
            '--sqlite-db', sqlite_db,
            '--log-file', log_file_path,  # Используем уже созданный файл
            '--target-1c', target_1c
        ]
        
        # Подготавливаем переменные окружения
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'
        env['PYTHONIOENCODING'] = 'utf-8'
        
        # Запускаем процесс с выводом в файл логов
        # На Windows используем os.devnull, на Unix - subprocess.DEVNULL
        if sys.platform == 'win32':
            devnull = open(os.devnull, 'w')
        else:
            devnull = subprocess.DEVNULL
        
        process = subprocess.Popen(
            cmd,
            stdout=devnull,  # Перенаправляем stdout
            stderr=devnull,  # Перенаправляем stderr
            cwd=BASE_DIR,
            env=env
        )
        
        # Ждем завершения процесса
        # Логи читаются через polling API напрямую из файла
        return_code = process.wait()
        
    except Exception as e:
        log_queue.put(f"\n[КРИТИЧЕСКАЯ ОШИБКА] {str(e)}\n")
        import traceback
        log_queue.put(traceback.format_exc() + "\n")
    finally:
        # Сигнал завершения (для совместимости, но не используется в polling API)
        log_queue.put(None)
        # Удаляем временный файл логов и путь из словаря после задержки (чтобы дать время на чтение)
        if log_file_path and os.path.exists(log_file_path):
            def cleanup_log_file():
                import time
                time.sleep(30)  # Даем больше времени на чтение через polling (30 секунд)
                try:
                    # Удаляем путь из словаря
                    if catalog_name in log_file_paths:
                        del log_file_paths[catalog_name]
                    # Удаляем файл
                    if os.path.exists(log_file_path):
                        os.remove(log_file_path)
                except:
                    pass  # Игнорируем ошибки удаления
            threading.Thread(target=cleanup_log_file, daemon=True).start()


def run_contract_versions_update(log_queue, target_key, target_1c):
    """
    Запускает обновление версий договоров
    """
    log_file_path = None
    catalog_name = "contract_versions"
    try:
        # Создаем временный файл для логов
        logs_dir = os.path.join(BASE_DIR, 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        log_file_path = os.path.join(logs_dir, f'contract_versions_update_{threading.current_thread().ident}.log')
        
        # Сохраняем путь к файлу логов для polling API
        log_file_paths[catalog_name] = log_file_path
        
        # Перенаправляем вывод в файл
        with open(log_file_path, 'w', encoding='utf-8') as log_file:
            def log_to_file(msg):
                log_file.write(msg)
                log_file.flush()
            
            # Начало процесса
            log_to_file("[ЗАПУСК] Запуск обновления версий договоров...\n")
            
            # Инициализируем COM для текущего потока (необходимо для многопоточных приложений)
            try:
                import pythoncom
                # Пытаемся инициализировать COM
                # Если уже инициализирован, это нормально - продолжаем
                try:
                    result = pythoncom.CoInitialize()
                    log_to_file("[ИНФО] COM инициализирован для потока\n")
                except pythoncom.com_error as e:
                    # Если COM уже инициализирован, это нормально
                    if e.hresult == -2147221008:  # CO_E_ALREADYINITIALIZED
                        log_to_file("[ИНФО] COM уже инициализирован\n")
                    else:
                        raise
            except Exception as com_init_e:
                log_to_file(f"[ПРЕДУПРЕЖДЕНИЕ] Не удалось инициализировать COM: {str(com_init_e)}\n")
                log_to_file("[ИНФО] Продолжаем попытку подключения...\n")
            
            # Подключаемся к 1С
            log_to_file("[ИНФО] Подключение к 1С приемнику...\n")
            try:
                from tools.onec_connector import connect_to_1c, resolve_connection_string, get_com_connector
                from tools.logger import set_verbose, set_log_file
                # Включаем verbose режим и настраиваем логирование в файл
                set_verbose(True)
                set_log_file(log_file_path)
                
                if not target_1c:
                    log_to_file("[ОШИБКА] Не указан приемник для обновления версий.\n")
                    return
                
                log_to_file(f"[ИНФО] Используется конфигурация: {target_1c[:50]}...\n")
                
                # Пытаемся получить описание строки подключения для диагностики
                try:
                    connection_string, description = resolve_connection_string(target_1c)
                    log_to_file(f"[ИНФО] Строка подключения: {description}\n")
                except Exception as resolve_e:
                    log_to_file(f"[ОШИБКА] Не удалось разрешить строку подключения: {str(resolve_e)}\n")
                    log_to_file(f"[ОШИБКА] Проверьте переменную окружения TARGET_1C или файл .env\n")
                    return
                
                # Пытаемся создать COM-коннектор
                try:
                    connector, progid = get_com_connector()
                    log_to_file(f"[ИНФО] COM-коннектор создан: {progid}\n")
                except Exception as connector_e:
                    log_to_file(f"[ОШИБКА] Не удалось создать COM-коннектор: {str(connector_e)}\n")
                    log_to_file("[ОШИБКА] Убедитесь, что установлена платформа 1С:Предприятие\n")
                    return
                
                # Подключаемся
                try:
                    com_object = connector.Connect(connection_string)
                    log_to_file("[ИНФО] Подключение успешно установлено\n")
                except Exception as connect_e:
                    log_to_file(f"[ОШИБКА] Ошибка подключения к базе данных ({progid}): {str(connect_e)}\n")
                    log_to_file("[ОШИБКА] Возможные причины:\n")
                    log_to_file("[ОШИБКА] 1. База данных недоступна или заблокирована\n")
                    log_to_file("[ОШИБКА] 2. Неверная строка подключения\n")
                    log_to_file("[ОШИБКА] 3. Недостаточно прав для подключения\n")
                    return
                
                if not com_object:
                    log_to_file("[ОШИБКА] Не удалось подключиться к 1С (com_object = None)\n")
                    return
                
                # Запускаем обновление
                try:
                    from tools.contract_version_manager import update_contract_versions
                except ImportError as import_e:
                    log_to_file(f"[ОШИБКА] Не удалось импортировать модуль обновления версий: {str(import_e)}\n")
                    log_to_file("[ОШИБКА] Убедитесь, что файл tools/contract_version_manager.py существует\n")
                    return
                
                # Создаем обертку для очереди, чтобы update_contract_versions мог писать в файл
                class LogQueueWrapper:
                    def put(self, msg):
                        log_to_file(msg)
                
                try:
                    log_to_file("[ИНФО] Запуск обновления версий договоров...\n")
                    update_contract_versions(com_object, LogQueueWrapper())
                    log_to_file("\n[ИНФО] Процесс обновления завершен успешно\n")
                except Exception as update_e:
                    log_to_file(f"\n[ОШИБКА] Ошибка при обновлении версий: {str(update_e)}\n")
                    import traceback
                    log_to_file(traceback.format_exc() + "\n")
                
            except Exception as e:
                log_to_file(f"\n[ОШИБКА] {str(e)}\n")
                import traceback
                log_to_file(traceback.format_exc() + "\n")
            
            # Всегда выводим сообщение о завершении
            log_to_file("\n[ЗАВЕРШЕНО] Обмен завершен\n")
            
            # Освобождаем COM для текущего потока
            try:
                import pythoncom
                pythoncom.CoUninitialize()
            except:
                pass
                
    except Exception as e:
        if log_file_path and os.path.exists(log_file_path):
            try:
                with open(log_file_path, 'a', encoding='utf-8') as log_file:
                    log_file.write(f"\n[КРИТИЧЕСКАЯ ОШИБКА] {str(e)}\n")
                    import traceback
                    log_file.write(traceback.format_exc() + "\n")
                    log_file.write("\n[ЗАВЕРШЕНО] Обмен завершен\n")
            except:
                pass
        log_queue.put(f"\n[КРИТИЧЕСКАЯ ОШИБКА] {str(e)}\n")
    finally:
        log_queue.put(None)
        # Удаляем временный файл логов после задержки
        if log_file_path and os.path.exists(log_file_path):
            def cleanup_log_file():
                import time
                time.sleep(30)
                try:
                    if catalog_name in log_file_paths:
                        del log_file_paths[catalog_name]
                    if os.path.exists(log_file_path):
                        os.remove(log_file_path)
                except:
                    pass
            threading.Thread(target=cleanup_log_file, daemon=True).start()


@app.route('/')
def index():
    """Главная страница с кнопками справочников"""
    catalogs = load_catalog_mapping()
    return render_template('index.html', catalogs=catalogs)


def _get_db_mtime(catalog_name: str) -> str | None:
    """Возвращает дату изменения БД (processed или raw) в формате ISO или None"""
    for processed in (True, False):
        db_path = get_db_path(catalog_name, processed)
        if os.path.exists(db_path):
            try:
                mtime = os.path.getmtime(db_path)
                from datetime import datetime
                return datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
            except OSError:
                pass
    return None


@app.route('/api/catalogs')
def get_catalogs():
    """API для получения списка справочников"""
    catalogs = load_catalog_mapping()
    result = []
    for key, value in catalogs.items():
        catalog_name = value.get('catalog_name', '')
        result.append({
            'name_1c': key,
            'catalog_name': catalog_name,
            'comment': value.get('comment', ''),
            'db_modified': _get_db_mtime(catalog_name) if catalog_name else None
        })
    return jsonify(result)


@app.route('/api/run/<catalog_name>', methods=['POST'])
def run_catalog(catalog_name):
    """API для запуска обмена справочника"""
    data = request.get_json(silent=True) or {}
    target_key = data.get('target_key')
    if not target_key:
        return jsonify({'error': 'Не указан приемник для обмена'}), 400
    
    try:
        _load_env()
        target_1c = _get_target_connection(target_key)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    
    # Проверяем, не запущен ли уже процесс для этого справочника
    if catalog_name in log_queues:
        # Проверяем, действительно ли процесс еще выполняется
        if catalog_name in running_threads:
            thread = running_threads[catalog_name]
            if thread.is_alive():
                return jsonify({'error': 'Обмен уже запущен для этого справочника'}), 400
            else:
                # Поток завершился, но очередь не была очищена - очищаем
                if catalog_name in log_queues:
                    del log_queues[catalog_name]
                if catalog_name in running_threads:
                    del running_threads[catalog_name]
        else:
            # Очередь есть, но потока нет - значит процесс завершился, очищаем
            if catalog_name in log_queues:
                del log_queues[catalog_name]
    
    # Удаляем старый путь к файлу логов, если есть (для нового обмена)
    if catalog_name in log_file_paths:
        # Старый файл будет удален автоматически через cleanup
        pass
    
    # Создаем очередь для логов
    log_queue = queue.Queue()
    log_queues[catalog_name] = log_queue
    
    # Запускаем обмен в отдельном потоке
    thread = threading.Thread(target=run_exchange, args=(catalog_name, log_queue, target_key, target_1c))
    thread.daemon = True
    running_threads[catalog_name] = thread
    thread.start()
    
    return jsonify({'status': 'started', 'catalog': catalog_name})


@app.route('/api/logs/<catalog_name>')
def get_logs(catalog_name):
    """
    Polling API для получения новых логов
    Возвращает новые строки логов с указанного индекса
    """
    # Получаем параметры запроса
    from_index = request.args.get('from', 0, type=int)  # Индекс строки, с которой начинать чтение
    
    # Проверяем, есть ли файл логов для этого справочника
    if catalog_name not in log_file_paths:
        return jsonify({
            'error': 'Файл логов не найден',
            'lines': [],
            'total_lines': 0,
            'finished': False
        }), 404
    
    log_file_path = log_file_paths[catalog_name]
    
    # Проверяем, существует ли файл
    if not os.path.exists(log_file_path):
        return jsonify({
            'error': 'Файл логов не существует',
            'lines': [],
            'total_lines': 0,
            'finished': False
        }), 404
    
    try:
        # Читаем файл полностью
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            all_lines = f.readlines()
        
        total_lines = len(all_lines)
        
        # Берем только новые строки (с указанного индекса)
        new_lines = all_lines[from_index:]
        
        # Обрабатываем строки - убираем переносы строк
        processed_lines = []
        for line in new_lines:
            line = line.rstrip('\n\r')
            if line:  # Пропускаем пустые строки
                processed_lines.append(line)
        
        # Проверяем, завершился ли процесс
        finished = False
        if catalog_name in running_threads:
            thread = running_threads[catalog_name]
            finished = not thread.is_alive()
        else:
            # Если потока нет, но файл существует, считаем завершенным
            finished = True
        
        return jsonify({
            'lines': processed_lines,
            'total_lines': total_lines,
            'next_index': total_lines,
            'finished': finished,
            'has_more': len(processed_lines) > 0
        })
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'lines': [],
            'total_lines': 0,
            'finished': False
        }), 500


@app.route('/api/status/<catalog_name>')
def get_status(catalog_name):
    """Проверка статуса обмена"""
    if catalog_name in running_threads:
        thread = running_threads[catalog_name]
        if thread.is_alive():
            return jsonify({'running': True})
        else:
            # Поток завершился, но не был очищен - очищаем
            if catalog_name in log_queues:
                del log_queues[catalog_name]
            if catalog_name in running_threads:
                del running_threads[catalog_name]
    return jsonify({'running': False})


@app.route('/api/clear/<catalog_name>', methods=['POST'])
def clear_catalog_queue(catalog_name):
    """API для принудительной очистки очереди справочника"""
    if catalog_name in log_queues:
        del log_queues[catalog_name]
    if catalog_name in running_threads:
        del running_threads[catalog_name]
    # Не удаляем log_file_paths сразу - оставляем для polling API
    # Удалим через некоторое время после завершения
    return jsonify({'status': 'cleared', 'catalog': catalog_name})


@app.route('/api/export-by-codes/<catalog_name>', methods=['POST'])
def export_by_codes(catalog_name):
    """API для запуска экспорта справочника по кодам или UUID"""
    # Проверяем, не запущен ли уже процесс для этого справочника
    if catalog_name in log_queues:
        if catalog_name in running_threads:
            thread = running_threads[catalog_name]
            if thread.is_alive():
                return jsonify({'error': 'Экспорт уже запущен для этого справочника'}), 400
            else:
                if catalog_name in log_queues:
                    del log_queues[catalog_name]
                if catalog_name in running_threads:
                    del running_threads[catalog_name]
        else:
            if catalog_name in log_queues:
                del log_queues[catalog_name]
    
    # Получаем данные из запроса
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Не указаны данные для фильтрации'}), 400
    
    codes = data.get('codes', [])
    uuids = data.get('uuids', [])
    target_key = data.get('target_key')
    
    # Для заказов доступен только экспорт по UUID
    export_uuid_only_catalogs = ('customer_orders', 'supplier_orders')
    if catalog_name in export_uuid_only_catalogs:
        if codes:
            return jsonify({'error': 'Для заказов доступен только экспорт по UUID. Укажите UUID.'}), 400
        if not uuids:
            return jsonify({'error': 'Для заказов укажите хотя бы один UUID'}), 400
    
    if not target_key:
        return jsonify({'error': 'Не указан приемник для экспорта'}), 400
    
    try:
        _load_env()
        target_1c = _get_target_connection(target_key)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    
    if not codes and not uuids:
        return jsonify({'error': 'Не указаны ни коды, ни UUID для фильтрации'}), 400
    
    if not isinstance(codes, list) or not isinstance(uuids, list):
        return jsonify({'error': 'Коды и UUID должны быть списками'}), 400
    
    # Удаляем старый путь к файлу логов, если есть
    if catalog_name in log_file_paths:
        pass
    
    # Создаем очередь для логов
    log_queue = queue.Queue()
    log_queues[catalog_name] = log_queue
    
    # Запускаем экспорт в отдельном потоке
    thread = threading.Thread(
        target=run_export_by_codes,
        args=(catalog_name, codes, uuids, log_queue, target_key, target_1c)
    )
    thread.daemon = True
    running_threads[catalog_name] = thread
    thread.start()
    
    return jsonify({
        'status': 'started', 
        'catalog': catalog_name, 
        'codes_count': len(codes),
        'uuids_count': len(uuids)
    })


@app.route('/api/update-contract-versions', methods=['POST'])
def update_contract_versions_route():
    """API для запуска обновления версий договоров"""
    catalog_name = "contract_versions"
    data = request.get_json(silent=True) or {}
    target_key = data.get('target_key')
    if not target_key:
        return jsonify({'error': 'Не указан приемник для обновления версий'}), 400
    
    try:
        _load_env()
        target_1c = _get_target_connection(target_key)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    
    # Проверяем, не запущен ли уже процесс
    if catalog_name in running_threads:
        thread = running_threads[catalog_name]
        if thread.is_alive():
            return jsonify({'error': 'Процесс обновления уже запущен'}), 400
    
    # Создаем очередь для логов (для совместимости)
    log_queue = queue.Queue()
    log_queues[catalog_name] = log_queue
    
    # Запускаем в отдельном потоке
    thread = threading.Thread(target=run_contract_versions_update, args=(log_queue, target_key, target_1c))
    thread.daemon = True
    running_threads[catalog_name] = thread
    thread.start()
    
    return jsonify({'status': 'started', 'catalog': catalog_name})


@app.route('/api/connections')
def get_connections():
    """API для получения путей к источнику и приемнику"""
    # Загружаем переменные окружения
    _load_env()
    
    source_1c = os.getenv("SOURCE_CONNECTION_STRING") or os.getenv("SOURCE_1C")
    if not source_1c:
        source_1c = "Недоступно"
    target_error = None
    try:
        target_1c = _get_target_connection("default")
    except ValueError as exc:
        target_1c = None
        target_error = str(exc)
    
    # Пытаемся получить читаемое представление пути
    try:
        from tools.onec_connector import resolve_connection_string
        try:
            source_display, _ = resolve_connection_string(source_1c)
        except:
            source_display = source_1c
        try:
            target_display, _ = resolve_connection_string(target_1c) if target_1c else ("Недоступно", None)
        except:
            target_display = target_1c or "Недоступно"
    except ImportError:
        source_display = source_1c
        target_display = target_1c or "Недоступно"
    
    return jsonify({
        'source': source_1c,
        'source_display': source_display,
        'target': target_1c,
        'target_display': target_display,
        'target_error': target_error
    })


@app.route('/api/targets')
def get_targets():
    """API для получения доступных приемников"""
    _load_env()
    return jsonify({'targets': _build_target_options()})


@app.route('/db/view/<catalog_name>')
def view_db(catalog_name):
    """Страница просмотра БД"""
    db_type = request.args.get('type', 'raw')  # 'raw' или 'processed'
    processed = (db_type == 'processed')
    return render_template('db_view.html', catalog_name=catalog_name, db_type=db_type)


@app.route('/api/db/tables/<catalog_name>')
def get_db_tables(catalog_name):
    """API для получения списка таблиц из БД"""
    db_type = request.args.get('type', 'raw')
    processed = (db_type == 'processed')
    
    db_path = get_db_path(catalog_name, processed)
    
    if not os.path.exists(db_path):
        return jsonify({'error': f'База данных не найдена: {db_path}'}), 404
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем список всех таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({'tables': tables, 'db_path': db_path})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/db/table/<catalog_name>/<table_name>')
def get_table_data(catalog_name, table_name):
    """API для получения данных из таблицы"""
    db_type = request.args.get('type', 'raw')
    processed = (db_type == 'processed')
    limit = request.args.get('limit', '1000', type=int)
    offset = request.args.get('offset', 0, type=int)
    search = request.args.get('search', '').strip()
    
    db_path = get_db_path(catalog_name, processed)
    
    if not os.path.exists(db_path):
        return jsonify({'error': f'База данных не найдена: {db_path}'}), 404
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Для доступа к колонкам по имени
        cursor = conn.cursor()
        
        # Проверяем, что таблица существует (защита от SQL injection)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({'error': f'Таблица "{table_name}" не найдена'}), 404
        
        # Получаем названия колонок
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns = [row[1] for row in cursor.fetchall()]
        
        # Формируем WHERE условие для поиска
        where_clause = ""
        search_params = []
        if search:
            # Ищем по всем текстовым колонкам
            search_conditions = []
            for col in columns:
                # Ищем по всем колонкам (имена колонок безопасны, т.к. получены из PRAGMA)
                # Используем экранирование через двойные кавычки для имен колонок
                search_conditions.append(f'CAST("{col}" AS TEXT) LIKE ?')
                search_params.append(f'%{search}%')
            
            if search_conditions:
                where_clause = "WHERE " + " OR ".join(search_conditions)
        
        # Получаем общее количество записей (с учетом поиска)
        count_query = f'SELECT COUNT(*) FROM "{table_name}" {where_clause}'
        cursor.execute(count_query, search_params)
        total_count = cursor.fetchone()[0]
        
        # Получаем данные с лимитом и смещением (с учетом поиска)
        data_query = f'SELECT * FROM "{table_name}" {where_clause} LIMIT ? OFFSET ?'
        cursor.execute(data_query, search_params + [limit, offset])
        rows = cursor.fetchall()
        
        # Преобразуем строки в словари
        data = []
        for row in rows:
            row_dict = {}
            for col in columns:
                value = row[col]
                # Обрабатываем специальные типы
                if isinstance(value, bytes):
                    value = value.hex()  # Бинарные данные в hex
                row_dict[col] = value
            data.append(row_dict)
        
        conn.close()
        
        return jsonify({
            'columns': columns,
            'data': data,
            'total_count': total_count,
            'limit': limit,
            'offset': offset,
            'has_more': (offset + len(data) < total_count),
            'search': search
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Запускаем Flask на порту 5000
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)

