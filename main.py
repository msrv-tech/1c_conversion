# -*- coding: utf-8 -*-
"""
Главный скрипт для синхронизации справочников между 1С и базой данных
Координирует работу модулей из папок IN и OUT
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

from tools.encoding_fix import fix_encoding

fix_encoding()

# Загружаем переменные окружения из .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv не установлен, используем системные переменные окружения

from export_stage import get_available_writers, load_from_db_to_1c
from load_stage import get_available_loaders, load_catalog_to_db
from process_stage import get_available_processors, process_catalog_to_db
from tools.logger import set_verbose, info_print, verbose_print, set_log_file, close_log_file
from tools.telegram_notifier import notify_catalog_export_completed


ALL_CATALOGS_TOKEN = "all"


def _collect_available_catalogs(base_dir: str) -> tuple[list[str], list[str], list[str], list[str]]:
    loaders = get_available_loaders(base_dir)
    writers = get_available_writers(base_dir)
    processors = get_available_processors(base_dir)

    catalogs = set()
    for loader in loaders:
        catalogs.add(loader.replace("_loader", ""))
    for writer in writers:
        catalogs.add(writer.replace("_writer", ""))
    for processor in processors:
        catalogs.add(processor.replace("_processor", ""))

    sorted_catalogs = sorted(catalogs)
    if sorted_catalogs:
        sorted_catalogs_with_all = [ALL_CATALOGS_TOKEN] + sorted_catalogs
    else:
        sorted_catalogs_with_all = [ALL_CATALOGS_TOKEN]

    return loaders, writers, processors, sorted_catalogs_with_all


def _resolve_db_root_path(path: Optional[str]) -> str:
    """
    Преобразует путь аргумента в директорию, где будут храниться БД справочников.
    Если передан путь к файлу, возвращается его директория.
    """
    if not path:
        raise ValueError("Не указан путь к базе данных SQLite.")

    abs_path = os.path.abspath(path)
    if abs_path.lower().endswith(".db"):
        dir_path = os.path.dirname(abs_path)
    else:
        dir_path = abs_path

    if not dir_path:
        dir_path = os.getcwd()

    os.makedirs(dir_path, exist_ok=True)
    return dir_path


def _build_catalog_db_path(root_dir: str, catalog_name: str, processed: bool = False) -> str:
    """
    Формирует путь к базе данных конкретного справочника.
    processed=True добавляет суффикс _processed перед расширением .db
    """
    suffix = "_processed" if processed else ""
    file_name = f"{catalog_name}{suffix}.db"
    return os.path.join(root_dir, file_name)




def main():
    """Основная функция"""
    base_dir = os.path.dirname(__file__)
    available_loaders, available_writers, available_processors, available_catalogs = _collect_available_catalogs(base_dir)

    parser = argparse.ArgumentParser(
        description='Синхронизация справочников между 1С и базой данных SQLite'
    )
    
    # Режимы работы
    parser.add_argument(
        '--import',
        dest='import_data',
        action='store_true',
        help='Загрузить данные из 1С источника в БД'
    )
    parser.add_argument(
        '--export',
        action='store_true',
        help='Загрузить данные из БД в 1С приемник'
    )
    parser.add_argument(
        '--process',
        dest='process_data',
        action='store_true',
        help='Обработать данные и сформировать отдельную базу данных'
    )
    parser.add_argument(
        '--fill-unfilled',
        action='store_true',
        help='Заполнить незаполненные ссылочные объекты из reference_objects.db'
    )
    parser.add_argument(
        '--fill-unfilled-catalog',
        type=str,
        help='Фильтр по латинскому названию справочника для --fill-unfilled (например, "managerial_contracts")'
    )
    
    parser.add_argument(
        '--catalog',
        type=str,
        default='contractors',
        choices=available_catalogs if available_catalogs else ['contractors'],
        help=f'Имя справочника (доступные: {", ".join(available_catalogs) if available_catalogs else "contractors"})'
    )
    
    # Параметры подключения
    parser.add_argument(
        '--source-1c',
        type=str,
        help='Путь к базе данных 1С (источник)'
    )
    parser.add_argument(
        '--target-1c',
        type=str,
        help='Путь к базе данных 1С (приемник)'
    )
    parser.add_argument(
        '--sqlite-db',
        type=str,
        default='BD',
        help='Путь к файлу базы данных SQLite (по умолчанию: BD)'
    )
    parser.add_argument(
        '--filters-db',
        type=str,
        help='Путь к базе данных с пользовательскими фильтрами (uuid) для выборочной загрузки'
    )
    parser.add_argument(
        '--processed-db',
        type=str,
        help='Путь к файлу обработанной базы данных (создается при выполнении этапа обработки)'
    )
    parser.add_argument(
        '--json-output',
        type=str,
        help='Путь к JSON-файлу с выгруженной структурой конфигурации'
    )
    parser.add_argument(
        '--mode',
        choices=['test', 'full', 'incremental'],
        default='test',
        help='Режим загрузки из 1С: test (первые 50 строк), full (полная выгрузка), incremental (полная выгрузка только отсутствующих справочников)',
    )
    parser.add_argument(
        '--metadata-db',
        type=str,
        default=os.getenv('SOURCE_METADATA', 'BD/upp_metadata.db'),
        help='Путь к базе метаданных для обновления типов (по умолчанию из SOURCE_METADATA или BD/upp_metadata.db)',
    )
    parser.add_argument(
        '--skip-type-update',
        action='store_true',
        help='Отключить автоматическое обновление типов полей после загрузки',
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Подробный вывод (без флага - краткий вывод)'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        help='Путь к файлу для записи логов (опционально)'
    )
    parser.add_argument(
        '--prod',
        action='store_true',
        help='Использовать продакшн базу приемника (TARGET_CONNECTION_STRING_PROD)'
    )
    args = parser.parse_args()
    
    load_mode = 'test' if args.mode == 'test' else 'full'
    
    # Значения по умолчанию для путей 1С из окружения
    if not args.source_1c:
        args.source_1c = os.getenv("SOURCE_1C") or "source"
    if not args.target_1c:
        args.target_1c = os.getenv("TARGET_1C") or "target"
    
    # Если указан --prod, используем продакшн базу приемника
    if args.prod:
        prod_connection_string = os.getenv("TARGET_CONNECTION_STRING_PROD")
        if not prod_connection_string:
            print("Ошибка: при использовании --prod необходимо указать TARGET_CONNECTION_STRING_PROD в .env файле")
            sys.exit(1)
        args.target_1c = prod_connection_string
        
        # Устанавливаем режим prod для БД reference_objects
        from tools.reference_objects import set_prod_mode
        set_prod_mode(True)

    # Устанавливаем режим логирования
    set_verbose(args.verbose)
    
    # Устанавливаем файл для логирования
    if args.log_file:
        # Если указан явно, используем его
        set_log_file(args.log_file)
    elif args.export:
        # Автоматически создаем файл лога при экспорте
        import datetime
        base_dir = os.path.dirname(__file__)
        logs_dir = os.path.join(base_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        
        # Формируем имя файла
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        catalog_part = args.catalog if args.catalog != ALL_CATALOGS_TOKEN else "all"
        prod_part = "_prod" if args.prod else ""
        log_filename = f"{catalog_part}_export{prod_part}_{timestamp}.log"
        log_file_path = os.path.join(logs_dir, log_filename)
        
        set_log_file(log_file_path)
    
    # Регистрируем обработчик для закрытия файла при завершении
    import atexit
    atexit.register(close_log_file)
    
    # Извлекаем имя справочника из имени модуля (убираем _loader или _writer)
    requested_catalog = args.catalog.replace('_loader', '').replace('_writer', '')
    catalog_name = requested_catalog
    multi_catalog_mode = requested_catalog == ALL_CATALOGS_TOKEN
    
    # Если не указан режим, используем загрузку из 1С в БД по умолчанию
    if not args.import_data and not args.export and not args.process_data and not args.fill_unfilled:
        args.import_data = True
    
    if args.import_data and not args.source_1c:
        print("Ошибка: укажите путь к базе 1С источника через --source-1c или переменную SOURCE_1C")
        sys.exit(1)

    if args.export and not args.target_1c:
        print("Ошибка: укажите путь к базе 1С приемника через --target-1c или переменную TARGET_1C")
        sys.exit(1)

    catalog_db_root = _resolve_db_root_path(args.sqlite_db)
    processed_db_root = _resolve_db_root_path(args.processed_db or catalog_db_root)

    def get_raw_db_path(catalog: str) -> str:
        return _build_catalog_db_path(catalog_db_root, catalog)

    def get_processed_db_path(catalog: str) -> str:
        return _build_catalog_db_path(processed_db_root, catalog, processed=True)

    loader_catalogs = {name.replace('_loader', '') for name in available_loaders}
    writer_catalogs = {name.replace('_writer', '') for name in available_writers}
    processor_catalogs = {name.replace('_processor', '') for name in available_processors}

    # Определяем список каталогов для обработки на каждом этапе
    if multi_catalog_mode:
        # Для режима 'all' исключаем configuration_structure (делается отдельно для маппинга)
        excluded_catalogs = {'configuration_structure'}
        filtered_loader_catalogs = loader_catalogs - excluded_catalogs
        filtered_processor_catalogs = processor_catalogs - excluded_catalogs
        filtered_writer_catalogs = writer_catalogs - excluded_catalogs
        
        # Для режима 'all' определяем каталоги для каждого этапа
        load_catalogs = sorted(filtered_loader_catalogs) if args.import_data else []
        process_catalogs = sorted(filtered_processor_catalogs) if args.process_data else []
        export_catalogs = sorted(filtered_writer_catalogs) if args.export else []
        
        # Если был этап загрузки, используем загруженные каталоги для обработки
        if args.import_data and args.process_data:
            # Используем пересечение загрузчиков и процессоров
            process_catalogs = sorted(filtered_loader_catalogs & filtered_processor_catalogs)
        
        # Если был этап обработки, используем обработанные каталоги для экспорта
        if args.process_data and args.export:
            # Используем пересечение процессоров и писателей
            export_catalogs = sorted(filtered_processor_catalogs & filtered_writer_catalogs)
        elif args.import_data and args.export:
            # Если обработки не было, но была загрузка, используем пересечение загрузчиков и писателей
            export_catalogs = sorted(filtered_loader_catalogs & filtered_writer_catalogs)
        
        if not load_catalogs and args.import_data:
            print("Ошибка: не найдено ни одного модуля загрузки.")
            sys.exit(1)
        if not process_catalogs and args.process_data:
            print("Ошибка: не найдено ни одного модуля обработки.")
            sys.exit(1)
        if not export_catalogs and args.export:
            print("Ошибка: не найдено ни одного модуля выгрузки.")
            sys.exit(1)
    else:
        # Для одного каталога
        if args.import_data and requested_catalog not in loader_catalogs:
            print(f"Ошибка: Модуль загрузки для справочника '{requested_catalog}' не найден")
            print(f"Доступные модули загрузки: {', '.join(sorted(loader_catalogs))}")
            sys.exit(1)
        if args.process_data and requested_catalog not in processor_catalogs:
            print(f"Ошибка: Модуль обработки для справочника '{requested_catalog}' не найден")
            print(f"Доступные модули обработки: {', '.join(sorted(processor_catalogs))}")
            sys.exit(1)
        if args.export and requested_catalog not in writer_catalogs:
            print(f"Ошибка: Модуль выгрузки для справочника '{requested_catalog}' не найден")
            print(f"Доступные модули выгрузки: {', '.join(sorted(writer_catalogs))}")
            sys.exit(1)
        
        load_catalogs = [requested_catalog] if args.import_data else []
        process_catalogs = [requested_catalog] if args.process_data else []
        export_catalogs = [requested_catalog] if args.export else []

    success = False
    
    # Этап загрузки
    if args.import_data:
        # Для загрузки метаданных конфигурации используем переменные окружения
        if requested_catalog == 'configuration_structure' or (multi_catalog_mode and 'configuration_structure' in load_catalogs):
            # Определяем источник (source или target) для выбора правильной переменной окружения
            if args.source_1c:
                # Проверяем, является ли это источником или приемником
                # Если в source_1c есть "target" или "приемник", используем TARGET_METADATA
                source_lower = str(args.source_1c).lower()
                if 'target' in source_lower or 'приемник' in source_lower or 'uh' in source_lower:
                    # Используем переменную окружения для метаданных приемника
                    args.sqlite_db = os.getenv('TARGET_METADATA', args.sqlite_db or 'CONF/uh_metadata.db')
                else:
                    # Используем переменную окружения для метаданных источника
                    args.sqlite_db = os.getenv('SOURCE_METADATA', args.sqlite_db or 'CONF/upp_metadata.db')
            else:
                # По умолчанию используем метаданные источника
                args.sqlite_db = os.getenv('SOURCE_METADATA', args.sqlite_db or 'CONF/upp_metadata.db')
        
        json_output_path = os.path.abspath(args.json_output) if args.json_output else None
        metadata_db_path = os.path.abspath(args.metadata_db) if args.metadata_db else None

        # Краткий вывод: подключение к источнику
        if not args.verbose:
            from tools.onec_connector import resolve_connection_string
            try:
                conn_string, _ = resolve_connection_string(args.source_1c)
                info_print(f"Подключение к Источнику: {conn_string}")
            except:
                info_print(f"Подключение к Источнику: {args.source_1c}")

        success = True
        for catalog_name in load_catalogs:
            verbose_print("\n" + "-" * 80)
            verbose_print(f"Обработка справочника: {catalog_name}")
            verbose_print("-" * 80)
            sqlite_db_file = args.sqlite_db if catalog_name == 'configuration_structure' else get_raw_db_path(catalog_name)
            if args.mode == 'incremental' and os.path.exists(sqlite_db_file):
                verbose_print(f"  [СКИП] Справочник '{catalog_name}' уже загружен ({sqlite_db_file})")
                continue
            catalog_success = load_catalog_to_db(
                base_dir=base_dir,
                catalog_name=catalog_name,
                source_db_path=args.source_1c,
                sqlite_db_file=sqlite_db_file,
                mode=load_mode,
                process_func=None,
                filters_db=args.filters_db,
                json_output_path=json_output_path,
                metadata_db=metadata_db_path,
                skip_type_update=args.skip_type_update,
            )
            if catalog_success:
                # Краткий вывод: загружен справочник
                if not args.verbose:
                    import sqlite3
                    try:
                        conn = sqlite3.connect(sqlite_db_file)
                        cursor = conn.cursor()
                        # Получаем имя таблицы из имени справочника
                        table_name = catalog_name.replace('_loader', '')
                        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                        count = cursor.fetchone()[0]
                        conn.close()
                        info_print(f"  Загружен справочник '{catalog_name}' в БД ({sqlite_db_file}) - {count} записей")
                    except:
                        info_print(f"  Загружен справочник '{catalog_name}' в БД ({sqlite_db_file})")
            else:
                success = False
                if not multi_catalog_mode:
                    break

        if multi_catalog_mode and not (args.process_data or args.export):
            # Если только загрузка в режиме all, завершаем здесь
            if success:
                print("\nВсе справочники загружены успешно.")
                print("\n" + "=" * 80)
                print("Операция завершена успешно!")
                print("=" * 80)
                sys.exit(0)
            else:
                print("\nЗагрузка остановлена из-за ошибки.")
                print("\n" + "=" * 80)
                print("Операция завершена с ошибками!")
                print("=" * 80)
                sys.exit(1)
    else:
        success = True

    # Этап обработки
    # Инициализируем флаг успешности обработки
    # Если обработки не будет, считаем её успешной для возможности экспорта (экспорт из существующей БД)
    if args.process_data:
        # Инициализируем как None - будет установлен внутри блока
        process_success = None
        processed_count = 0
        failed_count = 0
        if success or not args.import_data:
            process_success = False  # Начнем с False, станет True если хотя бы один справочник обработается успешно
            for catalog_name in process_catalogs:
                verbose_print("\n" + "-" * 80)
                verbose_print(f"Обработка справочника: {catalog_name}")
                verbose_print("-" * 80)
                source_db_path = get_raw_db_path(catalog_name)
                processed_db_path = get_processed_db_path(catalog_name)
                catalog_success = process_catalog_to_db(
                    base_dir,
                    catalog_name,
                    source_db_path,
                    processed_db_path,
                )
                if catalog_success:
                    processed_count += 1
                    process_success = True  # Хотя бы один справочник обработался успешно
                    # Краткий вывод: обработан справочник
                    if not args.verbose:
                        import sqlite3
                        try:
                            db_for_stats = processed_db_path if os.path.exists(processed_db_path) else source_db_path
                            conn = sqlite3.connect(db_for_stats)
                            cursor = conn.cursor()
                            table_name = catalog_name.replace('_processor', '')
                            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                            count = cursor.fetchone()[0]
                            conn.close()
                            info_print(f"  Обработан справочник '{catalog_name}' - {count} записей (БД: {db_for_stats})")
                        except:
                            info_print(f"  Обработан справочник '{catalog_name}'")
                else:
                    failed_count += 1
                    if not multi_catalog_mode:
                        break
            
            # Выводим статистику обработки (только в verbose режиме)
            verbose_print(f"\nСтатистика обработки: успешно - {processed_count}, с ошибками - {failed_count}")
            
            # Обновляем общий флаг success только если был этап обработки
            # В режиме all считаем успешным, если хотя бы один справочник обработался
            if args.process_data:
                success = process_success if multi_catalog_mode else (processed_count > 0 and failed_count == 0)
        else:
            print("\nПропущена обработка из-за ошибки на предыдущем этапе")
            process_success = False
            if args.process_data:
                success = False
    else:
        # Если не было этапа обработки, но есть обработанная БД - считаем обработку успешной для экспорта
        process_success = True
    
    # Этап экспорта
    if args.export:
        if not os.path.isdir(processed_db_root):
            print(f"Ошибка: директория обработанных БД не найдена: {processed_db_root}")
            print("Экспорт возможен только после обработки справочников.")
            sys.exit(1)

        # Разрешаем экспорт, если:
        # 1. Был этап обработки и он завершился успешно (process_success = True)
        # 2. ИЛИ не было этапа обработки, но есть обработанные БД (экспорт из существующих файлов)
        can_export = False
        if args.process_data:
            if process_success is not None:
                can_export = process_success
            else:
                can_export = False
        else:
            can_export = True
        
        if can_export:
            # Получаем читаемое представление базы приемника для уведомлений
            from tools.onec_connector import resolve_connection_string
            target_db_display = args.target_1c
            try:
                conn_string, _ = resolve_connection_string(args.target_1c)
                target_db_display = conn_string
            except:
                pass
            
            # Краткий вывод: подключение к приемнику
            if not args.verbose:
                info_print(f"Подключение к Приемнику: {target_db_display}")
            
            verbose_print("\n" + "=" * 80)
            verbose_print("НАЧАЛО ЭКСПОРТА В 1С ПРИЕМНИК")
            verbose_print("=" * 80)
            verbose_print(f"Каталог обработанных БД: {processed_db_root}")
            verbose_print(f"Количество справочников для экспорта: {len(export_catalogs)}")
            verbose_print("=" * 80)
            success = True
            for idx, catalog_name in enumerate(export_catalogs, 1):
                export_db_file = get_processed_db_path(catalog_name)
                if not os.path.exists(export_db_file):
                    message = f"Обработанная база данных не найдена: {export_db_file}"
                    verbose_print(f"✗ {message}")
                    if not args.verbose:
                        info_print(f"  ✗ {message}")
                    success = False
                    if not multi_catalog_mode:
                        break
                    else:
                        continue

                verbose_print("\n" + "-" * 80)
                verbose_print(f"[{idx}/{len(export_catalogs)}] Экспорт справочника: {catalog_name}")
                verbose_print("-" * 80)
                verbose_print(f"Начинаем экспорт справочника '{catalog_name}'...")
                catalog_success = load_from_db_to_1c(
                    base_dir,
                    catalog_name,
                    export_db_file,
                    args.target_1c,
                    None,
                    mode=load_mode,
                )
                if catalog_success:
                    verbose_print(f"✓ Справочник '{catalog_name}' успешно экспортирован")
                    # Краткий вывод: экспортирован справочник
                    record_count = None
                    if not args.verbose:
                        import sqlite3
                        try:
                            conn = sqlite3.connect(export_db_file)
                            cursor = conn.cursor()
                            table_name = catalog_name.replace('_writer', '')
                            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                            record_count = cursor.fetchone()[0]
                            conn.close()
                            info_print(f"  Экспортирован справочник '{catalog_name}' в приемник - {record_count} записей (БД: {export_db_file})")
                        except:
                            info_print(f"  Экспортирован справочник '{catalog_name}' в приемник")
                    else:
                        # В verbose режиме тоже получаем количество записей для уведомления
                        import sqlite3
                        try:
                            conn = sqlite3.connect(export_db_file)
                            cursor = conn.cursor()
                            table_name = catalog_name.replace('_writer', '')
                            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                            record_count = cursor.fetchone()[0]
                            conn.close()
                        except:
                            pass
                    
                    # Отправляем уведомление об успешном экспорте
                    notify_catalog_export_completed(catalog_name, success=True, record_count=record_count, target_db=target_db_display)
                else:
                    verbose_print(f"✗ Ошибка при экспорте справочника '{catalog_name}'")
                    if not args.verbose:
                        info_print(f"  ✗ Ошибка экспорта справочника '{catalog_name}'")
                    # Отправляем уведомление об ошибке экспорта
                    notify_catalog_export_completed(catalog_name, success=False, target_db=target_db_display)
                    success = False
                    if not multi_catalog_mode:
                        break
        else:
            print("\nПропущена загрузка в 1С из-за ошибки на предыдущем этапе")
            success = False
    
    # Этап заполнения незаполненных ссылочных объектов
    if args.fill_unfilled:
        from tools.fill_unfilled_references import fill_unfilled_references
        from tools.reference_objects import get_reference_objects_db_path
        
        if not os.path.isdir(processed_db_root):
            print("Ошибка: директория обработанных БД не найдена")
            print("Для заполнения незаполненных объектов требуется обработанная БД")
            sys.exit(1)
        
        if not args.target_1c:
            print("Ошибка: укажите путь к базе 1С приемника через параметр --target-1c")
            sys.exit(1)
        
        # Определяем путь к БД маппинга
        mapping_db_path = os.path.join(os.path.dirname(__file__), "CONF", "type_mapping.db")
        
        fill_success = fill_unfilled_references(
            processed_db_root,
            args.target_1c,
            None,  # Используется БД по умолчанию
            None,  # Используется маппинг по умолчанию
            None,  # Фильтр по типу (удален)
            mapping_db_path,  # Путь к БД маппинга
            args.fill_unfilled_catalog  # Фильтр по catalog_name (если указан)
        )
        
        # Сбрасываем режим prod перед завершением
        if args.prod:
            from tools.reference_objects import set_prod_mode
            set_prod_mode(False)
        
        if fill_success:
            print("\n" + "=" * 80)
            print("Заполнение незаполненных объектов завершено успешно!")
            print("=" * 80)
            sys.exit(0)
        else:
            print("\n" + "=" * 80)
            print("Заполнение незаполненных объектов завершено с ошибками!")
            print("=" * 80)
            sys.exit(1)
    
    # Сбрасываем режим prod перед завершением
    if args.prod:
        from tools.reference_objects import set_prod_mode
        set_prod_mode(False)
    
    if success:
        print("\n" + "=" * 80)
        print("Операция завершена успешно!")
        print("=" * 80)
        sys.exit(0)
    else:
        print("\n" + "=" * 80)
        print("Операция завершена с ошибками!")
        print("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    main()

