# -*- coding: utf-8 -*-
"""
Модуль для работы с базой данных 1С через COM.

Содержит общие процедуры, используемые модулями IN/ и OUT/:
- выбор и инициализация COM-коннектора;
- построение строки подключения;
- безопасная работа с COM-объектами и метаданными.
"""

import json
from typing import Dict, Iterable, Optional, Sequence, Tuple

import sqlite3

from tools.encoding_fix import fix_encoding

fix_encoding()

import win32com.client

DEFAULT_COM_PROGIDS: Tuple[str, ...] = (
    "V83.COMConnector",
    "V82.COMConnector",
)

# Кэш для поиска объектов по UUID
# Ключ: (uuid_value, type_name), Значение: ссылка на объект
_reference_by_uuid_cache = {}


def clear_reference_by_uuid_cache():
    """Очищает кэш поиска по UUID. Полезно при длительных операциях."""
    global _reference_by_uuid_cache
    _reference_by_uuid_cache.clear()


def call_if_callable(value, *args, **kwargs):
    """Вызывает объект, если он вызваемый, иначе возвращает как есть."""
    if callable(value) and not isinstance(value, win32com.client.CDispatch):
        try:
            return value(*args, **kwargs)
        except Exception:
            return None
    return value


def safe_getattr(obj, attr_name: str, default=None):
    """Безопасно получает атрибут COM-объекта."""
    try:
        return getattr(obj, attr_name)
    except Exception:
        return default


def _xml_type_name(com_object, value) -> str:
    type_info = None

    xml_type_method = safe_getattr(com_object, "XMLТип", None)
    if callable(xml_type_method):
        try:
            type_info = xml_type_method(value)
        except Exception:
            type_info = None

    if type_info is None:
        xml_type_value_method = safe_getattr(com_object, "XMLТипЗнч", None)
        if callable(xml_type_value_method):
            try:
                type_info = xml_type_value_method(value)
            except Exception:
                type_info = None

    type_info = call_if_callable(type_info)
    if type_info is None:
        return ""

    name = safe_getattr(type_info, "ИмяТипа", None)
    name = call_if_callable(name)
    if not name:
        name = safe_getattr(type_info, "Имя", None)
        name = call_if_callable(name)
    if not name:
        name = safe_getattr(type_info, "Name", None)
        name = call_if_callable(name)

    if name:
        try:
            name_str = str(name)
        except Exception:
            name_str = None
        if name_str:
            replacements = {
                "CatalogRef.": "Справочник.",
                "EnumRef.": "Перечисление.",
            }
            for old, new in replacements.items():
                if name_str.startswith(old):
                    name_str = name_str.replace(old, new, 1)
            return name_str

    try:
        return "" if type_info is None else str(type_info)
    except Exception:
        return ""


def _get_enum_value_string(com_object, enum_value, type_name: str) -> str:
    """
    Получает строковое представление значения перечисления в формате Перечисление.ИмяПеречисления.Значение.
    
    Args:
        com_object: COM-объект подключения к 1С
        enum_value: Значение перечисления (COM-объект)
        type_name: Тип перечисления (например, "Перечисление.ВидыОперацийРКО")
        
    Returns:
        Строка вида "Перечисление.ИмяПеречисления.Значение" или пустая строка
    """
    if not enum_value or not type_name or not type_name.startswith("Перечисление."):
        return ""
    
    try:
        # Извлекаем имя перечисления из типа (например, "Перечисление.ВидыОперацийРКО" -> "ВидыОперацийРКО")
        enum_name = type_name.replace("Перечисление.", "")
        
        # Получаем объект перечисления
        enums = com_object.Перечисления
        enum_ref = safe_getattr(enums, enum_name, None)
        if not enum_ref:
            return ""
        
        # Получаем индекс значения перечисления
        # ИндексЗначения = Перечисления.ВидыОперацийРКО.Индекс(ЗначениеПеречисления)
        index_method = safe_getattr(enum_ref, "Индекс", None)
        if not index_method:
            return ""
        
        try:
            value_index = call_if_callable(index_method, enum_value)
            if value_index is None:
                return ""
        except Exception:
            return ""
        
        # Получаем имя значения из метаданных
        # Метаданные.Перечисления.ВидыОперацийРКО.ЗначенияПеречисления[ИндексЗначения].Имя
        metadata = com_object.Метаданные
        metadata_enums = safe_getattr(metadata, "Перечисления", None)
        if not metadata_enums:
            return ""
        
        enum_metadata = safe_getattr(metadata_enums, enum_name, None)
        if not enum_metadata:
            return ""
        
        values_metadata = safe_getattr(enum_metadata, "ЗначенияПеречисления", None)
        if not values_metadata:
            return ""
        
        # Получаем значение по индексу
        try:
            value_metadata = values_metadata.Get(value_index)
            value_name = safe_getattr(value_metadata, "Имя", None)
            value_name = call_if_callable(value_name)
            if value_name:
                # Формируем строку Перечисление.ИмяПеречисления.Значение
                return f"{type_name}.{value_name}"
        except Exception:
            return ""
        
    except Exception:
        pass
    
    return ""


def _stringify_query_value(com_object, value, column_name: str) -> str:
    if value is None:
        return ""

    if column_name.endswith("_Тип"):
        type_name = _xml_type_name(com_object, value)
        if type_name:
            return type_name

    # Специальная обработка дат - PyTime объекты из 1С
    # Извлекаем дату-время напрямую из атрибутов, чтобы избежать проблем с часовыми поясами
    # PyTime объекты имеют атрибуты year, month, day, hour, minute, second
    # ВАЖНО: Проверяем PyTime ДО проверки _oleobj_, так как PyTime тоже имеет _oleobj_
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        try:
            # Проверяем, что это действительно PyTime (имеет атрибуты времени)
            # и не является обычным COM объектом
            year = int(value.year)
            month = int(value.month)
            day = int(value.day)
            
            # Если есть время, извлекаем его тоже
            if hasattr(value, "hour") and hasattr(value, "minute"):
                hour = int(value.hour)
                minute = int(value.minute)
                second = int(value.second) if hasattr(value, "second") else 0
                # Сохраняем в формате YYYY-MM-DD HH:MM:SS без timezone
                return f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
            else:
                # Только дата без времени
                return f"{year:04d}-{month:02d}-{day:02d}"
        except (ValueError, AttributeError, TypeError):
            # Если не удалось извлечь через атрибуты, пробуем через str()
            pass

    if hasattr(value, "_oleobj_"):
        try:
            text_value = str(value)
        except Exception:
            text_value = ""

        if text_value and "<COMObject" not in text_value:
            return text_value

        if column_name.endswith("_Тип"):
            return _xml_type_name(com_object, value)

        return ""

    try:
        return str(value)
    except Exception:
        return ""


def get_com_connector(progids: Optional[Sequence[str]] = None):
    """
    Инициализирует COM-коннектор 1С, перебирая переданные ProgID.
    Возвращает кортеж (коннектор, использованный ProgID) или возбуждает исключение.
    
    Автоматически инициализирует COM для текущего потока, если это необходимо.
    """
    # Пытаемся инициализировать COM для текущего потока (если еще не инициализирован)
    try:
        import pythoncom
        # Пытаемся инициализировать COM
        # Если COM уже инициализирован, CoInitialize вернет S_FALSE (0x00000001), но не вызовет исключение
        # Если не инициализирован, инициализирует и вернет S_OK (0x00000000)
        try:
            result = pythoncom.CoInitialize()
            # S_FALSE (0x00000001) означает, что COM уже был инициализирован - это нормально
            # S_OK (0x00000000) означает успешную инициализацию
        except pythoncom.com_error as e:
            # Если COM уже инициализирован в другом режиме (например, COINIT_APARTMENTTHREADED vs COINIT_MULTITHREADED)
            # может возникнуть ошибка - игнорируем её
            if e.hresult != -2147221008:  # CO_E_ALREADYINITIALIZED
                raise
    except ImportError:
        # pythoncom не доступен (маловероятно, но на всякий случай)
        pass
    except Exception:
        # Другая ошибка - продолжаем, возможно COM уже инициализирован
        pass
    
    errors = []
    for progid in progids or DEFAULT_COM_PROGIDS:
        try:
            connector = win32com.client.Dispatch(progid)
            from tools.logger import verbose_print
            verbose_print(f"Используется COM-коннектор: {progid}")
            return connector, progid
        except Exception as exc:
            errors.append((progid, exc))

    messages = "; ".join(f"{pid}: {err}" for pid, err in errors)
    raise RuntimeError(f"Не удалось создать COM-коннектор. Детали: {messages}")


def resolve_connection_string(db_path_or_config: str) -> Tuple[str, str]:
    """
    Определяет строку подключения к базе 1С.

    Возвращает кортеж (connection_string, human_readable_name).
    """
    # Имя конфигурации из config.py
    if db_path_or_config in ("source", "target"):
        try:
            from tools.config import DATABASE_CONFIGS
        except ImportError as exc:
            raise RuntimeError("Не удалось импортировать tools.config") from exc

        try:
            config = DATABASE_CONFIGS[db_path_or_config]
        except KeyError as exc:
            raise RuntimeError(
                f"Конфигурация '{db_path_or_config}' не найдена в config.py"
            ) from exc

        connection_string = config["connection_string"]
        description = f"{config['name']} ({config['description']})"
        return connection_string, description

    # Строка подключения серверной базы
    if "Srvr=" in db_path_or_config or "Ref=" in db_path_or_config:
        return db_path_or_config, "Пользовательская строка подключения"

    # Путь к файловой базе
    connection_string = f'File="{db_path_or_config}";Usr=;Pwd=;'
    return connection_string, f"Файловая база: {db_path_or_config}"


def connect_to_1c(db_path_or_config: str):
    """
    Подключается к базе данных 1С через COM.

    Args:
        db_path_or_config: имя конфигурации из config.py, строка подключения
            или путь к файловой базе.

    Returns:
        COM-объект соединения или None при ошибке.
    """
    try:
        connector, progid = get_com_connector()
    except Exception as exc:
        print(f"Ошибка создания COM-коннектора: {exc}")
        print("Убедитесь, что установлена платформа 1С:Предприятие.")
        return None

    try:
        connection_string, description = resolve_connection_string(db_path_or_config)
        from tools.logger import verbose_print
        verbose_print(f"Используется конфигурация: {description}")
    except Exception as exc:
        print(f"Ошибка подготовки строки подключения: {exc}")
        return None

    try:
        com_object = connector.Connect(connection_string)
        from tools.logger import verbose_print
        verbose_print("Подключение успешно!")
        return com_object
    except Exception as exc:
        print(f"Ошибка подключения к базе данных ({progid}): {exc}")
        return None


def create_query(com_object, query_text: str):
    """Создаёт объект запроса 1С с переданным текстом."""
    query = com_object.NewObject("Запрос")
    query.Текст = query_text
    return query


def execute_query(
    com_object,
    query_text: str,
    column_names,
    params: Optional[dict] = None,
    reference_attr: Optional[str] = None,
    uuid_column: Optional[str] = None,
    reference_columns: Optional[Iterable[str]] = None,
):
    """
    Выполняет запрос 1С и возвращает данные в виде списка словарей.
    """
    query = create_query(com_object, query_text)
    if params:
        for name, value in params.items():
            query.УстановитьПараметр(name, value)

    result = query.Выполнить()
    selection = result.Выбрать()

    rows = []
    references = []
    reference_columns = set(reference_columns or [])
    reference_rows = [] if reference_columns else None

    # Собираем информацию о типах колонок (для обработки перечислений)
    type_columns = {}
    for column_name in column_names:
        if column_name.endswith("_Тип"):
            base_column = column_name[:-4]  # Убираем "_Тип"
            type_columns[base_column] = column_name

    while selection.Следующий():
        row_dict = {}
        raw_refs = {}

        getter = safe_getattr(selection, "Получить", None)
        get_item = safe_getattr(selection, "__getitem__", None)

        # Сначала собираем все значения, включая типы
        temp_values = {}
        temp_types = {}
        
        for column_name in column_names:
            value = None
            if column_name in reference_columns and hasattr(selection, "Get"):
                try:
                    value = selection.Get(column_name)
                except Exception:
                    value = None
            if value is None:
                value = safe_getattr(selection, column_name, None)
            if callable(value) and not isinstance(value, win32com.client.CDispatch):
                value = None

            if value is None and callable(getter):
                try:
                    value = getter(column_name)
                except Exception:
                    value = None

            if value is None and callable(get_item):
                try:
                    value = get_item(column_name)
                except Exception:
                    value = None

            if column_name in reference_columns and hasattr(value, "_oleobj_"):
                raw_refs[column_name] = value

            # Сохраняем значение и тип
            temp_values[column_name] = value
            if column_name.endswith("_Тип"):
                type_name = _stringify_query_value(com_object, value, column_name)
                if type_name:
                    base_column = column_name[:-4]
                    temp_types[base_column] = type_name

        # Обрабатываем колонки с учетом типов
        for column_name in column_names:
            value = temp_values.get(column_name)
            
            # Проверяем, является ли поле перечислением
            if column_name in type_columns:
                type_name = temp_types.get(column_name, "")
                if type_name.startswith("Перечисление."):
                    # Это перечисление - формируем строку Перечисление.ИмяПеречисления.Значение
                    enum_value = _get_enum_value_string(com_object, value, type_name)
                    if enum_value:
                        row_dict[column_name] = enum_value
                    else:
                        row_dict[column_name] = _stringify_query_value(com_object, value, column_name)
                else:
                    # Не перечисление - обычная обработка
                    row_dict[column_name] = _stringify_query_value(com_object, value, column_name)
            else:
                # Обычная колонка
                row_dict[column_name] = _stringify_query_value(com_object, value, column_name)

        rows.append(row_dict)
        if reference_rows is not None:
            reference_rows.append(raw_refs)

        if reference_attr:
            reference_value = safe_getattr(selection, reference_attr, None)
            if callable(reference_value) and not isinstance(
                reference_value, win32com.client.CDispatch
            ):
                reference_value = None
            if reference_value is None:
                getter = safe_getattr(selection, "Get", None)
                if callable(getter):
                    try:
                        reference_value = getter(reference_attr)
                    except Exception:
                        reference_value = None
            if reference_value is None:
                rus_getter = safe_getattr(selection, "Получить", None)
                if callable(rus_getter):
                    try:
                        reference_value = rus_getter(reference_attr)
                    except Exception:
                        reference_value = None
            references.append(
                {
                    "uuid": row_dict.get(uuid_column) if uuid_column else None,
                    "reference": reference_value,
                }
            )

    if reference_attr and reference_rows is not None:
        return rows, references, reference_rows
    if reference_attr:
        return rows, references
    if reference_rows is not None:
        return rows, reference_rows
    return rows


def execute_batch_query(
    com_object,
    batch_query_text: str,
    column_names,
    params: Optional[dict] = None,
    reference_attr: Optional[str] = None,
    uuid_column: Optional[str] = None,
    reference_columns: Optional[Iterable[str]] = None,
):
    """
    Выполняет пакетный запрос 1С (несколько запросов, разделенных точкой с запятой)
    и возвращает данные последнего запроса в виде списка словарей.
    """
    query = create_query(com_object, batch_query_text)
    if params:
        for name, value in params.items():
            query.УстановитьПараметр(name, value)

    result = query.Выполнить()
    selection = result.Выбрать()

    rows = []
    references = []
    reference_columns = set(reference_columns or [])
    reference_rows = [] if reference_columns else None

    # Собираем информацию о типах колонок (для обработки перечислений)
    type_columns = {}
    for column_name in column_names:
        if column_name.endswith("_Тип"):
            base_column = column_name[:-4]  # Убираем "_Тип"
            type_columns[base_column] = column_name

    while selection.Следующий():
        row_dict = {}
        raw_refs = {}

        getter = safe_getattr(selection, "Получить", None)
        get_item = safe_getattr(selection, "__getitem__", None)

        # Сначала собираем все значения, включая типы
        temp_values = {}
        temp_types = {}
        
        for column_name in column_names:
            value = None
            if column_name in reference_columns and hasattr(selection, "Get"):
                try:
                    value = selection.Get(column_name)
                except Exception:
                    value = None
            if value is None:
                value = safe_getattr(selection, column_name, None)
            if callable(value) and not isinstance(value, win32com.client.CDispatch):
                value = None

            if value is None and callable(getter):
                try:
                    value = getter(column_name)
                except Exception:
                    value = None

            if value is None and callable(get_item):
                try:
                    value = get_item(column_name)
                except Exception:
                    value = None

            if column_name in reference_columns and hasattr(value, "_oleobj_"):
                raw_refs[column_name] = value

            # Сохраняем значение и тип
            temp_values[column_name] = value
            if column_name.endswith("_Тип"):
                type_name = _stringify_query_value(com_object, value, column_name)
                if type_name:
                    base_column = column_name[:-4]
                    temp_types[base_column] = type_name

        # Обрабатываем колонки с учетом типов
        for column_name in column_names:
            value = temp_values.get(column_name)
            
            # Проверяем, является ли поле перечислением
            if column_name in type_columns:
                type_name = temp_types.get(column_name, "")
                if type_name.startswith("Перечисление."):
                    # Это перечисление - формируем строку Перечисление.ИмяПеречисления.Значение
                    enum_value = _get_enum_value_string(com_object, value, type_name)
                    if enum_value:
                        row_dict[column_name] = enum_value
                    else:
                        row_dict[column_name] = _stringify_query_value(com_object, value, column_name)
                else:
                    # Не перечисление - обычная обработка
                    row_dict[column_name] = _stringify_query_value(com_object, value, column_name)
            else:
                # Обычная колонка
                row_dict[column_name] = _stringify_query_value(com_object, value, column_name)

        rows.append(row_dict)
        if reference_rows is not None:
            reference_rows.append(raw_refs)

        if reference_attr:
            reference_value = safe_getattr(selection, reference_attr, None)
            if callable(reference_value) and not isinstance(
                reference_value, win32com.client.CDispatch
            ):
                reference_value = None
            if reference_value is None:
                getter = safe_getattr(selection, "Get", None)
                if callable(getter):
                    try:
                        reference_value = getter(reference_attr)
                    except Exception:
                        reference_value = None
            if reference_value is None:
                rus_getter = safe_getattr(selection, "Получить", None)
                if callable(rus_getter):
                    try:
                        reference_value = rus_getter(reference_attr)
                    except Exception:
                        reference_value = None
            references.append(
                {
                    "uuid": row_dict.get(uuid_column) if uuid_column else None,
                    "reference": reference_value,
                }
            )

    if reference_attr and reference_rows is not None:
        return rows, references, reference_rows
    if reference_attr:
        return rows, references
    if reference_rows is not None:
        return rows, reference_rows
    return rows


def ensure_table_schema(
    connection: sqlite3.Connection,
    table_name: str,
    columns: Iterable[str],
    base_columns: Dict[str, str],
) -> None:
    """Создает таблицу и недостающие колонки."""
    cursor = connection.cursor()

    column_defs = [
        f'"{name}" {definition}' for name, definition in base_columns.items()
    ]

    for column in columns:
        if column in base_columns:
            continue
        column_defs.append(f'"{column}" TEXT')

    create_sql = (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n    '
        + ",\n    ".join(column_defs)
        + "\n)"
    )
    cursor.execute(create_sql)

    cursor.execute(f'PRAGMA table_info("{table_name}")')
    existing_columns = {row[1] for row in cursor.fetchall()}

    # Ensure base columns exist (if table already created earlier without them)
    for name, definition in base_columns.items():
        if name in existing_columns:
            continue
        cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{name}" {definition}')

    for column in columns:
        if column in existing_columns or column in base_columns:
            continue
        cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{column}" TEXT')

    connection.commit()
    cursor.close()


def upsert_rows(
    connection: sqlite3.Connection,
    table_name: str,
    rows,
    base_columns: Dict[str, str],
) -> int:
    """Вставляет или обновляет строки в таблице SQLite."""
    if not rows:
        return 0

    columns_order = list(base_columns.keys())
    for row in rows:
        for key in row.keys():
            # Исключаем служебные колонки (_Представление, _UUID, _Тип)
            # так как данные уже хранятся в JSON в основных полях
            if key.endswith("_Представление") or key.endswith("_UUID") or key.endswith("_Тип"):
                continue
            if key not in columns_order:
                columns_order.append(key)

    ensure_table_schema(connection, table_name, columns_order, base_columns)

    insert_columns = [col for col in columns_order]
    placeholders = ", ".join(["?"] * len(insert_columns))
    columns_sql = ", ".join([f'"{col}"' for col in insert_columns])

    conflict_clause = ""
    if "uuid" in base_columns and base_columns["uuid"].upper().startswith("TEXT PRIMARY KEY"):
        conflict_clause = (
            " ON CONFLICT(uuid) DO UPDATE SET "
            + ", ".join(
                [
                    f'"{col}" = excluded."{col}"'
                    for col in insert_columns
                    if col != "uuid"
                ]
            )
        )

    insert_sql = (
        f'INSERT INTO "{table_name}" ({columns_sql}) '
        f"VALUES ({placeholders})"
        + conflict_clause
    )

    cursor = connection.cursor()
    saved = 0
    for row in rows:
        values = [row.get(col) for col in insert_columns]
        try:
            cursor.execute(insert_sql, values)
            saved += 1
        except sqlite3.DatabaseError as error:
            print(f'Ошибка сохранения строки в "{table_name}": {error}')

    connection.commit()
    cursor.close()
    return saved


def find_object_by_uuid(com_object, uuid_value: str, type_name: str):
    """
    Универсальная функция поиска объекта по UUID через ПолучитьСсылку.
    Использует кэш для избежания повторных запросов к базе.
    
    Работает для справочников, документов и других типов объектов 1С.
    Использует метод ПолучитьСсылку напрямую, как в примере:
    СправочникСсылка = Справочники.ИмяСправочника.ПолучитьСсылку(Новый УникальныйИдентификатор(УИД));
    
    Args:
        com_object: COM-объект подключения к 1С
        uuid_value: UUID объекта в виде строки (например, "a763cfbb-f94f-4c67-8e13-0e96a3a7f353")
        type_name: Тип объекта (например, "Справочник.Контрагенты", "Документ.РеализацияТоваровУслуг")
        
    Returns:
        Ссылка на объект или None, если не найден
        
    Example:
        ref = find_object_by_uuid(com_object, "a763cfbb-f94f-4c67-8e13-0e96a3a7f353", "Справочник.Контрагенты")
        if ref:
            item = ref.ПолучитьОбъект()
    """
    if not uuid_value or uuid_value == "00000000-0000-0000-0000-000000000000":
        return None
    
    # Проверяем кэш
    cache_key = (uuid_value, type_name)
    if cache_key in _reference_by_uuid_cache:
        cached_ref = _reference_by_uuid_cache[cache_key]
        # Проверяем, что ссылка еще валидна
        try:
            # Простая проверка - пытаемся получить объект
            test_obj = cached_ref.ПолучитьОбъект()
            if test_obj:
                return cached_ref
        except Exception:
            # Если ссылка невалидна, удаляем из кэша и продолжаем поиск
            del _reference_by_uuid_cache[cache_key]
    
    try:
        # Создаем UUID объект из строки через NewObject
        # В 1С: Новый УникальныйИдентификатор(УИД)
        # Через COM: com_object.NewObject("УникальныйИдентификатор", uuid_value)
        uuid_obj = com_object.NewObject("УникальныйИдентификатор", uuid_value)
        
        # Определяем тип объекта и используем ПолучитьСсылку напрямую
        if type_name.startswith("Справочник."):
            catalog_name = type_name.replace("Справочник.", "")
            catalogs = com_object.Справочники
            catalog_ref = safe_getattr(catalogs, catalog_name, None)
            if catalog_ref:
                # Используем ПолучитьСсылку напрямую
                # СправочникСсылка = Справочники.ИмяСправочника.ПолучитьСсылку(Новый УникальныйИдентификатор(УИД));
                get_ref_method = safe_getattr(catalog_ref, "ПолучитьСсылку", None)
                if get_ref_method:
                    ref = call_if_callable(get_ref_method, uuid_obj)
                    # Проверяем, что ссылка не пустая (элемент существует)
                    if ref:
                        # Проверяем, что это действительно существующий элемент, а не пустая ссылка
                        try:
                            # Пробуем получить объект - если элемент не существует, будет ошибка
                            test_obj = ref.ПолучитьОбъект()
                            if test_obj:
                                # Сохраняем в кэш
                                _reference_by_uuid_cache[cache_key] = ref
                                return ref
                        except Exception:
                            # Элемент не существует
                            return None
        elif type_name.startswith("Документ."):
            document_name = type_name.replace("Документ.", "")
            documents = com_object.Документы
            document_ref = safe_getattr(documents, document_name, None)
            if document_ref:
                get_ref_method = safe_getattr(document_ref, "ПолучитьСсылку", None)
                if get_ref_method:
                    ref = call_if_callable(get_ref_method, uuid_obj)
                    if ref:
                        try:
                            test_obj = ref.ПолучитьОбъект()
                            if test_obj:
                                # Сохраняем в кэш
                                _reference_by_uuid_cache[cache_key] = ref
                                return ref
                        except Exception:
                            return None
        else:
            # Для других типов объектов (перечисления и т.д.) возвращаем None
            # Можно расширить при необходимости
            return None
                
    except Exception:
        # Если произошла ошибка, возвращаем None
        pass
    
    return None


def get_reference_uuid(com_object, reference) -> str:
    """Возвращает UUID ссылки 1С."""
    if reference is None:
        return ""

    try:
        query = com_object.NewObject("Запрос")
        query.Текст = """ВЫБРАТЬ
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(&Ref)) КАК UUID"""
        query.УстановитьПараметр("Ref", reference)
        result = query.Выполнить()
        selection = result.Выбрать()
        if selection.Следующий():
            value = None
            if hasattr(selection, "Get"):
                try:
                    value = selection.Get("UUID")
                except Exception:
                    value = None
            if value is None:
                value = safe_getattr(selection, "UUID", None)
            return "" if value is None else str(value)
    except Exception:
        pass

    return ""


def get_reference_type(reference) -> str:
    """
    Возвращает имя типа значения ссылки.
    """
    if reference is None:
        return ""

    try:
        metadata = safe_getattr(reference, "Метаданные", None)
        metadata = call_if_callable(metadata)
        if metadata:
            full_name = safe_getattr(metadata, "FullName", "")
            full_name = call_if_callable(full_name)
            if not full_name:
                full_name = safe_getattr(metadata, "Name", "")
                full_name = call_if_callable(full_name)
            if full_name:
                return str(full_name)
    except Exception:
        pass

    try:
        if hasattr(reference, "ПолучитьТип"):
            type_info = reference.ПолучитьТип()
        else:
            type_info = reference.ТИП()
        type_info = call_if_callable(type_info)
        if type_info is None:
            return ""
        name = safe_getattr(type_info, "Name", "")
        name = call_if_callable(name)
        return str(name) if name else str(type_info)
    except Exception:
        try:
            type_info = reference.ТИП()
            type_info = call_if_callable(type_info)
            name = safe_getattr(type_info, "Name", "")
            name = call_if_callable(name)
            return str(name) if name else str(type_info)
        except Exception:
            return ""


def describe_reference(com_object, reference) -> Optional[Dict[str, str]]:
    """Возвращает словарь с представлением, UUID и типом ссылки."""
    if reference is None:
        return None

    presentation = ""
    try:
        if hasattr(reference, "Представление"):
            presentation = reference.Представление()
        elif hasattr(reference, "Наименование"):
            presentation = reference.Наименование
        else:
            presentation = str(reference)
    except Exception:
        try:
            presentation = reference.Наименование
        except Exception:
            presentation = str(reference)

    return {
        "presentation": presentation,
        "uuid": get_reference_uuid(com_object, reference),
        "type": get_reference_type(reference),
    }


def build_reference_array(com_object, references):
    """Создаёт массив ссылок 1С из списка references."""
    array = com_object.NewObject("Массив")
    added = False
    for ref_info in references:
        reference = ref_info.get("reference")
        if reference:
            try:
                array.Добавить(reference)
                added = True
            except Exception:
                continue
    return array if added else None


def save_tabular_sections(
    com_object,
    connection: sqlite3.Connection,
    tabular_queries,
    references,
    base_columns: Optional[Dict[str, str]] = None,
):
    """
    Выполняет запросы табличных частей и сохраняет их в SQLite.
    """
    from tools.logger import verbose_print
    
    saved_counts: Dict[str, int] = {}
    if not tabular_queries:
        return saved_counts

    ref_array = build_reference_array(com_object, references)

    for section in tabular_queries:
        section_name = section["name"]
        table_name = section["table"]
        columns = section.get("columns", [])
        section_base_columns = section.get("base_columns")
        reference_columns = section.get("reference_columns") or []

        base = section_base_columns or base_columns or {"parent_uuid": "TEXT"}

        if not ref_array:
            if columns:
                ensure_table_schema(connection, table_name, columns, base)
            saved_counts[section_name] = 0
            continue

        try:
            rows = execute_query(
                com_object,
                section["query"],
                columns,
                params={"Ссылки": ref_array},
            )
        except Exception as e:
            # Если табличная часть не найдена в источнике, пропускаем её
            error_msg = str(e)
            if "Таблица не найдена" in error_msg or "не найдена" in error_msg.lower():
                verbose_print(f"    ⚠ Табличная часть '{section_name}' не найдена в источнике, пропускаем")
                saved_counts[section_name] = 0
                continue
            else:
                # Другие ошибки пробрасываем дальше
                raise

        # Исключаем служебные колонки из списка columns перед сохранением
        # так как данные уже хранятся в JSON в основных полях
        service_columns_to_remove = []
        if reference_columns:
            for column in reference_columns:
                service_columns_to_remove.extend([
                    f"{column}_Представление",
                    f"{column}_UUID",
                    f"{column}_Тип"
                ])
        
        # Удаляем служебные колонки из списка columns
        filtered_columns = [col for col in columns if col not in service_columns_to_remove]
        
        if reference_columns:
            for row in rows:
                for column in reference_columns:
                    presentation_key = f"{column}_Представление"
                    uuid_key = f"{column}_UUID"
                    type_key = f"{column}_Тип"
                    # Удаляем служебные поля из данных (если они есть)
                    presentation = row.pop(presentation_key, "")
                    uuid_value = row.pop(uuid_key, "")
                    type_value = row.pop(type_key, "")
                    if presentation or uuid_value or type_value:
                        row[column] = json.dumps(
                            {
                                "presentation": presentation,
                                "uuid": uuid_value,
                                "type": type_value,
                            },
                            ensure_ascii=False,
                        )
                    else:
                        row[column] = ""

        # Убеждаемся, что таблица создана (без служебных колонок)
        if filtered_columns:
            ensure_table_schema(connection, table_name, filtered_columns, base)
        
        if not rows:
            saved_counts[section_name] = 0
            continue
        
        # Если в запросе НомерСтроки = 1 для всех строк (как в запросах из других справочников),
        # нужно добавить правильную нумерацию
        if "НомерСтроки" in columns:
            # Группируем строки по parent_uuid и нумеруем их
            parent_uuid_idx = columns.index("parent_uuid") if "parent_uuid" in columns else -1
            номер_строки_idx = columns.index("НомерСтроки") if "НомерСтроки" in columns else -1
            
            if parent_uuid_idx >= 0 and номер_строки_idx >= 0:
                # Группируем по parent_uuid
                grouped_rows = {}
                for row in rows:
                    if isinstance(row, dict):
                        parent_uuid = row.get("parent_uuid", "")
                        if parent_uuid not in grouped_rows:
                            grouped_rows[parent_uuid] = []
                        grouped_rows[parent_uuid].append(row)
                    else:
                        parent_uuid = row[parent_uuid_idx] if parent_uuid_idx >= 0 else ""
                        if parent_uuid not in grouped_rows:
                            grouped_rows[parent_uuid] = []
                        grouped_rows[parent_uuid].append(row)
                
                # Нумеруем строки в каждой группе
                rows = []
                for parent_uuid, group_rows in grouped_rows.items():
                    for i, row in enumerate(group_rows, 1):
                        if isinstance(row, dict):
                            row["НомерСтроки"] = i
                        else:
                            # Преобразуем кортеж в список для изменения
                            row_list = list(row)
                            row_list[номер_строки_idx] = i
                            row = tuple(row_list)
                        rows.append(row)

        # Для табличных частей нужно очистить таблицу перед записью,
        # так как upsert_rows работает только для таблиц с PRIMARY KEY,
        # а у табличных частей может не быть PRIMARY KEY
        # Очищаем только записи для текущих parent_uuid
        if "parent_uuid" in base or any("parent_uuid" in col for col in columns):
            cursor = connection.cursor()
            # Собираем уникальные parent_uuid из загружаемых данных
            parent_uuids = set()
            for row in rows:
                if isinstance(row, dict):
                    parent_uuid = row.get("parent_uuid", "")
                else:
                    # Если row - это кортеж, нужно найти индекс parent_uuid
                    try:
                        parent_uuid_idx = columns.index("parent_uuid") if "parent_uuid" in columns else -1
                        parent_uuid = row[parent_uuid_idx] if parent_uuid_idx >= 0 else ""
                    except (ValueError, IndexError):
                        parent_uuid = ""
                
                if parent_uuid:
                    parent_uuids.add(parent_uuid)
            
            # Удаляем существующие записи для этих parent_uuid
            if parent_uuids:
                # Проверяем, что таблица существует
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,)
                )
                if cursor.fetchone():
                    cursor.execute(
                        "CREATE TEMP TABLE IF NOT EXISTS __tmp_parent_uuids(uuid TEXT)"
                    )
                    cursor.execute("DELETE FROM __tmp_parent_uuids")
                    cursor.executemany(
                        "INSERT INTO __tmp_parent_uuids(uuid) VALUES (?)",
                        ((uuid,) for uuid in parent_uuids),
                    )
                    cursor.execute(
                        f'''
                        DELETE FROM "{table_name}"
                        WHERE parent_uuid IN (SELECT uuid FROM __tmp_parent_uuids)
                        '''
                    )
                    cursor.execute("DELETE FROM __tmp_parent_uuids")
                    connection.commit()
        
        saved = upsert_rows(connection, table_name, rows, base)
        saved_counts[section_name] = saved

    return saved_counts


def find_catalog_metadata(com_object, possible_names: Iterable[str]):
    """
    Ищет метаданные справочника по списку допустимых имен.
    """
    metadata_attr = safe_getattr(com_object, "Metadata")
    metadata = call_if_callable(metadata_attr)
    if metadata is None:
        metadata = metadata_attr
    if metadata is None:
        raise RuntimeError("Метаданные 1С недоступны (com_object.Metadata вернул None)")

    catalogs_attr = safe_getattr(metadata, "Catalogs")
    catalogs = call_if_callable(catalogs_attr)
    if catalogs is None:
        catalogs = catalogs_attr
    if catalogs is None:
        raise RuntimeError("Metadata.Catalogs недоступен")

    for name in possible_names:
        try:
            catalog_metadata = catalogs.НайтиПоИмени(name)
            if catalog_metadata:
                return catalog_metadata, safe_getattr(catalog_metadata, "Name", name)
        except Exception:
            continue

    count = call_if_callable(safe_getattr(catalogs, "Count", None)) or 0
    for index in range(int(count)):
        catalog = catalogs.Get(index)
        catalog_name = safe_getattr(catalog, "Name", "")
        if catalog_name in possible_names:
            return catalog, catalog_name

    available = []
    for index in range(int(count)):
        catalog = catalogs.Get(index)
        available.append(safe_getattr(catalog, "Name", ""))

    raise RuntimeError(
        "Справочник не найден. Доступные справочники: "
        + ", ".join(filter(None, available))
    )
