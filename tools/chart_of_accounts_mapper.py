# -*- coding: utf-8 -*-
"""
Утилита для работы с маппингом плана счетов.

Обеспечивает загрузку маппинга из JSON файла и применение маппинга к данным счетов.
"""

import json
import os
import re
from typing import Dict, Optional, Any, Tuple, List


def load_mapping(mapping_file_path: str) -> Tuple[Dict[str, Optional[str]], Dict[str, Dict[str, str]]]:
    """
    Загружает маппинг плана счетов из JSON файла.
    
    Поддерживает два формата:
    - Простая строка: "90.01": "90.01"
    - Объект с субконто: "90.02": {"target": "90.02", "subconto1": "НоменклатурнаяГруппа", ...}
    
    Args:
        mapping_file_path: Путь к JSON файлу с маппингом
        
    Returns:
        Кортеж из двух словарей:
        - Словарь маппинга: {код_счета_источника: код_счета_приемника или None}
        - Словарь субконто: {код_счета_приемника: {subconto1: поле, subconto2: поле, ...}}
    """
    if not os.path.exists(mapping_file_path):
        return {}, {}
    
    try:
        with open(mapping_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Фильтруем служебные ключи (начинающиеся с _)
        raw_mapping = {
            k: v for k, v in data.items() 
            if not k.startswith('_')
        }
        
        mapping = {}
        subconto_info = {}
        
        # Обрабатываем каждую запись маппинга
        for source_code, target_value in raw_mapping.items():
            if target_value is None:
                mapping[source_code] = None
            elif isinstance(target_value, str):
                # Простая строка - только код счета
                mapping[source_code] = target_value
            elif isinstance(target_value, dict):
                # Объект - извлекаем код счета и информацию о субконто
                target_code = target_value.get("target")
                if target_code:
                    mapping[source_code] = target_code
                    
                    # Извлекаем информацию о субконто (все ключи кроме "target")
                    subconto_config = {
                        k: v for k, v in target_value.items()
                        if k != "target" and k.startswith("subconto")
                    }
                    if subconto_config:
                        subconto_info[target_code] = subconto_config
        
        return mapping, subconto_info
    except (json.JSONDecodeError, IOError) as e:
        from tools.logger import verbose_print
        verbose_print(f"Ошибка загрузки маппинга плана счетов из {mapping_file_path}: {e}")
        return {}, {}


def extract_account_code(presentation: str) -> Optional[str]:
    """
    Извлекает код счета из представления.
    
    Примеры:
        "26.01 Общехозяйственные расходы" -> "26.01"
        "26" -> "26"
        "90.01.1 Продажи" -> "90.01.1"
        "76.А Расчёты с арендаторами" -> "76.А"
        "76.АА Авансы от арендаторов" -> "76.АА"
    
    Args:
        presentation: Представление счета (например, "26.01 Общехозяйственные расходы")
        
    Returns:
        Код счета или None, если не удалось извлечь
    """
    if not presentation or not isinstance(presentation, str):
        return None
    
    # Убираем пробелы в начале и конце
    presentation = presentation.strip()
    
    # Ищем код счета: цифры, точки, дефисы, буквы (кириллица и латиница для субсчетов 76.А, 76.АА и т.д.)
    # Паттерн: цифры, затем опционально (. + цифры/буквы)*, затем пробел или конец
    # \u0400-\u04FF — диапазон кириллицы в Unicode
    match = re.match(r'^([0-9]+(?:\.[0-9A-Za-z\u0400-\u04FF]+)*(?:-[0-9]+)?)', presentation)
    if match:
        return match.group(1)
    
    return None


def get_mapped_account_code(source_code: str, mapping: Dict[str, Optional[str]]) -> Optional[str]:
    """
    Получает маппированный код счета.
    
    Args:
        source_code: Код счета источника
        mapping: Словарь маппинга
        
    Returns:
        Код счета приемника, None если счет не маппится, или исходный код если маппинг не найден
    """
    if not source_code or not isinstance(source_code, str):
        return None
    
    source_code = source_code.strip()
    
    # Прямой поиск в маппинге
    if source_code in mapping:
        return mapping[source_code]
    
    # Если не найден прямой маппинг, возвращаем исходный код
    return source_code


def apply_mapping_to_account_reference(
    account_data: Dict[str, Any],
    mapping: Dict[str, Optional[str]],
    field_name: str,
    source_db_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Применяет маппинг к ссылке на счет плана счетов.
    
    Обновляет код, UUID и представление счета на основе маппинга.
    
    Args:
        account_data: Словарь с данными элемента (может содержать поля field_name, field_name_UUID, field_name_Представление)
        mapping: Словарь маппинга счетов
        field_name: Имя поля со счетом (например, "СчетЗатрат")
        source_db_path: Путь к базе данных для поиска счета по коду (опционально)
        
    Returns:
        Обновленный словарь с данными
    """
    result = account_data.copy()
    
    # Определяем, в каком формате хранится ссылка на счет
    # Вариант 1: JSON строка в основном поле
    account_json = None
    if field_name in result:
        field_value = result[field_name]
        if isinstance(field_value, str) and field_value.strip().startswith('{'):
            try:
                account_json = json.loads(field_value)
            except json.JSONDecodeError:
                pass
    
    # Вариант 2: Отдельные поля _UUID, _Представление, _Тип
    uuid_field = f"{field_name}_UUID"
    presentation_field = f"{field_name}_Представление"
    type_field = f"{field_name}_Тип"
    
    # Извлекаем код счета
    source_code = None
    if account_json:
        presentation = account_json.get('presentation', '')
        if presentation:
            source_code = extract_account_code(presentation)
    elif presentation_field in result:
        presentation = result[presentation_field]
        if presentation:
            source_code = extract_account_code(str(presentation))
    
    if not source_code:
        # Не удалось извлечь код - возвращаем как есть
        return result
    
    # Получаем маппированный код
    mapped_code = get_mapped_account_code(source_code, mapping)
    
    # Если счет не маппится (mapped_code is None) или код не изменился
    if mapped_code is None:
        # Счет помечен как не маппируемый - оставляем как есть, но логируем
        from tools.logger import verbose_print
        verbose_print(f"  Счет {source_code} помечен как не маппируемый")
        return result
    
    if mapped_code == source_code:
        # Код не изменился - маппинг не требуется
        return result
    
    # Код изменился - нужно обновить данные счета
    # Если есть база данных, пытаемся найти счет по новому коду
    if source_db_path and os.path.exists(source_db_path):
        new_account_data = _find_account_by_code(source_db_path, mapped_code)
        if new_account_data:
            # Обновляем данные
            if account_json:
                # Обновляем JSON
                account_json['uuid'] = new_account_data.get('uuid', account_json.get('uuid', ''))
                account_json['presentation'] = new_account_data.get('presentation', account_json.get('presentation', ''))
                result[field_name] = json.dumps(account_json, ensure_ascii=False)
            else:
                # Обновляем отдельные поля
                if uuid_field in result:
                    result[uuid_field] = new_account_data.get('uuid', result.get(uuid_field, ''))
                if presentation_field in result:
                    result[presentation_field] = new_account_data.get('presentation', result.get(presentation_field, ''))
            
            from tools.logger import verbose_print
            verbose_print(f"  Счет {source_code} -> {mapped_code}")
        else:
            # Счет не найден - логируем предупреждение, но оставляем исходные данные
            from tools.logger import verbose_print
            verbose_print(f"  Предупреждение: счет {mapped_code} не найден в базе данных, оставляем исходный счет {source_code}")
    else:
        # База данных не указана - обновляем только представление (код в начале)
        if account_json:
            old_presentation = account_json.get('presentation', '')
            if old_presentation:
                # Заменяем код в представлении
                new_presentation = old_presentation.replace(source_code, mapped_code, 1)
                account_json['presentation'] = new_presentation
                result[field_name] = json.dumps(account_json, ensure_ascii=False)
        elif presentation_field in result:
            old_presentation = str(result[presentation_field])
            if old_presentation:
                new_presentation = old_presentation.replace(source_code, mapped_code, 1)
                result[presentation_field] = new_presentation
        
        from tools.logger import verbose_print
        verbose_print(f"  Счет {source_code} -> {mapped_code} (UUID не обновлен, база данных не указана)")
    
    return result


def _find_account_by_code(db_path: str, account_code: str) -> Optional[Dict[str, str]]:
    """
    Ищет счет плана счетов в базе данных по коду.
    
    Args:
        db_path: Путь к базе данных SQLite
        account_code: Код счета
        
    Returns:
        Словарь с данными счета (uuid, presentation) или None
    """
    try:
        import sqlite3
        from tools.db_manager import connect_to_sqlite
        
        connection = connect_to_sqlite(db_path)
        if not connection:
            return None
        
        cursor = connection.cursor()
        
        # Ищем счет в таблице плана счетов
        # Пробуем разные возможные имена таблиц
        table_names = ['chart_of_accounts', 'plan_schetov', 'план_счетов']
        
        for table_name in table_names:
            try:
                # Проверяем существование таблицы
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                if not cursor.fetchone():
                    continue
                
                # Ищем по коду (может быть в поле Код или в начале Представление)
                cursor.execute(f"""
                    SELECT uuid, Код, Наименование, Представление
                    FROM {table_name}
                    WHERE Код = ? OR Представление LIKE ?
                    LIMIT 1
                """, (account_code, f"{account_code}%"))
                
                row = cursor.fetchone()
                if row:
                    uuid = row[0] if row[0] else ""
                    code = row[1] if len(row) > 1 and row[1] else account_code
                    name = row[2] if len(row) > 2 and row[2] else ""
                    presentation = row[3] if len(row) > 3 and row[3] else f"{code} {name}".strip()
                    
                    connection.close()
                    return {
                        'uuid': uuid,
                        'presentation': presentation
                    }
            except sqlite3.OperationalError:
                continue
        
        connection.close()
        return None
        
    except Exception as e:
        from tools.logger import verbose_print
        verbose_print(f"  Ошибка поиска счета {account_code} в базе данных: {e}")
        return None


def validate_mapping(mapping: Dict[str, Optional[str]]) -> Tuple[bool, List[str]]:
    """
    Валидирует маппинг плана счетов.
    
    Проверяет на циклические зависимости и другие проблемы.
    
    Args:
        mapping: Словарь маппинга
        
    Returns:
        Кортеж (валиден_ли, список_ошибок)
    """
    errors = []
    
    # Проверка на циклические зависимости
    for source_code, target_code in mapping.items():
        if target_code is None:
            continue
        
        # Проверяем, не маппится ли целевой счет обратно на исходный
        if target_code in mapping:
            target_target = mapping[target_code]
            if target_target == source_code:
                errors.append(f"Циклическая зависимость: {source_code} -> {target_code} -> {source_code}")
    
    # Проверка на пустые коды
    for code in mapping.keys():
        if not code or not code.strip():
            errors.append("Найден пустой код счета в ключах маппинга")
    
    is_valid = len(errors) == 0
    return is_valid, errors

