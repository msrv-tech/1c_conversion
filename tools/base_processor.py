# -*- coding: utf-8 -*-
"""
Базовый класс для процессоров маппинга данных.

Содержит общую логику загрузки маппинга, преобразования полей и значений перечислений.
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Dict, List, Optional, Set, Tuple


class MappingProcessor:
    """Базовый класс для процессоров преобразования данных с использованием маппинга."""

    # Стандартные реквизиты справочников 1С, которые переносятся как есть
    STANDARD_REQUISITES = {
        "uuid",
        "Ссылка",
        "Код",
        "Наименование",
        "ПометкаУдаления",
        "Комментарий",
        "НаименованиеПолное",
        "Дата",
        "Номер",
        "ЭтоГруппа",  # Признак группы для иерархических справочников
        "Родитель_ЭтоГруппа",  # Признак группы родителя для иерархических справочников (временное поле для добавления в JSON)
    }

    def __init__(self, mapping_db_path: str, object_name: str, object_type: str = "catalog"):
        """
        Инициализация процессора.
        
        Args:
            mapping_db_path: Путь к базе данных с маппингом
            object_name: Имя объекта в маппинге (например, "Контрагенты", "НоменклатурныеГруппы")
            object_type: Тип объекта (по умолчанию "catalog")
        """
        self.mapping_db_path = mapping_db_path
        self.object_name = object_name
        self.object_type = object_type
        self.field_mapping: Dict[str, Dict[str, str]] = {}  # source_field -> {target_field, target_type}
        self.type_mapping: Dict[str, str] = {}  # source_type -> target_type
        self.enum_value_mapping: Dict[str, Dict[str, str]] = {}  # source_enum -> {source_value -> target_value}
        self.enum_type_mapping: Dict[str, str] = {}  # source_enum -> target_enum
        self.target_fields: Set[str] = set()  # Все поля приемника
        self.target_fields_types: Dict[str, str] = {}  # target_field -> target_type
        self.mapped_target_fields: Set[str] = set()  # Все целевые поля из маппинга
        self.chart_of_accounts_mapping: Dict[str, Optional[str]] = {}  # Маппинг плана счетов
        self.chart_of_accounts_subconto: Dict[str, Dict[str, str]] = {}  # Информация о субконто для счетов
        self._load_mapping()
        self._load_chart_of_accounts_mapping()

    def _load_mapping(self) -> None:
        """Загружает маппинг полей и типов из базы данных."""
        if not os.path.exists(self.mapping_db_path):
            from tools.logger import verbose_print
            verbose_print(f"Предупреждение: база маппинга не найдена: {self.mapping_db_path}")
            return

        conn = sqlite3.connect(self.mapping_db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Загружаем маппинг полей для указанного объекта
        cursor.execute("""
            SELECT field_name, target_field_name, source_type, target_type, status, search_method
            FROM field_mapping
            WHERE object_type = ? AND object_name = ?
        """, (self.object_type, self.object_name))

        for row in cursor.fetchall():
            source_field = row["field_name"]
            target_field = row["target_field_name"]
            source_type = row["source_type"]
            target_type = row["target_type"]
            status = row["status"]
            # Проверяем наличие search_method (может быть None)
            search_method = row["search_method"] if len(row) > 5 and row["search_method"] else None

            if status == "matched" and target_field:
                self.field_mapping[source_field] = {
                    "target_field": target_field,
                    "target_type": target_type or source_type,
                    "source_type": source_type,  # Сохраняем исходный тип для проверки
                    "search_method": search_method,  # Способ поиска/преобразования
                }
                # Сохраняем все смапленные целевые поля
                self.mapped_target_fields.add(target_field)

        # Загружаем маппинг типов
        cursor.execute("""
            SELECT source_type, target_type, status
            FROM type_mapping
            WHERE status IN ('exact', 'mapped', 'matched')
        """)

        for row in cursor.fetchall():
            source_type = row["source_type"]
            target_type = row["target_type"]
            if target_type:
                self.type_mapping[source_type] = target_type

        # Загружаем маппинг значений перечислений
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='enumeration_value_mapping'
        """)
        if cursor.fetchone():
            cursor.execute("""
                SELECT source_enum_type, source_value, target_enum_type, target_value
                FROM enumeration_value_mapping
            """)
            
            for row in cursor.fetchall():
                source_enum = row["source_enum_type"]
                source_val = row["source_value"]
                target_enum = row["target_enum_type"]
                target_val = row["target_value"]
                
                if source_enum not in self.enum_value_mapping:
                    self.enum_value_mapping[source_enum] = {}
                    self.enum_type_mapping[source_enum] = target_enum
                
                self.enum_value_mapping[source_enum][source_val] = target_val

        # Загружаем все поля приемника для поиска аналогий
        cursor.execute("""
            SELECT DISTINCT target_field_name, target_type
            FROM field_mapping
            WHERE object_type = ? 
                AND object_name = ?
                AND target_field_name IS NOT NULL
        """, (self.object_type, self.object_name))

        for row in cursor.fetchall():
            field_name = row["target_field_name"]
            field_type = row["target_type"]
            if field_name:
                self.target_fields.add(field_name)
                if field_type:
                    self.target_fields_types[field_name] = field_type

        conn.close()

        from tools.logger import verbose_print
        verbose_print(f"Загружено маппингов полей: {len(self.field_mapping)}")
        verbose_print(f"Загружено маппингов типов: {len(self.type_mapping)}")
        verbose_print(f"Поля приемника: {len(self.target_fields)}")

    def _load_chart_of_accounts_mapping(self) -> None:
        """Загружает маппинг плана счетов из JSON файла."""
        try:
            from tools.chart_of_accounts_mapper import load_mapping
            mapping_file_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "CONF",
                "chart_of_accounts_mapping.json"
            )
            self.chart_of_accounts_mapping, self.chart_of_accounts_subconto = load_mapping(mapping_file_path)
            if self.chart_of_accounts_mapping:
                from tools.logger import verbose_print
                verbose_print(f"Загружено маппингов плана счетов: {len(self.chart_of_accounts_mapping)}")
                if self.chart_of_accounts_subconto:
                    verbose_print(f"Загружено информации о субконто для счетов: {len(self.chart_of_accounts_subconto)}")
        except Exception as e:
            from tools.logger import verbose_print
            verbose_print(f"Предупреждение: не удалось загрузить маппинг плана счетов: {e}")

    def _is_standard_requisite(self, field_name: str) -> bool:
        """
        Проверяет, является ли поле стандартным реквизитом справочника/документа.
        
        Args:
            field_name: Имя поля
            
        Returns:
            True если это стандартный реквизит
        """
        return field_name in self.STANDARD_REQUISITES

    def _map_enum_value(self, enum_value: str, source_type: Optional[str] = None, target_type: Optional[str] = None) -> str:
        """
        Преобразует значение перечисления из источника в формат приемника.
        
        Args:
            enum_value: Значение перечисления в формате "Перечисление.Имя.Значение"
            source_type: Тип перечисления источника (например, "Перечисление.ЮрФизЛицо")
            target_type: Тип перечисления приемника (например, "Перечисление.ЮридическоеФизическоеЛицо")
            
        Returns:
            Преобразованное значение в формате "Перечисление.Имя.Значение" или исходное значение, если маппинг не найден
        """
        if not enum_value or not isinstance(enum_value, str):
            return enum_value
        
        # Парсим строку вида "Перечисление.Имя.Значение"
        if not enum_value.startswith("Перечисление."):
            return enum_value
        
        parts = enum_value.split(".", 2)
        if len(parts) != 3:
            return enum_value
        
        enum_prefix, source_enum_name, source_enum_value = parts
        
        # Если передан source_type, используем его
        if source_type and source_type.startswith("Перечисление."):
            source_enum_type = source_type
        else:
            source_enum_type = f"Перечисление.{source_enum_name}"
        
        # ВАЖНО: Логика определения целевого типа перечисления:
        # 1. Если есть маппинг значений перечислений (enum_value_mapping) И для текущего справочника
        #    есть поле с целевым типом из маппинга значений - используем целевой тип из маппинга значений
        #    Это позволяет иметь специфичные маппинги для конкретных справочников (например, ДоговорыКонтрагентов)
        # 2. Иначе используем target_type из маппинга поля (штатный маппинг)
        # 3. Если и его нет - используем маппинг типов или оставляем исходный тип
        
        # Проверяем, есть ли маппинг значений для данного перечисления
        has_value_mapping = source_enum_type in self.enum_value_mapping
        
        if has_value_mapping:
            # Получаем целевой тип из маппинга значений
            target_enum_type_from_mapping = self.enum_type_mapping.get(source_enum_type)
            
            # Проверяем, есть ли в текущем справочнике поле с таким целевым типом
            # Это означает, что маппинг значений предназначен для этого справочника
            has_target_type_in_catalog = False
            if target_enum_type_from_mapping:
                # Ищем в маппинге полей текущего справочника поле с таким целевым типом
                for field_mapping_info in self.field_mapping.values():
                    if field_mapping_info.get("target_type") == target_enum_type_from_mapping:
                        has_target_type_in_catalog = True
                        break
            
            if has_target_type_in_catalog:
                # Маппинг значений предназначен для этого справочника - используем его
                target_enum_type = target_enum_type_from_mapping
            else:
                # Маппинг значений не предназначен для этого справочника - используем target_type из маппинга поля
                if target_type and target_type.startswith("Перечисление."):
                    target_enum_type = target_type
                else:
                    # Пробуем найти в маппинге типов
                    target_enum_type = self.type_mapping.get(source_enum_type)
                    if not target_enum_type:
                        # Если не нашли, оставляем исходный тип
                        target_enum_type = source_enum_type
        else:
            # Если маппинга значений нет, используем target_type из маппинга поля (если передан)
            if target_type and target_type.startswith("Перечисление."):
                target_enum_type = target_type
            else:
                # Пробуем найти в маппинге типов
                target_enum_type = self.type_mapping.get(source_enum_type)
                if not target_enum_type:
                    # Если не нашли, оставляем исходный тип
                    target_enum_type = source_enum_type
        
        # Если тип перечисления не изменился, возвращаем исходное значение
        if target_enum_type == source_enum_type:
            return enum_value
        
        # Ищем маппинг значения
        # ВАЖНО: Маппинг значений применяется только если целевой тип из маппинга значений
        # совпадает с целевым типом, который мы используем для текущего справочника
        if source_enum_type in self.enum_value_mapping:
            target_enum_type_from_mapping = self.enum_type_mapping.get(source_enum_type)
            # Применяем маппинг значений только если целевой тип из маппинга значений
            # совпадает с целевым типом, который мы определили для текущего справочника
            if target_enum_type_from_mapping == target_enum_type:
                value_mapping = self.enum_value_mapping[source_enum_type]
                target_enum_value = value_mapping.get(source_enum_value, source_enum_value)
                
                # Формируем новую строку
                if target_enum_type.startswith("Перечисление."):
                    target_enum_name = target_enum_type.replace("Перечисление.", "")
                    return f"{enum_prefix}.{target_enum_name}.{target_enum_value}"
        
        # Если маппинг значения не найден, но тип перечисления изменился, просто заменяем тип
        # Это важно: даже если нет маппинга значений, но тип изменился, нужно заменить тип
        if target_enum_type != source_enum_type and target_enum_type.startswith("Перечисление."):
            target_enum_name = target_enum_type.replace("Перечисление.", "")
            return f"{enum_prefix}.{target_enum_name}.{source_enum_value}"
        
        return enum_value

    def _map_field_value(
        self, source_field: str, source_value: any, source_type: Optional[str] = None
    ) -> Tuple[Optional[str], any]:
        """
        Преобразует значение поля из источника в формат приемника.
        
        Args:
            source_field: Имя поля источника
            source_value: Значение поля
            source_type: Тип поля источника (опционально)
            
        Returns:
            Кортеж (имя_поля_приемника, преобразованное_значение)
        """
        # Стандартные реквизиты переносятся как есть
        if self._is_standard_requisite(source_field):
            return source_field, source_value

        # Проверяем прямой маппинг
        if source_field in self.field_mapping:
            mapping = self.field_mapping[source_field]
            target_field = mapping["target_field"]
            target_type = mapping.get("target_type")
            
            # Если значение - перечисление, преобразуем его
            mapped_value = source_value
            if isinstance(source_value, str) and source_value.startswith("Перечисление."):
                mapped_value = self._map_enum_value(source_value, source_type, target_type)
            
            return target_field, mapped_value

        # Поле не найдено в явном маппинге – по требованию ничего не переносим
        return None, None

    def process_item(self, item: Dict) -> Dict:
        """
        Преобразует элемент из формата источника в формат приемника.
        
        Должен быть переопределен в подклассах для специфичной логики.
        
        Args:
            item: Словарь с данными элемента из источника
            
        Returns:
            Словарь с данными элемента для приемника
        """
        result: Dict = {}
        unmapped_fields: List[str] = []
        processed_base_fields = set()
        
        for source_field, source_value in item.items():
            # Пропускаем служебные поля
            if source_field.endswith("_Тип") or source_field.endswith("_UUID") or source_field.endswith("_Представление"):
                continue

            # Поля *_Код (например КодПоОКОФ_Код) — служебные для поиска по коду при reference_by_code
            if source_field.endswith("_Код"):
                base_field = source_field[:-4]  # убираем "_Код"
                if base_field in self.field_mapping:
                    result[source_field] = source_value
                continue

            processed_base_fields.add(source_field)
            

            # Определяем тип поля
            source_type = None
            type_field = f"{source_field}_Тип"
            if type_field in item:
                source_type = item[type_field]

            # ВАЖНО: Сначала проверяем наличие связанных полей (_UUID, _Представление) и определяем тип из _Тип
            # Это нужно для правильного определения перечислений, которые тоже имеют эти поля
            if not source_type and (f"{source_field}_UUID" in item or f"{source_field}_Представление" in item):
                if type_field in item:
                    source_type = item[type_field]

            # Если тип не определен, но значение - JSON строка, пытаемся извлечь тип из JSON
            # ВАЖНО: Делаем это ДО вызова _map_field_value, чтобы правильно определить is_reference
            if not source_type and isinstance(source_value, str) and source_value.strip().startswith('{'):
                try:
                    json_data = json.loads(source_value)
                    if isinstance(json_data, dict) and 'type' in json_data:
                        source_type = json_data['type']
                except (json.JSONDecodeError, ValueError):
                    pass

            # СНАЧАЛА определяем, является ли поле ссылочным (до вызова _map_field_value)
            # Это критично для правильной проверки маппинга типа
            is_reference = False
            is_enumeration = False
            
            # Проверяем, является ли поле перечислением (ВАЖНО: проверяем ДО проверки ссылок)
            if source_type and source_type.startswith("Перечисление."):
                is_enumeration = True
            elif isinstance(source_value, str) and source_value.startswith("Перечисление."):
                is_enumeration = True
            
            # Проверяем по source_type (перечисления не считаются ссылками)
            if not is_enumeration and source_type and (source_type.startswith("Справочник.") or 
                                source_type.startswith("Документ.") or
                                source_type.startswith("ChartOfAccountsRef.") or
                                source_type.startswith("ChartOfCharacteristicTypesRef.") or
                                source_type.startswith("ПланСчетов") or
                                source_type.startswith("ПланВидовХарактеристик")):
                is_reference = True
            # Проверяем наличие связанных полей (_UUID, _Представление) - это признак ссылочного поля
            # НО только если это НЕ перечисление
            elif not is_enumeration and (f"{source_field}_UUID" in item or f"{source_field}_Представление" in item):
                is_reference = True
            # Проверяем JSON значение
            json_type_from_value = None
            if isinstance(source_value, str) and source_value.strip().startswith('{'):
                try:
                    json_data = json.loads(source_value)
                    if isinstance(json_data, dict) and 'type' in json_data:
                        json_type_from_value = json_data['type']
                        if json_type_from_value and (json_type_from_value.startswith("Справочник.") or 
                                          json_type_from_value.startswith("Документ.") or
                                          json_type_from_value.startswith("ChartOfAccountsRef.") or
                                          json_type_from_value.startswith("ChartOfCharacteristicTypesRef.") or
                                          json_type_from_value.startswith("ПланСчетов") or
                                          json_type_from_value.startswith("ПланВидовХарактеристик")):
                            is_reference = True
                            # Для JSON полей ВСЕГДА используем тип из JSON, а не из маппинга поля
                            source_type = json_type_from_value
                except (json.JSONDecodeError, ValueError):
                    pass
            
            # Дополнительная проверка: если в маппинге поля исходный тип ссылочный, считаем поле ссылочным
            # НО только если это не перечисление И если тип еще не определен из JSON
            if not is_reference and not is_enumeration and source_field in self.field_mapping:
                mapping = self.field_mapping[source_field]
                source_type_in_mapping = mapping.get("source_type")
                if source_type_in_mapping and (source_type_in_mapping.startswith("Справочник.") or 
                                                source_type_in_mapping.startswith("Документ.") or
                                                source_type_in_mapping.startswith("ChartOfAccountsRef.") or
                                                source_type_in_mapping.startswith("ChartOfCharacteristicTypesRef.") or
                                                source_type_in_mapping.startswith("ПланСчетов") or
                                                source_type_in_mapping.startswith("ПланВидовХарактеристик")):
                    is_reference = True
                    # Используем тип из маппинга только если он еще не определен (не из JSON)
                    if not source_type:
                        source_type = source_type_in_mapping
            
            # Преобразуем поле
            target_field, target_value = self._map_field_value(
                source_field, source_value, source_type
            )

            if target_field:
                # Стандартные реквизиты всегда добавляем в результат, независимо от типа
                if self._is_standard_requisite(source_field):
                    result[target_field] = target_value
                    continue
                
                # Для ссылочных полей требуется маппинг типа
                if is_reference:
                    # Для ссылочных полей требуется маппинг типа на ссылочный тип
                    # Нормализуем тип ChartOfAccountsRef.* в ПланСчетов.* для проверки маппинга
                    normalized_source_type = source_type
                    if source_type and source_type.startswith("ChartOfAccountsRef."):
                        normalized_source_type = source_type.replace("ChartOfAccountsRef.", "ПланСчетов.")
                    
                    # Проверяем, есть ли маппинг типа для исходного ссылочного типа
                    has_type_mapping = False
                    if normalized_source_type and normalized_source_type in self.type_mapping:
                        target_type_from_mapping = self.type_mapping[normalized_source_type]
                        # Маппинг типа считается валидным только если целевой тип тоже ссылочный
                        if target_type_from_mapping and (target_type_from_mapping.startswith("Справочник.") or 
                                                          target_type_from_mapping.startswith("Документ.") or
                                                          target_type_from_mapping.startswith("ChartOfAccountsRef.") or
                                                          target_type_from_mapping.startswith("ChartOfCharacteristicTypesRef.") or
                                                          target_type_from_mapping.startswith("ПланСчетов") or
                                                          target_type_from_mapping.startswith("ПланВидовХарактеристик")):
                            has_type_mapping = True
                    
                    
                    # Для ссылочных полей требуется маппинг типа на ссылочный тип (обязательно!)
                    # ВАЖНО: преобразование ссылки в примитивный тип через маппинг поля НЕ разрешено
                    # Ссылочные поля без валидного маппинга типа НЕ должны попадать в результат
                    if not has_type_mapping:
                        # Ссылочное поле без маппинга типа - не добавляем
                        unmapped_fields.append(source_field)
                        continue
                    else:
                        # Поле прошло проверку - применяем маппинг плана счетов, если это план счетов
                        item_to_process = item.copy()
                        # Применяем маппинг плана счетов для типов ПланСчетов.* и ChartOfAccountsRef.*
                        if source_type and (source_type.startswith("ПланСчетов") or source_type.startswith("ChartOfAccountsRef.")) and self.chart_of_accounts_mapping:
                            try:
                                from tools.chart_of_accounts_mapper import apply_mapping_to_account_reference
                                # Применяем маппинг плана счетов
                                item_to_process = apply_mapping_to_account_reference(
                                    item_to_process,
                                    self.chart_of_accounts_mapping,
                                    source_field
                                )
                                # Обновляем source_value если он изменился
                                if source_field in item_to_process and item_to_process[source_field] != source_value:
                                    source_value = item_to_process[source_field]
                                # Обновляем связанные поля если они изменились
                                uuid_field = f"{source_field}_UUID"
                                presentation_field = f"{source_field}_Представление"
                                if uuid_field in item_to_process and uuid_field in item:
                                    item[uuid_field] = item_to_process[uuid_field]
                                if presentation_field in item_to_process and presentation_field in item:
                                    item[presentation_field] = item_to_process[presentation_field]
                            except Exception as e:
                                from tools.logger import verbose_print
                                verbose_print(f"  Ошибка применения маппинга плана счетов для поля {source_field}: {e}")
                        
                        # Для JSON полей сохраняем JSON как есть, обновляя тип если нужно
                        if isinstance(source_value, str) and source_value.strip().startswith('{'):
                            try:
                                json_data = json.loads(source_value)
                                if isinstance(json_data, dict) and 'type' in json_data:
                                    # Обновляем тип в JSON, если есть маппинг
                                    json_type = json_data.get('type', '')
                                    if json_type and json_type in self.type_mapping:
                                        json_data['type'] = self.type_mapping[json_type]
                                    result[target_field] = json.dumps(json_data, ensure_ascii=False)
                                else:
                                    result[target_field] = target_value
                            except (json.JSONDecodeError, ValueError):
                                result[target_field] = target_value
                        else:
                            result[target_field] = target_value
                        
                        # Добавляем связанные поля для ссылок только если это ссылочный тип (не примитивный)
                        # И только если поля еще не в JSON формате
                        if has_type_mapping and not self._is_standard_requisite(source_field):
                            # Если значение уже в JSON, не добавляем отдельные поля
                            if not (isinstance(source_value, str) and source_value.strip().startswith('{')):
                                uuid_field = f"{source_field}_UUID"
                                presentation_field = f"{source_field}_Представление"

                                if uuid_field in item:
                                    result[f"{target_field}_UUID"] = item[uuid_field]
                                if presentation_field in item:
                                    result[f"{target_field}_Представление"] = item[presentation_field]

                                # Преобразуем тип, если есть маппинг
                                if source_type and source_type in self.type_mapping:
                                    result[f"{target_field}_Тип"] = self.type_mapping[source_type]
                                elif type_field in item and item[type_field]:
                                    result[f"{target_field}_Тип"] = item[type_field]
                else:
                    # Не ссылочное поле - стандартные реквизиты переносим всегда
                    if self._is_standard_requisite(source_field):
                        result[target_field] = target_value
                        continue

                    # Для перечислений добавляем в результат, если есть маппинг
                    if is_enumeration:
                        result[target_field] = target_value
                        continue

                    # Прочие поля добавляем ТОЛЬКО если есть маппинг
                    # ВАЖНО: если есть маппинг поля с преобразованием ссылки в примитивный тип,
                    # проверяем, что исходный тип был ссылочным и есть маппинг типа на ссылочный тип
                    has_field_mapping_check = source_field in self.field_mapping
                    if has_field_mapping_check:
                        mapping = self.field_mapping[source_field]
                        target_type = mapping.get("target_type")
                        source_type_in_mapping = mapping.get("source_type")
                        search_method = mapping.get("search_method")
                        
                        # Обработка способа "string_to_reference_by_name": преобразуем строку в ссылку
                        if search_method == "string_to_reference_by_name" and isinstance(source_value, str) and source_value:
                            # Для string_to_reference_by_name НЕ генерируем UUID в процессоре,
                            # так как поиск должен идти по наименованию, а UUID будет сгенерирован в writer только при создании нового элемента
                            # Создаем поля для ссылки без UUID
                            result[f"{target_field}_UUID"] = ""  # Пустой UUID - поиск будет только по наименованию
                            result[f"{target_field}_Представление"] = source_value
                            result[f"{target_field}_Тип"] = target_type or ""
                            # Основное поле оставляем пустым (будет обработано как ссылка)
                        
                        # Обработка способа "string_to_reference_by_full_name": преобразуем строку в ссылку по полному наименованию
                        elif search_method == "string_to_reference_by_full_name" and isinstance(source_value, str) and source_value:
                            # Для string_to_reference_by_full_name НЕ генерируем UUID в процессоре,
                            # так как поиск должен идти по полному наименованию, а UUID будет сгенерирован в writer только при создании нового элемента
                            # Создаем поля для ссылки без UUID
                            result[f"{target_field}_UUID"] = ""  # Пустой UUID - поиск будет только по полному наименованию
                            result[f"{target_field}_Представление"] = source_value  # Используем то же поле для полного наименования
                            result[f"{target_field}_Тип"] = target_type or ""
                            # Основное поле оставляем пустым (будет обработано как ссылка)
                            result[target_field] = ""
                            continue
                            result[target_field] = ""
                            continue
                        
                        # Если исходный тип в маппинге поля был ссылочным, но целевой тип примитивный,
                        # требуется маппинг типа на ссылочный тип
                        if source_type_in_mapping and (source_type_in_mapping.startswith("Справочник.") or 
                                                        source_type_in_mapping.startswith("Документ.") or
                                                        source_type_in_mapping.startswith("ChartOfAccountsRef.") or
                                                        source_type_in_mapping.startswith("ChartOfCharacteristicTypesRef.") or
                                                        source_type_in_mapping.startswith("ПланСчетов") or
                                                        source_type_in_mapping.startswith("ПланВидовХарактеристик")):
                            if target_type and not (target_type.startswith("Справочник.") or 
                                                    target_type.startswith("Документ.") or
                                                    target_type.startswith("ChartOfAccountsRef.") or
                                                    target_type.startswith("ChartOfCharacteristicTypesRef.")):
                                # Преобразование ссылки в примитивный тип - требуется маппинг типа на ссылочный тип
                                # ВАЖНО: преобразование ссылки в примитивный тип НЕ разрешено
                                # Поле не должно попадать в результат
                                unmapped_fields.append(source_field)
                                # Отладочный вывод
                                from tools.logger import verbose_print
                                if source_field == "Модель":
                                    verbose_print(f"  [ОТЛАДКА] Пропускаем поле Модель в блоке else: is_reference={is_reference}, source_type_in_mapping={source_type_in_mapping}, target_type={target_type}")
                                continue
                        
                        # Поле смапплено и прошло проверку - добавляем в результат
                        result[target_field] = target_value
                    else:
                        # Поле не смапплено - не добавляем (кроме стандартных реквизитов)
                        unmapped_fields.append(source_field)
            else:
                # Если поле не найдено в маппинге
                # Стандартные реквизиты всегда добавляем (критичны для работы)
                if self._is_standard_requisite(source_field):
                    result[source_field] = source_value
                else:
                    unmapped_fields.append(source_field)

        if unmapped_fields:
            from tools.logger import verbose_print
            verbose_print(f"  Несмаппированные поля: {', '.join(unmapped_fields[:5])}")
            if len(unmapped_fields) > 5:
                verbose_print(f"  ... и еще {len(unmapped_fields) - 5} полей")
        
        return result
    
    def get_mapped_target_fields(self) -> Set[str]:
        """
        Возвращает множество всех целевых полей из маппинга.
        
        Returns:
            Множество имен целевых полей
        """
        return self.mapped_target_fields.copy()
    
    def extend_base_columns_with_mapped_fields(self, base_columns: Dict[str, str]) -> Dict[str, str]:
        """
        Расширяет base_columns всеми смапленными полями из маппинга.
        
        Args:
            base_columns: Словарь базовых колонок {имя: определение}
            
        Returns:
            Расширенный словарь колонок со всеми смапленными полями
        """
        extended = base_columns.copy()
        
        # Добавляем все смапленные целевые поля, которых еще нет в base_columns
        for target_field in self.mapped_target_fields:
            if target_field not in extended:
                # Определяем тип на основе target_fields_types
                field_type = self.target_fields_types.get(target_field, "TEXT")
                
                # Преобразуем типы с точками (например, "Справочник.Контрагенты") в валидный SQL тип
                # Все ссылочные типы и перечисления сохраняются как TEXT
                if isinstance(field_type, str) and ("." in field_type or 
                                                     field_type.startswith("Справочник") or 
                                                     field_type.startswith("Документ") or 
                                                     field_type.startswith("Перечисление") or
                                                     field_type.startswith("ПланСчетов") or
                                                     field_type.startswith("ПланВидовХарактеристик") or
                                                     field_type.startswith("ПланВидовРасчета")):
                    field_type = "TEXT"
                elif field_type not in ["TEXT", "INTEGER", "REAL", "NUMERIC", "BLOB", "DATE", "DATETIME"]:
                    # Для неизвестных типов используем TEXT
                    field_type = "TEXT"
                
                extended[target_field] = field_type
        
        return extended

