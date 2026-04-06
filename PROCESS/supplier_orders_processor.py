# -*- coding: utf-8 -*-
"""
Модуль обработки документов «Заказ поставщику» для преобразования в справочник «ДоговорыКонтрагентов».

Читает документы ЗаказПоставщику из исходной БД, применяет маппинг из type_mapping.db
и сохраняет результат в новую БД в формате приемника (УХ) - справочник ДоговорыКонтрагентов
с видом соглашения "Спецификация".
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Добавляем путь к корню проекта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding
from tools.db_manager import connect_to_sqlite, ensure_database_exists
from tools.onec_connector import upsert_rows

from tools.base_processor import MappingProcessor
from tools.processor_utils import read_from_db
from tools.logger import verbose_print  # noqa: E402

fix_encoding()


class SupplierOrdersMappingProcessor(MappingProcessor):
    """Процессор для преобразования документов ЗаказПоставщику в справочник ДоговорыКонтрагентов."""

    def __init__(self, mapping_db_path: str = "CONF/type_mapping.db"):
        """
        Инициализация процессора.
        
        Args:
            mapping_db_path: Путь к базе данных с маппингом
        """
        # Используем маппинг для document.ЗаказПоставщику -> catalog.ДоговорыКонтрагентов
        super().__init__(mapping_db_path, "ЗаказПоставщику", "document")
        self.target_object_name = "ДоговорыКонтрагентов"

    def process_item_single(self, item: Dict) -> Dict:
        """
        Преобразует документ ЗаказПоставщику в элемент справочника ДоговорыКонтрагентов.
        
        Args:
            item: Словарь с данными документа из источника
            
        Returns:
            Словарь с данными элемента справочника для приемника
        """
        # ВАЖНО: Сохраняем UUID ДО применения базового маппинга, чтобы он не был потерян
        # UUID может быть в поле uuid (если был установлен в загрузчике) или в ЗаказПоставщику_UUID
        source_uuid = item.get("uuid") or item.get("ЗаказПоставщику_UUID")
        
        # Применяем базовый маппинг полей
        result = self.process_item(item)
        
        # Сохраняем UUID из исходного документа для предотвращения дублей
        if source_uuid:
            result["uuid"] = str(source_uuid).strip()
            if len(result["uuid"]) > 8:
                verbose_print(f"    ✓ Сохранен UUID: {result['uuid'][:8]}...")
            else:
                verbose_print(f"    ✓ Сохранен UUID: {result['uuid']}")
        else:
            verbose_print(f"    ⚠ UUID не найден в исходных данных (поля: uuid={item.get('uuid')}, ЗаказПоставщику_UUID={item.get('ЗаказПоставщику_UUID')})")
        
        # Устанавливаем ВидСоглашения = Спецификация
        result["ВидСоглашения"] = "Перечисление.ВидыСоглашений.Спецификация"
        
        # Заполняем ВидДоговораУХ = "СПоставщиком" (так как это заказы поставщику)
        self._fill_vid_dogovora_uh(result, item)
        
        # Заполняем поля из customСубподрядныеАктыАсуп (данные уже загружены через JOIN)
        self._fill_from_subcontractor_acts_asup(result, item)
        
        # Заполняем Номер и Дата из актов внешняя система (приоритет над значениями из заказа)
        self._fill_number_and_date_from_acts_asup(result, item)
        
        # Вычисляем ДатаОплаты
        self._calculate_payment_date(result, item)
        
        # Заполняем custom_СодержаниеУслуги из первой строки табличной части Услуги
        self._fill_soderzhanie_uslugi(result, item)
        
        # Заполняем специальные поля
        self._fill_special_fields(result, item)
        
        return result
    
    def _fill_from_subcontractor_acts_asup(self, result: Dict, source_item: Dict) -> None:
        """
        Заполняет поля из customСубподрядныеАктыАсуп, если данные доступны.
        
        Args:
            result: Словарь с обработанными данными (будет изменен)
            source_item: Словарь с исходными данными
        """
        # Поля с префиксом custom_ из customСубподрядныеАктыАсуп
        subcontractor_fields_mapping = {
            "Код": "custom_Кодвнешняя система",
            "Этап": "custom_Этап",
            "ЭтапГен": "custom_ЭтапГен",
        }
        
        for source_field, target_field in subcontractor_fields_mapping.items():
            # Проверяем, не обработано ли поле уже базовым процессором
            target_uuid_field = f"{target_field}_UUID"
            target_presentation_field = f"{target_field}_Представление"
            
            # Если базовый процессор уже обработал поле, пропускаем его
            if target_uuid_field in result or target_presentation_field in result:
                verbose_print(f"    ℹ️ {target_field}: уже обработано базовым процессором")
                continue
            
            # Пробуем получить значение из загруженных данных
            value = source_item.get(source_field)
            
            if value is not None and value != "":
                result[target_field] = value
                verbose_print(f"    ✓ {target_field}: заполнено из customСубподрядныеАктыАсуп: '{value}'")
    
    def _fill_number_and_date_from_acts_asup(self, result: Dict, source_item: Dict) -> None:
        """
        Заполняет Номер и Дата из актов внешняя система (customСубподрядныеАктыАсуп) в поля Номер и Дата договора.
        Приоритет над значениями из заказа.
        
        Args:
            result: Словарь с обработанными данными (будет изменен)
            source_item: Словарь с исходными данными
        """
        # Приоритет: Номер и Дата из актов внешняя система (НомерАкты, ДатаАкты), затем из заказа
        nomer_akty = source_item.get("НомерАкты")
        if nomer_akty and str(nomer_akty).strip():
            result["Номер"] = str(nomer_akty).strip()
            verbose_print(f"    ✓ Номер заполнен из актов внешняя система: {nomer_akty}")
        elif not result.get("Номер"):
            # Если Номер из внешняя система нет, используем Номер из заказа
            nomer = source_item.get("Номер")
            if nomer and str(nomer).strip():
                result["Номер"] = str(nomer).strip()
                verbose_print(f"    ✓ Номер заполнен из заказа: {nomer}")
        
        data_akty = source_item.get("ДатаАкты")
        if data_akty and str(data_akty).strip() and str(data_akty) != "0001-01-01":
            result["Дата"] = str(data_akty).strip()
            verbose_print(f"    ✓ Дата заполнена из актов внешняя система: {data_akty}")
        elif not result.get("Дата"):
            # Если Дата из внешняя система нет, используем Дата из заказа
            data = source_item.get("Дата")
            if data and str(data).strip() and str(data) != "0001-01-01":
                result["Дата"] = str(data).strip()
                verbose_print(f"    ✓ Дата заполнена из заказа: {data}")
    
    def _calculate_payment_date(self, result: Dict, source_item: Dict) -> None:
        """
        Вычисляет ДатаОплаты по формуле:
        ?(ЗначениеЗаполнено(ДатаПодписания), ДатаПодписания, Дата) + БазовыйДоговор.СрокОплаты * 86400
        
        Args:
            result: Словарь с обработанными данными (будет изменен)
            source_item: Словарь с исходными данными
        """
        # Получаем ДатаПодписания
        date_signed = source_item.get("ДатаПодписания")
        
        # Определяем базовую дату: если ДатаПодписания заполнена, используем её, иначе Дата
        if date_signed and str(date_signed).strip() and str(date_signed) != "0001-01-01":
            base_date = date_signed
        else:
            base_date = source_item.get("Дата") or source_item.get("ДатаАкты")
        
        if not base_date or str(base_date).strip() == "0001-01-01" or not str(base_date).strip():
            result["ДатаОплаты"] = "0001-01-01"
            return
        
        try:
            # Парсим базовую дату
            if isinstance(base_date, str):
                base_date_obj = datetime.strptime(base_date[:10], "%Y-%m-%d")
            else:
                base_date_obj = base_date
            
            # Получаем СрокОплаты из БазовогоДоговора
            # БазовыйДоговор = ДоговорКонтрагента из заказа
            # Пока используем значение по умолчанию 0, так как СрокОплаты нужно получать из БазовыйДоговор
            # В будущем можно добавить загрузку этого поля из БазовыйДоговор
            payment_term_days = 0
            
            # Вычисляем дату оплаты: базовая дата + СрокОплаты дней
            payment_date = base_date_obj + timedelta(days=payment_term_days)
            result["ДатаОплаты"] = payment_date.strftime("%Y-%m-%d")
            verbose_print(f"    ✓ ДатаОплаты вычислена: {result['ДатаОплаты']}")
        except Exception as e:
            verbose_print(f"    ⚠ Ошибка вычисления ДатаОплаты: {e}")
            result["ДатаОплаты"] = "0001-01-01"
    
    def _fill_vid_dogovora_uh(self, result: Dict, source_item: Dict) -> None:
        """
        Заполняет поле ВидДоговораУХ значением "СПоставщиком" для заказов поставщику.
        
        Args:
            result: Словарь с обработанными данными элемента (будет изменен)
            source_item: Словарь с исходными данными элемента
        """
        # Проверяем, нужно ли заполнять ВидДоговораУХ
        vid_dogovora_uh_field = result.get("ВидДоговораУХ", "")
        if vid_dogovora_uh_field:
            # Проверяем, является ли это пустым JSON
            try:
                if isinstance(vid_dogovora_uh_field, str) and vid_dogovora_uh_field.strip().startswith('{'):
                    vid_uh_json = json.loads(vid_dogovora_uh_field)
                    if vid_uh_json.get("uuid") and vid_uh_json.get("uuid") != "00000000-0000-0000-0000-000000000000":
                        # Поле уже заполнено валидной ссылкой, не перезаписываем
                        return
            except (json.JSONDecodeError, AttributeError):
                pass
        
        # Для заказов поставщику всегда устанавливаем ВидДоговораУХ = "СПоставщиком"
        # Формируем предопределенное значение
        predefined_value = "Справочник.ВидыДоговоровКонтрагентовУХ.СПоставщиком"
        result["ВидДоговораУХ"] = predefined_value
        verbose_print(f"    ✓ Заполнено поле ВидДоговораУХ: {predefined_value} (для заказа поставщику)")
    
    def _fill_special_fields(self, result: Dict, source_item: Dict) -> None:
        """
        Заполняет специальные поля согласно маппингу из таблицы.
        
        Args:
            result: Словарь с обработанными данными (будет изменен)
            source_item: Словарь с исходными данными
        """
        # Заполняем ДеньГарантийногоУдержания, ПроцентГарантийногоУдержания из ДоговорКонтрагента (СубподрядныеАктыАсуп)
        den_gu = source_item.get("ДеньГарантийногоУдержания")
        if den_gu and str(den_gu).strip() and str(den_gu) != "0001-01-01":
            result["custom_ДатаГарантийногоУдержания"] = den_gu
            verbose_print(f"    ✓ custom_ДатаГарантийногоУдержания заполнена из ДоговорКонтрагента")

        percent_gu = source_item.get("ПроцентГарантийногоУдержания")
        if percent_gu is not None:
            result["custom_ПроцентГарантийногоУдержания"] = percent_gu
            verbose_print(f"    ✓ custom_ПроцентГарантийногоУдержания заполнена из ДоговорКонтрагента")

        # Заполняем custom_ТипДоговора из ДоговорКонтрагента (СубподрядныеАктыАсуп)
        custom_tip = source_item.get("customТипДоговора")
        if custom_tip and str(custom_tip).strip():
            val = str(custom_tip).strip()
            if val.startswith("Перечисление.customТипыДоговоровКонтрагентов"):
                val = val.replace(
                    "Перечисление.customТипыДоговоровКонтрагентов",
                    "Перечисление.custom_ТипыДоговоровКонтрагентов",
                    1
                )
            result["custom_ТипДоговора"] = val
            verbose_print(f"    ✓ custom_ТипДоговора заполнена из ДоговорКонтрагента")

        # Заполняем custom_Подразделение (из Соответствие), custom_ДоходныйДоговор
        _EMPTY_UUID = "00000000-0000-0000-0000-000000000000"
        for ref_field in ("custom_Подразделение", "custom_ДоходныйДоговор"):
            val = source_item.get(ref_field)
            ref_uuid = source_item.get(ref_field + "_UUID", "")
            if val and isinstance(val, str) and val.strip().startswith("{") and _EMPTY_UUID not in str(val):
                result[ref_field] = val
                src = "Соответствие.ПодразделениеОрганизации" if ref_field == "custom_Подразделение" else "ДоговорКонтрагента.customГенДоговор"
                verbose_print(f"    ✓ {ref_field}: заполнено из {src}")
            elif ref_field + "_UUID" in source_item and ref_uuid and ref_uuid != _EMPTY_UUID:
                ref_data = {
                    "uuid": ref_uuid,
                    "presentation": source_item.get(ref_field + "_Представление", ""),
                    "type": source_item.get(ref_field + "_Тип", ""),
                }
                result[ref_field] = json.dumps(ref_data, ensure_ascii=False)
                src = "Соответствие.ПодразделениеОрганизации" if ref_field == "custom_Подразделение" else "ДоговорКонтрагента.customГенДоговор"
                verbose_print(f"    ✓ {ref_field}: заполнено из {src}")

        # Заполняем Номенклатура из первой строки табличной части Услуги
        nom = source_item.get("Номенклатура")
        if nom and isinstance(nom, str) and nom.strip().startswith("{"):
            result["Номенклатура"] = nom
            verbose_print("    ✓ Номенклатура: заполнена из первой строки Услуги")
        elif "Номенклатура_UUID" in source_item and source_item.get("Номенклатура_UUID"):
            ref_data = {
                "uuid": source_item["Номенклатура_UUID"],
                "presentation": source_item.get("Номенклатура_Представление", ""),
                "type": source_item.get("Номенклатура_Тип", "Справочник.Номенклатура")
            }
            result["Номенклатура"] = json.dumps(ref_data, ensure_ascii=False)
            verbose_print("    ✓ Номенклатура: заполнена из первой строки Услуги")

        # Заполняем НоменклатурнаяГруппа (поиск по строке customДоговорвнешняя система.НоменклатурнаяГруппа в справочнике НоменклатурныеГруппы)
        ng = source_item.get("НоменклатурнаяГруппа")
        if ng and isinstance(ng, str) and ng.strip().startswith("{"):
            result["НоменклатурнаяГруппа"] = ng
            verbose_print("    ✓ НоменклатурнаяГруппа: заполнена (поиск по Наименованию)")
        elif "НоменклатурнаяГруппа_UUID" in source_item and source_item.get("НоменклатурнаяГруппа_UUID"):
            ref_data = {
                "uuid": source_item["НоменклатурнаяГруппа_UUID"],
                "presentation": source_item.get("НоменклатурнаяГруппа_Представление", ""),
                "type": source_item.get("НоменклатурнаяГруппа_Тип", "Справочник.НоменклатурныеГруппы")
            }
            result["НоменклатурнаяГруппа"] = json.dumps(ref_data, ensure_ascii=False)
            verbose_print("    ✓ НоменклатурнаяГруппа: заполнена (поиск по Наименованию)")

        # Заполняем Наименование из НаименованиеСубподрядныхАктовАсуп (если есть)
        nazvanie_subcontractor = source_item.get("НаименованиеСубподрядныхАктовАсуп")
        if nazvanie_subcontractor and str(nazvanie_subcontractor).strip():
            result["Наименование"] = str(nazvanie_subcontractor)
            result["НаименованиеПолное"] = str(nazvanie_subcontractor)
            verbose_print(f"    ✓ Наименование заполнено из Субподрядных актов внешняя система: '{nazvanie_subcontractor}'")
        
        # Заполняем СтавкаНДС из customСубподрядныеАктыАсуп
        stavka_nds = source_item.get("СтавкаНДСАкты")
        if stavka_nds:
            # Если это ссылочное поле, сохраняем как JSON
            uuid_field = "СтавкаНДСАкты_UUID"
            presentation_field = "СтавкаНДСАкты_Представление"
            type_field = "СтавкаНДСАкты_Тип"
            
            if uuid_field in source_item and source_item.get(uuid_field):
                ref_data = {
                    "uuid": source_item[uuid_field],
                    "presentation": source_item.get(presentation_field, ""),
                    "type": source_item.get(type_field, "")
                }
                result["СтавкаНДС"] = json.dumps(ref_data, ensure_ascii=False)
            else:
                result["СтавкаНДС"] = stavka_nds
            verbose_print(f"    ✓ СтавкаНДС заполнена из customСубподрядныеАктыАсуп")
        
        # Заполняем СуммаНДС из табличной части Услуги
        summa_nds = source_item.get("СуммаНДСУслуг")
        if summa_nds:
            result["СуммаНДС"] = summa_nds
            verbose_print(f"    ✓ СуммаНДС заполнена: {summa_nds}")
        
        # Заполняем Сумма: итого по табличной части Услуги (СуммаУслуг + СуммаНДСУслуг)
        # Приоритет: из табличной части Услуги (с НДС), затем из customСубподрядныеАктыАсуп
        summa = None
        summa_uslug = source_item.get("СуммаУслуг")
        summa_nds_uslug = source_item.get("СуммаНДСУслуг")
        
        # Если есть данные из табличной части Услуги, считаем итого с НДС
        if summa_uslug is not None or summa_nds_uslug is not None:
            summa_uslug = float(summa_uslug or 0)
            summa_nds_uslug = float(summa_nds_uslug or 0)
            summa = summa_uslug + summa_nds_uslug
            if summa > 0:
                result["Сумма"] = summa
                verbose_print(f"    ✓ Сумма заполнена из табличной части Услуги (с НДС): {summa} (СуммаУслуг={summa_uslug} + СуммаНДСУслуг={summa_nds_uslug})")
        
        # Если не заполнили из табличной части, используем fallback
        if not summa or summa == 0:
            summa = source_item.get("СуммаАкты")
            if summa:
                result["Сумма"] = summa
                verbose_print(f"    ✓ Сумма заполнена из внешняя система: {summa}")
        
        # Устанавливаем ПорядокРасчетов = Перечисления.ПорядокРасчетов.ПоДоговорамКонтрагентов
        result["ПорядокРасчетов"] = "Перечисление.ПорядокРасчетов.ПоДоговорамКонтрагентов"
        
        # Устанавливаем СпособЗаполненияСтавкиНДС = Перечисления.СпособыЗаполненияСтавкиНДС.Автоматически
        result["СпособЗаполненияСтавкиНДС"] = "Перечисление.СпособыЗаполненияСтавкиНДС.Автоматически"
    
    def _fill_soderzhanie_uslugi(self, result: Dict, source_item: Dict) -> None:
        """
        Заполняет поле custom_СодержаниеУслуги из первой строки табличной части Услуги.
        
        Args:
            result: Словарь с обработанными данными (будет изменен)
            source_item: Словарь с исходными данными
        """
        soderzhanie = source_item.get("СодержаниеУслуги")
        if soderzhanie and str(soderzhanie).strip():
            result["custom_СодержаниеУслуги"] = str(soderzhanie).strip()
            verbose_print(f"    ✓ custom_СодержаниеУслуги заполнено из первой строки Услуги: '{soderzhanie[:50]}...'")
        else:
            verbose_print(f"    ℹ️ СодержаниеУслуги не найдено в исходных данных")

    def process_items(self, items: List[Dict]) -> List[Dict]:
        """
        Преобразует список документов.
        
        Args:
            items: Список словарей с данными документов из источника
            
        Returns:
            Список словарей с данными элементов справочника для приемника
        """
        processed = []
        for item in items:
            try:
                processed_item = self.process_item_single(item)
                processed.append(processed_item)
            except Exception as e:
                verbose_print(f"Ошибка обработки документа {item.get('ЗаказПоставщику_UUID', 'unknown')}: {e}")
                import traceback
                verbose_print(traceback.format_exc())
                continue
        
        return processed

    def process_and_save_items(
        self, items: List[Dict], output_db_path: str, table_name: str = "supplier_orders_processed"
    ) -> bool:
        """
        Обрабатывает список элементов и сохраняет результат в БД.
        
        Args:
            items: Список словарей с данными элементов из источника
            output_db_path: Путь к выходной базе данных SQLite
            table_name: Имя таблицы для сохранения
            
        Returns:
            True если успешно, False если ошибка
        """
        verbose_print(f"\nОбработка {len(items)} документов ЗаказПоставщику...")
        
        processed = self.process_items(items)
        
        verbose_print(f"Обработано документов: {len(processed)}")
        
        if not processed:
            verbose_print("Нет обработанных документов для сохранения")
            return False
        
        # Преобразуем ссылочные поля в JSON формат
        reference_fields = set()
        
        for item in processed:
            for field_name in item.keys():
                if field_name.endswith("_UUID") or field_name.endswith("_Представление") or field_name.endswith("_Тип"):
                    base_field = field_name.rsplit("_", 1)[0]
                    reference_fields.add(base_field)
        
        for item in processed:
            for ref_field in reference_fields:
                uuid_field = f"{ref_field}_UUID"
                presentation_field = f"{ref_field}_Представление"
                type_field = f"{ref_field}_Тип"
                
                # Проверяем, есть ли уже JSON в поле (из исходной БД или после process_item)
                existing_json = None
                if ref_field in item and isinstance(item[ref_field], str) and item[ref_field].strip().startswith('{'):
                    try:
                        existing_json = json.loads(item[ref_field])
                    except (json.JSONDecodeError, ValueError):
                        pass
                
                # Если JSON уже есть, обновляем тип в нем
                if existing_json and isinstance(existing_json, dict) and 'type' in existing_json:
                    json_type = existing_json.get('type', '')
                    json_presentation = existing_json.get('presentation', '')
                    
                    # Для полей с search_method = "string_to_reference_by_name" 
                    # нужно использовать target_type из маппинга, а не исходный тип
                    # Проверяем, есть ли маппинг для этого поля
                    target_type_from_mapping = None
                    if ref_field in self.field_mapping:
                        target_type_from_mapping = self.field_mapping[ref_field].get("target_type")
                    else:
                        # Ищем маппинг по source_field
                        for source_field, mapping_info in self.field_mapping.items():
                            if mapping_info.get("target_field") == ref_field:
                                target_type_from_mapping = mapping_info.get("target_type")
                                break
                    
                    # Если нашли target_type из маппинга и это поле с search_method = "string_to_reference_by_name",
                    # используем target_type вместо исходного типа
                    if target_type_from_mapping:
                        existing_json['type'] = target_type_from_mapping
                    elif json_type:
                        # Пробуем найти маппинг для типа из JSON
                        if json_type in self.type_mapping:
                            existing_json['type'] = self.type_mapping[json_type]
                        elif json_type.startswith("Справочник.") and not json_type.startswith("СправочникСсылка."):
                            # Пробуем найти маппинг для типа с "Ссылка"
                            json_type_with_link = json_type.replace("Справочник.", "СправочникСсылка.", 1)
                            if json_type_with_link in self.type_mapping:
                                existing_json['type'] = self.type_mapping[json_type_with_link]
                    
                    # Обновляем presentation из служебных полей (приоритет над JSON)
                    # Если presentation в JSON - это вложенный JSON (строка), извлекаем из него
                    if presentation_field in item and item[presentation_field]:
                        presentation_value = item[presentation_field]
                        # Если presentation_value - это JSON-строка, пытаемся извлечь из него
                        if isinstance(presentation_value, str) and presentation_value.strip().startswith('{'):
                            try:
                                nested_json = json.loads(presentation_value)
                                if isinstance(nested_json, dict) and 'presentation' in nested_json:
                                    existing_json['presentation'] = nested_json['presentation']
                                else:
                                    existing_json['presentation'] = presentation_value
                            except:
                                existing_json['presentation'] = presentation_value
                        else:
                            existing_json['presentation'] = presentation_value
                    elif json_presentation:
                        # Если presentation в JSON - это вложенный JSON (строка), извлекаем из него
                        if isinstance(json_presentation, str) and json_presentation.strip().startswith('{'):
                            try:
                                nested_json = json.loads(json_presentation)
                                if isinstance(nested_json, dict) and 'presentation' in nested_json:
                                    existing_json['presentation'] = nested_json['presentation']
                            except:
                                pass
                    
                    item[ref_field] = json.dumps(existing_json, ensure_ascii=False)
                    # Удаляем служебные поля, если они есть
                    item.pop(uuid_field, None)
                    item.pop(presentation_field, None)
                    item.pop(type_field, None)
                    continue
                
                # Если нет JSON, но есть UUID или представление (для string_to_reference_by_name), создаем JSON
                ref_uuid = item.get(uuid_field, "") or ""
                ref_presentation = item.get(presentation_field, "") or ""
                ref_type = item.get(type_field, "") or ""
                
                # Создаем JSON если есть UUID или если есть представление и тип (для string_to_reference_by_name)
                # Проверяем наличие полей UUID, Представление или Тип (даже если UUID пустой)
                has_uuid_field = uuid_field in item
                has_presentation_field = presentation_field in item
                has_type_field = type_field in item
                
                if has_uuid_field or has_presentation_field or has_type_field:
                    # Если есть хотя бы одно из служебных полей, создаем JSON
                    # Применяем маппинг типа, если есть
                    if ref_type and ref_type in self.type_mapping:
                        ref_type = self.type_mapping[ref_type]
                    
                    json_data = {
                        "uuid": ref_uuid.strip() if ref_uuid else "",  # Может быть пустым для string_to_reference_by_name
                        "presentation": ref_presentation.strip() if ref_presentation else "",
                        "type": ref_type.strip() if ref_type else ""
                    }
                    
                    ref_json = json.dumps(json_data, ensure_ascii=False)
                    item[ref_field] = ref_json

                    item.pop(uuid_field, None)
                    item.pop(presentation_field, None)
                    item.pop(type_field, None)
        
        # Сохраняем в БД
        connection = None
        try:
            if not ensure_database_exists(output_db_path):
                verbose_print("Не удалось подготовить базу данных SQLite.")
                return False
            
            connection = connect_to_sqlite(output_db_path)
            if not connection:
                verbose_print("Не удалось подключиться к SQLite.")
                return False
            
            # Определяем базовые колонки для таблицы
            base_columns = {
                "uuid": "TEXT PRIMARY KEY",
                "Номер": "TEXT",
                "Дата": "TEXT",
            }
            
            saved = upsert_rows(
                connection,
                table_name,
                processed,
                base_columns,
            )
            
            connection.commit()
            verbose_print(f"\nСохранено документов в БД: {len(processed)}")
            return True
            
        except Exception as error:
            if connection:
                connection.rollback()
            print(f"Ошибка при сохранении документов: {error}")
            import traceback
            print(traceback.format_exc())
            return False
        finally:
            if connection:
                connection.close()


def process_supplier_orders(
    source_db_path: str,
    processed_db_path: str,
) -> bool:
    """
    Обрабатывает документы ЗаказПоставщику из исходной БД и сохраняет в новую БД.
    
    Args:
        source_db_path: Путь к исходной базе данных SQLite
        processed_db_path: Путь к выходной базе данных SQLite
        
    Returns:
        True если успешно, False если ошибка
    """
    mapping_db_path = "CONF/type_mapping.db"
    table_name = "supplier_orders"
    
    verbose_print("=" * 80)
    verbose_print("ОБРАБОТКА ДОКУМЕНТОВ ЗАКАЗПОСТАВЩИКУ С МАППИНГОМ")
    verbose_print("=" * 80)
    
    # Читаем данные из исходной БД
    verbose_print(f"\n[1/5] Чтение документов из исходной БД: {source_db_path}")
    items = read_from_db(source_db_path, table_name)
    if not items:
        verbose_print("Документы ЗаказПоставщику не найдены в исходной БД")
        return False
    verbose_print(f"Прочитано документов: {len(items)}")
    
    # Инициализируем процессор
    verbose_print("\n[2/5] Инициализация процессора маппинга...")
    processor = SupplierOrdersMappingProcessor(mapping_db_path)
    
    # Обрабатываем данные
    verbose_print("\n[3/5] Обработка документов с использованием маппинга...")
    success = processor.process_and_save_items(items, processed_db_path, "supplier_orders_processed")
    
    if success:
        verbose_print("\n[4/5] Обработка завершена успешно")
        verbose_print(f"\n[5/5] Результат сохранен в: {processed_db_path}")
    else:
        verbose_print("\n[4/5] Ошибка при обработке документов")
    
    return success


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Обработка документов ЗаказПоставщику с маппингом")
    parser.add_argument("--source-db", required=True, help="Путь к исходной БД SQLite")
    parser.add_argument("--processed-db", required=True, help="Путь к обработанной БД SQLite")
    parser.add_argument("--mapping-db", default="CONF/type_mapping.db", help="Путь к БД маппинга")
    
    args = parser.parse_args()
    
    success = process_supplier_orders(args.source_db, args.processed_db)
    sys.exit(0 if success else 1)

