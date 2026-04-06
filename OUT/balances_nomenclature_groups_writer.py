# -*- coding: utf-8 -*-
"""
Модуль выгрузки номенклатурных групп из остатков в документ «Ввод начальных остатков».
"""

import os
import sys
import json
from typing import Dict, List, Optional

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field
from tools.base_writer import create_reference_by_uuid
from tools.onec_connector import connect_to_1c, safe_getattr, call_if_callable
from tools.logger import verbose_print

DOCUMENT_NAME = "ВводНачальныхОстатков"
TABLE_NAME = "РасчетыПоНалогамИСборам"
COMMENT_MARKER = "### Загрузка номенклатурных групп из остатков (31.12.2025) ###"

def write_balances_nomenclature_groups_to_1c(sqlite_db_file: str, com_object: object, process_func=None) -> bool:
    """Записывает номенклатурные группы в документ Ввод начальных остатков."""
    verbose_print("=" * 80)
    verbose_print("ВЫГРУЗКА НОМЕНКЛАТУРНЫХ ГРУПП В ДОКУМЕНТ ВВОД ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        verbose_print("Ошибка: COM-объект не передан.")
        return False

    # Шаг 1: Чтение из БД
    db_connection = connect_to_sqlite(sqlite_db_file)
    if not db_connection:
        return False
    
    items = get_from_db(db_connection, "balances_nomenclature_groups")
    db_connection.close()

    if not items:
        verbose_print("Нет данных для записи.")
        return True

    verbose_print(f"Прочитано групп для записи: {len(items)}")

    try:
        # Шаг 2: Поиск или создание документа
        documents_manager = safe_getattr(com_object, "Документы", None)
        doc_manager = safe_getattr(documents_manager, DOCUMENT_NAME, None)
        if not doc_manager:
            verbose_print(f"Ошибка: Менеджер документа {DOCUMENT_NAME} не найден.")
            return False

        # Пытаемся найти существующий документ по комментарию
        query_text = f"""ВЫБРАТЬ ПЕРВЫЕ 1
            Док.Ссылка КАК Ссылка
        ИЗ
            Документ.{DOCUMENT_NAME} КАК Док
        ГДЕ
            Док.Комментарий ПОДОБНО "%{COMMENT_MARKER}%"
            И Док.Дата МЕЖДУ ДАТАВРЕМЯ(2025, 12, 31, 0, 0, 0) И ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59)
        """
        
        query = com_object.NewObject("Запрос")
        query.Текст = query_text
        
        result = query.Выполнить()
        selection = result.Выбрать()
        
        doc_obj = None
        if selection.Следующий():
            verbose_print("Найден существующий документ Ввод начальных остатков. Будем обновлять.")
            doc_obj = selection.Ссылка.ПолучитьОбъект()
        else:
            verbose_print("Создаем новый документ Ввод начальных остатков.")
            doc_obj = doc_manager.СоздатьДокумент()
            # Попытка установить дату как строку в формате 1С
            # Если 1С COM не принимает NewObject("Дата"), попробуем строку
            doc_obj.Дата = "20251231235959"
            doc_obj.Комментарий = f"{COMMENT_MARKER}\nЗагружено автоматически."

        # Шаг 3: Заполнение табличной части
        # Очищаем старые строки, если мы обновляем документ
        tabular_section = safe_getattr(doc_obj, TABLE_NAME, None)
        if tabular_section:
            tabular_section.Очистить()
            
            ng_manager = com_object.Справочники.НоменклатурныеГруппы
            
            for item in items:
                ng_json = item.get("НоменклатурнаяГруппа")
                if not ng_json:
                    continue
                
                ref_info = parse_reference_field(ng_json)
                if not ref_info or not ref_info.get("uuid"):
                    continue
                
                # Создаем или находим объект с минимальными данными (UUID + Наименование)
                # Это также зарегистрирует объект в reference_objects.db для дальнейшего дозаполнения
                ng_ref = create_reference_by_uuid(
                    com_object,
                    ref_info["uuid"],
                    "Справочник.НоменклатурныеГруппы",
                    ref_presentation=ref_info.get("presentation", ""),
                    processed_db=sqlite_db_file
                )
                
                if ng_ref:
                    new_row = tabular_section.Добавить()
                    new_row.НоменклатурнаяГруппа = ng_ref
                # Другие поля оставляем пустыми по условию задачи

        # Шаг 4: Запись
        doc_obj.ОбменДанными.Загрузка = True
        try:
            doc_obj.Записать()
            verbose_print(f"Документ успешно записан: {doc_obj.Ссылка}")
        except Exception as write_err:
            verbose_print(f"Ошибка при записи документа: {write_err}")
            return False

        return True

    except Exception as e:
        verbose_print(f"Критическая ошибка при записи в 1С: {e}")
        import traceback
        verbose_print(traceback.format_exc())
        return False

if __name__ == "__main__":
    # Для автономного запуска
    target = os.getenv("TARGET_1C", "target")
    com = connect_to_1c(target)
    if com:
        write_balances_nomenclature_groups_to_1c("BD/balances_nomenclature_groups_processed.db", com)

