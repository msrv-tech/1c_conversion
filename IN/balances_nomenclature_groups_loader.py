# -*- coding: utf-8 -*-
"""
Модуль загрузки номенклатурных групп из остатков регистра бухгалтерии.
"""

import os
import sys
from typing import List

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.onec_connector import (  # noqa: E402
    connect_to_1c,
    execute_query,
    upsert_rows,
)
from tools.db_manager import (  # noqa: E402
    connect_to_sqlite,
    ensure_database_exists,
    process_reference_fields,
)
from tools.logger import verbose_print  # noqa: E402

# Константы
TABLE_NAME = "balances_nomenclature_groups"
DB_FILE = "BD/balances_nomenclature_groups.db"

ACCOUNTS = [
    "20.01.1", "23.01", "23.03", "29.01",
    "90.01.1", "90.02.1", "90.03", "90.07.1", "90.08.1"
]

COLUMNS = [
    "НоменклатурнаяГруппа",
    "НоменклатурнаяГруппа_Представление",
    "НоменклатурнаяГруппа_UUID",
    "НоменклатурнаяГруппа_Тип",
]

REFERENCE_COLUMNS = ["НоменклатурнаяГруппа"]

def load_balances_nomenclature_groups(
    sqlite_db_file: str,
    com_object,
    mode: str = "test",
    process_func=None,
    filters_db: str | None = None,
) -> bool:
    verbose_print("=" * 80)
    verbose_print("ЗАГРУЗКА НОМЕНКЛАТУРНЫХ ГРУПП ИЗ ОСТАТКОВ")
    verbose_print("=" * 80)

    if not com_object:
        return False

    limit_clause = "ПЕРВЫЕ 10" if mode == "test" else ""
    query_text = f"""ВЫБРАТЬ {limit_clause}
    Т.НоменклатурнаяГруппа КАК НоменклатурнаяГруппа,
    ПРЕДСТАВЛЕНИЕ(Т.НоменклатурнаяГруппа) КАК НоменклатурнаяГруппа_Представление,
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Т.НоменклатурнаяГруппа)) КАК НоменклатурнаяГруппа_UUID,
    ТИПЗНАЧЕНИЯ(Т.НоменклатурнаяГруппа) КАК НоменклатурнаяГруппа_Тип
ИЗ
    (ВЫБРАТЬ РАЗЛИЧНЫЕ
        ХозрасчетныйОстатки.Субконто1 КАК НоменклатурнаяГруппа
    ИЗ
        РегистрБухгалтерии.Хозрасчетный.Остатки(ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59), Счет.Код В (&КодыСчетов), , ) КАК ХозрасчетныйОстатки
    ГДЕ
        ТИПЗНАЧЕНИЯ(ХозрасчетныйОстатки.Субконто1) = ТИП(Справочник.НоменклатурныеГруппы)

    ОБЪЕДИНИТЬ

    ВЫБРАТЬ РАЗЛИЧНЫЕ
        ХозрасчетныйОстатки.Субконто2
    ИЗ
        РегистрБухгалтерии.Хозрасчетный.Остатки(ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59), Счет.Код В (&КодыСчетов), , ) КАК ХозрасчетныйОстатки
    ГДЕ
        ТИПЗНАЧЕНИЯ(ХозрасчетныйОстатки.Субконто2) = ТИП(Справочник.НоменклатурныеГруппы)

    ОБЪЕДИНИТЬ

    ВЫБРАТЬ РАЗЛИЧНЫЕ
        ХозрасчетныйОстатки.Субконто3
    ИЗ
        РегистрБухгалтерии.Хозрасчетный.Остатки(ДАТАВРЕМЯ(2025, 12, 31, 23, 59, 59), Счет.Код В (&КодыСчетов), , ) КАК ХозрасчетныйОстатки
    ГДЕ
        ТИПЗНАЧЕНИЯ(ХозрасчетныйОстатки.Субконто3) = ТИП(Справочник.НоменклатурныеГруппы)) КАК Т
"""

    try:
        # Подготовка списка кодов счетов для запроса
        codes_list = com_object.NewObject("СписокЗначений")
        for code in ACCOUNTS:
            codes_list.Add(code)

        params = {
            "КодыСчетов": codes_list
        }

        verbose_print(f"Выполнение запроса остатков на 31.12.2025 (режим: {mode}) по счетам: {', '.join(ACCOUNTS)}...")
        
        rows = execute_query(
            com_object,
            query_text,
            COLUMNS,
            params=params
        )

        if not rows:
            verbose_print("Остатки по номенклатурным группам не найдены.")
            return True

        verbose_print(f"Найдено уникальных групп в остатках: {len(rows)}")

        if not ensure_database_exists(sqlite_db_file):
            return False

        connection = connect_to_sqlite(sqlite_db_file)
        if not connection:
            return False

        try:
            # Обрабатываем ссылочные поля (превращаем в JSON)
            rows = process_reference_fields(rows, REFERENCE_COLUMNS)

            # Сохраняем в SQLite. Используем UUID как первичный ключ для автоматической дедупликации (upsert)
            # Но в данном случае у нас колонка НоменклатурнаяГруппа_UUID (после process_reference_fields она пропала из row, 
            # так как ушла в JSON поля НоменклатурнаяГруппа). 
            # Нам нужно вытащить UUID обратно для первичного ключа или использовать НоменклатурнаяГруппа как текст.
            
            # Для простоты добавим колонку uuid в данные
            import json
            for row in rows:
                ref_data = json.loads(row["НоменклатурнаяГруппа"])
                row["uuid"] = ref_data["uuid"]

            saved = upsert_rows(
                connection,
                TABLE_NAME,
                rows,
                {
                    "uuid": "TEXT PRIMARY KEY",
                    "НоменклатурнаяГруппа": "TEXT",
                }
            )
            verbose_print(f"Сохранено в SQLite: {saved} записей.")
        finally:
            connection.close()

    except Exception as e:
        verbose_print(f"Ошибка при загрузке: {e}")
        import traceback
        verbose_print(traceback.format_exc())
        return False
    
    return True

if __name__ == "__main__":
    # Для автономного запуска
    source = os.getenv("SOURCE_1C", "source")
    com = connect_to_1c(source)
    if com:
        load_balances_nomenclature_groups(DB_FILE, com)
