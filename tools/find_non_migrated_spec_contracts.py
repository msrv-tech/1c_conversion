# -*- coding: utf-8 -*-
"""
Скрипт для поиска договоров в УХ с видом соглашения Спецификация,
которые были созданы НЕ с помощью переноса (отсутствуют в reference_objects_prod
и в обработанных БД customer_orders_processed / supplier_orders_processed).

Использование:
    python tools/find_non_migrated_spec_contracts.py --target target
    python tools/find_non_migrated_spec_contracts.py --prod
"""

import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from tools.encoding_fix import fix_encoding
fix_encoding()

from tools.onec_connector import connect_to_1c, execute_query, find_object_by_uuid
from tools.reference_objects import set_prod_mode, get_reference_objects_db_path

QUERY_TEXT = """
ВЫБРАТЬ
    ПРЕДСТАВЛЕНИЕ(УНИКАЛЬНЫЙИДЕНТИФИКАТОР(Договор.Ссылка)) КАК UUID,
    Договор.Наименование КАК Наименование
ИЗ
    Справочник.ДоговорыКонтрагентов КАК Договор
ГДЕ
    НЕ Договор.ЭтоГруппа
    И Договор.ВидСоглашения = ЗНАЧЕНИЕ(Перечисление.ВидыСоглашений.Спецификация)
"""

COLUMNS = ["UUID", "Наименование"]


def load_migrated_uuids(base_dir: str) -> set:
    """Загружает UUID договоров, созданных через перенос."""
    bd_dir = os.path.join(base_dir, "BD") if base_dir else "BD"
    migrated = set()

    # 1. reference_objects_prod — все договоры, записанные нашим экспортом
    set_prod_mode(True)
    refs_path = get_reference_objects_db_path(base_dir)
    if os.path.exists(refs_path):
        conn = sqlite3.connect(refs_path)
        cur = conn.cursor()
        cur.execute(
            """SELECT ref_uuid FROM reference_objects
               WHERE ref_type = 'Справочник.ДоговорыКонтрагентов'"""
        )
        for row in cur.fetchall():
            if row[0] and row[0] != "00000000-0000-0000-0000-000000000000":
                migrated.add(row[0].strip().lower())
        conn.close()

    # 2. customer_orders_processed — заказы покупателей (uuid = uuid договора)
    co_path = os.path.join(bd_dir, "customer_orders_processed.db")
    if os.path.exists(co_path):
        conn = sqlite3.connect(co_path)
        cur = conn.cursor()
        cur.execute("SELECT uuid FROM customer_orders_processed")
        for row in cur.fetchall():
            if row[0]:
                migrated.add(str(row[0]).strip().lower())
        conn.close()

    # 3. supplier_orders_processed — заказы поставщиков
    so_path = os.path.join(bd_dir, "supplier_orders_processed.db")
    if os.path.exists(so_path):
        conn = sqlite3.connect(so_path)
        cur = conn.cursor()
        cur.execute("SELECT uuid FROM supplier_orders_processed")
        for row in cur.fetchall():
            if row[0]:
                migrated.add(str(row[0]).strip().lower())
        conn.close()

    return migrated


def main():
    parser = argparse.ArgumentParser(
        description="Поиск договоров Спецификация, созданных не через перенос"
    )
    parser.add_argument(
        "--target",
        default=None,
        help="Конфигурация 1С (target) или путь к базе",
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Использовать TARGET_CONNECTION_STRING_PROD",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Сохранить наименования в файл",
    )
    parser.add_argument(
        "--mark-deletion",
        action="store_true",
        help="Пометить на удаление договоры с наименованием 'Основной договор'",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="При --mark-deletion: только показать, не помечать",
    )
    args = parser.parse_args()

    if not args.target and not args.prod:
        print("Укажите --target или --prod")
        sys.exit(1)

    target = args.target
    if args.prod:
        target = os.getenv("TARGET_CONNECTION_STRING_PROD")
        if not target:
            print("Ошибка: TARGET_CONNECTION_STRING_PROD не задан в .env")
            sys.exit(1)
        set_prod_mode(True)

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Загружаем UUID перенесённых договоров
    print("Загрузка UUID перенесённых договоров...")
    migrated = load_migrated_uuids(base_dir)
    print(f"  Найдено в reference_objects_prod + processed: {len(migrated)}")

    # Подключаемся к 1С и получаем все договоры Спецификация
    print("\nПодключение к 1С и запрос договоров с ВидСоглашения=Спецификация...")
    com = connect_to_1c(target)
    if not com:
        print("Ошибка подключения к 1С")
        sys.exit(1)

    try:
        rows = execute_query(com, QUERY_TEXT, COLUMNS)
    except Exception as e:
        print(f"Ошибка запроса: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print(f"  Всего договоров Спецификация в УХ: {len(rows)}")

    # Фильтруем: оставляем только те, которых нет в migrated
    non_migrated = []
    for r in rows:
        uuid_val = r.get("UUID") or r.get("uuid") or ""
        if isinstance(uuid_val, str):
            uuid_clean = uuid_val.strip().lower()
        else:
            uuid_clean = str(uuid_val).strip().lower()
        if not uuid_clean or uuid_clean == "00000000-0000-0000-0000-000000000000":
            continue
        if uuid_clean not in migrated:
            name = r.get("Наименование") or r.get("name") or ""
            if hasattr(name, "strip"):
                name = str(name).strip()
            else:
                name = str(name)
            non_migrated.append((uuid_clean, name))

    print(f"\nДоговоров Спецификация, созданных НЕ через перенос: {len(non_migrated)}")

    # Фильтр "Основной договор" для пометки на удаление
    osnovnoy_name = "Основной договор"
    to_mark = [(u, n) for u, n in non_migrated if (n or "").strip() == osnovnoy_name]

    if args.mark_deletion:
        if not to_mark:
            print(f"\nДоговоров с наименованием '{osnovnoy_name}' не найдено.")
        elif args.dry_run:
            print(f"\n[--dry-run] Будет помечено на удаление {len(to_mark)} договоров '{osnovnoy_name}':")
            for uuid_val, name in to_mark[:20]:
                print(f"  {uuid_val[:8]}...")
            if len(to_mark) > 20:
                print(f"  ... и ещё {len(to_mark) - 20}")
        else:
            print(f"\nПометка на удаление {len(to_mark)} договоров '{osnovnoy_name}'...")
            marked = 0
            for uuid_val, name in to_mark:
                try:
                    ref = find_object_by_uuid(com, uuid_val, "Справочник.ДоговорыКонтрагентов")
                    if ref and not ref.Пустая():
                        obj = ref.ПолучитьОбъект()
                        if obj and not getattr(obj, "ПометкаУдаления", True):
                            obj.ПометкаУдаления = True
                            obj.ОбменДанными.Загрузка = True
                            obj.Записать()
                            marked += 1
                            print(f"  Помечен: {uuid_val[:8]}...")
                except Exception as e:
                    print(f"  Ошибка {uuid_val[:8]}...: {e}")
            print(f"\nПомечено на удаление: {marked}")
        return

    print("\nНаименования:")
    print("-" * 80)
    for uuid_val, name in sorted(non_migrated, key=lambda x: (x[1] or "")):
        print(f"  {name or '(пусто)'}  [{uuid_val[:8]}...]")

    if args.output and non_migrated:
        with open(args.output, "w", encoding="utf-8") as f:
            for uuid_val, name in non_migrated:
                f.write(f"{uuid_val}\t{name}\n")
        print(f"\nСохранено в {args.output}")


if __name__ == "__main__":
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
    main()
