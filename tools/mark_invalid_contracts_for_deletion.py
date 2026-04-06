# -*- coding: utf-8 -*-
"""
Разовый скрипт: пометить на удаление ошибочно созданные договоры из other_debtors_balances.

В остатках 76 поле «Договор» может содержать ссылки не на ДоговорыКонтрагентов, а на:
- Справочник.РасходыБудущихПериодов
- Документы (ПлатежноеПоручение, КорректировкаРеализации и т.д.)

При предыдущем экспорте такие ссылки ошибочно создавались как ДоговорыКонтрагентов.
Скрипт находит их по БД остатков и помечает на удаление в 1С.

Использование:
    python tools/mark_invalid_contracts_for_deletion.py [--db BD/other_debtors_balances_processed.db] [--target target]
    python tools/mark_invalid_contracts_for_deletion.py --dry-run   # только показать, не помечать
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_manager import connect_to_sqlite
from tools.writer_utils import get_from_db, parse_reference_field
from tools.onec_connector import connect_to_1c, safe_getattr, find_object_by_uuid


VALID_CONTRACT_TYPES = ("ДоговорыКонтрагентов", "Справочник.ДоговорыКонтрагентов", "СправочникСсылка.ДоговорыКонтрагентов")


def is_valid_contract_type(ref_type: str) -> bool:
    if not ref_type:
        return False
    return any(v in (ref_type or "") for v in VALID_CONTRACT_TYPES)


def main():
    parser = argparse.ArgumentParser(description="Пометить на удаление ошибочные договоры из остатков 76")
    parser.add_argument("--db", default="BD/other_debtors_balances_processed.db", help="Путь к БД остатков 76")
    parser.add_argument("--target", default="target", help="Инфобаз 1С (target/source)")
    parser.add_argument("--dry-run", action="store_true", help="Только показать список, не помечать")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    if not os.path.exists(db_path):
        print(f"Ошибка: БД не найдена: {db_path}")
        return 1

    conn = connect_to_sqlite(db_path)
    if not conn:
        return 1

    items = get_from_db(conn, "other_debtors_balances")
    conn.close()

    if not items:
        print("Нет данных в other_debtors_balances.")
        return 0

    invalid_uuids = {}
    for item in items:
        dog_info = parse_reference_field(item.get("Договор"))
        if dog_info and dog_info.get("uuid"):
            ref_type = dog_info.get("type", "")
            if not is_valid_contract_type(ref_type):
                uid = dog_info["uuid"]
                if uid not in invalid_uuids:
                    invalid_uuids[uid] = {"type": ref_type, "presentation": dog_info.get("presentation", "")[:50]}

    if not invalid_uuids:
        print("Ошибочных договоров не найдено.")
        return 0

    print(f"Найдено ошибочных ссылок (не ДоговорыКонтрагентов): {len(invalid_uuids)}")
    for uid, info in list(invalid_uuids.items())[:10]:
        print(f"  {uid[:20]}... | {info['type']} | {info['presentation']}")
    if len(invalid_uuids) > 10:
        print(f"  ... и ещё {len(invalid_uuids) - 10}")

    if args.dry_run:
        print("\n[--dry-run] Пометить на удаление не выполняется.")
        return 0

    com = connect_to_1c(args.target)
    if not com:
        return 1

    print("\nПодключено к 1С. Пометка на удаление...")
    marked = 0
    for uid in invalid_uuids:
        if not uid or uid == "00000000-0000-0000-0000-000000000000":
            continue
        try:
            ref = find_object_by_uuid(com, uid, "Справочник.ДоговорыКонтрагентов")
            if ref and not ref.Пустая():
                obj = ref.ПолучитьОбъект()
                if obj and not getattr(obj, "ПометкаУдаления", True):
                    obj.ПометкаУдаления = True
                    obj.ОбменДанными.Загрузка = True
                    obj.Записать()
                    marked += 1
                    print(f"  Помечен: {invalid_uuids[uid]['presentation']} ({uid[:8]}...)")
        except Exception as e:
            print(f"  Ошибка UUID {uid[:8]}...: {e}")

    print(f"\nПомечено на удаление: {marked}")
    return 0


if __name__ == "__main__":
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    sys.exit(main())
