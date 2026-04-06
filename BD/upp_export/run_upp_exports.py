# -*- coding: utf-8 -*-
"""
Групповой экспорт по файлам UUID из BD/upp_export.
Запускает export_by_code.py последовательно для каждого *_uuids.txt.

Использование:
    python BD/upp_export/run_upp_exports.py
    python BD/upp_export/run_upp_exports.py --uuids-dir BD/upp_export --target-1c target
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys

# Порядок экспорта (зависимости: contractors -> contractor_contracts, nomenclature -> characteristics/series)
DEFAULT_ORDER = [
    "contractors",
    "contractor_contracts",
    "bank_accounts",
    "nomenclature",
    "nomenclature_characteristics",
    "nomenclature_series",
    "ppe_usage_purposes",
    "customer_orders",
    "supplier_orders",
]

UUID_FILE_PATTERN = re.compile(r"^(.+)_uuids\.txt$")


def main() -> int:
    parser = argparse.ArgumentParser(description="Групповой экспорт по файлам UUID из upp_export")
    parser.add_argument(
        "--uuids-dir",
        default=None,
        help="Директория с файлами *_uuids.txt (по умолчанию — директория скрипта)",
    )
    parser.add_argument(
        "--target-1c",
        default="target",
        help="Приемник 1С (как в export_by_code.py)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Передать --verbose в export_by_code",
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Использовать продакшн базу приемника (TARGET_CONNECTION_STRING_PROD)",
    )
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(os.path.dirname(script_dir))  # проект C:\1c
    uuids_dir = os.path.join(base_dir, args.uuids_dir) if args.uuids_dir else script_dir
    if not os.path.isdir(uuids_dir):
        print(f"Ошибка: директория не найдена: {uuids_dir}")
        return 1

    # Собираем файлы: {catalog: path}
    catalog_files: dict[str, str] = {}
    for name in os.listdir(uuids_dir):
        m = UUID_FILE_PATTERN.match(name)
        if m:
            catalog = m.group(1)
            catalog_files[catalog] = os.path.join(uuids_dir, name)

    # Сортируем: сначала из DEFAULT_ORDER, затем остальные по алфавиту
    order_set = set(DEFAULT_ORDER)
    ordered = [c for c in DEFAULT_ORDER if c in catalog_files]
    extra = sorted(c for c in catalog_files if c not in order_set)
    catalogs = ordered + extra

    if not catalogs:
        print(f"Нет файлов *_uuids.txt в {uuids_dir}")
        return 0

    export_script = os.path.join(base_dir, "export_by_code.py")
    cmd_base = [sys.executable, export_script, "--target-1c", args.target_1c]
    if args.verbose:
        cmd_base.append("--verbose")
    if args.prod:
        cmd_base.append("--prod")

    for i, catalog in enumerate(catalogs, 1):
        path = catalog_files[catalog]
        print()
        print(f"========== [{i}/{len(catalogs)}] Экспорт: {catalog} ==========")
        cmd = cmd_base + ["--catalog", catalog, "--uuids-file", path]
        ret = subprocess.call(cmd)
        if ret != 0:
            print(f"Ошибка: {catalog} завершился с кодом {ret}")
            return ret

    print()
    print("========== Все экспорты завершены ==========")
    return 0


if __name__ == "__main__":
    sys.exit(main())
