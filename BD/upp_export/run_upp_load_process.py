# -*- coding: utf-8 -*-
"""
Чтение и обработка всех справочников, используемых в потоке UPP-экспорта.
Запускает main.py --import --process для каждого справочника из run_upp_exports.

Использование:
    python BD/upp_export/run_upp_load_process.py
    python BD/upp_export/run_upp_load_process.py --source-1c source --mode full --verbose
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

# Справочники потока UPP (collect_upp_document_uuids -> run_upp_exports)
# Порядок: зависимости первыми (contractors -> contractor_contracts, nomenclature -> characteristics/series)
UPP_CATALOGS = [
    "contractors",
    "contractor_contracts",
    "bank_accounts",
    "nomenclature",
    "nomenclature_groups",
    "nomenclature_characteristics",
    "nomenclature_series",
    "ppe_usage_purposes",
    "customer_orders",
    "supplier_orders",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Чтение и обработка справочников потока UPP (import + process)"
    )
    parser.add_argument(
        "--source-1c",
        default="source",
        help="Источник 1С (как в main.py)",
    )
    parser.add_argument(
        "--sqlite-db",
        default="BD",
        help="Директория для БД SQLite",
    )
    parser.add_argument(
        "--mode",
        default="full",
        choices=["test", "full"],
        help="Режим: test (первые 50) или full (все записи)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Подробный вывод",
    )
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(os.path.dirname(script_dir))  # проект C:\1c
    main_script = os.path.join(base_dir, "main.py")

    cmd_base = [
        sys.executable,
        main_script,
        "--import",
        "--process",
        "--source-1c", args.source_1c,
        "--sqlite-db", args.sqlite_db,
        "--mode", args.mode,
    ]
    if args.verbose:
        cmd_base.append("--verbose")

    for i, catalog in enumerate(UPP_CATALOGS, 1):
        print()
        print(f"========== [{i}/{len(UPP_CATALOGS)}] Загрузка и обработка: {catalog} ==========")
        cmd = cmd_base + ["--catalog", catalog]
        ret = subprocess.call(cmd)
        if ret != 0:
            print(f"Ошибка: {catalog} завершился с кодом {ret}")
            return ret

    print()
    print("========== Все загрузки и обработки завершены ==========")
    return 0


if __name__ == "__main__":
    sys.exit(main())
