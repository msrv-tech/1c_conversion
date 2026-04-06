#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скрипт для экспорта всех справочников с load_type = "Все",
кроме Контрагентов и Контактных лиц.

Генерирует команды для обоих режимов:
- prod: только экспорт в 1C (--export --prod)
- full: полный цикл (--import --process --export --mode full)
"""

import sys


def get_catalogs_to_export():
    """
    Возвращает список справочников для экспорта.
    Справочники с load_type = "Все", исключая Контрагентов и Контактные лица.
    """
    catalogs = [
        "amortization_expense_methods",
        "bank_accounts",
        "banks",
        "budget_turnover_items",
        "cash_flow_items",
        "cash_registers",
        "construction_objects",
        "cost_items",
        "currencies",
        "fixed_asset_events",
        "fixed_asset_writeoff_reasons",
        "nomenclature_types",
        "organization_departments",
        "other_income_and_expenses",
        "ppe_usage_purposes",
        "reserves",
        "salary_posting_methods",
        "strict_reporting_forms",
        "subkonto",
        "tax_registrations",
        "units_classifier",
        "warehouses",
    ]
    
    return sorted(catalogs)


def generate_command_for_catalog(catalog_name):
    """Генерирует команду для одного справочника с флагами prod и full (без verbose)."""
    return f'python main.py --export --catalog {catalog_name} --prod --mode full'


def main():
    """Основная функция."""
    # Получаем список справочников для экспорта
    catalogs = get_catalogs_to_export()
    
    if not catalogs:
        print("Не найдено справочников для экспорта", file=sys.stderr)
        sys.exit(1)
    
    # Генерируем команды для каждого справочника
    commands = [generate_command_for_catalog(catalog) for catalog in catalogs]
    
    # Объединяем все команды через ; для PowerShell
    single_command = " ; ".join(commands)
    
    print("# Экспорт всех справочников с load_type = 'Все'")
    print("# Исключены: contractors, contact_persons")
    print("# Команда с флагами --prod и --mode full (без --verbose)")
    print("# Для PowerShell используется ; вместо &&")
    print(f"# Всего справочников: {len(catalogs)}\n")
    print(single_command)


if __name__ == "__main__":
    main()

