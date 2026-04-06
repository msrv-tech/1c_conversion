# -*- coding: utf-8 -*-
"""Обновляет маппинг Описание -> НаименованиеПолное для справочника Проекты."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.manual_mapping import add_manual_field_mapping, export_mapping_to_json_compact

db = str(Path(__file__).resolve().parents[1] / "CONF" / "type_mapping.db")
ok = add_manual_field_mapping(db, "catalog", "Проекты", "Описание", "НаименованиеПолное")
if ok:
    export_mapping_to_json_compact(db)
sys.exit(0 if ok else 1)
