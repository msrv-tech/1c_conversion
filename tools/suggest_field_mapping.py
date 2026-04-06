# -*- coding: utf-8 -*-
"""
Универсальный механизм для предложения ручного маппинга несмаппированных полей.

Использование:
    python tools/suggest_field_mapping.py --catalog Контрагенты
    python tools/suggest_field_mapping.py --catalog Контрагенты --mapping-db CONF/type_mapping.db
    python tools/suggest_field_mapping.py --catalog Контрагенты --output suggestions.txt
"""

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Добавляем корень проекта в sys.path для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.encoding_fix import fix_encoding

fix_encoding()

# Загружаем переменные окружения из .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv не установлен, используем системные переменные окружения


def normalize(text: Optional[str]) -> str:
    """Нормализует текст для сравнения."""
    if not text:
        return ""
    return text.lower().replace(" ", "").replace("_", "").replace("-", "")


def load_target_fields(
    target_db_path: str, catalog_name: str
) -> Dict[Tuple[str, str, str], Dict]:
    """
    Загружает все поля приемника для указанного справочника.
    
    Returns:
        Словарь: (field_kind, section_name, field_name) -> поле
    """
    conn = sqlite3.connect(target_db_path)
    cursor = conn.cursor()
    target_field_map = {}

    try:
        cursor.execute(
            """
            SELECT requisites_json, tabular_sections_json
            FROM metadata_catalogs
            WHERE name = ?
        """,
            (catalog_name,),
        )

        target_row = cursor.fetchone()

        if not target_row:
            return target_field_map

        requisites_json, tabular_sections_json = target_row

        # Загружаем реквизиты
        try:
            requisites = json.loads(requisites_json or "[]")
            for item in requisites:
                field_name = item.get("name", "")
                field_type = item.get("type", "")
                if field_name:
                    key = ("requisite", "", field_name)
                    target_field_map[key] = {
                        "name": field_name,
                        "type": field_type,
                        "kind": "requisite",
                        "section": None,
                    }
        except (TypeError, json.JSONDecodeError):
            pass

        # Загружаем табличные части
        try:
            sections = json.loads(tabular_sections_json or "[]")
            for section in sections:
                section_name = section.get("name", "")
                # Атрибуты табличных частей
                for item in section.get("attributes", []):
                    field_name = item.get("name", "")
                    field_type = item.get("type", "")
                    if field_name:
                        key = ("tabular_attribute", section_name or "", field_name)
                        target_field_map[key] = {
                            "name": field_name,
                            "type": field_type,
                            "kind": "tabular_attribute",
                            "section": section_name,
                        }
                # Реквизиты табличных частей
                for item in section.get("requisites", []):
                    field_name = item.get("name", "")
                    field_type = item.get("type", "")
                    if field_name:
                        key = ("tabular_requisite", section_name or "", field_name)
                        target_field_map[key] = {
                            "name": field_name,
                            "type": field_type,
                            "kind": "tabular_requisite",
                            "section": section_name,
                        }
        except (TypeError, json.JSONDecodeError):
            pass
    finally:
        conn.close()

    return target_field_map


def find_similar_fields(
    source_field_name: str,
    source_field_kind: str,
    source_section: str,
    target_field_map: Dict[Tuple[str, str, str], Dict],
) -> List[Dict]:
    """
    Находит похожие поля в приемнике для указанного поля источника.
    
    Returns:
        Список словарей с информацией о похожих полях, отсортированный по оценке
    """
    source_normalized = normalize(source_field_name)
    matches = []

    # Определяем типы полей для поиска
    search_kinds = (
        ["requisite"]
        if source_field_kind == "requisite"
        else ["tabular_attribute", "tabular_requisite"]
    )

    for target_key, target_info in target_field_map.items():
        target_kind, target_section, target_name = target_key

        if target_kind not in search_kinds:
            continue

        # Для табличных частей проверяем также section_name
        if source_field_kind != "requisite":
            if (source_section or "") != (target_section or ""):
                continue

        target_normalized = normalize(target_name)

        # Исключаем служебные поля
        target_lower = target_name.lower()
        if any(
            target_lower.startswith(prefix)
            for prefix in ["удалить", "пометка", "флаг"]
        ):
            continue

        # Точное совпадение
        if source_normalized == target_normalized:
            matches.append(
                {
                    "name": target_name,
                    "type": target_info["type"],
                    "section": target_section,
                    "score": 100,
                    "match_type": "exact",
                }
            )
        # Частичное совпадение (содержит часть имени)
        elif source_normalized in target_normalized or target_normalized in source_normalized:
            # Вычисляем оценку похожести по длине общего префикса/суффикса
            source_clean = source_normalized
            target_clean = target_normalized

            # Удаляем общие префиксы
            common_prefix_len = 0
            for i in range(min(len(source_clean), len(target_clean))):
                if source_clean[i] == target_clean[i]:
                    common_prefix_len += 1
                else:
                    break

            # Удаляем общие суффиксы
            common_suffix_len = 0
            for i in range(1, min(len(source_clean), len(target_clean)) + 1):
                if source_clean[-i] == target_clean[-i]:
                    common_suffix_len += 1
                else:
                    break

            # Вычисляем похожесть
            max_len = max(len(source_normalized), len(target_normalized))
            similarity = (common_prefix_len + common_suffix_len) / max_len * 100

            # Также проверяем общие символы
            common_chars = sum(1 for c in source_normalized if c in target_normalized)
            char_similarity = (common_chars / max_len) * 100

            # Берем максимальную похожесть
            score = int(max(similarity, char_similarity))

            if score > 60:  # Минимальный порог
                matches.append(
                    {
                        "name": target_name,
                        "type": target_info["type"],
                        "section": target_section,
                        "score": score,
                        "match_type": "partial",
                    }
                )

    # Сортируем по оценке
    matches.sort(key=lambda x: x["score"], reverse=True)
    return matches


def suggest_mappings(
    catalog_name: str,
    mapping_db_path: str = None,
    source_metadata_db: str = None,
    target_metadata_db: str = None,
) -> Tuple[List[Dict], Dict]:
    """
    Предлагает ручные маппинги для несмаппированных полей указанного справочника.
    
    Returns:
        (suggestions, stats) - список предложений и статистика
    """
    # Используем переменные окружения, если параметры не переданы
    if mapping_db_path is None:
        mapping_db_path = os.getenv('TYPE_MAPPING_DB', 'CONF/type_mapping.db')
    if source_metadata_db is None:
        source_metadata_db = os.getenv('SOURCE_METADATA', 'CONF/upp_metadata.db')
    if target_metadata_db is None:
        target_metadata_db = os.getenv('TARGET_METADATA', 'CONF/uh_metadata.db')
    
    mapping_conn = sqlite3.connect(mapping_db_path)
    mapping_cursor = mapping_conn.cursor()

    # Получаем несмаппированные поля для указанного справочника
    mapping_cursor.execute(
        """
        SELECT 
            field_kind, section_name, field_name, source_type
        FROM field_mapping
        WHERE object_type = 'catalog' 
        AND object_name = ?
        AND status = 'missing_target'
        ORDER BY field_kind, section_name, field_name
    """,
        (catalog_name,),
    )

    unmapped_fields = mapping_cursor.fetchall()
    mapping_conn.close()

    if not unmapped_fields:
        return [], {"total": 0, "with_suggestions": 0, "without_suggestions": 0}

    # Загружаем поля приемника
    target_field_map = load_target_fields(target_metadata_db, catalog_name)

    # Группируем несмаппированные поля (убираем дубликаты)
    by_kind = {}
    seen_fields = set()

    for field_kind, section_name, field_name, source_type in unmapped_fields:
        section_key = section_name or ""

        # Объединяем tabular_attribute и tabular_requisite в один вывод
        display_kind = "requisite" if field_kind == "requisite" else "tabular"
        field_key = (display_kind, section_key, field_name)

        if field_key in seen_fields:
            continue

        seen_fields.add(field_key)

        if display_kind not in by_kind:
            by_kind[display_kind] = {}
        if section_key not in by_kind[display_kind]:
            by_kind[display_kind][section_key] = []
        by_kind[display_kind][section_key].append((field_name, source_type, field_kind))

    # Подбираем соответствия
    suggestions = []

    for field_kind in sorted(by_kind.keys()):
        for section_name in sorted(by_kind[field_kind].keys()):
            for field_name, source_type, original_kind in by_kind[field_kind][section_name]:
                matches = find_similar_fields(
                    field_name, original_kind, section_name, target_field_map
                )

                if matches:
                    for match in matches[:3]:  # Топ-3
                        suggestions.append(
                            {
                                "source_field": field_name,
                                "source_type": source_type,
                                "source_kind": original_kind,
                                "source_section": section_name or "",
                                "target_field": match["name"],
                                "target_type": match["type"],
                                "target_section": match["section"],
                                "score": match["score"],
                                "match_type": match["match_type"],
                            }
                        )

    # Статистика
    total_unmapped = len(seen_fields)
    fields_with_suggestions = len(
        set((s["source_field"], s["source_section"]) for s in suggestions)
    )
    fields_without_suggestions = total_unmapped - fields_with_suggestions

    stats = {
        "total": total_unmapped,
        "with_suggestions": fields_with_suggestions,
        "without_suggestions": fields_without_suggestions,
    }

    return suggestions, stats


def print_suggestions(
    catalog_name: str,
    suggestions: List[Dict],
    stats: Dict,
    output_file: Optional[str] = None,
):
    """Выводит предложения по маппингу."""
    output_lines = []

    output_lines.append("=" * 80)
    output_lines.append(f"ПРЕДЛОЖЕНИЯ РУЧНОГО МАППИНГА ДЛЯ СПРАВОЧНИКА: {catalog_name}")
    output_lines.append("=" * 80)

    output_lines.append(f"\nСтатистика:")
    output_lines.append(f"  Всего несмаппированных полей: {stats['total']}")
    output_lines.append(f"  С предложениями: {stats['with_suggestions']}")
    output_lines.append(f"  Без предложений: {stats['without_suggestions']}")

    if not suggestions:
        output_lines.append("\n✅ Все поля смаппированы или предложения не найдены!")
        output_text = "\n".join(output_lines)
        if output_file:
            Path(output_file).write_text(output_text, encoding="utf-8")
        else:
            print(output_text)
        return

    # Группируем по уверенности
    high_confidence = [s for s in suggestions if s["score"] >= 80]
    medium_confidence = [s for s in suggestions if 60 <= s["score"] < 80]
    low_confidence = [s for s in suggestions if s["score"] < 60]

    output_lines.append("\n" + "=" * 80)
    output_lines.append("КОМАНДЫ ДЛЯ ДОБАВЛЕНИЯ РУЧНЫХ МАППИНГОВ")
    output_lines.append("=" * 80)

    if high_confidence:
        output_lines.append("\nВысокая уверенность (score >= 80):")
        for sug in high_confidence:
            target_type_arg = (
                f' --target-type "{sug["target_type"]}"' if sug["target_type"] else ""
            )
            source_type_arg = (
                f' --source-type "{sug["source_type"]}"' if sug["source_type"] else ""
            )
            section_arg = (
                f' --section-name "{sug["source_section"]}"'
                if sug["source_section"]
                else ""
            )

            output_lines.append("")
            output_lines.append(
                f"# {sug['source_field']} → {sug['target_field']} (похожесть: {sug['score']}%, {sug['match_type']})"
            )
            # Формируем команду в одну строку (работает и в CMD, и в PowerShell)
            cmd = f'python tools/auto_mapping.py add-field --mapping-db CONF/type_mapping.db --object-type catalog --object-name "{catalog_name}" --field-name "{sug["source_field"]}" --target-field-name "{sug["target_field"]}"{source_type_arg}{target_type_arg}'
            output_lines.append(cmd)

    if medium_confidence:
        output_lines.append("\n\nСредняя уверенность (60 <= score < 80):")
        for sug in medium_confidence:
            target_type_arg = (
                f' --target-type "{sug["target_type"]}"' if sug["target_type"] else ""
            )
            source_type_arg = (
                f' --source-type "{sug["source_type"]}"' if sug["source_type"] else ""
            )

            output_lines.append("")
            output_lines.append(
                f"# {sug['source_field']} → {sug['target_field']} (похожесть: {sug['score']}%, {sug['match_type']})"
            )
            # Формируем команду в одну строку (работает и в CMD, и в PowerShell)
            cmd = f'python tools/auto_mapping.py add-field --mapping-db CONF/type_mapping.db --object-type catalog --object-name "{catalog_name}" --field-name "{sug["source_field"]}" --target-field-name "{sug["target_field"]}"{source_type_arg}{target_type_arg}'
            output_lines.append(cmd)

    if low_confidence:
        output_lines.append("\n\nНизкая уверенность (score < 60):")
        output_lines.append("(Рекомендуется проверить вручную)")
        for sug in low_confidence:
            output_lines.append(
                f"  {sug['source_field']} ({sug['source_type']}) → {sug['target_field']} ({sug['target_type']}) - {sug['score']}%"
            )

    output_lines.append("\n" + "=" * 80)

    output_text = "\n".join(output_lines)

    if output_file:
        Path(output_file).write_text(output_text, encoding="utf-8")
        print(f"Предложения сохранены в: {output_file}")
    else:
        print(output_text)


def main():
    parser = argparse.ArgumentParser(
        description="Предложение ручного маппинга для несмаппированных полей справочника"
    )
    parser.add_argument(
        "--catalog",
        required=True,
        help="Имя справочника (например, 'Контрагенты')",
    )
    parser.add_argument(
        "--mapping-db",
        default="CONF/type_mapping.db",
        help="Путь к базе маппинга (по умолчанию: CONF/type_mapping.db)",
    )
    parser.add_argument(
        "--source-metadata",
        default=os.getenv('SOURCE_METADATA', 'CONF/upp_metadata.db'),
        help="Путь к базе метаданных источника (по умолчанию из SOURCE_METADATA или CONF/upp_metadata.db)",
    )
    parser.add_argument(
        "--target-metadata",
        default=os.getenv('TARGET_METADATA', 'CONF/uh_metadata.db'),
        help="Путь к базе метаданных приемника (по умолчанию из TARGET_METADATA или CONF/uh_metadata.db)",
    )
    parser.add_argument(
        "--output",
        help="Путь к файлу для сохранения предложений (опционально)",
    )

    args = parser.parse_args()

    suggestions, stats = suggest_mappings(
        args.catalog, args.mapping_db, args.source_metadata, args.target_metadata
    )

    print_suggestions(args.catalog, suggestions, stats, args.output)


if __name__ == "__main__":
    main()

