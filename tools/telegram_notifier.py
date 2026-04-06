# -*- coding: utf-8 -*-
"""
Модуль для отправки уведомлений через Telegram бота
"""

import requests
import sys
import json
import os
from typing import Optional

# Загружаем переменные окружения из .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv не установлен, используем системные переменные окружения

# Токен бота и chat_id из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Формируем URL API только если токен задан
if TELEGRAM_BOT_TOKEN:
    TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
else:
    TELEGRAM_API_URL = None


def send_telegram_message(message: str, chat_id: Optional[str] = None) -> bool:
    """
    Отправляет сообщение в Telegram.
    
    Args:
        message: Текст сообщения
        chat_id: ID чата (если не указан, используется TELEGRAM_CHAT_ID)
    
    Returns:
        True если сообщение отправлено успешно, False в противном случае
    """
    # Проверяем наличие токена и URL
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_API_URL:
        return False
    
    if not chat_id:
        chat_id = TELEGRAM_CHAT_ID
    
    if not chat_id:
        return False
    
    try:
        url = f"{TELEGRAM_API_URL}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        # Не выводим ошибку в консоль, чтобы не мешать основному процессу
        # Можно добавить логирование при необходимости
        return False
    except Exception as e:
        return False


def get_catalog_russian_name(catalog_name: str) -> Optional[str]:
    """
    Получает русское название справочника из catalog_mapping.json.
    
    Args:
        catalog_name: Английское имя справочника (например, "currencies")
    
    Returns:
        Русское название справочника или None, если не найдено
    """
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        mapping_path = os.path.join(base_dir, "CONF", "catalog_mapping.json")
        
        if not os.path.exists(mapping_path):
            return None
        
        with open(mapping_path, "r", encoding="utf-8") as f:
            mapping = json.load(f)
        
        # Ищем справочник по catalog_name
        for key, value in mapping.items():
            if isinstance(value, dict) and value.get("catalog_name") == catalog_name:
                # Извлекаем русское название из ключа (убираем префикс "Справочник.")
                if key.startswith("Справочник."):
                    return key.replace("Справочник.", "")
                return key
        
        return None
    except Exception:
        return None


def notify_catalog_export_completed(catalog_name: str, success: bool = True, 
                                     record_count: Optional[int] = None,
                                     error_message: Optional[str] = None,
                                     target_db: Optional[str] = None) -> bool:
    """
    Отправляет уведомление об окончании экспорта справочника в 1С.
    
    Args:
        catalog_name: Имя справочника (английское)
        success: True если экспорт успешен, False если была ошибка
        record_count: Количество записей (опционально)
        error_message: Сообщение об ошибке (если success=False)
        target_db: Путь или название базы приемника (опционально)
    
    Returns:
        True если уведомление отправлено успешно, False в противном случае
    """
    # Получаем русское название справочника
    russian_name = get_catalog_russian_name(catalog_name)
    catalog_display = russian_name if russian_name else catalog_name
    
    if success:
        message = f"✅ <b>Экспорт завершен</b>\n"
        message += f"Справочник: <b>{catalog_display}</b> (<code>{catalog_name}</code>)\n"
        if target_db:
            message += f"База приемник: <code>{target_db}</code>\n"
        if record_count is not None:
            message += f"Записей: {record_count}"
    else:
        message = f"❌ <b>Ошибка экспорта</b>\n"
        message += f"Справочник: <b>{catalog_display}</b> (<code>{catalog_name}</code>)\n"
        if target_db:
            message += f"База приемник: <code>{target_db}</code>\n"
        if error_message:
            message += f"Ошибка: {error_message}"
        else:
            message += "Экспорт завершился с ошибкой"
    
    return send_telegram_message(message)

