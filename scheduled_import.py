# -*- coding: utf-8 -*-
"""
Скрипт для регламентного запуска импорта и обработки справочников по расписанию.
Запускает чтение из 1С и обработку (load -> process) для выбранных справочников в режиме full.
БЕЗ записи в 1С-приемник.
"""

import os
import subprocess
import sys
import datetime
import logging
from tools.telegram_notifier import notify_catalog_export_completed

# Настройка логирования
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(LOGS_DIR, f"scheduled_import_{timestamp}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

def get_catalogs_to_process():
    """
    Возвращает список справочников для регламентной обработки.
    """
    return [
        "contractor_contracts",           # Договоры
        "contractors",                    # Контрагенты
        "supplier_orders",                # Заказы поставщиков
        "customer_orders",                # Заказы покупателей
        "managerial_contracts",           # управл договоры
        "managerial_additional_agreements", # управл доп соглашения
        "managerial_stages",              # управл этапы договров
        "managerial_stages_additional_agreements", # управл этапы доп соглашений
    ]

def run_catalog_import_process(catalog_name):
    """Запускает импорт и обработку одного справочника через main.py"""
    logging.info(f"Начало импорта и обработки справочника: {catalog_name}")
    
    cmd = [
        sys.executable, 
        "main.py",
        "--import",
        "--process",
        "--catalog", catalog_name,
        "--mode", "full"
    ]
    
    try:
        # Запускаем процесс и ждем завершения
        # Используем shell=False для безопасности, передаем список аргументов
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
        
        if result.returncode == 0:
            logging.info(f"Успешно завершен импорт и обработка: {catalog_name}")
            return True, result.stdout
        else:
            logging.error(f"Ошибка при импорте/обработке {catalog_name}: {result.stderr}")
            return False, result.stderr
            
    except Exception as e:
        logging.exception(f"Исключение при запуске импорта {catalog_name}: {str(e)}")
        return False, str(e)

def main():
    catalogs = get_catalogs_to_process()
    logging.info(f"Запуск регламентного импорта и обработки {len(catalogs)} справочников")
    
    results = []
    for catalog in catalogs:
        success, output = run_catalog_import_process(catalog)
        results.append((catalog, success))
    
    # Итоговый отчет
    success_count = sum(1 for _, s in results if s)
    fail_count = len(results) - success_count
    
    summary = f"Регламентный импорт завершен. Успешно: {success_count}, Ошибок: {fail_count}"
    logging.info(summary)
    
    if fail_count > 0:
        failed_list = ", ".join([c for c, s in results if not s])
        logging.error(f"Список неудачных: {failed_list}")
    
    # Отправка уведомления в Telegram (используем существующий механизм)
    try:
        from tools.telegram_notifier import send_telegram_message
        msg = f"📅 *Регламентный импорт (mode: full)*\n\n"
        msg += f"✅ Успешно: {success_count}\n"
        msg += f"❌ Ошибок: {fail_count}\n"
        if fail_count > 0:
            msg += f"⚠️ Проблемные: {failed_list}"
        
        send_telegram_message(msg)
    except Exception as e:
        logging.error(f"Не удалось отправить уведомление в Telegram: {e}")

if __name__ == "__main__":
    main()

