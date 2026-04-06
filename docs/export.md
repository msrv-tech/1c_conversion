# Экспорт

Экспорт записывает обработанные данные из SQLite в 1С приемник через COM. За стадию отвечают `main.py --export`, `export_stage.py` и модули из `OUT/`.

## Базовый сценарий

Поток выполнения:

1. `main.py` берет обработанную БД `BD/<catalog>_processed.db`.
2. `export_stage.py` поднимает COM-подключение к приемнику.
3. Загружается writer `OUT/<catalog>_writer.py`.
4. Вызывается `write_<catalog>_to_1c(sqlite_db, com_object, process_func)`.
5. При успешной выгрузке может отправляться уведомление через `tools/telegram_notifier.py`.

Экспорт работает только из обработанной БД. Если `*_processed.db` отсутствует, стадия завершается ошибкой.

## Основные флаги CLI

- `--export` включает стадию экспорта.
- `--catalog` задает каталог или `all`.
- `--target-1c` задает приемник.
- `--sqlite-db` задает корень сырых БД, нужен для единого запуска пайплайна.
- `--processed-db` задает корень обработанных БД, если он не совпадает с `--sqlite-db`.
- `--mode test|full` влияет на лимит чтения writer.
- `--prod` переключает экспорт на `TARGET_CONNECTION_STRING_PROD` и одновременно переводит `reference_objects` в отдельную prod-базу.
- `--verbose` включает подробный лог.
- `--log-file` позволяет явно указать файл логов.

## Типовой запуск

Экспорт одного каталога:

```bash
python main.py --export --catalog contractors --target-1c target --sqlite-db BD
```

Полный конвейер для одного каталога:

```bash
python main.py --import --process --export --catalog contractors --source-1c source --target-1c target --sqlite-db BD --mode test
```

Экспорт всех подготовленных каталогов:

```bash
python main.py --export --catalog all --target-1c target --sqlite-db BD
```

Экспорт в прод:

```bash
python main.py --export --catalog contractors --sqlite-db BD --prod
```

## Что важно про writers

Writer берет уже подготовленные данные и отвечает за прикладную запись в 1С:

- поиск или создание объекта в приемнике;
- заполнение реквизитов;
- запись табличных частей;
- разрешение ссылок;
- сохранение служебных состояний.

Если каталог поддерживает частичный экспорт, для него может использоваться отдельный сценарий `export_by_code.py`.

## Доэкспорт незаполненных ссылок

После основного экспорта можно отдельно догружать ссылочные объекты, которые были накоплены, но не заполнены полностью.

Команда:

```bash
python main.py --fill-unfilled --target-1c target --processed-db BD
```

Дополнительно можно ограничить обработку одним каталогом:

```bash
python main.py --fill-unfilled --target-1c target --processed-db BD --fill-unfilled-catalog managerial_contracts
```

Этот сценарий использует:

- `tools/fill_unfilled_references.py`
- `BD/reference_objects.db` или `BD/reference_objects_prod.db`
- `CONF/catalog_mapping.json`
- `CONF/type_mapping.db`

## Логи

При экспорте `main.py` автоматически создает лог-файл в каталоге `logs/`, если `--log-file` не задан явно.

Это особенно важно для:

- массового экспорта `all`;
- запуска из веб-интерфейса;
- диагностики ошибок на COM-операциях.

## Что проверять при отладке

- существует ли `OUT/<catalog>_writer.py`;
- есть ли обработанная БД `*_processed.db`;
- совпадает ли имя таблицы в БД с ожиданием writer;
- корректен ли приемник и строка подключения;
- не остались ли незаполненные ссылки в `reference_objects`.

## Связанные файлы

- `main.py`
- `export_stage.py`
- `OUT/`
- `export_by_code.py`
- `tools/fill_unfilled_references.py`
- `tools/reference_objects.py`
