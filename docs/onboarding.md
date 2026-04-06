# Онбординг

Этот документ нужен для первого запуска проекта и для регулярного обновления метаданных обеих конфигураций: источника `УПП` и приемника `УХ`.

## Что нужно подготовить

- Windows-машина с установленной платформой 1С и доступом к COM.
- Python `3.10+`.
- Файл `.env`, созданный из `.env.example`.
- Доступ к обеим базам 1С: источник и приемник.

Минимально обязательные переменные в `.env`:

```env
SOURCE_CONNECTION_STRING=
TARGET_CONNECTION_STRING=
```

Для продового и дополнительных приемников веб-интерфейс и CLI также читают:

- `TARGET_CONNECTION_STRING_PREPROD`
- `TARGET_CONNECTION_STRING_MATVEEV`
- `TARGET_CONNECTION_STRING_PROD`

Для путей к базам метаданных можно использовать:

- `SOURCE_METADATA`
- `TARGET_METADATA`

## Что считается метаданными в проекте

Проект использует два слоя описания конфигураций:

- SQLite-базы метаданных, которые строит `CONF/configuration_structure_loader.py`;
- JSON-выгрузки, которые удобно читать и хранить в Git.

В репозитории уже лежат:

- `CONF/upp_metadata.json`
- `CONF/uh_metadata.json`
- `CONF/type_mapping.db`
- `CONF/type_mapping.json`

SQLite-базы метаданных обычно обновляются локально и используются инструментами маппинга и обновления типов.

## Как обновить метаданные источника УПП

Команда:

```bash
python main.py --import --catalog configuration_structure --source-1c source --sqlite-db CONF/upp_metadata.db --json-output CONF/upp_metadata.json --verbose
```

Что делает команда:

- подключается к источнику 1С;
- читает справочники, документы и перечисления через COM;
- сохраняет структуру в `CONF/upp_metadata.db`;
- параллельно выгружает JSON в `CONF/upp_metadata.json`.

Если вместо алиаса `source` используется реальная строка подключения, можно передать ее напрямую в `--source-1c`.

## Как обновить метаданные приемника УХ

Команда:

```bash
python main.py --import --catalog configuration_structure --source-1c target --sqlite-db CONF/uh_metadata.db --json-output CONF/uh_metadata.json --verbose
```

Что важно:

- загрузчик метаданных вызывается тем же флагом `--import`;
- для приемника используется тот же модуль `configuration_structure_loader`;
- фактически проект рассматривает приемник как еще один источник структуры.

Если нужен конкретный приемник, вместо `target` можно передать нужную строку подключения из `.env`.

## Что проверить после обновления

Проверьте, что появились или обновились файлы:

- `CONF/upp_metadata.db`
- `CONF/uh_metadata.db`
- `CONF/upp_metadata.json`
- `CONF/uh_metadata.json`

Проверьте содержательно:

- в `metadata_catalogs` есть нужные справочники;
- в `metadata_documents` есть документы, если они участвуют в переносе;
- в `metadata_enumerations` есть перечисления со `values_json`.

Это критично, потому что:

- `tools/auto_mapping.py` строит маппинг по метаданным;
- `tools/update_reference_types.py` нормализует типы перечислений после импорта;
- процессоры опираются на `type_mapping.db`, который обычно актуализируется после обновления метаданных.

## Рекомендуемый порядок запуска на новом проекте

1. Настроить `.env`.
2. Обновить метаданные источника.
3. Обновить метаданные приемника.
4. Пересобрать или актуализировать `CONF/type_mapping.db`.
5. Проверить `CONF/catalog_mapping.json`.
6. Выполнить тестовый импорт одного справочника.
7. Прогнать обработку и экспорт на тестовом каталоге.

## Связанные файлы

- `main.py`
- `CONF/configuration_structure_loader.py`
- `tools/onec_connector.py`
- `CONF/upp_metadata.json`
- `CONF/uh_metadata.json`
