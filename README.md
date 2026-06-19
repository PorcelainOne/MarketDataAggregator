# Market Data Aggregator

Имитация системы сбора, нормализации, дедупликации и хранения биржевых тиков.

## Состав

- `src/MarketDataAggregator.Simulator` - локальные WebSocket-источники с тремя форматами сообщений.
- `src/MarketDataAggregator.Worker` - pipeline: WebSocket client, parser, dedup, batch writer, metrics.
- `src/MarketDataAggregator.Infrastructure` - transport, parsers, repository, SQL builder.
- `tests/MarketDataAggregator.Tests` - self-contained test runner без внешних пакетов.

## Запуск

1. Поднять PostgreSQL:

```powershell
docker compose up -d
```

2. Запустить simulator:

```powershell
dotnet run --project src/MarketDataAggregator.Simulator
```

3. Запустить worker:

```powershell
dotnet run --project src/MarketDataAggregator.Worker
```

4. Запустить тесты:

```powershell
dotnet run --project tests/MarketDataAggregator.Tests
```

## Архитектура

Каждый источник — отдельный `BackgroundService` с `ClientWebSocket` и reconnect-циклом. Сообщения пишутся в `Channel<RawExchangeMessage>`, откуда парсер нормализует их в `Channel<MarketTick>`. `Channel` выбран вместо `ConcurrentQueue` — он даёт backpressure через `BoundedChannelFullMode.Wait` и async-ожидание без polling.

Дедупликация двухуровневая: `SlidingWindowDuplicateDetector` отсекает повторы в памяти, `ON CONFLICT DO NOTHING` закрывает кейс перезапуска процесса. Batch writer сбрасывает данные по размеру (100 тиков) или по таймеру (500 мс) — что наступит раньше.

## PostgreSQL

Схема лежит в `src/MarketDataAggregator.Infrastructure/Persistence/Schema.sql`.

Репозиторий использует `DbProviderFactories`, поэтому конкретный провайдер можно подставить через `Storage:Mode=postgres`, `Storage:ProviderName` и `Storage:ConnectionString`.
В текущей конфигурации worker по умолчанию использует in-memory repository. Для `postgres` нужен зарегистрированный provider, например `Npgsql`.

## Ограничения

- in-memory deduplication работает в рамках одного процесса;
- для горизонтального масштабирования нужен внешний shared state;
- при долгом outage БД production-версия потребовала бы durable queue;
- в текущем виде репозиторий запускается без внешних пакетов; для `postgres`-режима нужен зарегистрированный provider в окружении, например `Npgsql`.

## AI Usage

ИИ использовался для ускорения, не для замены понимания.

Scaffolding, черновые реализации парсеров и SQL builder генерировались. Из того, что пришлось переделать руками: первая версия `WriteLoopAsync` блокировала чтение канала на 500 мс при каждом вызове `WaitForNextTickAsync` — вынес таймерный flush в отдельную задачу с `SemaphoreSlim`. Исходная фабрика репозитория тихо падала в InMemory при неверном конфиге — переписал с явным исключением. `Activator.CreateInstance` заменил на `DbProviderFactories`.

Kafka, MediatR, CQRS не добавлялись — задача этого не требует, а объяснять на ревью лишние слои неудобно.