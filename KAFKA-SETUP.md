# Запуск: два узла (Producer и Consumer)

Kafka поднимается **один раз** — в репозитории Producer. Consumer подключается к этой же Kafka по общей сети `spark-network`.

---

## Узел 1: Producer (Kafka + Kafka UI + Producer)

В репозитории **Producer** (где у тебя уже есть docker-compose с Kafka и Kafka UI):

### Шаг 1. Поднять Kafka и Kafka UI

```bash
cd путь\к\проекту\Producer
docker-compose up -d
```

Должны подняться контейнеры **kafka** и **kafka-ui**. В compose должна быть сеть с именем **spark-network** (или как у тебя названа — тогда ту же сеть укажешь для Consumer).

Проверка: в браузере открыть http://localhost:9090 — должен открыться Kafka UI.

### Шаг 2. Поднять Producer

Если Producer у тебя описан в том же docker-compose — он уже запустился. Если нет — запусти образ Producer в той же сети:

```bash
docker run -d --name producer --network spark-network -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 твой-образ-producer
```

После этого Kafka работает, Producer пишет в топик `user-transactions`.

---

## Узел 2: Consumer

В репозитории **SparkTransaction** (Consumer):

### Шаг 3. Поднять только Consumer

Kafka уже запущена на узле 1. Здесь поднимаем **только** контейнер Consumer и подключаем его к сети `spark-network`:

```bash
cd d:\SparkConsumer\SparkTransaction
docker-compose -f docker-compose.consumer-only.yml up -d --build
```

Будет собран образ Consumer и запущен один контейнер `spark-transaction-consumer`. Он подключится к той же сети, что и Kafka, и будет читать из `kafka:9092`.

Проверка логов:

```bash
docker logs -f spark-transaction-consumer
```

Должны появляться строки вида: `✓ Получено: partition=..., offset=..., data=...`.

---

## Порядок по шагам (кратко)

| Шаг | Где | Действие |
|-----|-----|----------|
| 1 | **Producer** (репозиторий) | `docker-compose up -d` → Kafka + Kafka UI (и при необходимости Producer) |
| 2 | **Consumer** (SparkTransaction) | `docker-compose -f docker-compose.consumer-only.yml up -d --build` → только Consumer |

Важно: сначала всегда запускается Kafka на узле Producer, затем Consumer на узле Consumer. Имя сети в Producer должно совпадать с тем, что в `docker-compose.consumer-only.yml` (сейчас там `spark-network`).

---

## Остановка

- **Узел 1:** в папке Producer: `docker-compose down`
- **Узел 2:** в папке SparkTransaction: `docker-compose -f docker-compose.consumer-only.yml down`

---

## Если сеть называется иначе

В Producer в `docker-compose` указано, например:

```yaml
networks:
  app-network:
    name: spark-network
```

В файле `docker-compose.consumer-only.yml` в SparkTransaction должно быть то же имя:

```yaml
networks:
  app-network:
    external: true
    name: spark-network
```

Если у тебя в Producer сеть названа по-другому — поменяй `name: spark-network` на своё имя в `docker-compose.consumer-only.yml`.
