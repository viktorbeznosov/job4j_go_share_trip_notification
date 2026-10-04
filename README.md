# ShareTrip Notification Service

Минимальный отдельный сервис уведомлений для проекта **ShareTrip**. Сервис отвечает за хранение уведомлений и предоставляет простой REST API для создания и получения уведомления по идентификатору.

## Назначение

Сервис выделяется из монолита только тогда, когда для него появляется самостоятельная зона ответственности. В текущем задании `notification` — это отдельный микросервис, который хранит уведомления, но пока не отправляет их во внешние каналы доставки.

### Что делает сервис

- принимает запрос на создание уведомления;
- сохраняет уведомление в базе данных;
- возвращает уведомление по `id`;
- хранит произвольные данные уведомления в `payload`.

## Контракт сервиса

Минимальный контракт состоит из двух эндпоинтов:

- `POST /notifications` — создать уведомление;
- `GET /notifications/{id}` — получить уведомление по идентификатору.

## REST API

### Создать уведомление

`POST /notifications`

**Headers**

- `Content-Type: application/json`

**Request body**

```json
{
  "recipientId": "client-123",
  "type": "trip_published",
  "payload": {
    "tripId": "trip-456"
  }
}
```

**Response: 201 Created**

```json
{
  "id": "notification-789",
  "recipientId": "client-123",
  "type": "trip_published",
  "status": "created",
  "payload": {
    "tripId": "trip-456"
  },
  "createdAt": "2026-07-06T12:00:00Z"
}
```

**Ошибки**

- `400 Bad Request` — некорректное тело запроса;
- `500 Internal Server Error` — внутренняя ошибка сервиса.

### Получить уведомление

`GET /notifications/{id}`

**Response: 200 OK**

```json
{
  "id": "notification-789",
  "recipientId": "client-123",
  "type": "trip_published",
  "status": "created",
  "payload": {
    "tripId": "trip-456"
  },
  "createdAt": "2026-07-06T12:00:00Z"
}
```

**Ошибки**

- `404 Not Found` — уведомление не найдено;
- `500 Internal Server Error` — внутренняя ошибка сервиса.

## Доменные сущности

### Notification

Основная сущность сервиса — `Notification`.

**Поля сущности:**

- `id` — идентификатор уведомления;
- `recipientId` — получатель уведомления;
- `type` — тип уведомления, например `trip_published`;
- `status` — текущее состояние уведомления, например `created`;
- `payload` — данные уведомления в формате JSON;
- `createdAt` — дата создания уведомления.

### Статус уведомления

На текущем этапе используется только один статус:

- `created` — уведомление создано и сохранено.

Позже можно будет добавить дополнительные статусы:

- `created`;
- `sent`;
- `failed`;

## Схема хранения

Для хранения уведомлений используется таблица `notifications`.

```sql
CREATE TABLE notifications (
    id UUID PRIMARY KEY,
    recipient_id TEXT NOT NULL,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Назначение полей

| Поле | Назначение |
| --- | --- |
| `id` | Идентификатор уведомления |
| `recipient_id` | Получатель уведомления |
| `type` | Тип уведомления, например `trip_published` |
| `status` | Текущее состояние уведомления, например `created` |
| `payload` | Данные уведомления в формате JSON |
| `created_at` | Дата создания уведомления |

## Структура проекта

```bash
.
├── cmd
│   └── main
│       └── main.go
├── config
│   ├── app.go
│   └── env.go
├── deploy
│   └── docker-compose.yml
├── internal
│   ├── api
│   │   ├── ready.go
│   │   ├── route.go
│   │   └── server.go
│   ├── app
│   │   └── logger.go
│   ├── domain
│   │   └── notification
│   │       ├── entity
│   │       │   └── notification.go
│   │       ├── handler
│   │       │   ├── create_handler.go
│   │       │   ├── get_by_id_handler.go
│   │       │   ├── notification_handler.go
│   │       │   ├── ready_handler.go
│   │       │   ├── request
│   │       │   │   └── create_notification_request.go
│   │       │   └── response
│   │       │       ├── create_notification_response.go
│   │       │       └── get_notification_response.go
│   │       ├── repository
│   │       │   └── notification_repository.go
│   │       └── service
│   │           ├── create_service.go
│   │           ├── get_by_id_service.go
│   │           └── service.go
│   └── storage
│       └── db.go
├── logs
│   └── app.log
├── migrations
│   └── 20260731182834_001_create_notifications_table.sql
├── go.mod
├── go.sum
├── Makefile
└── README.md

## Компоненты приложения

### Controller

HTTP-слой, который принимает запросы к `/notifications` и возвращает ответы в формате JSON.

### Service

Содержит бизнес-логику создания и получения уведомлений.

### Repository

Отвечает за сохранение и чтение уведомлений из базы данных.

### Entity

Отображает запись таблицы `notifications` в объект приложения.

### DTO

Используется для входящих и исходящих REST-запросов.

## Поведение сервиса

### Создание уведомления

1. Клиент отправляет `POST /notifications`.
2. Сервис валидирует входные данные.
3. Создает доменную сущность `Notification`.
4. Сохраняет запись в таблице `notifications`.
5. Возвращает созданное уведомление с `201 Created`.

### Получение уведомления

1. Клиент отправляет `GET /notifications/{id}`.
2. Сервис ищет уведомление по идентификатору.
3. Если уведомление найдено, возвращает его с `200 OK`.
4. Если уведомление не найдено, возвращает `404 Not Found`.

## Технологические ожидания

В рамках задания ожидается:

- отдельный репозиторий для сервиса;
- REST API без лишних методов;
- миграция для таблицы `notifications`;
- минимальная доменная модель;
- отсутствие интеграций с внешними каналами доставки на этом этапе.

## Будущее развитие

После появления реальных каналов доставки сервис можно расширить:

- добавить отправку email;
- добавить SMS и push;
- ввести обработку статусов `sent` и `failed`;
- реализовать асинхронную обработку;
- добавить retry-политику;
- расширить API и версионирование контракта.

## Как расследовать: уведомление не пришло

1. Взять `event_id` из Kafka headers или payload.
2. Найти лог `operation=ConsumeTripPublished` с `correlation_id`, `causation_id`, `trace_id`.
3. Inbox: `operation=InboxInsert` — `created` или `duplicate`.
4. Отправка: `operation=SendNotification`, метрики `notification_consume_total`, `notification_inbox_duplicate_total`, `notification_send_total`.

Полный порядок расследования — в README ShareTrip, раздел «Как расследовать: уведомление не пришло».

## Кратко

Этот сервис — базовый микросервис уведомлений для ShareTrip. Сейчас он хранит уведомления и предоставляет минимальный контракт, а в будущем сможет стать точкой интеграции с внешними каналами доставки.
