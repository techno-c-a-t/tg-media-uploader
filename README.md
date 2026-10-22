# Telegram File Proxy (Go + Docker)

Высокоэффективный прокси-сервер для многопоточного скачивания файлов через HTTP Range и их трансляции в Telegram частями (чанками) с мгновенной очисткой диска.

Разработан специально для серверов с **жёсткими лимитами памяти (RAM ~300 МБ)** и **маленьким диском (1–2 ГБ)**.

---

## Архитектура решения

1. **VPS (Go + Docker):**
   * Получает ссылку на файл (через REST API или CLI).
   * Выполняет `HEAD`/`GET` зондирование: проверяет заголовок `Accept-Ranges` и размер.
   * Скачивает файл порциями по **500 МБ** (настраивается) во временный файл.
   * Считает MD5-хеш каждого блока на лету.
   * Загружает блок в Telegram по нативному протоколу **MTProto** (до 2 ГБ для обычных аккаунтов / ботов, до 4 ГБ для Premium).
   * **Сразу после отправки удаляет блок с диска** — на сервере всегда занято не более одного чанка!
   * Отправляет итоговый `manifest.json` в Telegram.

2. **Клиент на Ubuntu (`tg-assembler`):**
   * Читает манифест.
   * Собирает части в правильном порядке.
   * Валидирует контрольные суммы каждой части.

---

## Требования для Telegram

1. Получите `api_id` и `api_hash` на [my.telegram.org](https://my.telegram.org) (раздел *API development tools*).
2. Создайте закрытый канал (Private Channel) и добавьте туда вашего бота/аккаунт с правами администратора (или используйте `self` для «Избранного»).
3. Создайте бота через [@BotFather](https://t.me/BotFather) и скопируйте его токен.

---

## Быстрый запуск на VPS (Docker)

1. Клонируйте репозиторий на сервер:
   ```bash
   git clone <repo_url> tg_file_proxy
   cd tg_file_proxy
   ```

2. Скопируйте и настройте `.env`:
   ```bash
   cp .env.example .env
   nano .env
   ```
   Укажите `TG_APP_ID`, `TG_APP_HASH`, `TG_BOT_TOKEN` и `TG_PEER` (например, `@my_channel`).

3. Запустите контейнер:
   ```bash
   docker compose up -d --build
   ```

> [!NOTE]
> В `docker-compose.yml` выставлен строгий лимит памяти: `mem_limit: 300M`. Сервис никогда не съест память вашей системы или соседних сервисов.

---

## Как отправлять задачи серверу

### Вариант 1: Через REST API (curl)

```bash
curl -X POST http://YOUR_VPS_IP:8080/download \
  -H "Content-Type: application/json" \
  -d '{"url": "https://releases.ubuntu.com/24.04/ubuntu-24.04-desktop-amd64.iso"}'
```

### Вариант 2: Запуск одной командой через CLI (без фонового демона)

```bash
./tg_proxy -url "https://example.com/large_file.zip"
```

---

## Сборка файла на Ubuntu (Клиент)

1. Соберите утилиту сборщика:
   ```bash
   go build -o tg-assembler ./cmd/assembler
   ```

2. Скачайте все полученные части из Telegram в одну папку (например, `~/Downloads/chunks`).
3. Скопируйте туда же текст манифеста в файл `manifest.json`.
4. Запустите сборщик:
   ```bash
   ./tg-assembler -manifest ~/Downloads/chunks/manifest.json -chunks ~/Downloads/chunks
   ```
   Утилита проверит MD5 каждого куска, объединит их в исходный файл и выдаст готовый результат.
