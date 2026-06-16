# Telegram Signal K2

Telegram-бот для crypto market signals. Він бере свічки й обсяги з Binance USD-M Futures, рахує `L2 KDJ with Whale Pump Detector`, надсилає сигнали в Telegram-гілки, додає PNG-графік до сигналу і має optional combined timeframe mode.

Головний production-варіант у цьому проєкті: **звичайний Python start command без Docker**.

## Як Працює Бот

- Telegram: за замовчуванням `long polling`.
- Market scanner: окремий async loop всередині процесу бота.
- Binance: REST polling `/fapi/v1/klines`.
- State: `data/state.json`, тому після рестарту не губляться `chat_id`, прив'язані гілки, монети й combined config.
- Charts: PNG-графік через `matplotlib`; якщо графік не створився, бот відправляє текстовий сигнал.
- Trading: бот не відкриває угоди, тільки надсилає алерти.

Через постійний scanner loop бот має працювати як **worker/server process 24/7**.

## Швидкий Старт Локально

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
Copy-Item .env.example .env
```

У `.env` заміни:

```text
TELEGRAM_BOT_TOKEN=123456:replace_me
```

на реальний токен з BotFather.

Запуск:

```powershell
python -m telegram_signal_k2
```

Або:

```powershell
telegram-signal-k2
```

Перевірка:

```powershell
telegram-signal-k2-healthcheck --json
python -m unittest discover -s tests
```

## Найважливіший Start Command

Для Render, Railway, VPS, PM2 або systemd start command однаковий:

```bash
python -m telegram_signal_k2
```

Build/install command:

```bash
pip install -e .
```

Required env variable:

```text
TELEGRAM_BOT_TOKEN=your_botfather_token
```

Recommended env variables:

```text
TELEGRAM_RUN_MODE=polling
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,HYPEUSDT
TIMEFRAMES=1m,3m,5m,15m,30m,1h,4h
STATE_FILE=data/state.json
CHARTS_ENABLED=true
```

## Render Без Docker

На Render обирай **Background Worker**, не Web Service.

1. Push код у GitHub.
2. Render Dashboard -> New -> Background Worker.
3. Connect GitHub repo.
4. Environment: Python.
5. Build Command:

```bash
pip install -e .
```

6. Start Command:

```bash
python -m telegram_signal_k2
```

7. Environment Variables:

```text
TELEGRAM_BOT_TOKEN=...
TELEGRAM_RUN_MODE=polling
STATE_FILE=data/state.json
```

Важливо: якщо хочеш, щоб `data/state.json` точно переживав redeploy/restart на Render, потрібне persistent storage або зовнішнє сховище. Без persistent disk після redeploy платформа може не зберегти локальний файл.

Render Background Worker підходить, бо бот є long-running worker process.

## Railway Без Docker

На Railway теж запускай як звичайний service/worker зі start command.

1. Push код у GitHub.
2. Railway -> New Project -> Deploy from GitHub.
3. У service settings задай:

Build Command:

```bash
pip install -e .
```

Start Command:

```bash
python -m telegram_signal_k2
```

4. Variables:

```text
TELEGRAM_BOT_TOKEN=...
TELEGRAM_RUN_MODE=polling
STATE_FILE=data/state.json
```

У репозиторії також є [Procfile](./Procfile):

```text
worker: python -m telegram_signal_k2
```

Якщо Railway сам підхопить Procfile, окремий start command може не знадобитися. Але для людини без DevOps досвіду найпростіше явно прописати `python -m telegram_signal_k2`.

## VPS Без Docker Через systemd

Це найстабільніший простий варіант для 24/7.

1. Зайди на сервер:

```bash
ssh root@your-server-ip
```

2. Постав базові пакети:

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip
```

3. Завантаж код:

```bash
cd /opt
git clone <your-repo-url> Telegram-Signal-K2
cd /opt/Telegram-Signal-K2
```

4. Створи venv і встанови залежності:

```bash
python3 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install -e .
```

5. Створи `.env`:

```bash
cp .env.example .env
nano .env
```

Мінімум заповни:

```text
TELEGRAM_BOT_TOKEN=...
TELEGRAM_RUN_MODE=polling
STATE_FILE=data/state.json
```

6. Перевір ручний запуск:

```bash
./.venv/bin/python -m telegram_signal_k2
```

Зупинити ручний запуск: `Ctrl+C`.

7. Увімкни systemd:

```bash
sudo cp deploy/systemd/telegram-signal-k2.service /etc/systemd/system/telegram-signal-k2.service
sudo systemctl daemon-reload
sudo systemctl enable telegram-signal-k2
sudo systemctl start telegram-signal-k2
```

8. Подивитися статус:

```bash
sudo systemctl status telegram-signal-k2
```

9. Подивитися логи:

```bash
journalctl -u telegram-signal-k2 -f
```

`systemd` автоматично перезапустить бота після падіння, бо service має:

```ini
Restart=always
RestartSec=5
```

## VPS Без Docker Через PM2

PM2 теж може тримати Python-процес онлайн, хоча він частіше використовується для Node.js.

1. Постав Node.js і PM2:

```bash
sudo apt update
sudo apt install -y nodejs npm
sudo npm install -g pm2
```

2. Підготуй Python-проєкт так само, як у VPS/systemd секції:

```bash
cd /opt/Telegram-Signal-K2
python3 -m venv .venv
./.venv/bin/python -m pip install -e .
cp .env.example .env
nano .env
```

3. Запусти через PM2:

```bash
pm2 start ecosystem.config.cjs
pm2 status
pm2 logs telegram-signal-k2
```

4. Зроби автозапуск після reboot:

```bash
pm2 save
pm2 startup
```

PM2 покаже команду, яку треба скопіювати й виконати через `sudo`. Після цього бот стартуватиме після перезавантаження сервера.

## Windows 24/7 Без Docker

Для домашнього Windows ПК є Task Scheduler.

1. Підготуй `.venv` і `.env`:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
Copy-Item .env.example .env
```

2. У `.env` встав токен BotFather.

3. Перевір ручний запуск:

```powershell
.\scripts\run_bot.ps1
```

Логи:

```text
logs\bot.log
```

4. Додай автозапуск після входу в Windows:

```powershell
.\scripts\install_windows_task.ps1 -RunNow
```

Видалити задачу:

```powershell
.\scripts\uninstall_windows_task.ps1
```

## Vercel

Для основного режиму цього бота Vercel **не підходить**.

Причина проста: бот має постійно працювати й сканити ринок. Йому потрібні:

- long-running Python process;
- Telegram polling або persistent webhook server;
- постійний Binance market polling;
- state storage для `data/state.json`.

Vercel Functions підходять для коротких serverless HTTP-запитів, а не для нескінченного worker loop.

Що можна на Vercel теоретично:

- зробити окремий webhook endpoint для прийому Telegram updates;
- винести state у зовнішню базу;
- винести market scanner на окремий VPS/worker.

У поточному боті найпростіше й правильно: Render Background Worker, Railway service/worker або VPS через systemd/PM2.

## Optional Webhook Mode

За замовчуванням використовуй polling:

```text
TELEGRAM_RUN_MODE=polling
```

Webhook потрібен тільки якщо ти розумієш, що маєш HTTPS domain/reverse proxy:

```text
TELEGRAM_RUN_MODE=webhook
WEBHOOK_URL=https://your-domain.com
WEBHOOK_LISTEN=0.0.0.0
WEBHOOK_PORT=8080
WEBHOOK_PATH=/telegram/webhook
WEBHOOK_SECRET_TOKEN=some-secret
```

Навіть у webhook mode market scanner все одно потребує постійного worker/server процесу.

## Telegram Setup

1. Створи бота через BotFather.
2. Додай бота в supergroup з увімкненими Topics.
3. Дай права писати повідомлення і читати команди.
4. Створи гілки: `1хв`, `3хв`, `5хв`, `15хв`, `30хв`, `1г`, `4г`.
5. У кожній гілці напиши:

```text
/bind 1m
/bind 3m
/bind 5m
/bind 15m
/bind 30m
/bind 1h
/bind 4h
```

## Commands

- `/menu` - головна панель кнопок.
- `/status` - стан бота.
- `/topics` - прив'язані гілки.
- `/add SOL` - додати пару `SOLUSDT`.
- `/remove SOL` - прибрати пару.
- `/indicator SOL 5m` - поточний L2 KDJ зі шкалою `J`.
- `/signals_on` / `/signals_off` - увімкнути або вимкнути сигнали.
- `/combined_on` - увімкнути combined mode для поточної групи.
- `/combined_off` - вимкнути combined mode.
- `/combined_set 15m 1h 4h` - задати таймфрейми.
- `/combined_rule all_match` - правило `all_match` або `majority_match`.
- `/combined_bind` - прив'язати поточну Telegram-гілку для combined сигналів.
- `/combined_status` - показати combined налаштування.

Combined-команди в групі доступні тільки адміністраторам.

## Signal Charts

Для кожного сигналу бот пробує створити PNG-графік:

- останні `CHART_CANDLES` свічок;
- symbol, timeframe, direction;
- ціну сигналу;
- мітку LONG/SHORT на останній свічці;
- `K`, `D`, `J`, `Whale Pump`;
- volume subplot.

Якщо графік створений, Telegram отримує `photo + caption`. Якщо графік не створився, бот відправить text fallback.

## Combined Timeframe Mode

Приклад:

```text
/combined_set 15m 1h
/combined_rule all_match
/combined_bind
/combined_on
```

Після цього бот надсилає combined signal тільки коли обрані таймфрейми збігаються.

Якщо хочеш окрему гілку для combined сигналів:

1. Створи в Telegram гілку, наприклад `Combined`.
2. Зайди саме в цю гілку.
3. Напиши:

```text
/combined_bind
```

Після цього combined сигнали будуть надсилатися в цю гілку. Якщо `/combined_bind` не виконувати, combined сигнали йдуть у головний чат групи.

Правила:

- `all_match`: всі required timeframes мають бути `LONG` або всі `SHORT`.
- `majority_match`: більшість timeframes збігається і немає протилежного сигналу.

## Docker Optional

Dockerfile і `docker-compose.yml` залишені в репозиторії тільки як optional. Для цього запиту Docker не є основним способом деплою.

Якщо Docker колись знадобиться:

```bash
docker compose up -d --build
```

## Troubleshooting

### `TELEGRAM_BOT_TOKEN still contains placeholder`

Відкрий `.env` і заміни:

```text
TELEGRAM_BOT_TOKEN=123456:replace_me
```

на реальний токен BotFather.

### Бот стартує, але не пише в гілки

У потрібній Telegram-гілці виконай:

```text
/bind 5m
```

### Перевірка стану

```bash
python -m telegram_signal_k2.healthcheck --json
```

або:

```bash
telegram-signal-k2-healthcheck --json
```
