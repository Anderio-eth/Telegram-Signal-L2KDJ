from __future__ import annotations

import asyncio
import html
import io
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal

import aiohttp
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from telegram_signal_k2.binance import BinanceFuturesClient, Kline
from telegram_signal_k2.charting import ChartOptions, generate_signal_chart, signal_chart_data
from telegram_signal_k2.combined import (
    CombinedDirection,
    CombinedEvaluation,
    TimeframeSignal,
    evaluate_combined_signal,
)
from telegram_signal_k2.config import Settings, display_symbol, normalize_symbol
from telegram_signal_k2.indicators import (
    Signal,
    SignalKind,
    SignalRules,
    calculate_kdj,
    detect_signal,
)
from telegram_signal_k2.state import BotState, CombinedConfig, StateStore


logger = logging.getLogger(__name__)


TIMEFRAME_SECONDS = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "8h": 28800,
    "12h": 43200,
    "1d": 86400,
}

MENU_TEXT = "Панель керування сигналами. Обери монети або дію нижче:"

SIGNAL_TITLES = {
    SignalKind.LONG: "ЛОНГ",
    SignalKind.SHORT: "ШОРТ",
}

SIGNAL_MARKERS = {
    SignalKind.LONG: "🟩",
    SignalKind.SHORT: "🟥",
}


@dataclass
class BotRuntime:
    settings: Settings
    store: StateStore
    rules: SignalRules
    chart_options: ChartOptions
    monitor_task: asyncio.Task[None] | None = None
    pending_actions: dict[tuple[int | str, int], str] = field(default_factory=dict)


def create_application(settings: Settings) -> Application:
    initial_state = BotState(
        chat_id=settings.telegram_chat_id,
        available_symbols=settings.symbols,
        enabled_symbols=settings.symbols,
        topic_threads=settings.topic_threads,
        signals_enabled=True,
    )
    store = StateStore(settings.state_file, initial_state)
    rules = SignalRules(
        kdj_n1=settings.kdj_n1,
        kdj_m1=settings.kdj_m1,
        kdj_m2=settings.kdj_m2,
        buy_alert_limit=settings.buy_alert_limit,
        sell_alert_limit=settings.sell_alert_limit,
        indicator_scale_min=settings.indicator_scale_min,
        indicator_scale_max=settings.indicator_scale_max,
        volume_ma_period=settings.volume_ma_period,
        min_volume_ratio=settings.min_volume_ratio,
        strong_volume_ratio=settings.strong_volume_ratio,
        require_volume_for_confirmed=settings.require_volume_for_confirmed,
    )

    application = (
        ApplicationBuilder()
        .token(settings.telegram_bot_token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    chart_options = ChartOptions(
        enabled=settings.charts_enabled,
        candles=settings.chart_candles,
        width=settings.chart_width,
        height=settings.chart_height,
    )
    application.bot_data["runtime"] = BotRuntime(
        settings=settings,
        store=store,
        rules=rules,
        chart_options=chart_options,
    )

    application.add_handler(CommandHandler(["start", "menu"], menu_command))
    application.add_handler(CommandHandler("bind", bind_command))
    application.add_handler(CommandHandler("topics", topics_command))
    application.add_handler(CommandHandler("add", add_command))
    application.add_handler(CommandHandler("remove", remove_command))
    application.add_handler(CommandHandler("signals_on", signals_on_command))
    application.add_handler(CommandHandler("signals_off", signals_off_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("indicator", indicator_command))
    application.add_handler(CommandHandler("combined_on", combined_on_command))
    application.add_handler(CommandHandler("combined_off", combined_off_command))
    application.add_handler(CommandHandler("combined_set", combined_set_command))
    application.add_handler(CommandHandler("combined_status", combined_status_command))
    application.add_handler(CommandHandler("combined_rule", combined_rule_command))
    application.add_handler(CommandHandler("combined_bind", combined_bind_command))
    application.add_handler(CommandHandler("combined_menu", combined_menu_command))
    application.add_handler(CallbackQueryHandler(callback_router))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, pending_text_handler))
    application.add_error_handler(error_handler)
    return application


async def post_init(application: Application) -> None:
    runtime: BotRuntime = application.bot_data["runtime"]
    await runtime.store.load()
    await application.bot.set_my_commands(
        [
            BotCommand("menu", "панель кнопок"),
            BotCommand("status", "стан бота"),
            BotCommand("topics", "прив'язані гілки"),
            BotCommand("add", "додати пару, наприклад /add SOL"),
            BotCommand("remove", "прибрати пару, наприклад /remove SOL"),
            BotCommand("bind", "прив'язати гілку, наприклад /bind 5m"),
            BotCommand("indicator", "поточний L2 KDJ, наприклад /indicator SOL 5m"),
            BotCommand("combined_on", "увімкнути combined timeframe mode"),
            BotCommand("combined_off", "вимкнути combined timeframe mode"),
            BotCommand("combined_set", "таймфрейми, наприклад /combined_set 1m 3m 5m 15m"),
            BotCommand("combined_status", "стан combined mode"),
            BotCommand("combined_rule", "правило all_match або majority_match"),
            BotCommand("combined_bind", "прив'язати поточну гілку для combined"),
            BotCommand("combined_menu", "кнопки combined таймфреймів"),
            BotCommand("signals_on", "увімкнути сигнали"),
            BotCommand("signals_off", "вимкнути сигнали"),
        ]
    )
    runtime.monitor_task = application.create_task(monitor_loop(application))
    logger.info("Bot initialized")


async def post_shutdown(application: Application) -> None:
    runtime: BotRuntime = application.bot_data["runtime"]
    if runtime.monitor_task:
        runtime.monitor_task.cancel()
        try:
            await runtime.monitor_task
        except asyncio.CancelledError:
            pass


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    error = context.error
    if isinstance(error, BadRequest) and "Message is not modified" in str(error):
        logger.debug("Ignored Telegram no-op edit")
        return
    if error is None:
        logger.error("Unhandled Telegram update failed without exception context")
        return
    logger.error("Unhandled Telegram update failed", exc_info=(type(error), error, error.__traceback__))


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return

    await runtime.store.update(lambda state: setattr(state, "chat_id", chat.id))
    state = await runtime.store.get()
    await message.reply_text(MENU_TEXT, reply_markup=build_main_keyboard(state, runtime.settings.timeframes))


async def bind_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return

    if not context.args:
        await message.reply_text("Формат: /bind 5m")
        return

    timeframe = context.args[0].strip()
    if timeframe not in runtime.settings.timeframes:
        await message.reply_text(
            f"Таймфрейм {html.escape(timeframe)} не входить у TIMEFRAMES: "
            f"{', '.join(runtime.settings.timeframes)}"
        )
        return

    thread_id = message.message_thread_id
    if thread_id is None:
        await message.reply_text("Цю команду треба виконати всередині гілки Telegram.")
        return

    await bind_topic(runtime, chat.id, timeframe, thread_id)
    await message.reply_text(f"Готово: {timeframe} прив'язано до цієї гілки.")


async def topics_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    if not message:
        return
    state = await runtime.store.get()
    await message.reply_text(build_topics_text(state, runtime.settings.timeframes))


async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    if not message or not context.args:
        if message:
            await message.reply_text("Формат: /add SOL")
        return

    symbol = normalize_symbol(context.args[0])
    await add_symbol(runtime, symbol)
    await message.reply_text(f"Додав {display_symbol(symbol)}.", reply_markup=await current_keyboard(runtime))


async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    if not message or not context.args:
        if message:
            await message.reply_text("Формат: /remove SOL")
        return

    symbol = normalize_symbol(context.args[0])
    await remove_symbol(runtime, symbol)
    await message.reply_text(f"Прибрав {display_symbol(symbol)}.", reply_markup=await current_keyboard(runtime))


async def signals_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    await runtime.store.update(lambda state: setattr(state, "signals_enabled", True))
    if message:
        await message.reply_text("Сигнали увімкнено.", reply_markup=await current_keyboard(runtime))


async def signals_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    await runtime.store.update(lambda state: setattr(state, "signals_enabled", False))
    if message:
        await message.reply_text("Сигнали вимкнено.", reply_markup=await current_keyboard(runtime))


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    if not message:
        return
    state = await runtime.store.get()
    await message.reply_text(build_status_text(state, runtime))


async def indicator_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    if not message:
        return

    state = await runtime.store.get()
    symbol = normalize_symbol(context.args[0]) if context.args else first_symbol(state)
    timeframe = context.args[1].strip() if len(context.args) > 1 else first_timeframe(runtime)
    if timeframe not in runtime.settings.timeframes:
        await message.reply_text(f"Невідомий таймфрейм {timeframe}. Доступні: {', '.join(runtime.settings.timeframes)}")
        return

    text = await fetch_indicator_text(runtime, symbol, timeframe)
    await message.reply_text(text, parse_mode=ParseMode.HTML)


async def combined_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or not await ensure_group_admin(update, context):
        return

    config = await get_or_create_combined_config(runtime, chat.id)

    def mutate(state: BotState) -> None:
        state.chat_id = chat.id
        state.combined_configs[str(chat.id)] = config
        state.combined_configs[str(chat.id)].enabled = True

    state = await runtime.store.update(mutate)
    await message.reply_text(format_combined_status(chat.id, state.combined_configs[str(chat.id)]))


async def combined_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or not await ensure_group_admin(update, context):
        return

    config = await get_or_create_combined_config(runtime, chat.id)

    def mutate(state: BotState) -> None:
        state.chat_id = chat.id
        state.combined_configs[str(chat.id)] = config
        state.combined_configs[str(chat.id)].enabled = False

    state = await runtime.store.update(mutate)
    await message.reply_text(format_combined_status(chat.id, state.combined_configs[str(chat.id)]))


async def combined_set_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or not await ensure_group_admin(update, context):
        return

    requested = [item.strip() for item in context.args if item.strip()]
    if not requested:
        await message.reply_text("Формат: /combined_set 1m 3m 5m 15m")
        return

    invalid = [timeframe for timeframe in requested if timeframe not in runtime.settings.timeframes]
    if invalid:
        await message.reply_text(
            f"Невідомі таймфрейми: {', '.join(invalid)}. Доступні: {', '.join(runtime.settings.timeframes)}"
        )
        return
    requested = [timeframe for timeframe in runtime.settings.timeframes if timeframe in set(requested)]

    config = await get_or_create_combined_config(runtime, chat.id)

    def mutate(state: BotState) -> None:
        state.chat_id = chat.id
        state.combined_configs[str(chat.id)] = config
        state.combined_configs[str(chat.id)].timeframes = requested

    state = await runtime.store.update(mutate)
    await message.reply_text(format_combined_status(chat.id, state.combined_configs[str(chat.id)]))


async def combined_rule_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or not await ensure_group_admin(update, context):
        return

    if not context.args:
        await message.reply_text("Формат: /combined_rule all_match або /combined_rule majority_match")
        return

    rule = context.args[0].strip().lower()
    if rule not in {"all_match", "majority_match"}:
        await message.reply_text("Підтримуються правила: all_match, majority_match")
        return

    config = await get_or_create_combined_config(runtime, chat.id)

    def mutate(state: BotState) -> None:
        state.chat_id = chat.id
        state.combined_configs[str(chat.id)] = config
        state.combined_configs[str(chat.id)].rule = rule

    state = await runtime.store.update(mutate)
    await message.reply_text(format_combined_status(chat.id, state.combined_configs[str(chat.id)]))


async def combined_bind_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or not await ensure_group_admin(update, context):
        return

    if message.message_thread_id is None:
        await message.reply_text(
            "Цю команду треба виконати всередині гілки, куди слати combined сигнали."
        )
        return

    config = await get_or_create_combined_config(runtime, chat.id)
    thread_id = message.message_thread_id

    def mutate(state: BotState) -> None:
        state.chat_id = chat.id
        state.combined_configs[str(chat.id)] = config
        state.combined_configs[str(chat.id)].thread_id = thread_id

    state = await runtime.store.update(mutate)
    await message.reply_text(
        "Готово: ця гілка прив'язана для combined сигналів.\n\n"
        + format_combined_status(chat.id, state.combined_configs[str(chat.id)])
    )


async def combined_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat:
        return

    config = await get_or_create_combined_config(runtime, chat.id)
    await message.reply_text(format_combined_status(chat.id, config))


async def combined_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or not await ensure_group_admin(update, context):
        return

    config = await get_or_create_combined_config(runtime, chat.id)
    await message.reply_text(
        build_combined_menu_text(chat.id, config),
        reply_markup=build_combined_keyboard(config, runtime.settings.timeframes),
    )


async def pending_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = get_runtime(context)
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user or not message.text:
        return

    key = (chat.id, user.id)
    action = runtime.pending_actions.pop(key, None)
    if action != "add_symbol":
        return

    try:
        symbol = normalize_symbol(message.text)
    except ValueError:
        await message.reply_text("Не зрозумів пару. Напиши, наприклад: SOL або SOLUSDT.")
        return

    await add_symbol(runtime, symbol)
    await message.reply_text(f"Додав {display_symbol(symbol)}.", reply_markup=await current_keyboard(runtime))


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return

    data = query.data
    if data.startswith("toggle:"):
        await toggle_symbol_callback(query, context, data.removeprefix("toggle:"))
    elif data == "menu:refresh":
        await refresh_menu_callback(query, context)
    elif data.startswith("action:"):
        await action_callback(query, context, data.removeprefix("action:"))
    elif data.startswith("remove_symbol:"):
        await remove_symbol_callback(query, context, data.removeprefix("remove_symbol:"))
    elif data.startswith("bind_tf:"):
        await bind_timeframe_callback(query, context, data.removeprefix("bind_tf:"))
    elif data == "indicator_menu":
        await indicator_menu_callback(query, context)
    elif data.startswith("indicator_symbol:"):
        await indicator_symbol_callback(query, context, data.removeprefix("indicator_symbol:"))
    elif data.startswith("indicator:"):
        await indicator_value_callback(query, context, data)
    elif data == "combined_menu":
        await combined_menu_callback(query, context)
    elif data.startswith("combined_tf:"):
        await combined_timeframe_callback(query, context, data.removeprefix("combined_tf:"))
    elif data == "combined_toggle_enabled":
        await combined_toggle_enabled_callback(query, context)
    elif data == "combined_toggle_rule":
        await combined_toggle_rule_callback(query, context)
    elif data == "combined_bind_here":
        await combined_bind_here_callback(query, context)
    elif data == "combined_refresh":
        await combined_menu_callback(query, context)
    elif data == "combined_signal_status":
        await query.answer("Це статус таймфреймів у combined сигналі")
    else:
        await query.answer("Невідома кнопка")


async def toggle_symbol_callback(query, context: ContextTypes.DEFAULT_TYPE, symbol: str) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    await query.answer()

    def mutate(state: BotState) -> None:
        if symbol in state.enabled_symbols:
            state.enabled_symbols = [item for item in state.enabled_symbols if item != symbol]
        else:
            if symbol not in state.available_symbols:
                state.available_symbols.append(symbol)
            state.enabled_symbols.append(symbol)

    state = await runtime.store.update(mutate)
    await safe_edit_reply_markup(query, build_main_keyboard(state, runtime.settings.timeframes))


async def refresh_menu_callback(query, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    await query.answer("Оновлено")
    state = await runtime.store.get()
    await safe_edit_message_text(
        query,
        MENU_TEXT,
        reply_markup=build_main_keyboard(state, runtime.settings.timeframes),
    )


async def action_callback(query, context: ContextTypes.DEFAULT_TYPE, action: str) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    state = await runtime.store.get()

    if action == "status":
        await query.answer()
        await reply_to_query(query, build_status_text(state, runtime))
    elif action == "topics":
        await query.answer()
        await reply_to_query(query, build_topics_text(state, runtime.settings.timeframes))
    elif action == "signals_on":
        state = await runtime.store.update(lambda item: setattr(item, "signals_enabled", True))
        await query.answer("Сигнали увімкнено")
        await safe_edit_reply_markup(query, build_main_keyboard(state, runtime.settings.timeframes))
    elif action == "signals_off":
        state = await runtime.store.update(lambda item: setattr(item, "signals_enabled", False))
        await query.answer("Сигнали вимкнено")
        await safe_edit_reply_markup(query, build_main_keyboard(state, runtime.settings.timeframes))
    elif action == "add_prompt":
        chat = query.message.chat if query.message else None
        user = query.from_user
        if chat and user:
            runtime.pending_actions[(chat.id, user.id)] = "add_symbol"
        await query.answer()
        await reply_to_query(query, "Напиши тикер одним повідомленням: SOL, HYPE або SOLUSDT.")
    elif action == "remove_menu":
        await query.answer()
        await reply_to_query(query, "Вибери пару, яку треба прибрати:", reply_markup=build_remove_keyboard(state))
    elif action == "bind_menu":
        await query.answer()
        await reply_to_query(
            query,
            "Відкрий потрібну гілку і натисни її таймфрейм тут, або напиши /bind 5m.",
            reply_markup=build_timeframe_keyboard("bind_tf", runtime.settings.timeframes),
        )
    else:
        await query.answer("Невідома дія")


async def remove_symbol_callback(query, context: ContextTypes.DEFAULT_TYPE, symbol: str) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    await remove_symbol(runtime, symbol)
    state = await runtime.store.get()
    await query.answer(f"Прибрав {display_symbol(symbol)}")
    await safe_edit_message_text(
        query,
        "Пару прибрано.",
        reply_markup=build_main_keyboard(state, runtime.settings.timeframes),
    )


async def bind_timeframe_callback(query, context: ContextTypes.DEFAULT_TYPE, timeframe: str) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    message = query.message
    if not message:
        await query.answer("Немає повідомлення для прив'язки", show_alert=True)
        return
    if timeframe not in runtime.settings.timeframes:
        await query.answer("Невідомий таймфрейм", show_alert=True)
        return
    if message.message_thread_id is None:
        await query.answer("Це треба натиснути всередині гілки", show_alert=True)
        return

    await bind_topic(runtime, message.chat.id, timeframe, message.message_thread_id)
    await query.answer(f"{timeframe} прив'язано")
    await reply_to_query(query, f"Готово: {timeframe} прив'язано до цієї гілки.")


async def indicator_menu_callback(query, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    state = await runtime.store.get()
    await query.answer()
    await reply_to_query(
        query,
        "По якій парі показати L2 KDJ зараз?",
        reply_markup=build_symbol_pick_keyboard(state.enabled_symbols or state.available_symbols, "indicator_symbol"),
    )


async def indicator_symbol_callback(query, context: ContextTypes.DEFAULT_TYPE, symbol: str) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    await query.answer()
    await reply_to_query(
        query,
        f"Обери таймфрейм для {display_symbol(symbol)}:",
        reply_markup=build_timeframe_keyboard(f"indicator:{symbol}", runtime.settings.timeframes),
    )


async def indicator_value_callback(query, context: ContextTypes.DEFAULT_TYPE, data: str) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    parts = data.split(":")
    if len(parts) != 3:
        await query.answer("Не можу прочитати пару/таймфрейм", show_alert=True)
        return
    _, symbol, timeframe = parts
    await query.answer("Рахую L2 KDJ...")
    text = await fetch_indicator_text(runtime, symbol, timeframe)
    await reply_to_query(query, text, parse_mode=ParseMode.HTML)


async def combined_menu_callback(query, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    chat_id = await combined_query_chat_id(query, context)
    if chat_id is None:
        return

    config = await get_or_create_combined_config(runtime, chat_id)
    await query.answer("Оновлено")
    await safe_edit_message_text(
        query,
        build_combined_menu_text(chat_id, config),
        reply_markup=build_combined_keyboard(config, runtime.settings.timeframes),
    )


async def combined_timeframe_callback(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    timeframe: str,
) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    chat_id = await combined_query_chat_id(query, context)
    if chat_id is None:
        return
    if timeframe not in runtime.settings.timeframes:
        await query.answer("Невідомий таймфрейм", show_alert=True)
        return

    config = await get_or_create_combined_config(runtime, chat_id)

    def mutate(state: BotState) -> None:
        state.chat_id = chat_id
        state.combined_configs[str(chat_id)] = config
        current = set(state.combined_configs[str(chat_id)].timeframes)
        if timeframe in current:
            current.remove(timeframe)
        else:
            current.add(timeframe)
        state.combined_configs[str(chat_id)].timeframes = [
            item for item in runtime.settings.timeframes if item in current
        ]

    state = await runtime.store.update(mutate)
    next_config = state.combined_configs[str(chat_id)]
    await query.answer(f"{timeframe_label(timeframe)} оновлено")
    await safe_edit_message_text(
        query,
        build_combined_menu_text(chat_id, next_config),
        reply_markup=build_combined_keyboard(next_config, runtime.settings.timeframes),
    )


async def combined_toggle_enabled_callback(query, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    chat_id = await combined_query_chat_id(query, context)
    if chat_id is None:
        return
    config = await get_or_create_combined_config(runtime, chat_id)

    def mutate(state: BotState) -> None:
        state.chat_id = chat_id
        state.combined_configs[str(chat_id)] = config
        state.combined_configs[str(chat_id)].enabled = not state.combined_configs[str(chat_id)].enabled

    state = await runtime.store.update(mutate)
    next_config = state.combined_configs[str(chat_id)]
    await query.answer("Combined увімкнено" if next_config.enabled else "Combined вимкнено")
    await safe_edit_message_text(
        query,
        build_combined_menu_text(chat_id, next_config),
        reply_markup=build_combined_keyboard(next_config, runtime.settings.timeframes),
    )


async def combined_toggle_rule_callback(query, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    chat_id = await combined_query_chat_id(query, context)
    if chat_id is None:
        return
    config = await get_or_create_combined_config(runtime, chat_id)

    def mutate(state: BotState) -> None:
        state.chat_id = chat_id
        state.combined_configs[str(chat_id)] = config
        current = state.combined_configs[str(chat_id)].rule
        state.combined_configs[str(chat_id)].rule = (
            "majority_match" if current == "all_match" else "all_match"
        )

    state = await runtime.store.update(mutate)
    next_config = state.combined_configs[str(chat_id)]
    await query.answer(f"Правило: {next_config.rule}")
    await safe_edit_message_text(
        query,
        build_combined_menu_text(chat_id, next_config),
        reply_markup=build_combined_keyboard(next_config, runtime.settings.timeframes),
    )


async def combined_bind_here_callback(query, context: ContextTypes.DEFAULT_TYPE) -> None:  # type: ignore[no-untyped-def]
    runtime = get_runtime(context)
    chat_id = await combined_query_chat_id(query, context)
    if chat_id is None:
        return
    message = query.message
    if not message or message.message_thread_id is None:
        await query.answer("Натисни це всередині потрібної гілки", show_alert=True)
        return

    config = await get_or_create_combined_config(runtime, chat_id)
    thread_id = message.message_thread_id

    def mutate(state: BotState) -> None:
        state.chat_id = chat_id
        state.combined_configs[str(chat_id)] = config
        state.combined_configs[str(chat_id)].thread_id = thread_id

    state = await runtime.store.update(mutate)
    next_config = state.combined_configs[str(chat_id)]
    await query.answer("Гілку прив'язано")
    await safe_edit_message_text(
        query,
        build_combined_menu_text(chat_id, next_config),
        reply_markup=build_combined_keyboard(next_config, runtime.settings.timeframes),
    )


def build_main_keyboard(state: BotState, timeframes: list[str]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []

    for symbol in state.available_symbols:
        active = symbol in state.enabled_symbols
        label = f"{'✅' if active else '⬜'} {display_symbol(symbol)}"
        row.append(InlineKeyboardButton(label, callback_data=f"toggle:{symbol}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    signal_label = "🟢 Сигнали ON" if state.signals_enabled else "🔴 Сигнали OFF"
    signal_action = "signals_off" if state.signals_enabled else "signals_on"
    buttons.extend(
        [
            [
                InlineKeyboardButton("📊 Статус", callback_data="action:status"),
                InlineKeyboardButton("🧵 Гілки", callback_data="action:topics"),
            ],
            [
                InlineKeyboardButton(signal_label, callback_data=f"action:{signal_action}"),
                InlineKeyboardButton("📈 Індикатор зараз", callback_data="indicator_menu"),
            ],
            [
                InlineKeyboardButton("➕ Додати пару", callback_data="action:add_prompt"),
                InlineKeyboardButton("➖ Забрати пару", callback_data="action:remove_menu"),
            ],
            [
                InlineKeyboardButton("🔀 Combined", callback_data="combined_menu"),
            ],
            [
                InlineKeyboardButton("🔗 Прив'язати гілку", callback_data="action:bind_menu"),
                InlineKeyboardButton("🔄 Оновити", callback_data="menu:refresh"),
            ],
        ]
    )
    return InlineKeyboardMarkup(buttons)


def build_combined_keyboard(config: CombinedConfig, timeframes: list[str]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    selected = set(config.timeframes)
    row: list[InlineKeyboardButton] = []

    for timeframe in timeframes:
        marker = "✅" if timeframe in selected else "⬜"
        row.append(
            InlineKeyboardButton(
                f"{marker} {timeframe_label(timeframe)}",
                callback_data=f"combined_tf:{timeframe}",
            )
        )
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    enabled_label = "🟢 Combined ON" if config.enabled else "🔴 Combined OFF"
    rule_label = "Rule: всі" if config.rule == "all_match" else "Rule: більшість"
    buttons.extend(
        [
            [
                InlineKeyboardButton(enabled_label, callback_data="combined_toggle_enabled"),
                InlineKeyboardButton(rule_label, callback_data="combined_toggle_rule"),
            ],
            [
                InlineKeyboardButton("📍 Ця гілка для combined", callback_data="combined_bind_here"),
            ],
            [
                InlineKeyboardButton("🔄 Оновити", callback_data="combined_refresh"),
                InlineKeyboardButton("⬅️ Головне меню", callback_data="menu:refresh"),
            ],
        ]
    )
    return InlineKeyboardMarkup(buttons)


def build_combined_menu_text(chat_id: int | str, config: CombinedConfig) -> str:
    selected = ", ".join(timeframe_label(item) for item in config.timeframes) or "нічого не обрано"
    thread = config.thread_id if config.thread_id is not None else "головний чат"
    rule = (
        "усі крім найбільшого TF дають підготовку, усі TF дають фінал"
        if config.rule == "all_match"
        else "достатньо більшості без протилежного сигналу"
    )
    return (
        "Combined timeframe menu\n\n"
        "Натискай таймфрейми нижче, щоб об'єднати їх в один combined сигнал.\n"
        "Для all_match бот спершу дає прогрес, коли зібрані всі TF крім найбільшого, "
        "а фінал надсилає після підтвердження найбільшого TF.\n\n"
        f"Chat: {chat_id}\n"
        f"Status: {'ON' if config.enabled else 'OFF'}\n"
        f"Selected: {selected}\n"
        f"Rule: {config.rule} ({rule})\n"
        f"Topic: {thread}\n\n"
        "Приклад: 1хв + 3хв + 5хв + 15хв -> 3/4 зібрано, потім ЛОНГ/ШОРТ 4/4."
    )


def timeframe_label(timeframe: str) -> str:
    labels = {
        "1m": "1хв",
        "3m": "3хв",
        "5m": "5хв",
        "15m": "15хв",
        "30m": "30хв",
        "1h": "1г",
        "2h": "2г",
        "4h": "4г",
        "6h": "6г",
        "8h": "8г",
        "12h": "12г",
        "1d": "1д",
    }
    return labels.get(timeframe, timeframe)


def build_remove_keyboard(state: BotState) -> InlineKeyboardMarkup:
    if not state.available_symbols:
        return InlineKeyboardMarkup([[InlineKeyboardButton("Немає пар", callback_data="menu:refresh")]])
    buttons = [
        [InlineKeyboardButton(f"➖ {display_symbol(symbol)}", callback_data=f"remove_symbol:{symbol}")]
        for symbol in state.available_symbols
    ]
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu:refresh")])
    return InlineKeyboardMarkup(buttons)


def build_symbol_pick_keyboard(symbols: list[str], prefix: str) -> InlineKeyboardMarkup:
    if not symbols:
        return InlineKeyboardMarkup([[InlineKeyboardButton("Немає активних пар", callback_data="menu:refresh")]])
    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for symbol in symbols:
        row.append(InlineKeyboardButton(display_symbol(symbol), callback_data=f"{prefix}:{symbol}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu:refresh")])
    return InlineKeyboardMarkup(buttons)


def build_timeframe_keyboard(prefix: str, timeframes: list[str]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for timeframe in timeframes:
        row.append(InlineKeyboardButton(timeframe, callback_data=f"{prefix}:{timeframe}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu:refresh")])
    return InlineKeyboardMarkup(buttons)


async def current_keyboard(runtime: BotRuntime) -> InlineKeyboardMarkup:
    state = await runtime.store.get()
    return build_main_keyboard(state, runtime.settings.timeframes)


async def monitor_loop(application: Application) -> None:
    runtime: BotRuntime = application.bot_data["runtime"]
    next_due = {timeframe: 0.0 for timeframe in runtime.settings.timeframes}
    next_combined_due = 0.0

    async with aiohttp.ClientSession() as session:
        client = BinanceFuturesClient(session)
        while True:
            started_at = time.monotonic()
            try:
                state = await runtime.store.get()
                if state.signals_enabled and state.chat_id:
                    now = time.monotonic()
                    for timeframe in runtime.settings.timeframes:
                        if now < next_due.get(timeframe, 0.0):
                            continue
                        next_due[timeframe] = now + max(15, runtime.settings.poll_seconds)
                        await scan_timeframe(application, client, timeframe)
                    if now >= next_combined_due:
                        next_combined_due = now + max(30, runtime.settings.poll_seconds)
                        await scan_combined_configs(application, client)
                elif not state.chat_id:
                    logger.debug("No chat_id yet; waiting for /start")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Monitor loop iteration failed")

            elapsed = time.monotonic() - started_at
            await asyncio.sleep(max(1.0, runtime.settings.poll_seconds - elapsed))


async def scan_timeframe(
    application: Application,
    client: BinanceFuturesClient,
    timeframe: str,
) -> None:
    runtime: BotRuntime = application.bot_data["runtime"]
    state = await runtime.store.get()
    symbols = list(state.enabled_symbols)
    if not symbols:
        return

    for symbol in symbols:
        try:
            raw_klines = await client.klines(symbol, timeframe, limit=runtime.settings.kline_limit)
            klines = closed_klines(raw_klines)
            points = calculate_kdj(klines, runtime.rules)
            signal = detect_signal(points, runtime.rules)
            if signal:
                await record_latest_confirmed_signal(runtime, timeframe, symbol, signal)
                if not await is_signal_on_cooldown(runtime, timeframe, symbol, signal):
                    sent = await send_signal(application, timeframe, symbol, signal, klines)
                    if sent:
                        await mark_signal_sent(runtime, timeframe, symbol, signal)
        except Exception:
            logger.exception("Failed to scan %s %s", symbol, timeframe)
        await asyncio.sleep(0.2)


async def scan_combined_configs(application: Application, _client: BinanceFuturesClient) -> None:
    runtime: BotRuntime = application.bot_data["runtime"]
    state = await runtime.store.get()
    enabled_configs = {
        chat_id: config for chat_id, config in state.combined_configs.items() if config.enabled
    }
    if not enabled_configs or not state.enabled_symbols:
        return

    for chat_id, config in enabled_configs.items():
        symbols = combined_symbols(state.enabled_symbols, config)
        if not symbols:
            logger.info("Combined mode for chat %s skipped: no symbols after filters", chat_id)
            continue

        for symbol in symbols:
            timeframe_signals: dict[str, TimeframeSignal] = {}
            for timeframe in config.timeframes:
                stored = state.latest_confirmed_signals.get(latest_signal_key(symbol, timeframe))
                timeframe_signals[timeframe] = combined_timeframe_signal_from_payload(timeframe, stored)

            evaluation = evaluate_combined_signal(
                symbol=symbol,
                timeframe_signals=timeframe_signals,
                rule=config.rule,
                required_timeframes=config.timeframes,
            )
            if not evaluation.is_signal:
                logger.debug(
                    "Combined %s %s rejected: %s",
                    chat_id,
                    symbol,
                    evaluation.rejected_reason,
                )
                continue

            if await is_combined_on_cooldown(runtime, chat_id, config, evaluation):
                continue

            sent = await send_combined_signal(
                application,
                chat_id,
                config,
                evaluation,
            )
            if sent:
                await mark_combined_sent(runtime, chat_id, config, evaluation)


async def send_combined_signal(
    application: Application,
    chat_id: str,
    config: CombinedConfig,
    evaluation: CombinedEvaluation,
) -> bool:
    await application.bot.send_message(
        chat_id=int(chat_id) if chat_id.lstrip("-").isdigit() else chat_id,
        message_thread_id=config.thread_id,
        text=format_combined_message(evaluation, config),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=build_combined_signal_keyboard(evaluation, config),
    )
    return True


async def record_latest_confirmed_signal(
    runtime: BotRuntime,
    timeframe: str,
    symbol: str,
    signal: Signal,
) -> None:
    direction = "LONG" if signal.kind == SignalKind.LONG else "SHORT"
    payload = {
        "symbol": symbol,
        "timeframe": timeframe,
        "direction": direction,
        "close_time": signal.point.close_time,
        "price": format_decimal(signal.point.close),
        "j": signal.point.j,
    }
    key = latest_signal_key(symbol, timeframe)

    def mutate(state: BotState) -> None:
        state.latest_confirmed_signals[key] = payload

    await runtime.store.update(mutate)


def latest_signal_key(symbol: str, timeframe: str) -> str:
    return f"{symbol}:{timeframe}"


def combined_timeframe_signal_from_payload(
    timeframe: str,
    payload: dict[str, object] | None,
) -> TimeframeSignal:
    if not payload:
        return TimeframeSignal(timeframe, CombinedDirection.NEUTRAL, reason="no confirmed signal yet")

    try:
        direction = CombinedDirection(str(payload.get("direction", CombinedDirection.NEUTRAL.value)))
    except ValueError:
        direction = CombinedDirection.NEUTRAL

    indicator_value = None
    if payload.get("j") is not None:
        try:
            indicator_value = float(payload["j"])
        except (TypeError, ValueError):
            indicator_value = None

    close_time = None
    if payload.get("close_time") is not None:
        try:
            close_time = int(payload["close_time"])
        except (TypeError, ValueError):
            close_time = None

    price = str(payload["price"]) if payload.get("price") is not None else None
    return TimeframeSignal(
        timeframe=timeframe,
        direction=direction,
        indicator_value=indicator_value,
        price=price,
        close_time=close_time,
        reason="latest confirmed",
    )


def combined_symbols(symbols: list[str], config: CombinedConfig) -> list[str]:
    result = list(symbols)
    if config.symbols_whitelist:
        allowed = set(config.symbols_whitelist)
        result = [symbol for symbol in result if symbol in allowed]
    if config.symbols_blacklist:
        blocked = set(config.symbols_blacklist)
        result = [symbol for symbol in result if symbol not in blocked]
    return result


def closed_klines(klines: list[Kline]) -> list[Kline]:
    now_ms = int(time.time() * 1000)
    return [item for item in klines if item.close_time <= now_ms - 1000]


async def is_signal_on_cooldown(
    runtime: BotRuntime,
    timeframe: str,
    symbol: str,
    signal: Signal,
) -> bool:
    state = await runtime.store.get()
    cooldown_ms = (
        TIMEFRAME_SECONDS.get(timeframe, 60)
        * 1000
        * max(1, runtime.settings.signal_cooldown_candles)
    )
    key = f"{timeframe}:{symbol}:{signal.kind.value}"
    last_sent_close_time = state.last_alerts.get(key, 0)
    return signal.point.close_time - last_sent_close_time < cooldown_ms


async def mark_signal_sent(
    runtime: BotRuntime,
    timeframe: str,
    symbol: str,
    signal: Signal,
) -> None:
    key = f"{timeframe}:{symbol}:{signal.kind.value}"

    def mutate(next_state: BotState) -> None:
        next_state.last_alerts[key] = signal.point.close_time

    await runtime.store.update(mutate)


async def is_combined_on_cooldown(
    runtime: BotRuntime,
    chat_id: str,
    config: CombinedConfig,
    evaluation: CombinedEvaluation,
) -> bool:
    state = await runtime.store.get()
    key = combined_alert_key(chat_id, config, evaluation)
    return key in state.combined_last_alerts


async def mark_combined_sent(
    runtime: BotRuntime,
    chat_id: str,
    config: CombinedConfig,
    evaluation: CombinedEvaluation,
) -> None:
    key = combined_alert_key(chat_id, config, evaluation)
    now = int(time.time())

    def mutate(state: BotState) -> None:
        state.combined_last_alerts[key] = now

    await runtime.store.update(mutate)


def combined_alert_key(chat_id: str, config: CombinedConfig, evaluation: CombinedEvaluation) -> str:
    timeframes = ",".join(config.timeframes)
    signature_parts = []
    for timeframe in config.timeframes:
        item = evaluation.details.get(timeframe)
        if item is None:
            signature_parts.append(f"{timeframe}:missing:0")
        else:
            signature_parts.append(f"{timeframe}:{item.direction.value}:{item.close_time or 0}")
    signature = ",".join(signature_parts)
    return (
        f"{chat_id}:{evaluation.symbol}:{evaluation.direction.value}:{evaluation.stage}:"
        f"{config.rule}:{timeframes}:{signature}"
    )


async def send_signal(
    application: Application,
    timeframe: str,
    symbol: str,
    signal: Signal,
    klines: list[Kline],
) -> bool:
    runtime: BotRuntime = application.bot_data["runtime"]
    state = await runtime.store.get()
    chat_id = state.chat_id
    if not chat_id:
        return False

    thread_id = state.topic_threads.get(timeframe)
    if thread_id is None:
        logger.warning("No topic bound for timeframe %s", timeframe)
        return False

    text = format_signal_message(timeframe, symbol, signal)
    return await send_signal_with_chart(
        application=application,
        chat_id=chat_id,
        message_thread_id=thread_id,
        symbol=symbol,
        timeframe=timeframe,
        signal=signal,
        klines=klines,
        caption=text,
    )


async def send_signal_with_chart(
    *,
    application: Application,
    chat_id: int | str,
    message_thread_id: int | None,
    symbol: str,
    timeframe: str,
    signal: Signal,
    klines: list[Kline],
    caption: str,
) -> bool:
    runtime: BotRuntime = application.bot_data["runtime"]
    chart = generate_signal_chart(
        signal_chart_data(symbol, timeframe, signal, klines),
        runtime.chart_options,
    )
    reply_markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📈 Індикатор зараз",
                    callback_data=f"indicator:{symbol}:{timeframe}",
                ),
                InlineKeyboardButton("Open chart", url=tradingview_url(symbol)),
            ]
        ]
    )

    if chart:
        try:
            await application.bot.send_photo(
                chat_id=chat_id,
                message_thread_id=message_thread_id,
                photo=InputFile(io.BytesIO(chart), filename=f"{symbol}_{timeframe}_signal.png"),
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
            return True
        except Exception:
            logger.exception("Failed to send chart photo for %s %s; falling back to text", symbol, timeframe)

    await application.bot.send_message(
        chat_id=chat_id,
        message_thread_id=message_thread_id,
        text=caption,
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup,
        disable_web_page_preview=True,
    )
    return True


def format_signal_message(timeframe: str, symbol: str, signal: Signal) -> str:
    title = SIGNAL_TITLES[signal.kind]
    marker = SIGNAL_MARKERS[signal.kind]
    symbol_display = display_symbol(symbol)
    headline = f"{marker} {title} {symbol_display}"
    return f"<b>{html.escape(headline)}</b>"


async def fetch_indicator_text(runtime: BotRuntime, symbol: str, timeframe: str) -> str:
    async with aiohttp.ClientSession() as session:
        client = BinanceFuturesClient(session)
        raw_klines = await client.klines(symbol, timeframe, limit=runtime.settings.kline_limit)

    points = calculate_kdj(closed_klines(raw_klines), runtime.rules)
    if not points:
        return f"Недостатньо свічок для {display_symbol(symbol)} {timeframe}."

    current = points[-1]
    previous = points[-2] if len(points) > 1 else current
    direction = "вгору" if current.j > previous.j else "вниз" if current.j < previous.j else "рівно"
    gauge = indicator_gauge(current.j, runtime.rules.indicator_scale_min, runtime.rules.indicator_scale_max)
    zone = indicator_zone(current.j, runtime.rules)

    return (
        f"<b>📈 L2 KDJ зараз: {html.escape(display_symbol(symbol))} {html.escape(timeframe)}</b>\n"
        f"J: <code>{current.j:.2f}</code> ({html.escape(direction)}, {html.escape(zone)})\n"
        f"<code>{html.escape(gauge)}</code>\n"
        f"K=<code>{current.k:.2f}</code>, D=<code>{current.d:.2f}</code>, "
        f"Whale Pump=<code>{current.whale_pump:.4f}</code>\n"
        f"Close: <code>{format_decimal(current.close)}</code>\n"
        f"Обсяг: <code>{format_compact_usdt(current.quote_volume)}</code> USDT "
        f"(<code>x{current.volume_ratio:.2f}</code> до MA)"
    )


def indicator_gauge(value: float, minimum: float, maximum: float, width: int = 24) -> str:
    if maximum <= minimum:
        maximum = minimum + 1
    ratio = (value - minimum) / (maximum - minimum)
    ratio = max(0.0, min(1.0, ratio))
    marker_index = round(ratio * (width - 1))
    parts = ["━"] * width
    parts[marker_index] = "●"
    return f"{format_float(minimum)} [{''.join(parts)}] {format_float(maximum)}"


def indicator_zone(value: float, rules: SignalRules) -> str:
    if value <= rules.buy_alert_limit:
        return "нижня зона LONG"
    if value >= rules.sell_alert_limit:
        return "верхня зона SHORT"
    return "нейтральна зона"


async def add_symbol(runtime: BotRuntime, symbol: str) -> None:
    def mutate(state: BotState) -> None:
        if symbol not in state.available_symbols:
            state.available_symbols.append(symbol)
        if symbol not in state.enabled_symbols:
            state.enabled_symbols.append(symbol)

    await runtime.store.update(mutate)


async def remove_symbol(runtime: BotRuntime, symbol: str) -> None:
    def mutate(state: BotState) -> None:
        state.available_symbols = [item for item in state.available_symbols if item != symbol]
        state.enabled_symbols = [item for item in state.enabled_symbols if item != symbol]

    await runtime.store.update(mutate)


async def bind_topic(runtime: BotRuntime, chat_id: int | str, timeframe: str, thread_id: int) -> None:
    def mutate(state: BotState) -> None:
        state.chat_id = chat_id
        state.topic_threads[timeframe] = thread_id

    await runtime.store.update(mutate)


def build_status_text(state: BotState, runtime: BotRuntime) -> str:
    enabled = ", ".join(display_symbol(symbol) for symbol in state.enabled_symbols) or "немає"
    topics_ready = sum(1 for timeframe in runtime.settings.timeframes if timeframe in state.topic_threads)
    return (
        "Стан бота\n"
        f"Сигнали: {'увімкнено' if state.signals_enabled else 'вимкнено'}\n"
        f"Чат: {state.chat_id or 'не задано'}\n"
        f"Монети: {enabled}\n"
        f"Гілки: {topics_ready}/{len(runtime.settings.timeframes)}\n"
        f"L2 KDJ: n1={runtime.rules.kdj_n1}, m1={runtime.rules.kdj_m1}, "
        f"m2={runtime.rules.kdj_m2}, B>{runtime.rules.buy_alert_limit}, "
        f"S<{runtime.rules.sell_alert_limit}\n"
        f"Опитування Binance: кожні {runtime.settings.poll_seconds} сек."
    )


def build_topics_text(state: BotState, timeframes: list[str]) -> str:
    lines = ["Прив'язані гілки:"]
    for timeframe in timeframes:
        thread_id = state.topic_threads.get(timeframe)
        status = str(thread_id) if thread_id is not None else "не прив'язано"
        lines.append(f"- {timeframe}: {status}")
    return "\n".join(lines)


async def get_or_create_combined_config(runtime: BotRuntime, chat_id: int | str) -> CombinedConfig:
    state = await runtime.store.get()
    existing = state.combined_configs.get(str(chat_id))
    if existing:
        return existing
    return CombinedConfig(
        enabled=False,
        timeframes=list(runtime.settings.combined_default_timeframes),
        rule=runtime.settings.combined_default_rule,
        cooldown_seconds=runtime.settings.combined_default_cooldown_seconds,
    )


def format_combined_status(chat_id: int | str, config: CombinedConfig) -> str:
    return (
        "Combined timeframe mode\n"
        f"Chat: {chat_id}\n"
        f"Enabled: {'yes' if config.enabled else 'no'}\n"
        f"Timeframes: {', '.join(config.timeframes) if config.timeframes else 'not set'}\n"
        f"Rule: {config.rule}\n"
        "Repeat guard: exact same combined state is sent once\n"
        f"Topic thread: {config.thread_id if config.thread_id is not None else 'main chat'}\n"
        f"Whitelist: {', '.join(config.symbols_whitelist) if config.symbols_whitelist else 'none'}\n"
        f"Blacklist: {', '.join(config.symbols_blacklist) if config.symbols_blacklist else 'none'}"
    )


def format_combined_message(evaluation: CombinedEvaluation, config: CombinedConfig) -> str:
    marker = "🟩" if evaluation.direction == CombinedDirection.LONG else "🟥"
    direction = "ЛОНГ" if evaluation.direction == CombinedDirection.LONG else "ШОРТ"
    count = len(evaluation.matched_timeframes)
    total = evaluation.total_count or len(config.timeframes)
    symbol = html.escape(display_symbol(evaluation.symbol))

    if evaluation.stage == "partial":
        pending = ", ".join(timeframe_label(item) for item in evaluation.pending_timeframes)
        pending_text = html.escape(pending or timeframe_label(config.timeframes[-1]))
        return (
            f"<b>{marker} {direction} {symbol}</b>\n"
            f"<b>{count}/{total} зібрано</b>\n"
            f"Готуйтесь до наступного підтвердження на <b>{pending_text}</b>!"
        )

    return (
        f"<b>{marker} {direction} {symbol}</b>\n"
        f"<b>{count}/{total} зібрано</b>"
    )


def build_combined_signal_keyboard(
    evaluation: CombinedEvaluation,
    config: CombinedConfig,
) -> InlineKeyboardMarkup:
    marker = "🟩" if evaluation.direction == CombinedDirection.LONG else "🟥"
    row = []
    for timeframe in config.timeframes:
        item = evaluation.details.get(timeframe)
        status = marker if item and item.direction == evaluation.direction else "⬜"
        row.append(
            InlineKeyboardButton(
                f"{status} {timeframe_label(timeframe)}",
                callback_data="combined_signal_status",
            )
        )
    return InlineKeyboardMarkup([row])


async def combined_query_chat_id(query, context: ContextTypes.DEFAULT_TYPE) -> int | str | None:  # type: ignore[no-untyped-def]
    if not query.message:
        await query.answer("Немає повідомлення для меню", show_alert=True)
        return None
    if not await ensure_query_admin(query, context):
        return None
    return query.message.chat.id


async def ensure_query_admin(query, context: ContextTypes.DEFAULT_TYPE) -> bool:  # type: ignore[no-untyped-def]
    message = query.message
    chat = message.chat if message else None
    user = query.from_user
    if not chat or not user:
        await query.answer("Не можу перевірити права", show_alert=True)
        return False

    if chat.type == "private":
        return True

    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        status = str(member.status).lower()
        if "administrator" in status or "creator" in status or "owner" in status:
            return True
    except Exception:
        logger.exception("Failed to verify admin status for chat %s user %s", chat.id, user.id)

    await query.answer("Тільки адміністратори можуть міняти combined mode", show_alert=True)
    return False


async def ensure_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat = update.effective_chat
    user = update.effective_user
    message = update.effective_message
    if not chat or not user:
        return False

    if chat.type == "private":
        return True

    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        status = str(member.status).lower()
        if "administrator" in status or "creator" in status or "owner" in status:
            return True
    except Exception:
        logger.exception("Failed to verify admin status for chat %s user %s", chat.id, user.id)

    if message:
        await message.reply_text("Ця команда доступна тільки адміністраторам групи.")
    return False


def tradingview_url(symbol: str) -> str:
    normalized = symbol.replace("/", "").replace("-", "").upper()
    return f"https://www.tradingview.com/chart/?symbol=BINANCE:{normalized}.P"


def first_symbol(state: BotState) -> str:
    symbols = state.enabled_symbols or state.available_symbols
    return symbols[0] if symbols else "BTCUSDT"


def first_timeframe(runtime: BotRuntime) -> str:
    return runtime.settings.timeframes[0] if runtime.settings.timeframes else "5m"


async def reply_to_query(query, text: str, **kwargs) -> None:  # type: ignore[no-untyped-def]
    if query.message:
        await query.message.reply_text(text, **kwargs)


async def safe_edit_reply_markup(query, reply_markup: InlineKeyboardMarkup) -> None:  # type: ignore[no-untyped-def]
    try:
        await query.edit_message_reply_markup(reply_markup=reply_markup)
    except BadRequest as exc:
        if "Message is not modified" not in str(exc):
            raise


async def safe_edit_message_text(
    query,
    text: str,
    *,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:  # type: ignore[no-untyped-def]
    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup)
    except BadRequest as exc:
        if "Message is not modified" not in str(exc):
            raise


def format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def format_compact_usdt(value: Decimal) -> str:
    number = float(value)
    if number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.2f}B"
    if number >= 1_000_000:
        return f"{number / 1_000_000:.2f}M"
    if number >= 1_000:
        return f"{number / 1_000:.2f}K"
    return f"{number:.2f}"


def format_float(value: float) -> str:
    return f"{value:g}"


def get_runtime(context: ContextTypes.DEFAULT_TYPE) -> BotRuntime:
    return context.application.bot_data["runtime"]
