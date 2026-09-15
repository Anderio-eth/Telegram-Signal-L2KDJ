"""Interface text in Ukrainian and English.

One dictionary rather than translated copies of each screen: a screen that exists twice drifts,
and the version nobody is reading is the one that silently goes stale.

Keys are grouped by where they appear. Values use str.format placeholders, so a translation is
free to reorder them — word order differs between the two languages and a fixed order would force
awkward phrasing in one of them.

Exchange vocabulary stays untranslated on purpose: Master, Follower, LONG, SHORT, hedge, PnL. They
match what MEXC itself shows, and translating them would mean the bot and the exchange calling the
same thing by different names.
"""

from __future__ import annotations

BREAK = chr(10) * 2

UK = "uk"
EN = "en"
LANGUAGES = (UK, EN)
DEFAULT = UK

STRINGS: dict[str, dict[str, str]] = {
    # ── buttons ────────────────────────────────────────────────────────────
    "btn_start": {UK: "▶️ СТАРТ", EN: "▶️ START"},
    "btn_stop": {UK: "⏹ СТОП", EN: "⏹ STOP"},
    "btn_positions": {UK: "📊 Позиції", EN: "📊 Positions"},
    "btn_accounts": {UK: "👥 Акаунти", EN: "👥 Accounts"},
    "btn_history": {UK: "📜 Історія", EN: "📜 History"},
    "btn_mode": {UK: "⚙️ Режим", EN: "⚙️ Mode"},
    "btn_refresh": {UK: "🔄 Оновити", EN: "🔄 Refresh"},
    "btn_emergency": {UK: "🛑 АВАРІЙНИЙ СТОП", EN: "🛑 EMERGENCY STOP"},
    "btn_ladder": {UK: "⏱ Запланований вхід", EN: "⏱ Scheduled entry"},
    "btn_add_account": {UK: "➕ Додати акаунт", EN: "➕ Add account"},
    "acc_grouped_title": {UK: "👥 <b>АКАУНТИ</b>", EN: "👥 <b>ACCOUNTS</b>"},
    "acc_grouped_hint": {
        UK: "Натисни на акаунт, щоб перекинути його між групами. "
            "<b>{one}</b> відкриває ЛОНГ, <b>{two}</b> — ШОРТ. ✏️ — назва, 🗑 — видалити.",
        EN: "Tap an account to move it between groups. "
            "<b>{one}</b> opens LONG, <b>{two}</b> SHORT. ✏️ rename, 🗑 remove.",
    },
    "acc_grouped_empty": {UK: "Ще немає акаунтів. Додай перший.", EN: "No accounts yet. Add the first."},
    "btn_ladder_new": {UK: "➕ Новий вхід", EN: "➕ New entry"},
    "btn_ladder_cancel": {UK: "✖ Скасувати {symbol} {when}", EN: "✖ Cancel {symbol} {when}"},
    "btn_ladder_time": {UK: "🕒 Час T", EN: "🕒 Time T"},
    "btn_ladder_swap": {UK: "🔁 Поміняти боки", EN: "🔁 Swap sides"},
    "btn_ladder_preview": {UK: "👁 Показати план", EN: "👁 Preview plan"},
    "btn_ladder_arm": {UK: "✅ Армувати", EN: "✅ Arm"},
    "ladder_title": {UK: "⏱ <b>ЗАПЛАНОВАНІ ВХОДИ</b>", EN: "⏱ <b>SCHEDULED ENTRIES</b>"},
    "ladder_explain": {
        UK: "Бот сам відкриє позицію на двох акаунтах у різні боки, дробленням, у заданий час. "
            "Працює у фоні — можна закрити телеграм.",
        EN: "The bot opens a position on two accounts, opposite sides, sliced, at a set time. "
            "It runs in the background — you can close Telegram.",
    },
    "ladder_form_title": {UK: "⏱ <b>НОВИЙ ВХІД</b>", EN: "⏱ <b>NEW ENTRY</b>"},
    "ladder_not_set": {UK: "—", EN: "—"},
    "ladder_f_symbol": {UK: "Символ: <b>{v}</b>", EN: "Symbol: <b>{v}</b>"},
    "ladder_f_long": {UK: "🟢 Лонг: <b>{v}</b>", EN: "🟢 Long: <b>{v}</b>"},
    "ladder_f_short": {UK: "🔴 Шорт: <b>{v}</b>", EN: "🔴 Short: <b>{v}</b>"},
    "ladder_f_g1": {UK: "🟢 Група 1 (лонг): <b>{v}</b>", EN: "🟢 Group 1 (long): <b>{v}</b>"},
    "ladder_f_g2": {UK: "🔴 Група 2 (шорт): <b>{v}</b>", EN: "🔴 Group 2 (short): <b>{v}</b>"},
    "ladder_group_empty": {UK: "порожня", EN: "empty"},
    "ladder_need_groups": {UK: "Спершу признач акаунти: хоча б один у Групі 1 і один у Групі 2 (через ⚙️ Режим).",
                           EN: "Assign accounts first: at least one in Group 1 and one in Group 2 (via ⚙️ Mode)."},
    "ladder_f_leverage": {UK: "Плече: <b>{v}x</b>", EN: "Leverage: <b>{v}x</b>"},
    "ladder_f_margin": {UK: "Маржа: <b>{v}</b> → розмір {size}/акаунт", EN: "Margin: <b>{v}</b> → size {size}/account"},
    "ladder_f_parts": {UK: "Частин: <b>{v}</b>", EN: "Slices: <b>{v}</b>"},
    "ladder_f_step": {UK: "Крок: <b>{v}s</b>", EN: "Step: <b>{v}s</b>"},
    "ladder_f_target": {UK: "Час T: <b>{v}</b>", EN: "Time T: <b>{v}</b>"},
    "ladder_in": {UK: "через {s}s", EN: "in {s}s"},
    "ladder_past": {UK: "вже минув", EN: "already past"},
    "ladder_pick_long": {UK: "Обери акаунт для <b>ЛОНГ</b>:", EN: "Pick the <b>LONG</b> account:"},
    "ladder_pick_short": {UK: "Обери акаунт для <b>ШОРТ</b>:", EN: "Pick the <b>SHORT</b> account:"},
    "ladder_ask_symbol": {UK: "Надішли символ (напр. XAG_USDT):", EN: "Send the symbol (e.g. XAG_USDT):"},
    "ladder_ask_leverage": {UK: "Надішли плече (напр. 1000):", EN: "Send the leverage (e.g. 1000):"},
    "ladder_ask_margin": {UK: "Надішли маржу в $ на акаунт (розмір = маржа × плече):", EN: "Send the margin in $ per account (size = margin × leverage):"},
    "ladder_ask_parts": {UK: "Надішли кількість частин:", EN: "Send the number of slices:"},
    "ladder_ask_step": {UK: "Надішли крок між ордерами в секундах (напр. 1):", EN: "Send the step between orders in seconds (e.g. 1):"},
    "ladder_ask_target": {UK: "Надішли час T, коли позиція має бути ПОВНІСТЮ відкрита.\n"
                              "Формат: <code>16:30</code>, <code>16:30:00</code>, <code>2026-09-15 16:30:00</code>, "
                              "або <code>+90</code> (через 90с). Час київський.",
                          EN: "Send time T when the position must be FULLY open.\n"
                              "Format: <code>16:30</code>, <code>16:30:00</code>, <code>2026-09-15 16:30:00</code>, "
                              "or <code>+90</code> (in 90s). Kyiv time."},
    "ladder_bad_value": {UK: "Не зрозумів значення, спробуй ще раз:", EN: "Could not read that, try again:"},
    "ladder_need_fields": {UK: "Спершу заповни: лонг-акаунт, шорт-акаунт, маржу і час T.", EN: "First set: long account, short account, margin and time T."},
    "ladder_plan_title": {UK: "👁 <b>ПЛАН</b>", EN: "👁 <b>PLAN</b>"},
    "ladder_slices": {UK: "частин", EN: "slices"},
    "ladder_armed_alert": {UK: "✅ Заплановано. Бот відкриє у заданий час.", EN: "✅ Armed. The bot will open at the set time."},
    "btn_lang": {UK: "🌐 English", EN: "🌐 Українська"},
    "btn_add_master": {UK: "👤 Додати Master акаунт", EN: "👤 Add Master account"},
    "btn_add_follower": {UK: "➕ Додати Follower акаунт", EN: "➕ Add Follower account"},
    "btn_remove": {UK: "🗑 Видалити акаунт", EN: "🗑 Remove account"},
    "btn_change_master": {UK: "🔄 Змінити Master (нові ключі)", EN: "🔄 Change the Master (new keys)"},
    "btn_dir_copy": {UK: "1️⃣ Група 1", EN: "1️⃣ Group 1"},
    "btn_dir_reverse": {UK: "2️⃣ Група 2", EN: "2️⃣ Group 2"},
    # The legs are numbered rather than named after a direction, because which way each one trades
    # is decided by whoever opens first — it is not a property of the group.
    "rename_group_prompt": {
        UK: "✏️ Нова назва для групи {n}?" + BREAK + "Надішли її повідомленням. /cancel — скасувати.",
        EN: "✏️ New name for group {n}?" + BREAK + "Send it as a message. /cancel to abort.",
    },
    "rename_account_prompt": {
        UK: "✏️ Нова назва для <b>{name}</b>?" + BREAK + "Надішли її повідомленням. /cancel — скасувати.",
        EN: "✏️ New name for <b>{name}</b>?" + BREAK + "Send it as a message. /cancel to abort.",
    },
    "group_one": {UK: "Група 1", EN: "Group 1"},
    "group_two": {UK: "Група 2", EN: "Group 2"},
    "group_side": {UK: "{name} — {side}", EN: "{name} — {side}"},
    "dir_pick": {
        UK: "Тисни на акаунт, щоб перекинути його в іншу групу.",
        EN: "Tap an account to move it to the other group.",
    },
    "btn_promote": {UK: "⬆️ Зробити Master з Follower", EN: "⬆️ Promote a Follower"},
    "btn_folder": {UK: "📁 {name}", EN: "📁 {name}"},
    "btn_new_folder": {UK: "➕ Нова папка", EN: "➕ New folder"},
    "btn_rename_folder": {UK: "✏️ Перейменувати", EN: "✏️ Rename"},
    "btn_delete_folder": {UK: "🗑 Видалити папку", EN: "🗑 Delete folder"},
    "btn_folder_exchange": {UK: "🔁 Біржа: {current} → {other}", EN: "🔁 Exchange: {current} → {other}"},
    "btn_confirm_delete": {UK: "✅ Так, видалити", EN: "✅ Yes, delete"},
    "btn_confirm_change": {UK: "✅ Так, змінити", EN: "✅ Yes, change it"},
    "btn_back": {UK: "« Назад", EN: "« Back"},
    "btn_cancel": {UK: "Скасувати", EN: "Cancel"},
    "btn_cancel_x": {UK: "✖ Скасувати", EN: "✖ Cancel"},
    "btn_close_all": {UK: "ТАК, ЗАКРИТИ ВСЕ", EN: "YES, CLOSE ALL"},
    "btn_to_copy": {UK: "📋 Перемкнути на КОПІЮВАННЯ", EN: "📋 Switch to COPY"},
    "btn_to_reverse": {UK: "🔁 Перемкнути на РЕВЕРС", EN: "🔁 Switch to REVERSE"},
    "btn_pick_hedge": {UK: "🔁 Обрати акаунт для хеджу", EN: "🔁 Choose hedge account"},

    # ── main menu ──────────────────────────────────────────────────────────
    "status_no_master": {UK: "⚪️ НЕМАЄ MASTER", EN: "⚪️ NO MASTER"},
    "status_running": {UK: "🟢 ПРАЦЮЄ", EN: "🟢 RUNNING"},
    "status_reconnecting": {UK: "🟡 ПРАЦЮЄ (майстер перепідключається)", EN: "🟡 RUNNING (master reconnecting)"},
    "status_stopped": {UK: "🔴 ЗУПИНЕНО", EN: "🔴 STOPPED"},
    "menu_status": {UK: "Статус: {status}", EN: "Status: {status}"},
    "menu_mode_reverse": {UK: "Режим: 🔁 <b>РЕВЕРС</b> → {target}", EN: "Mode: 🔁 <b>REVERSE</b> → {target}"},
    "menu_mode_copy": {UK: "Режим: 📋 Копіювання (усі followers)", EN: "Mode: 📋 Copy (all followers)"},
    # REVERSE stopped meaning "one nominated account" when every account got its own direction,
    # but the menu still read the old nominated-account field — which is now always empty, so it
    # permanently warned "no account chosen" about a setting that no longer exists.
    "menu_mode_reverse_split": {
        UK: "Режим: 🔁 <b>ГРУПИ</b> — {copy} у групі 1, {reverse} у групі 2",
        EN: "Mode: 🔁 <b>GROUPS</b> — {copy} in group 1, {reverse} in group 2",
    },
    "menu_mode_reverse_none": {
        UK: "Режим: 🔁 <b>ГРУПИ</b> — ще нема акаунтів",
        EN: "Mode: 🔁 <b>GROUPS</b> — no accounts yet",
    },
    "master_not_set": {UK: "👤 <b>Master:</b> не додано", EN: "👤 <b>Master:</b> not set"},
    "master_mode": {UK: "     Режим: {mode}", EN: "     Mode: {mode}"},
    "followers_count": {UK: "👥 <b>Followers:</b> {n}/{max}", EN: "👥 <b>Followers:</b> {n}/{max}"},
    "paused": {UK: "  (на паузі)", EN: "  (paused)"},
    "total": {UK: "     <b>Разом:</b> {amount}{suffix}", EN: "     <b>Total:</b> {amount}{suffix}"},
    "reporting_suffix": {UK: " (відповіли {n}/{total})", EN: " (of {n}/{total} reporting)"},
    "available": {UK: "{equity} (вільно {available})", EN: "{equity} (available {available})"},
    # Shown only when part of the wallet cannot back a position — bonus credit, usually. Without
    # it the menu reads "there is money" while every order is refused.
    "menu_button_hint": {
        UK: "Кнопка «Menu» тепер завжди під рукою — над полем вводу.",
        EN: "The Menu button now sits above the message box, always within reach.",
    },
    "btn_close_column": {UK: "✖ Закрити всі", EN: "✖ Close all"},
    "closed_column": {
        UK: "✖ <b>ЗАКРИТО НАПРЯМОК: {side}</b>",
        EN: "✖ <b>CLOSED THE {side} LEG</b>",
    },
    "column_as_master": {UK: "група 1", EN: "group 1"},
    "column_opposite": {UK: "група 2", EN: "group 2"},
    "column_nothing_open": {
        UK: "У цьому напрямку немає жодної відкритої позиції — нічого не відправлено.",
        EN: "Nothing is open on this leg — nothing was sent.",
    },
    "column_skipped": {UK: "Пропущено (вже без позиції): {names}", EN: "Skipped (already flat): {names}"},
    "not_openable": {
        UK: "  ⚠️ {amount} не йде під позицію",
        EN: "  ⚠️ {amount} cannot back a position",
    },

    # ── trade reports ──────────────────────────────────────────────────────
    "act_open": {UK: "ПОЗИЦІЮ ВІДКРИТО", EN: "POSITION OPENED"},
    "act_increase": {UK: "ПОЗИЦІЮ ЗБІЛЬШЕНО", EN: "POSITION INCREASED"},
    "act_decrease": {UK: "ПОЗИЦІЮ ЗМЕНШЕНО", EN: "POSITION DECREASED"},
    "act_close": {UK: "ПОЗИЦІЮ ЗАКРИТО", EN: "POSITION CLOSED"},
    "size_usd": {UK: "Обсяг: {amount}", EN: "Size: {amount}"},
    "size_contracts": {UK: "Обсяг: {n} контрактів", EN: "Size: {n} contracts"},
    "leverage": {UK: "Плече: {n}x", EN: "Leverage: {n}x"},
    "closed": {UK: "ЗАКРИТО", EN: "CLOSED"},
    "pnl_pending": {UK: "  (PnL рахується)", EN: "  (PnL pending)"},
    "failed": {UK: "помилка", EN: "failed"},
    "success_count": {UK: "Успішно: {ok}/{total}", EN: "Success: {ok}/{total}"},
    "total_pnl": {UK: "<b>Загальний PnL: {amount}</b>{suffix}", EN: "<b>Total PnL: {amount}</b>{suffix}"},
    "counted_suffix": {UK: "  (порахували {n}/{total})", EN: "  (of {n}/{total} reported)"},
    "master_line": {UK: "👑 Master — {detail}", EN: "👑 Master — {detail}"},

    # ── mode screen ────────────────────────────────────────────────────────
    "mode_title": {UK: "⚙️ <b>РЕЖИМ</b>", EN: "⚙️ <b>MODE</b>"},
    "mode_now_reverse": {UK: "Зараз: 🔁 <b>ГРУПИ</b>", EN: "Currently: 🔁 <b>GROUPS</b>"},
    "mode_now_copy": {UK: "Зараз: 📋 <b>КОПІЮВАННЯ</b>", EN: "Currently: 📋 <b>COPY</b>"},
    "mode_mirroring_all": {
        UK: "Дзеркалить майстра на всі {n} акаунт(и).",
        EN: "Mirroring the master onto all {n} follower(s).",
    },
    "mode_explain_copy": {
        UK: "📋 <b>Копіювання</b> — кожен follower відкриває <i>ту саму</i> сторону, що майстер.",
        EN: "📋 <b>Copy</b> — every follower opens the <i>same</i> side as the master.",
    },
    "mode_explain_reverse": {
        UK: "🔁 <b>Групи</b> — майстра немає. Хто перший відкриє позицію руками, той і задає напрям: "
            "його група заходить так само, друга — у протилежний бік. Токен, розмір і плече однакові. "
            "Закриття не копіюється — руками або кнопкою під колонкою.",
        EN: "🔁 <b>Groups</b> — there is no master. Whoever opens a position by hand sets the "
            "direction: their group takes the same side, the other group takes the opposite, at "
            "the same symbol, size and leverage. Exits are never copied — by hand, or with the "
            "button under a column.",
    },
    "mode_one_at_a_time": {UK: "Одночасно працює лише один режим.", EN: "Only one mode runs at a time."},
    "limits_title": {UK: "📌 <b>Лімітні ордери:</b> копіюються завжди", EN: "📌 <b>Limit orders:</b> always mirrored"},
    "limits_explain": {
        UK: "Лімітка, що стоїть у майстра, виставляється на всіх followers за тією самою ціною — "
            "і на відкриття, і на закриття. Вони заповнюються разом із майстром, а не наздоганяють "
            "його маркетом.",
        EN: "A limit resting on the master is placed on every follower at the same price, on the "
            "open and on the close, so they fill alongside it instead of chasing afterwards.",
    },

    # ── accounts ───────────────────────────────────────────────────────────
    "accounts_title": {UK: "👥 <b>АКАУНТИ</b>", EN: "👥 <b>ACCOUNTS</b>"},
    "acc_connected": {UK: "🟢 підключено", EN: "🟢 connected"},
    "acc_error": {UK: "🔴 помилка", EN: "🔴 error"},
    "acc_master_not_set": {UK: "👑 Master: не додано", EN: "👑 Master: not set"},
    "acc_no_followers": {UK: "Followers ще немає.", EN: "No followers yet."},
    "acc_followers": {UK: "<b>Followers:</b>", EN: "<b>Followers:</b>"},
    "acc_none": {UK: "Акаунтів немає.", EN: "No accounts."},
    "acc_pick_remove": {UK: "Обери акаунт для видалення:", EN: "Select an account to remove:"},

    # ── positions / history ────────────────────────────────────────────────
    "positions_title": {UK: "📊 <b>ПОЗИЦІЇ</b>", EN: "📊 <b>POSITIONS</b>"},
    "no_position": {UK: "позицій немає", EN: "no position"},
    "history_title": {UK: "📜 <b>ІСТОРІЯ</b>", EN: "📜 <b>HISTORY</b>"},
    "history_empty": {UK: "📜 <b>ІСТОРІЯ</b>\n\nЩе нічого не копіювалось.", EN: "📜 <b>HISTORY</b>\n\nNothing copied yet."},

    # ── adding an account ──────────────────────────────────────────────────
    "add_send_key": {
        UK: "Додаю {kind} акаунт.\n\nНадішли <b>API Key</b> від {exchange} повідомленням:",
        EN: "Adding {kind} account.\n\nSend the {exchange} <b>API Key</b> as a message:",
    },
    "add_send_key_for": {
        UK: "🔐 Додаю {kind} акаунт у папку <b>{folder}</b> ({exchange}).\n\n"
            "Тут, в особистих, ключі не бачить ніхто, крім тебе.\n\n"
            "Надішли <b>API Key</b> від {exchange} повідомленням:",
        EN: "🔐 Adding {kind} account to folder <b>{folder}</b> ({exchange}).\n\n"
            "Here, in private, nobody but you sees the keys.\n\n"
            "Send the {exchange} <b>API Key</b> as a message:",
    },
    "add_in_private": {
        UK: "🔐 Ключі {exchange} вводяться <b>тільки в особистих повідомленнях</b> з ботом.\n\n"
            "У гілці їх прочитали б усі учасники групи — навіть якщо повідомлення одразу видалити, "
            "сповіщення вже прийде.\n\nНатисни кнопку нижче: відкриється приватний чат, "
            "там додаси акаунт, і меню оновиться тут.",
        EN: "🔐 {exchange} keys are entered <b>only in a private chat</b> with the bot.\n\n"
            "In the topic every group member would read them — deleting the message right away does "
            "not help, the notification is already out.\n\nPress the button below: a private chat "
            "opens, you add the account there, and the menu updates here.",
    },
    "btn_open_private": {UK: "🔐 Відкрити особистий чат", EN: "🔐 Open private chat"},
    "add_link_invalid": {
        UK: "❌ Посилання недійсне: папку не знайдено серед твоїх, або вона на іншій біржі. "
            "Натисни «Додати акаунт» у гілці ще раз.",
        EN: "❌ This link is not valid: the folder is not one of yours, or it is on another exchange. "
            "Press “Add account” in the topic again.",
    },
    "add_done_go_back": {
        UK: "✅ Акаунт додано. Меню в гілці оновлено — можна повертатись туди.",
        EN: "✅ Account added. The menu in the topic is updated — you can go back there.",
    },
    "add_send_secret": {UK: "Тепер надішли <b>Secret Key</b>:", EN: "Now send the <b>Secret Key</b>:"},
    "add_validating": {UK: "Перевіряю ключі на {exchange}…", EN: "Validating with {exchange}…"},
    "add_cannot_connect": {UK: "❌ Не вдалося підключитись: {error}", EN: "❌ Could not connect: {error}"},
    "add_cannot_save": {UK: "❌ Не вдалося зберегти: {error}", EN: "❌ Could not save: {error}"},
    "add_done": {UK: "✅ <b>{label} додано</b>{warning}", EN: "✅ <b>{label} added</b>{warning}"},
    "add_mode_mismatch": {
        UK: "\n\n⚠️ Режим позицій тут {theirs}, а в майстра {masters}. Зроби однаковими до торгівлі.",
        EN: "\n\n⚠️ Position mode is {theirs}, but the master uses {masters}. Make them match before trading.",
    },
    "cancelled": {UK: "Скасовано.", EN: "Cancelled."},

    # ── control ────────────────────────────────────────────────────────────
    "not_authorized": {UK: "Немає доступу", EN: "Not authorized"},
    # ── forum topics ───────────────────────────────────────────────────────
    "topic_use_a_topic": {
        UK: "Тут бот не працює — відкрий гілку <b>MEXC</b> або <b>HIBT</b>. Якщо їх ще немає, напиши /setup.",
        EN: "The bot does not work here — open the <b>MEXC</b> or <b>HIBT</b> topic. If there are none yet, send /setup.",
    },
    "group_not_yours": {
        UK: "Ця група належить іншому користувачу. Додай бота у <b>свою</b> групу з гілками.",
        EN: "This group belongs to another user. Add the bot to a group of <b>your own</b>.",
    },
    "group_not_set_up": {
        UK: "Бот у цій групі ще не налаштований. Напиши /setup.",
        EN: "The bot is not set up in this group yet. Send /setup.",
    },
    "group_refused_leaving": {
        UK: "Додавати цього бота можуть лише користувачі з доступом. Виходжу з групи.",
        EN: "Only users with access can add this bot. Leaving the group.",
    },
    "group_welcome": {
        UK: "👋 Привіт, {name}! Ця група тепер твоя — тут буде твоє керування ботом.\n\n"
            "Щоб налаштувати:\n"
            "1. Увімкни <b>«Теми»</b> в налаштуваннях групи.\n"
            "2. Зроби бота <b>адміністратором</b> з правом <b>«Керувати темами»</b>.\n"
            "3. Напиши /setup — я сам створю гілки <b>MEXC</b> і <b>HIBT</b>.",
        EN: "👋 Hi, {name}! This group is now yours — this is where you control the bot.\n\n"
            "To set it up:\n"
            "1. Turn on <b>Topics</b> in the group settings.\n"
            "2. Make the bot an <b>admin</b> with the <b>Manage topics</b> right.\n"
            "3. Send /setup — I will create the <b>MEXC</b> and <b>HIBT</b> topics myself.",
    },
    "bot_promoted": {
        UK: "Бот тепер адміністратор. Напиши /setup.",
        EN: "The bot is an admin now. Send /setup.",
    },
    "setup_not_forum": {
        UK: "У цій групі вимкнені <b>«Теми»</b>. Увімкни їх у налаштуваннях групи й напиши /setup ще раз.",
        EN: "<b>Topics</b> are off in this group. Turn them on in the group settings and send /setup again.",
    },
    "setup_need_admin": {
        UK: "Зроби бота <b>адміністратором</b> групи з правом <b>«Керувати темами»</b> і напиши /setup ще раз.",
        EN: "Make the bot a group <b>admin</b> with the <b>Manage topics</b> right and send /setup again.",
    },
    "setup_need_topics_right": {
        UK: "Боту бракує права <b>«Керувати темами»</b>, щоб створити гілки: {names}. "
            "Дай йому це право й напиши /setup — або створи гілки з такими назвами сам, я їх розпізнаю.",
        EN: "The bot lacks the <b>Manage topics</b> right to create: {names}. "
            "Grant it and send /setup — or create topics with those names yourself, I will recognise them.",
    },
    "setup_topic_ready": {
        UK: "Гілка <b>{exchange}</b> готова. Відкрий своє меню: /menu",
        EN: "The <b>{exchange}</b> topic is ready. Open your menu: /menu",
    },
    "setup_done": {
        UK: "✅ Готово. Гілки: <b>{topics}</b>.\n\n"
            "Далі — /menu у потрібній гілці. Ключі API додаються лише в особистих повідомленнях з ботом.",
        EN: "✅ Done. Topics: <b>{topics}</b>.\n\n"
            "Next — /menu in the topic you want. API keys are added only in a private chat with the bot.",
    },
    "topic_not_bound": {
        UK: "Ця гілка ще не прив'язана до біржі. Напиши тут <code>/bind mexc</code> або "
            "<code>/bind hibt</code>.",
        EN: "This topic is not bound to an exchange yet. Send <code>/bind mexc</code> or "
            "<code>/bind hibt</code> here.",
    },
    "screen_not_yours": {
        UK: "Це меню іншого користувача. Відкрий своє: /menu",
        EN: "This menu belongs to someone else. Open your own: /menu",
    },
    "screen_stale": {
        UK: "Це меню застаріло. Відкрий нове: /menu",
        EN: "This menu is out of date. Open a new one: /menu",
    },
    "bind_only_in_topic": {
        UK: "Цю команду треба писати всередині гілки форум-групи.",
        EN: "Send this command inside a topic of a forum group.",
    },
    "bind_usage": {
        UK: "Вкажи біржу: <code>/bind mexc</code> або <code>/bind hibt</code>.",
        EN: "Name the exchange: <code>/bind mexc</code> or <code>/bind hibt</code>.",
    },
    "bind_done": {
        UK: "✅ Гілку прив'язано до <b>{exchange}</b>. Кожен бачить тут лише свої акаунти {exchange}.",
        EN: "✅ Topic bound to <b>{exchange}</b>. Everyone sees only their own {exchange} accounts here.",
    },
    "bind_auto": {
        UK: "✅ Гілку розпізнано як <b>{exchange}</b>. Відкрити своє меню: /menu",
        EN: "✅ Topic recognised as <b>{exchange}</b>. Open your menu: /menu",
    },
    "unbind_done": {
        UK: "Гілку відв'язано. Бот тут більше не працює, поки її не прив'язати знову.",
        EN: "Topic unbound. The bot will not work here until it is bound again.",
    },
    "topics_title": {UK: "📌 <b>Гілки цієї групи</b>", EN: "📌 <b>Topics in this group</b>"},
    "topics_none": {UK: "Жодна гілка не прив'язана.", EN: "No topic is bound."},
    "bot_not_admin": {
        UK: "⚠️ Бот не адміністратор групи. Зроби його адміном — інакше він не побачить назви й "
            "ціни, які ти вводиш у відповідь на його запитання, і не зможе прибирати старі меню.",
        EN: "⚠️ The bot is not a group admin. Make it one — otherwise it cannot see the names and "
            "prices you type in reply to its questions, and cannot tidy away old menus.",
    },
    "folder_exchange_in_topic": {
        UK: "У гілці біржа фіксована. Щоб працювати з іншою біржею — перейди в її гілку.",
        EN: "A topic's exchange is fixed. To work with another exchange, go to its topic.",
    },
    "emergency_confirm": {
        UK: "⚠️ <b>АВАРІЙНИЙ СТОП</b>\n\nЦе зупинить копіювання І закриє всі позиції на "
            "<b>твоїх</b> follower-акаунтах. Скасувати неможливо.",
        EN: "⚠️ <b>EMERGENCY STOP</b>\n\nThis stops copying AND closes every position on <b>your</b> "
            "follower accounts. It cannot be undone.",
    },
    "emergency_working": {UK: "Закриваю всі позиції на follower-акаунтах…", EN: "Closing all follower positions…"},
    "emergency_done": {UK: "🛑 <b>АВАРІЙНИЙ СТОП ВИКОНАНО</b>", EN: "🛑 <b>EMERGENCY STOP COMPLETE</b>"},
    "stop_before_mode": {
        UK: "Спершу зупини копіювання — інакше відкриті позиції лишаться не на тій стороні.",
        EN: "Stop copying before changing mode — open positions would be left on the wrong side.",
    },
    "reverse_needs_follower": {
        UK: "Для реверсу потрібен другий акаунт. Спершу додай follower.",
        EN: "Reverse mode needs a second account to hedge on. Add a follower first.",
    },
    # ── stuck accounts ─────────────────────────────────────────────────────
    "btn_stuck": {UK: "⚠️ Завислі ({n})", EN: "⚠️ Stuck ({n})"},
    "stuck_title": {UK: "⚠️ <b>ЗАВИСЛІ АКАУНТИ</b>", EN: "⚠️ <b>STUCK ACCOUNTS</b>"},
    "stuck_none": {
        UK: "⚠️ <b>ЗАВИСЛІ АКАУНТИ</b>\n\nЗараз таких немає — усі йдуть за майстром.",
        EN: "⚠️ <b>STUCK ACCOUNTS</b>\n\nNone right now — every account is following the master.",
    },
    "stuck_kind_exit": {UK: "ВИХІД", EN: "EXIT"},
    "stuck_kind_entry": {UK: "ВХІД", EN: "ENTRY"},
    "stuck_group_head": {
        UK: "<b>Група #{id}</b> · {kind} · {symbol} {side} · {time}",
        EN: "<b>Group #{id}</b> · {kind} · {symbol} {side} · {time}",
    },
    "stuck_group_limit": {UK: "Лімітка на {price}", EN: "Limit at {price}"},
    "stuck_group_nolimit": {UK: "Лімітки немає", EN: "No resting limit"},
    "stuck_explain_exit": {
        UK: "Тримають позицію, з якої майстер уже вийшов.",
        EN: "Holding a position the master has already exited.",
    },
    "stuck_explain_entry": {
        UK: "Не увійшли в позицію, яку майстер відкрив.",
        EN: "Never entered the position the master opened.",
    },
    "btn_close_market": {UK: "💥 Закрити маркетом", EN: "💥 Close at market"},
    "btn_enter_market": {UK: "💥 Увійти маркетом", EN: "💥 Enter at market"},
    "btn_move_limit": {UK: "✏️ Змінити ціну лімітки", EN: "✏️ Move the limit"},
    "btn_drop_entry": {UK: "🚫 Скасувати вхід", EN: "🚫 Cancel the entry"},
    "pick_accounts": {
        UK: "Обери акаунти. Позначені — ті, на яких дія виконається.",
        EN: "Choose the accounts. Ticked ones are the ones that will be acted on.",
    },
    "btn_do_it": {UK: "✅ Виконати ({n})", EN: "✅ Do it ({n})"},
    "nothing_selected": {UK: "Жодного акаунта не обрано", EN: "No accounts selected"},
    "move_limit_title": {UK: "✏️ <b>ЗМІНИТИ ЦІНУ</b>", EN: "✏️ <b>MOVE THE LIMIT</b>"},
    "move_limit_market": {UK: "Ринок:        <b>{price}</b>", EN: "Market:      <b>{price}</b>"},
    "move_limit_current": {UK: "Ваша лімітка: <b>{price}</b>", EN: "Your limit:  <b>{price}</b>"},
    "move_limit_ask": {
        UK: "Надішли нову ціну повідомленням.",
        EN: "Send the new price as a message.",
    },
    "move_limit_bad": {UK: "Це не схоже на ціну. Спробуй ще раз.", EN: "That is not a price. Try again."},
    "done_title": {UK: "✅ <b>ГОТОВО</b>", EN: "✅ <b>DONE</b>"},
    "back_under_master": {
        UK: "Розрулені акаунти знову під майстром. У поточну його позицію вони не заходять — "
            "чекають наступного входу.",
        EN: "Sorted accounts are back under the master. They do not join the position it is "
            "already in — they wait for the next entry.",
    },

    "change_master_warn": {
        UK: "🔄 <b>ЗМІНА MASTER АКАУНТА</b>\n\n"
            "Поточний Master (…{hint}) буде <b>видалено</b>, разом з його ключами.\n\n"
            "Позиції на follower-акаунтах <b>не чіпаються</b> — вони лишаться як є. Але новий "
            "Master їх не знає: копіювання почнеться з його наступної дії.\n\n"
            "Якщо на followers зараз щось відкрито — закрий це до заміни, інакше воно зависне "
            "без нагляду.",
        EN: "🔄 <b>CHANGE THE MASTER</b>\n\n"
            "The current Master (…{hint}) will be <b>deleted</b>, keys and all.\n\n"
            "Follower positions are <b>left alone</b>. But the new Master knows nothing about "
            "them: copying starts from its next action.\n\n"
            "If the followers are holding anything right now, close it first, or it will sit "
            "there unattended.",
    },
    "master_changed": {
        UK: "✅ <b>Master змінено</b>\n\nНовий Master: …{hint}",
        EN: "✅ <b>Master changed</b>\n\nNew Master: …{hint}",
    },

    "promote_pick": {
        UK: "⬆️ <b>ЗРОБИТИ MASTER</b>\n\n"
            "Обери акаунт. Він стане Master, а поточний Master (…{hint}) займе його місце серед "
            "followers — ключі нікуди не зникають.",
        EN: "⬆️ <b>PROMOTE TO MASTER</b>\n\n"
            "Pick an account. It becomes the Master, and the current Master (…{hint}) takes its "
            "place among the followers — no keys are lost.",
    },
    "promoted": {
        UK: "✅ <b>Ролі змінено</b>\n\nНовий Master: <b>{new}</b>\nСтарий Master тепер follower.",
        EN: "✅ <b>Roles swapped</b>\n\nNew Master: <b>{new}</b>\nThe old Master is now a follower.",
    },

    # ── folders ────────────────────────────────────────────────────────────
    "folders_title": {UK: "📁 <b>ПАПКИ</b>", EN: "📁 <b>FOLDERS</b>"},
    "folders_explain": {
        UK: "Кожна папка — окремий набір: свій Master, свої followers, свій режим і свій "
            "СТАРТ/СТОП.\n\nПапки працюють <b>незалежно</b>. Перемикання лише змінює те, "
            "що ти бачиш — папка, яка копіює, копіює далі.",
        EN: "Each folder is a separate setup: its own Master, followers, mode and START/STOP."
            "\n\nFolders run <b>independently</b>. Switching only changes what you see — a "
            "folder that is copying keeps copying.",
    },
    "folder_row": {
        UK: "{mark} <b>{name}</b> [{exchange}] — {accounts} акаунт(ів) · {state}",
        EN: "{mark} <b>{name}</b> [{exchange}] — {accounts} account(s) · {state}",
    },
    "folder_running": {UK: "🟢 працює", EN: "🟢 running"},
    "folder_exchange_set": {
        UK: "Біржа папки: {exchange}. Тепер додай акаунти з ключами саме цієї біржі.",
        EN: "Folder exchange: {exchange}. Now add accounts with keys from this exchange.",
    },
    "folder_exchange_not_empty": {
        UK: "Біржу можна змінити лише в порожній папці: ключі належать одній біржі. "
            "Створи нову папку або видали акаунти з цієї.",
        EN: "The exchange can only be changed on an empty folder: keys belong to one exchange. "
            "Create a new folder or remove this one's accounts.",
    },
    "folder_exchange_refused": {
        UK: "Не вдалося змінити біржу — папка не порожня або працює.",
        EN: "Could not change the exchange — the folder is not empty or is running.",
    },
    "folder_stopped": {UK: "🔴 зупинено", EN: "🔴 stopped"},
    "folder_name_ask": {
        UK: "Надішли назву папки повідомленням (до 40 символів).",
        EN: "Send the folder name as a message (up to 40 characters).",
    },
    "folder_created": {
        UK: "✅ Папку <b>{name}</b> створено й обрано.",
        EN: "✅ Folder <b>{name}</b> created and selected.",
    },
    "folder_renamed": {UK: "✅ Тепер це <b>{name}</b>.", EN: "✅ Renamed to <b>{name}</b>."},
    "folder_switched": {UK: "📁 Обрано <b>{name}</b>.", EN: "📁 Switched to <b>{name}</b>."},
    "folder_delete_warn": {
        UK: "🗑 <b>ВИДАЛИТИ ПАПКУ</b>\n\n<b>{name}</b> та всі її акаунти ({accounts}) "
            "будуть видалені разом із ключами.\n\nПозиції на біржі <b>не закриваються</b> — "
            "закрий їх до видалення, інакше вони лишаться без нагляду.",
        EN: "🗑 <b>DELETE FOLDER</b>\n\n<b>{name}</b> and all {accounts} of its accounts "
            "will be deleted, keys included.\n\nPositions on the exchange are <b>not "
            "closed</b> — close them first, or they are left unattended.",
    },
    "folder_deleted": {UK: "🗑 Папку видалено.", EN: "🗑 Folder deleted."},
    "folder_last_one": {
        UK: "Це єдина папка — її не можна видалити. Створи іншу спершу.",
        EN: "This is the only folder — it cannot be deleted. Create another first.",
    },
    "folder_stop_first": {
        UK: "Спершу зупини копіювання в цій папці.",
        EN: "Stop copying in this folder first.",
    },

    "reverse_pick": {
        UK: "Який акаунт має відкривати <b>протилежну</b> сторону до майстра?",
        EN: "Which account should take the <b>opposite</b> side of the master?",
    },
}


def t(lang: str | None, key: str, **kwargs) -> str:
    """One string in the caller's language.

    An unknown language falls back to the default rather than raising: a missing setting must
    never be the reason a trade report fails to send.
    """
    entry = STRINGS[key]
    return entry.get(lang or DEFAULT, entry[DEFAULT]).format(**kwargs)
