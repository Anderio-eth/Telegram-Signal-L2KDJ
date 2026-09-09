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
    "btn_lang": {UK: "🌐 English", EN: "🌐 Українська"},
    "btn_add_master": {UK: "👤 Додати Master акаунт", EN: "👤 Add Master account"},
    "btn_add_follower": {UK: "➕ Додати Follower акаунт", EN: "➕ Add Follower account"},
    "btn_remove": {UK: "🗑 Видалити акаунт", EN: "🗑 Remove account"},
    "btn_change_master": {UK: "🔄 Змінити Master (нові ключі)", EN: "🔄 Change the Master (new keys)"},
    "btn_dir_copy": {UK: "📋 як майстер", EN: "📋 same as master"},
    "btn_dir_reverse": {UK: "🔁 навпаки", EN: "🔁 opposite"},
    "dir_pick": {
        UK: "Тисни на акаунт, щоб змінити його напрям.",
        EN: "Tap an account to change which way it trades.",
    },
    "btn_promote": {UK: "⬆️ Зробити Master з Follower", EN: "⬆️ Promote a Follower"},
    "btn_folder": {UK: "📁 {name}", EN: "📁 {name}"},
    "btn_new_folder": {UK: "➕ Нова папка", EN: "➕ New folder"},
    "btn_rename_folder": {UK: "✏️ Перейменувати", EN: "✏️ Rename"},
    "btn_delete_folder": {UK: "🗑 Видалити папку", EN: "🗑 Delete folder"},
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
        UK: "Режим: 🔁 <b>РЕВЕРС</b> — {reverse} навпаки, {copy} як майстер",
        EN: "Mode: 🔁 <b>REVERSE</b> — {reverse} opposite, {copy} same as master",
    },
    "menu_mode_reverse_none": {
        UK: "Режим: 🔁 <b>РЕВЕРС</b> — ще нема followers",
        EN: "Mode: 🔁 <b>REVERSE</b> — no followers yet",
    },
    "master_not_set": {UK: "👤 <b>Master:</b> не додано", EN: "👤 <b>Master:</b> not set"},
    "master_mode": {UK: "     Режим: {mode}", EN: "     Mode: {mode}"},
    "followers_count": {UK: "👥 <b>Followers:</b> {n}/{max}", EN: "👥 <b>Followers:</b> {n}/{max}"},
    "paused": {UK: "  (на паузі)", EN: "  (paused)"},
    "total": {UK: "     <b>Разом:</b> {amount}{suffix}", EN: "     <b>Total:</b> {amount}{suffix}"},
    "reporting_suffix": {UK: " (відповіли {n}/{total})", EN: " (of {n}/{total} reporting)"},
    "available": {UK: "{equity} (вільно {available})", EN: "{equity} (available {available})"},

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

    # ── mode screen ────────────────────────────────────────────────────────
    "mode_title": {UK: "⚙️ <b>РЕЖИМ</b>", EN: "⚙️ <b>MODE</b>"},
    "mode_now_reverse": {UK: "Зараз: 🔁 <b>РЕВЕРС</b>", EN: "Currently: 🔁 <b>REVERSE</b>"},
    "mode_now_copy": {UK: "Зараз: 📋 <b>КОПІЮВАННЯ</b>", EN: "Currently: 📋 <b>COPY</b>"},
    "mode_hedging_on": {UK: "Хеджує на: <b>{label}</b>", EN: "Hedging on: <b>{label}</b>"},
    "mode_no_hedge": {
        UK: "⚠️ Акаунт не обрано — нічого копіюватись не буде.",
        EN: "⚠️ No account chosen — nothing will be mirrored.",
    },
    "mode_mirroring_all": {
        UK: "Дзеркалить майстра на всі {n} акаунт(и).",
        EN: "Mirroring the master onto all {n} follower(s).",
    },
    "mode_explain_copy": {
        UK: "📋 <b>Копіювання</b> — кожен follower відкриває <i>ту саму</i> сторону, що майстер.",
        EN: "📋 <b>Copy</b> — every follower opens the <i>same</i> side as the master.",
    },
    "mode_explain_reverse": {
        UK: "🔁 <b>Реверс</b> — кожен follower має <i>свій</i> напрям. Позначені 📋 йдуть за майстром, "
            "позначені 🔁 — у протилежну сторону. Майстер у LONG: перші в LONG, другі в SHORT.",
        EN: "🔁 <b>Reverse</b> — each follower has <i>its own</i> direction. Those marked 📋 follow "
            "the master, those marked 🔁 take the opposite side. Master goes LONG: the first go "
            "LONG, the others SHORT.",
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
        UK: "Додаю {kind} акаунт.\n\nНадішли <b>API Key</b> від MEXC повідомленням:",
        EN: "Adding {kind} account.\n\nSend the MEXC <b>API Key</b> as a message:",
    },
    "add_send_secret": {UK: "Тепер надішли <b>Secret Key</b>:", EN: "Now send the <b>Secret Key</b>:"},
    "add_validating": {UK: "Перевіряю ключі на MEXC…", EN: "Validating with MEXC…"},
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
        UK: "{mark} <b>{name}</b> — {accounts} акаунт(ів) · {state}",
        EN: "{mark} <b>{name}</b> — {accounts} account(s) · {state}",
    },
    "folder_running": {UK: "🟢 працює", EN: "🟢 running"},
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
