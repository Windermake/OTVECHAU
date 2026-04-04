import asyncio
import json
import logging
import os
import random
from dataclasses import dataclass
from typing import Any

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

SETTINGS_FILE = "bot_settings.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("main")


@dataclass
class TwitchAuth:
    token: str
    expires_in: int


@dataclass
class PendingAction:
    action: str


@dataclass
class AppState:
    settings: dict[str, Any]
    admin_user_ids: set[int]
    pending_actions: dict[int, PendingAction]
    lock: asyncio.Lock


def normalize_settings(raw: dict[str, Any]) -> dict[str, Any]:
    required = ["streamers_to_track", "allowed_chat_ids", "random_phrases", "check_interval"]
    missing = [key for key in required if key not in raw]
    if missing:
        raise ValueError(f"В settings отсутствуют обязательные поля: {', '.join(missing)}")

    if not isinstance(raw["check_interval"], int) or raw["check_interval"] <= 0:
        raise ValueError("check_interval должен быть положительным числом (секунды)")

    raw["streamers_to_track"] = [str(s).strip().lower() for s in raw["streamers_to_track"] if str(s).strip()]
    raw["allowed_chat_ids"] = [str(c).strip() for c in raw["allowed_chat_ids"] if str(c).strip()]
    raw["random_phrases"] = [str(p).strip() for p in raw["random_phrases"] if str(p).strip()]

    if not raw["streamers_to_track"]:
        raise ValueError("Список streamers_to_track пуст")
    if not raw["allowed_chat_ids"]:
        raise ValueError("Список allowed_chat_ids пуст")
    if not raw["random_phrases"]:
        raise ValueError("Список random_phrases пуст")

    # Настройка для панели доступа админа (опциональна)
    admins = raw.get("admin_user_ids", [])
    if not isinstance(admins, list):
        admins = []
    raw["admin_user_ids"] = admins

    return raw


def load_settings(path: str = SETTINGS_FILE) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return normalize_settings(raw)


def save_settings(settings: dict[str, Any], path: str = SETTINGS_FILE) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)


def parse_admin_user_ids(settings: dict[str, Any]) -> set[int]:
    ids: set[int] = set()

    for value in settings.get("admin_user_ids", []):
        try:
            ids.add(int(value))
        except (TypeError, ValueError):
            continue

    env_value = os.getenv("ADMIN_USER_IDS", "").strip()
    if env_value:
        for part in env_value.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                ids.add(int(part))
            except ValueError:
                logger.warning("Некорректный ADMIN_USER_IDS элемент: %s", part)

    return ids


def is_admin(user_id: int, admin_user_ids: set[int]) -> bool:
    return user_id in admin_user_ids


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Текущие настройки", callback_data="menu:show")],
            [InlineKeyboardButton(text="👤 Стримеры", callback_data="menu:streamers")],
            [InlineKeyboardButton(text="💬 Фразы", callback_data="menu:phrases")],
            [InlineKeyboardButton(text="⏱ Интервал", callback_data="menu:interval")],
            [InlineKeyboardButton(text="📣 Чаты", callback_data="menu:chats")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu:refresh")],
        ]
    )


def streamers_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить", callback_data="streamers:add")],
            [InlineKeyboardButton(text="➖ Удалить", callback_data="streamers:remove")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")],
        ]
    )


def phrases_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить", callback_data="phrases:add")],
            [InlineKeyboardButton(text="➖ Удалить", callback_data="phrases:remove")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")],
        ]
    )


def chats_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить", callback_data="chats:add")],
            [InlineKeyboardButton(text="➖ Удалить", callback_data="chats:remove")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")],
        ]
    )


def interval_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Изменить интервал", callback_data="interval:set")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")],
        ]
    )


def settings_text(settings: dict[str, Any]) -> str:
    streamers = "\n".join(f"• {s}" for s in settings["streamers_to_track"]) or "—"
    phrases = "\n".join(f"• {p}" for p in settings["random_phrases"]) or "—"
    chats = "\n".join(f"• {c}" for c in settings["allowed_chat_ids"]) or "—"
    admins = ", ".join(str(x) for x in settings.get("admin_user_ids", [])) or "не задано"

    return (
        "Текущие настройки бота:\n\n"
        f"Интервал проверки: {settings['check_interval']} сек\n"
        f"Админы (user_id): {admins}\n\n"
        "Стримеры:\n"
        f"{streamers}\n\n"
        "Фразы:\n"
        f"{phrases}\n\n"
        "Чаты для уведомлений:\n"
        f"{chats}"
    )


def parse_lines(text: str) -> list[str]:
    items = [line.strip() for line in text.replace(",", "\n").splitlines()]
    return [item for item in items if item]


class TwitchClient:
    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self._auth: TwitchAuth | None = None

    async def refresh_token(self, session: aiohttp.ClientSession) -> None:
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }

        async with session.post(url, params=params, timeout=20) as response:
            data = await response.json()
            if response.status != 200:
                raise RuntimeError(f"Не удалось получить Twitch token: {response.status}, {data}")

        self._auth = TwitchAuth(
            token=data["access_token"],
            expires_in=int(data.get("expires_in", 0)),
        )
        logger.info("Получен новый Twitch token")

    async def get_live_streams(self, session: aiohttp.ClientSession, streamers: list[str]) -> set[str]:
        if self._auth is None:
            await self.refresh_token(session)

        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self._auth.token}",
        }

        live: set[str] = set()

        for login in streamers:
            async with session.get(
                "https://api.twitch.tv/helix/streams",
                headers=headers,
                params={"user_login": login},
                timeout=20,
            ) as response:
                if response.status == 401:
                    await self.refresh_token(session)
                    headers["Authorization"] = f"Bearer {self._auth.token}"
                    async with session.get(
                        "https://api.twitch.tv/helix/streams",
                        headers=headers,
                        params={"user_login": login},
                        timeout=20,
                    ) as retry_response:
                        data = await retry_response.json()
                        if retry_response.status != 200:
                            logger.error("Twitch API ошибка для %s: %s %s", login, retry_response.status, data)
                            continue
                        if data.get("data"):
                            live.add(login)
                    continue

                data = await response.json()
                if response.status != 200:
                    logger.error("Twitch API ошибка для %s: %s %s", login, response.status, data)
                    continue

                if data.get("data"):
                    live.add(login)

        return live


async def send_message(bot: Bot, chat_ids: list[str], text: str) -> None:
    for chat_id in chat_ids:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
            logger.info("Отправлено в %s", chat_id)
        except Exception as e:
            logger.exception("Ошибка отправки в %s: %s", chat_id, e)


def setup_handlers(dp: Dispatcher, state: AppState) -> None:
    router = Router()

    async def try_bootstrap_admin(user_id: int) -> bool:
        if state.admin_user_ids:
            return False

        async with state.lock:
            if state.admin_user_ids:
                return False
            state.admin_user_ids.add(user_id)
            state.settings["admin_user_ids"] = sorted(state.admin_user_ids)
            save_settings(state.settings)
            return True

    async def deny_if_not_admin(message: Message) -> bool:
        user = message.from_user
        if user is None:
            return True
        if not is_admin(user.id, state.admin_user_ids):
            await message.answer("Нет доступа к панели администратора.")
            return True
        if message.chat.type != "private":
            await message.answer("Панель доступна только в личных сообщениях с ботом.")
            return True
        return False

    async def deny_callback_if_not_admin(callback: CallbackQuery) -> bool:
        user = callback.from_user
        if user is None:
            return True
        if not is_admin(user.id, state.admin_user_ids):
            await callback.answer("Нет доступа", show_alert=True)
            return True
        if callback.message and callback.message.chat.type != "private":
            await callback.answer("Только в личке", show_alert=True)
            return True
        return False

    @router.message(CommandStart())
    @router.message(Command("panel"))
    async def cmd_panel(message: Message) -> None:
        if message.chat.type != "private":
            await message.answer("Панель доступна только в личных сообщениях с ботом.")
            return

        user = message.from_user
        if user is None:
            return

        bootstrap_done = await try_bootstrap_admin(user.id)
        if not is_admin(user.id, state.admin_user_ids):
            await message.answer("Нет доступа к панели администратора.")
            return

        await message.answer(
            "Панель управления ботом. Выберите раздел:"
            + ("\n\nВы назначены первым администратором этого бота." if bootstrap_done else ""),
            reply_markup=main_menu_keyboard(),
        )

    @router.message(Command("cancel"))
    async def cmd_cancel(message: Message) -> None:
        if await deny_if_not_admin(message):
            return
        user = message.from_user
        if user is None:
            return

        removed = state.pending_actions.pop(user.id, None)
        if removed is None:
            await message.answer("Нет активного действия.")
        else:
            await message.answer("Действие отменено.", reply_markup=main_menu_keyboard())

    @router.callback_query(F.data == "menu:back")
    @router.callback_query(F.data == "menu:refresh")
    async def menu_back(callback: CallbackQuery) -> None:
        if await deny_callback_if_not_admin(callback):
            return
        await callback.answer()
        if callback.message:
            await callback.message.edit_text(
                "Панель управления ботом. Выберите раздел:",
                reply_markup=main_menu_keyboard(),
            )

    @router.callback_query(F.data == "menu:show")
    async def menu_show(callback: CallbackQuery) -> None:
        if await deny_callback_if_not_admin(callback):
            return
        await callback.answer()
        async with state.lock:
            text = settings_text(state.settings)
        if callback.message:
            await callback.message.edit_text(text, reply_markup=main_menu_keyboard())

    @router.callback_query(F.data == "menu:streamers")
    async def menu_streamers(callback: CallbackQuery) -> None:
        if await deny_callback_if_not_admin(callback):
            return
        await callback.answer()
        async with state.lock:
            current = "\n".join(f"• {s}" for s in state.settings["streamers_to_track"])
        if callback.message:
            await callback.message.edit_text(
                f"Управление стримерами:\n\n{current}",
                reply_markup=streamers_keyboard(),
            )

    @router.callback_query(F.data == "menu:phrases")
    async def menu_phrases(callback: CallbackQuery) -> None:
        if await deny_callback_if_not_admin(callback):
            return
        await callback.answer()
        async with state.lock:
            current = "\n".join(f"• {s}" for s in state.settings["random_phrases"])
        if callback.message:
            await callback.message.edit_text(
                f"Управление фразами:\n\n{current}",
                reply_markup=phrases_keyboard(),
            )

    @router.callback_query(F.data == "menu:chats")
    async def menu_chats(callback: CallbackQuery) -> None:
        if await deny_callback_if_not_admin(callback):
            return
        await callback.answer()
        async with state.lock:
            current = "\n".join(f"• {s}" for s in state.settings["allowed_chat_ids"])
        if callback.message:
            await callback.message.edit_text(
                f"Управление чатами для уведомлений:\n\n{current}",
                reply_markup=chats_keyboard(),
            )

    @router.callback_query(F.data == "menu:interval")
    async def menu_interval(callback: CallbackQuery) -> None:
        if await deny_callback_if_not_admin(callback):
            return
        await callback.answer()
        async with state.lock:
            interval = state.settings["check_interval"]
        if callback.message:
            await callback.message.edit_text(
                f"Текущий интервал: {interval} сек",
                reply_markup=interval_keyboard(),
            )

    @router.callback_query(F.data.endswith(":add") | F.data.endswith(":remove") | (F.data == "interval:set"))
    async def ask_input(callback: CallbackQuery) -> None:
        if await deny_callback_if_not_admin(callback):
            return
        user = callback.from_user
        if user is None:
            return

        data = callback.data or ""
        if data == "streamers:add":
            state.pending_actions[user.id] = PendingAction("streamers_add")
            text = "Отправьте логины Twitch для добавления (по одному на строку или через запятую)."
        elif data == "streamers:remove":
            state.pending_actions[user.id] = PendingAction("streamers_remove")
            text = "Отправьте логины Twitch для удаления (по одному на строку или через запятую)."
        elif data == "phrases:add":
            state.pending_actions[user.id] = PendingAction("phrases_add")
            text = "Отправьте новые фразы (каждая строка = отдельная фраза)."
        elif data == "phrases:remove":
            state.pending_actions[user.id] = PendingAction("phrases_remove")
            text = "Отправьте фразы для удаления (точный текст, по одной на строку)."
        elif data == "chats:add":
            state.pending_actions[user.id] = PendingAction("chats_add")
            text = "Отправьте chat_id или @username для добавления (по одному на строку)."
        elif data == "chats:remove":
            state.pending_actions[user.id] = PendingAction("chats_remove")
            text = "Отправьте chat_id или @username для удаления (по одному на строку)."
        elif data == "interval:set":
            state.pending_actions[user.id] = PendingAction("interval_set")
            text = "Отправьте новый интервал в секундах (целое число > 0)."
        else:
            await callback.answer("Неизвестное действие", show_alert=True)
            return

        await callback.answer()
        if callback.message:
            await callback.message.answer(text + "\n\nДля отмены: /cancel")

    @router.message(F.text)
    async def process_pending_input(message: Message) -> None:
        if message.chat.type != "private":
            return

        user = message.from_user
        if user is None:
            return
        if not is_admin(user.id, state.admin_user_ids):
            return

        pending = state.pending_actions.get(user.id)
        if pending is None:
            return

        text = (message.text or "").strip()
        if not text:
            await message.answer("Пустой ввод. Повторите или /cancel")
            return

        try:
            async with state.lock:
                settings = state.settings

                if pending.action == "streamers_add":
                    new_items = [x.lower() for x in parse_lines(text)]
                    before = set(settings["streamers_to_track"])
                    before.update(new_items)
                    settings["streamers_to_track"] = sorted(before)
                    result = "Стримеры добавлены."

                elif pending.action == "streamers_remove":
                    remove_items = {x.lower() for x in parse_lines(text)}
                    settings["streamers_to_track"] = [
                        s for s in settings["streamers_to_track"] if s.lower() not in remove_items
                    ]
                    if not settings["streamers_to_track"]:
                        raise ValueError("Нельзя удалить всех стримеров: список не должен быть пуст")
                    result = "Стримеры удалены."

                elif pending.action == "phrases_add":
                    new_items = parse_lines(text)
                    settings["random_phrases"].extend(new_items)
                    result = "Фразы добавлены."

                elif pending.action == "phrases_remove":
                    remove_items = set(parse_lines(text))
                    settings["random_phrases"] = [p for p in settings["random_phrases"] if p not in remove_items]
                    if not settings["random_phrases"]:
                        raise ValueError("Нельзя удалить все фразы: список не должен быть пуст")
                    result = "Фразы удалены."

                elif pending.action == "chats_add":
                    new_items = parse_lines(text)
                    merged = set(settings["allowed_chat_ids"])
                    merged.update(new_items)
                    settings["allowed_chat_ids"] = sorted(merged)
                    result = "Чаты добавлены."

                elif pending.action == "chats_remove":
                    remove_items = set(parse_lines(text))
                    settings["allowed_chat_ids"] = [c for c in settings["allowed_chat_ids"] if c not in remove_items]
                    if not settings["allowed_chat_ids"]:
                        raise ValueError("Нельзя удалить все чаты: список не должен быть пуст")
                    result = "Чаты удалены."

                elif pending.action == "interval_set":
                    interval = int(text)
                    if interval <= 0:
                        raise ValueError("Интервал должен быть больше 0")
                    settings["check_interval"] = interval
                    result = f"Интервал обновлен: {interval} сек."

                else:
                    result = "Неизвестное действие"

                normalize_settings(settings)
                save_settings(settings)

        except ValueError as e:
            await message.answer(f"Ошибка: {e}\nПовторите ввод или /cancel")
            return
        except Exception as e:
            logger.exception("Ошибка обработки ввода админа: %s", e)
            await message.answer("Не удалось сохранить изменения. Попробуйте снова.")
            return
        finally:
            # Оставляем действие активным только если была ошибка валидации.
            pass

        state.pending_actions.pop(user.id, None)
        await message.answer(result)
        async with state.lock:
            text_after = settings_text(state.settings)
        await message.answer(text_after, reply_markup=main_menu_keyboard())

    dp.include_router(router)


async def monitor_streams(bot: Bot, twitch: TwitchClient, state: AppState) -> None:
    notified_streamers: set[str] = set()
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with state.lock:
                    streamers = list(state.settings["streamers_to_track"])
                    chat_ids = list(state.settings["allowed_chat_ids"])
                    phrases = list(state.settings["random_phrases"])
                    check_interval = int(state.settings["check_interval"])

                logger.info("Проверка стримов...")
                live_streams = await twitch.get_live_streams(session, streamers)

                logger.info("Активные стримы: %s", sorted(live_streams))
                logger.info("Уже уведомлены: %s", sorted(notified_streamers))

                for streamer in sorted(live_streams):
                    if streamer in notified_streamers:
                        continue

                    text = random.choice(phrases)
                    message = f"🔴 {streamer} начал стрим!\n\n{text}"
                    await send_message(bot, chat_ids, message)
                    notified_streamers.add(streamer)
                    logger.info("Уведомление отправлено: %s", streamer)

                notified_streamers.intersection_update(live_streams)

            except Exception as e:
                logger.exception("Глобальная ошибка цикла мониторинга: %s", e)
                check_interval = 30

            logger.info("Следующая проверка через %s сек", check_interval)
            await asyncio.sleep(check_interval)


async def run() -> None:
    settings = load_settings()

    bot_token = os.getenv("BOT_TOKEN") or settings.get("bot_token")
    twitch_client_id = os.getenv("TWITCH_CLIENT_ID") or settings.get("twitch_client_id")
    twitch_client_secret = os.getenv("TWITCH_CLIENT_SECRET") or settings.get("twitch_client_secret")

    if not bot_token or not twitch_client_id or not twitch_client_secret:
        raise RuntimeError(
            "Не найдены креды бота. Задайте BOT_TOKEN/TWITCH_CLIENT_ID/TWITCH_CLIENT_SECRET в окружении "
            "или bot_token/twitch_client_id/twitch_client_secret в bot_settings.json"
        )

    admin_user_ids = parse_admin_user_ids(settings)
    state = AppState(
        settings=settings,
        admin_user_ids=admin_user_ids,
        pending_actions={},
        lock=asyncio.Lock(),
    )

    bot = Bot(token=bot_token)
    dp = Dispatcher()
    twitch = TwitchClient(twitch_client_id, twitch_client_secret)

    setup_handlers(dp, state)

    logger.info("Бот запущен")
    if admin_user_ids:
        logger.info("Доступ к панели для admin_user_ids: %s", sorted(admin_user_ids))
    else:
        logger.warning("admin_user_ids не заданы. Первый пользователь, открывший /start в личке, станет админом.")

    monitor_task = asyncio.create_task(monitor_streams(bot, twitch, state))

    try:
        await dp.start_polling(bot)
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(run())
