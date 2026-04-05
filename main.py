import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, Message

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


@dataclass
class StreamPostState:
    phrase: str
    link: str
    thumbnail_template: str
    messages: dict[str, int]
    last_update_ts: float


def normalize_settings(raw: dict[str, Any]) -> dict[str, Any]:
    required = ["streamers_to_track", "allowed_chat_ids", "random_phrases", "check_interval"]
    missing = [key for key in required if key not in raw]
    if missing:
        raise ValueError(f"В settings отсутствуют обязательные поля: {', '.join(missing)}")

    if not isinstance(raw["check_interval"], int) or raw["check_interval"] <= 0:
        raise ValueError("check_interval должен быть положительным числом (секунды)")

    screenshot_update_interval = raw.get("screenshot_update_interval", 30)
    if not isinstance(screenshot_update_interval, int) or screenshot_update_interval <= 0:
        raise ValueError("screenshot_update_interval должен быть положительным числом (секунды)")
    raw["screenshot_update_interval"] = screenshot_update_interval

    stream_links_count = raw.get("stream_links_count", 3)
    if not isinstance(stream_links_count, int) or stream_links_count <= 0:
        raise ValueError("stream_links_count должен быть положительным числом")
    raw["stream_links_count"] = stream_links_count

    raw["streamers_to_track"] = [str(s).strip().lower() for s in raw["streamers_to_track"] if str(s).strip()]
    raw["allowed_chat_ids"] = [str(c).strip() for c in raw["allowed_chat_ids"] if str(c).strip()]
    raw["random_phrases"] = [str(p).strip() for p in raw["random_phrases"] if str(p).strip()]

    if not raw["streamers_to_track"]:
        raise ValueError("Список streamers_to_track пуст")
    if not raw["allowed_chat_ids"]:
        raise ValueError("Список allowed_chat_ids пуст")
    if not raw["random_phrases"]:
        raise ValueError("Список random_phrases пуст")

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


def pick_secret(settings: dict[str, Any], env_keys: list[str], settings_keys: list[str]) -> str:
    for key in env_keys:
        value = os.getenv(key)
        if value and value.strip():
            return value.strip()
    for key in settings_keys:
        value = settings.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def build_post_caption(phrase: str, link: str, link_count: int) -> str:
    links = "\n".join([link] * max(1, link_count))
    return f"🔴 {phrase}\n\n{links}"


def build_thumbnail_url(template: str) -> str:
    url = template.replace("{width}", "1280").replace("{height}", "720")
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}t={int(time.time())}"


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Текущие настройки", callback_data="menu:show")],
            [InlineKeyboardButton(text="👤 Стримеры", callback_data="menu:streamers")],
            [InlineKeyboardButton(text="💬 Фразы", callback_data="menu:phrases")],
            [InlineKeyboardButton(text="⏱ Интервалы", callback_data="menu:interval")],
            [InlineKeyboardButton(text="📣 Чаты", callback_data="menu:chats")],
            [InlineKeyboardButton(text="🧪 Тест-пост", callback_data="menu:test_post")],
            [InlineKeyboardButton(text="🔐 Проверить Twitch", callback_data="menu:check_twitch")],
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
            [InlineKeyboardButton(text="Изменить check_interval", callback_data="interval:set_check")],
            [InlineKeyboardButton(text="Изменить screenshot_interval", callback_data="interval:set_screenshot")],
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
        f"check_interval: {settings['check_interval']} сек\n"
        f"screenshot_update_interval: {settings['screenshot_update_interval']} сек\n"
        f"stream_links_count: {settings['stream_links_count']}\n"
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

    async def get_live_streams_info(self, session: aiohttp.ClientSession, streamers: list[str]) -> dict[str, dict[str, str]]:
        if self._auth is None:
            await self.refresh_token(session)

        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self._auth.token}",
        }

        live: dict[str, dict[str, str]] = {}

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
                            row = data["data"][0]
                            stream_login = str(row.get("user_login") or login).lower()
                            live[stream_login] = {
                                "link": f"https://www.twitch.tv/{stream_login}",
                                "thumbnail_template": str(row.get("thumbnail_url") or ""),
                            }
                    continue

                data = await response.json()
                if response.status != 200:
                    logger.error("Twitch API ошибка для %s: %s %s", login, response.status, data)
                    continue

                if data.get("data"):
                    row = data["data"][0]
                    stream_login = str(row.get("user_login") or login).lower()
                    live[stream_login] = {
                        "link": f"https://www.twitch.tv/{stream_login}",
                        "thumbnail_template": str(row.get("thumbnail_url") or ""),
                    }

        return live


def setup_handlers(dp: Dispatcher, state: AppState, twitch: TwitchClient) -> None:
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

    @router.callback_query(F.data == "menu:test_post")
    async def menu_test_post(callback: CallbackQuery) -> None:
        if await deny_callback_if_not_admin(callback):
            return
        await callback.answer("Отправляю тест-пост...")

        async with state.lock:
            phrases = list(state.settings["random_phrases"])
            streamers = list(state.settings["streamers_to_track"])
            links_count = int(state.settings["stream_links_count"])

        if callback.message is None:
            return

        phrase = random.choice(phrases)
        fallback_streamer = streamers[0]
        info = {
            "link": f"https://www.twitch.tv/{fallback_streamer}",
            "thumbnail_template": f"https://static-cdn.jtvnw.net/previews-ttv/live_user_{fallback_streamer}-{{width}}x{{height}}.jpg",
        }

        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                live_info = await twitch.get_live_streams_info(session, streamers)
                if live_info:
                    info = live_info[sorted(live_info.keys())[0]]
        except Exception as e:
            logger.warning("Не удалось получить live-данные для тест-поста: %s", e)

        caption = build_post_caption(phrase, info["link"], links_count)
        photo_url = build_thumbnail_url(info["thumbnail_template"])
        await callback.message.answer_photo(photo=photo_url, caption=caption)

    @router.callback_query(F.data == "menu:check_twitch")
    async def menu_check_twitch(callback: CallbackQuery) -> None:
        if await deny_callback_if_not_admin(callback):
            return
        await callback.answer("Проверяю Twitch client_id и secret...")

        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                await twitch.refresh_token(session)
            expires = twitch._auth.expires_in if twitch._auth else 0
            text = (
                "Проверка Twitch пройдена успешно.\n"
                f"client_id/client_secret валидны, токен получен (expires_in={expires} сек)."
            )
        except Exception as e:
            text = (
                "Проверка Twitch не пройдена.\n"
                "Проверьте TWITCH_CLIENT_ID и TWITCH_CLIENT_SECRET.\n"
                f"Детали: {e}"
            )

        if callback.message:
            await callback.message.answer(text)

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
            check_interval = state.settings["check_interval"]
            screenshot_interval = state.settings["screenshot_update_interval"]
        if callback.message:
            await callback.message.edit_text(
                f"check_interval: {check_interval} сек\nscreenshot_update_interval: {screenshot_interval} сек",
                reply_markup=interval_keyboard(),
            )

    @router.callback_query(
        F.data.endswith(":add")
        | F.data.endswith(":remove")
        | (F.data == "interval:set_check")
        | (F.data == "interval:set_screenshot")
    )
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
        elif data == "interval:set_check":
            state.pending_actions[user.id] = PendingAction("interval_set_check")
            text = "Отправьте новый check_interval в секундах (целое число > 0)."
        elif data == "interval:set_screenshot":
            state.pending_actions[user.id] = PendingAction("interval_set_screenshot")
            text = "Отправьте новый screenshot_update_interval в секундах (целое число > 0)."
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
        if user is None or not is_admin(user.id, state.admin_user_ids):
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
                    merged = set(settings["streamers_to_track"])
                    merged.update(new_items)
                    settings["streamers_to_track"] = sorted(merged)
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
                    settings["random_phrases"].extend(parse_lines(text))
                    result = "Фразы добавлены."

                elif pending.action == "phrases_remove":
                    remove_items = set(parse_lines(text))
                    settings["random_phrases"] = [p for p in settings["random_phrases"] if p not in remove_items]
                    if not settings["random_phrases"]:
                        raise ValueError("Нельзя удалить все фразы: список не должен быть пуст")
                    result = "Фразы удалены."

                elif pending.action == "chats_add":
                    merged = set(settings["allowed_chat_ids"])
                    merged.update(parse_lines(text))
                    settings["allowed_chat_ids"] = sorted(merged)
                    result = "Чаты добавлены."

                elif pending.action == "chats_remove":
                    remove_items = set(parse_lines(text))
                    settings["allowed_chat_ids"] = [c for c in settings["allowed_chat_ids"] if c not in remove_items]
                    if not settings["allowed_chat_ids"]:
                        raise ValueError("Нельзя удалить все чаты: список не должен быть пуст")
                    result = "Чаты удалены."

                elif pending.action == "interval_set_check":
                    interval = int(text)
                    if interval <= 0:
                        raise ValueError("Интервал должен быть больше 0")
                    settings["check_interval"] = interval
                    result = f"check_interval обновлен: {interval} сек."

                elif pending.action == "interval_set_screenshot":
                    interval = int(text)
                    if interval <= 0:
                        raise ValueError("Интервал должен быть больше 0")
                    settings["screenshot_update_interval"] = interval
                    result = f"screenshot_update_interval обновлен: {interval} сек."

                else:
                    raise ValueError("Неизвестное действие")

                normalize_settings(settings)
                save_settings(settings)

        except ValueError as e:
            await message.answer(f"Ошибка: {e}\nПовторите ввод или /cancel")
            return
        except Exception as e:
            logger.exception("Ошибка обработки ввода админа: %s", e)
            await message.answer("Не удалось сохранить изменения. Попробуйте снова.")
            return

        state.pending_actions.pop(user.id, None)
        await message.answer(result)
        async with state.lock:
            text_after = settings_text(state.settings)
        await message.answer(text_after, reply_markup=main_menu_keyboard())

    dp.include_router(router)


async def create_stream_posts(
    bot: Bot,
    chat_ids: list[str],
    info: dict[str, str],
    phrase: str,
    links_count: int,
) -> dict[str, int]:
    messages: dict[str, int] = {}
    caption = build_post_caption(phrase, info["link"], links_count)
    photo_url = build_thumbnail_url(info["thumbnail_template"])

    for chat_id in chat_ids:
        try:
            sent = await bot.send_photo(chat_id=chat_id, photo=photo_url, caption=caption)
            messages[str(chat_id)] = sent.message_id
            logger.info("Создан пост стрима %s в %s", info["link"], chat_id)
        except Exception as e:
            logger.exception("Ошибка отправки поста стрима в %s: %s", chat_id, e)

    return messages


async def refresh_stream_posts(
    bot: Bot,
    post: StreamPostState,
    links_count: int,
) -> None:
    caption = build_post_caption(post.phrase, post.link, links_count)
    photo_url = build_thumbnail_url(post.thumbnail_template)

    for chat_id, message_id in list(post.messages.items()):
        try:
            await bot.edit_message_media(
                chat_id=chat_id,
                message_id=message_id,
                media=InputMediaPhoto(media=photo_url, caption=caption),
            )
        except Exception as e:
            # Если пост уже удален в чате, просто убираем его из отслеживания.
            logger.warning("Не удалось обновить пост %s/%s: %s", chat_id, message_id, e)


async def delete_stream_posts(bot: Bot, post: StreamPostState) -> None:
    for chat_id, message_id in list(post.messages.items()):
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.info("Пост стрима удален в %s (message_id=%s)", chat_id, message_id)
        except Exception as e:
            logger.warning("Не удалось удалить пост %s/%s: %s", chat_id, message_id, e)


async def monitor_streams(bot: Bot, twitch: TwitchClient, state: AppState) -> None:
    posts_by_streamer: dict[str, StreamPostState] = {}
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with state.lock:
                    streamers = list(state.settings["streamers_to_track"])
                    chat_ids = list(state.settings["allowed_chat_ids"])
                    phrases = list(state.settings["random_phrases"])
                    check_interval = int(state.settings["check_interval"])
                    screenshot_update_interval = int(state.settings["screenshot_update_interval"])
                    links_count = int(state.settings["stream_links_count"])

                live_info = await twitch.get_live_streams_info(session, streamers)
                now = time.time()

                logger.info("Активные стримы: %s", sorted(live_info.keys()))

                for streamer, info in live_info.items():
                    existing = posts_by_streamer.get(streamer)
                    if existing is None:
                        phrase = random.choice(phrases)
                        messages = await create_stream_posts(bot, chat_ids, info, phrase, links_count)
                        if messages:
                            posts_by_streamer[streamer] = StreamPostState(
                                phrase=phrase,
                                link=info["link"],
                                thumbnail_template=info["thumbnail_template"],
                                messages=messages,
                                last_update_ts=now,
                            )
                        continue

                    existing.link = info["link"]
                    existing.thumbnail_template = info["thumbnail_template"]

                    if now - existing.last_update_ts >= screenshot_update_interval:
                        await refresh_stream_posts(bot, existing, links_count)
                        existing.last_update_ts = now

                ended = [streamer for streamer in posts_by_streamer.keys() if streamer not in live_info]
                for streamer in ended:
                    post = posts_by_streamer.pop(streamer)
                    await delete_stream_posts(bot, post)

            except Exception as e:
                logger.exception("Глобальная ошибка цикла мониторинга: %s", e)
                check_interval = 30
                screenshot_update_interval = 30

            sleep_for = max(5, min(check_interval, screenshot_update_interval))
            logger.info("Следующая проверка/обновление через %s сек", sleep_for)
            await asyncio.sleep(sleep_for)


async def run() -> None:
    settings = load_settings()

    bot_token = pick_secret(
        settings,
        env_keys=["BOT_TOKEN", "TELEGRAM_BOT_TOKEN"],
        settings_keys=["bot_token", "telegram_bot_token", "BOT_TOKEN"],
    )
    twitch_client_id = pick_secret(
        settings,
        env_keys=["TWITCH_CLIENT_ID"],
        settings_keys=["twitch_client_id", "TWITCH_CLIENT_ID"],
    )
    twitch_client_secret = pick_secret(
        settings,
        env_keys=["TWITCH_CLIENT_SECRET"],
        settings_keys=["twitch_client_secret", "TWITCH_CLIENT_SECRET"],
    )

    missing: list[str] = []
    if not bot_token:
        missing.append("BOT_TOKEN")
    if not twitch_client_id:
        missing.append("TWITCH_CLIENT_ID")
    if not twitch_client_secret:
        missing.append("TWITCH_CLIENT_SECRET")

    if missing:
        raise RuntimeError(
            "Не найдены обязательные креды: "
            + ", ".join(missing)
            + ". Укажите их в env или в bot_settings.json."
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

    setup_handlers(dp, state, twitch)

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
