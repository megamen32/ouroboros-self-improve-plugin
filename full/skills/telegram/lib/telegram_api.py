from __future__ import annotations

import base64
import mimetypes
import re
from typing import Any, Dict, Optional

import httpx

# Telegram hard-caps a single sendMessage at 4096 chars. We chunk the RAW text
# (pre-HTML) on line/space boundaries below this so each chunk is converted and
# sent independently — avoiding both the silent "message is too long" 400 that
# dropped long replies and tag-splitting corruption from cutting inside HTML.
_TELEGRAM_TEXT_LIMIT = 4096
_TELEGRAM_CHUNK_RAW = 3500
# Match Ouroboros's existing per-photo transfer ceiling for every Telegram download.
_MAX_TELEGRAM_DOWNLOAD_BYTES = 10 * 1024 * 1024
_NOT_MODIFIED_PREFIX = "bad request: message is not modified"


class TelegramRequestRejected(RuntimeError):
    """Telegram returned an explicit negative API response."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 0,
        plain_retry_safe: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = int(status_code or 0)
        self.plain_retry_safe = bool(plain_retry_safe)

    @property
    def transient(self) -> bool:
        return self.status_code == 429 or self.status_code >= 500


class TelegramTransportError(RuntimeError):
    """Telegram could not return a trustworthy API response yet."""


def _chunk_raw_text(text: str, limit: int = _TELEGRAM_CHUNK_RAW) -> list[str]:
    """Split raw text into <=limit-char pieces on line, then space, boundaries."""
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    buf = ""
    for line in text.split("\n"):
        while len(line) > limit:
            # A single very long line: break on the last space within the window,
            # else hard-cut at the limit.
            cut = line.rfind(" ", 0, limit)
            cut = cut if cut > 0 else limit
            piece = line[:cut]
            if buf:
                chunks.append(buf)
                buf = ""
            chunks.append(piece)
            line = line[cut:].lstrip(" ")
        candidate = f"{buf}\n{line}" if buf else line
        if len(candidate) > limit:
            if buf:
                chunks.append(buf)
            buf = line
        else:
            buf = candidate
    if buf:
        chunks.append(buf)
    return chunks or [""]


def markdown_to_telegram_html(text: str) -> str:
    """Convert standard rich Markdown text into Telegram-compliant HTML syntax."""
    if not text:
        return text

    # 1. Escape HTML special characters
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # Placeholder dictionaries
    pre_placeholder_map = {}
    code_placeholder_map = {}

    # 2. Extract multi-line code blocks before parsing other markdown
    def replace_pre(match: re.Match) -> str:
        code_content = match.group(2)
        placeholder = f"PREPLACEHOLDER{len(pre_placeholder_map)}"
        pre_placeholder_map[placeholder] = f"<pre>{code_content}</pre>"
        return placeholder

    text = re.sub(r"```([A-Za-z0-9_-]*)\s*\n?(.*?)```", replace_pre, text, flags=re.DOTALL)

    # 3. Extract inline code blocks
    def replace_code(match: re.Match) -> str:
        inner = match.group(1)
        placeholder = f"CODEPLACEHOLDER{len(code_placeholder_map)}"
        code_placeholder_map[placeholder] = f"<code>{inner}</code>"
        return placeholder

    text = re.sub(r"`([^`\n]+)`", replace_code, text)

    # 4. Headers and list markdown formatting line-by-line
    lines = []
    for line in text.split("\n"):
        header_match = re.match(r"^(\s*)#{1,6}\s+(.+)$", line)
        if header_match:
            indent = header_match.group(1) or ""
            content = header_match.group(2)
            lines.append(f"{indent}<b>{content}</b>")
        else:
            # Replace starting list bullet * or - with •
            bullet_match = re.match(r"^(\s*)[*-]\s+(.+)$", line)
            if bullet_match:
                lines.append(f"{bullet_match.group(1)}• {bullet_match.group(2)}")
            else:
                lines.append(line)
    text = "\n".join(lines)

    # 5. Bold and Italic replacing outside of inline blocks.
    # Asterisk patterns match anywhere — `**bold**` and `*italic*` are unambiguous.
    # Underscore patterns require non-word context on both sides so identifiers
    # like `chat_id`, `state_dir`, `OUROBOROS_MODEL` inside bold spans do NOT
    # trigger spurious italic wraps that cross outer tag boundaries (which
    # would produce malformed nested HTML and a Telegram 400 Bad Request).
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<!\w)__(?=\S)([^_\n]+?)(?<=\S)__(?!\w)", r"<b>\1</b>", text)
    text = re.sub(r"\*(.+?)\*", r"<i>\1</i>", text)
    text = re.sub(r"(?<!\w)_(?=\S)([^_\n]+?)(?<=\S)_(?!\w)", r"<i>\1</i>", text)

    # 6. Links [text](url) -> <a href="url">text</a>
    text = re.sub(r"\[(.*?)\]\((.*?)\)", r'<a href="\2">\1</a>', text)

    # 7. Reconstruct codeblocks (sorted by length descending to prevent sub-string prefix collisions)
    for placeholder, code_html in sorted(pre_placeholder_map.items(), key=lambda x: len(x[0]), reverse=True):
        text = text.replace(placeholder, code_html)
    for placeholder, code_html in sorted(code_placeholder_map.items(), key=lambda x: len(x[0]), reverse=True):
        text = text.replace(placeholder, code_html)

    return text


class TelegramClient:
    def __init__(self, token: str):
        self.token = str(token or "").strip()
        if not self.token:
            raise ValueError("TELEGRAM_BOT_TOKEN is missing")
        self.api_base = f"https://api.telegram.org/bot{self.token}"
        self.file_base = f"https://api.telegram.org/file/bot{self.token}"

    async def call(self, method: str, *, data: Optional[dict] = None, files: Optional[dict] = None, timeout: int = 30) -> Dict[str, Any]:
        method_text = str(method or "")
        safe_method = method_text if re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,63}", method_text) else "request"
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(f"{self.api_base}/{method_text}", data=data, files=files)
        except httpx.TimeoutException:
            raise TelegramTransportError(f"Telegram API timed out during {safe_method}.") from None
        except httpx.HTTPError as exc:
            raise TelegramTransportError(
                f"Telegram API transport failed during {safe_method} ({type(exc).__name__})."
            ) from None
        try:
            payload = response.json()
        except ValueError:
            raise TelegramTransportError(
                f"Telegram API returned invalid JSON during {safe_method}."
            ) from None
        if not isinstance(payload, dict):
            raise TelegramTransportError(
                f"Telegram API returned an invalid response during {safe_method}."
            )
        try:
            api_status = int(payload.get("error_code") or response.status_code)
        except (TypeError, ValueError):
            api_status = response.status_code
        description = str(payload.get("description") or "").strip()
        if safe_method == "editMessageText" and description.casefold().startswith(_NOT_MODIFIED_PREFIX):
            # Preserve the one benign signal callers rely on without forwarding
            # Telegram's free-form suffix (which may echo the token-bearing URL).
            raise TelegramRequestRejected(
                "Telegram API editMessageText: message is not modified.",
                status_code=response.status_code,
            )
        if response.status_code >= 400:
            raise TelegramRequestRejected(
                f"Telegram API {safe_method} returned HTTP {response.status_code}.",
                status_code=response.status_code,
                plain_retry_safe=(safe_method == "sendMessage" and response.status_code == 400),
            )
        if not payload.get("ok"):
            raise TelegramRequestRejected(
                f"Telegram API rejected {safe_method}.",
                status_code=api_status,
                plain_retry_safe=(safe_method == "sendMessage" and api_status == 400),
            )
        return payload

    async def _download_bytes(self, file_path: str) -> bytes:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                async with client.stream("GET", f"{self.file_base}/{file_path}") as response:
                    if response.status_code >= 400:
                        raise RuntimeError(f"Telegram file download returned HTTP {response.status_code}")
                    try:
                        announced = int(response.headers.get("content-length", ""))
                    except (TypeError, ValueError):
                        announced = 0
                    if announced > _MAX_TELEGRAM_DOWNLOAD_BYTES:
                        raise RuntimeError("Telegram file exceeds the 10 MiB download limit.")
                    content = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(content) + len(chunk) > _MAX_TELEGRAM_DOWNLOAD_BYTES:
                            raise RuntimeError("Telegram file exceeds the 10 MiB download limit.")
                        content.extend(chunk)
        except httpx.TimeoutException:
            raise RuntimeError("Telegram file download timed out.") from None
        except httpx.HTTPError as exc:
            raise RuntimeError(
                f"Telegram file download transport failed ({type(exc).__name__})."
            ) from None
        return bytes(content)

    async def get_updates(self, offset: int) -> list[dict]:
        payload = await self.call("getUpdates", data={"timeout": 20, "offset": offset}, timeout=25)
        return list(payload.get("result") or [])

    async def send_message(self, chat_id: int, text: str, parse_mode: str = "HTML") -> int:
        """Send a text message, chunking past Telegram's 4096-char limit.

        Long text is split on line/space boundaries and each chunk is sent as its
        own message (converted independently so HTML tags never span a cut).
        Returns the LAST chunk's message_id (0 on parse failure / empty input).
        """
        chunks = _chunk_raw_text(str(text or ""))
        last_message_id = 0
        for chunk in chunks:
            formatted = markdown_to_telegram_html(chunk) if parse_mode == "HTML" else chunk
            data = {"chat_id": str(chat_id), "text": formatted}
            if parse_mode:
                data["parse_mode"] = parse_mode
            try:
                payload = await self.call("sendMessage", data=data, timeout=20)
            except TelegramRequestRejected as exc:
                if parse_mode != "HTML" or not exc.plain_retry_safe:
                    raise
                payload = await self.call(
                    "sendMessage",
                    data={"chat_id": str(chat_id), "text": chunk},
                    timeout=20,
                )
            try:
                last_message_id = int((payload.get("result") or {}).get("message_id") or 0)
            except (TypeError, ValueError):
                last_message_id = 0
        return last_message_id

    async def edit_message_text(self, chat_id: int, message_id: int, text: str, parse_mode: str = "HTML") -> bool:
        """Replace the text of an existing message in-place (silent mode). Returns True on success.

        Failures (message too old, deleted, identical content, parse error) are
        suppressed so the caller can fall back to send_message + reset tracking.
        """
        formatted = markdown_to_telegram_html(text) if parse_mode == "HTML" else text
        data = {
            "chat_id": str(chat_id),
            "message_id": str(message_id),
            "text": formatted,
        }
        if parse_mode:
            data["parse_mode"] = parse_mode
        try:
            await self.call("editMessageText", data=data, timeout=20)
            return True
        except Exception as exc:
            # "message is not modified" means the bubble already shows this exact
            # text — treat it as success so the caller does NOT post a duplicate.
            if "not modified" in str(exc).lower():
                return True
            return False

    async def send_chat_action(self, chat_id: int, action: str = "typing") -> None:
        await self.call("sendChatAction", data={"chat_id": str(chat_id), "action": action}, timeout=10)

    async def send_photo(self, chat_id: int, image_base64: str, *, caption: str = "", mime: str = "image/png", parse_mode: str = "HTML") -> None:
        filename = "image.png" if mime == "image/png" else "image.jpg"
        files = {"photo": (filename, base64.b64decode(image_base64), mime)}
        formatted = markdown_to_telegram_html(caption) if parse_mode == "HTML" else caption
        data = {"chat_id": str(chat_id), "caption": formatted}
        if parse_mode:
            data["parse_mode"] = parse_mode
        await self.call("sendPhoto", data=data, files=files, timeout=30)

    async def send_document(
        self, chat_id: int, file_bytes: bytes, filename: str = "file", *, caption: str = "", parse_mode: str = "HTML"
    ) -> None:
        """Send an arbitrary document/file to a chat via sendDocument."""
        safe_name = (str(filename or "file").replace("\r", " ").replace("\n", " ").strip() or "file")
        files = {"document": (safe_name, file_bytes, "application/octet-stream")}
        formatted = markdown_to_telegram_html(caption) if (caption and parse_mode == "HTML") else caption
        data = {"chat_id": str(chat_id)}
        if formatted:
            data["caption"] = formatted
            if parse_mode:
                data["parse_mode"] = parse_mode
        await self.call("sendDocument", data=data, files=files, timeout=60)

    async def send_message_with_inline_keyboard(
        self, chat_id: int, text: str, keyboard: list[list[dict]], parse_mode: str = "HTML"
    ) -> None:
        """Send a message with an inline keyboard (list of button rows)."""
        import json as _json
        reply_markup = _json.dumps({"inline_keyboard": keyboard})
        formatted = markdown_to_telegram_html(text) if parse_mode == "HTML" else text
        data = {"chat_id": str(chat_id), "text": formatted, "reply_markup": reply_markup}
        if parse_mode:
            data["parse_mode"] = parse_mode
        await self.call(
            "sendMessage",
            data=data,
            timeout=20,
        )

    async def answer_callback_query(self, callback_query_id: str, *, text: str = "") -> None:
        """Acknowledge a callback query from an inline button press."""
        data: dict = {"callback_query_id": callback_query_id}
        if text:
            data["text"] = text
        await self.call("answerCallbackQuery", data=data, timeout=10)

    async def download_photo(self, file_id: str) -> tuple[str, str]:
        payload = await self.call("getFile", data={"file_id": file_id}, timeout=20)
        file_path = str((payload.get("result") or {}).get("file_path") or "").strip()
        if not file_path:
            raise RuntimeError("Telegram file path is missing")
        content = await self._download_bytes(file_path)
        mime = mimetypes.guess_type(file_path)[0] or "image/jpeg"
        return base64.b64encode(content).decode("ascii"), mime

    async def download_file(self, file_id: str) -> bytes:
        """Download an arbitrary file from Telegram servers and return its raw bytes."""
        payload = await self.call("getFile", data={"file_id": file_id}, timeout=20)
        file_path = str((payload.get("result") or {}).get("file_path") or "").strip()
        if not file_path:
            raise RuntimeError("Telegram file path is missing")
        return await self._download_bytes(file_path)

    async def edit_message_text_with_inline_keyboard(
        self, chat_id: int, message_id: int, text: str, keyboard: list[list[dict]], parse_mode: str = "HTML"
    ) -> bool:
        """Edit a message and keyboard in-place; return whether it is current."""
        import json as _json
        reply_markup = _json.dumps({"inline_keyboard": keyboard})
        formatted = markdown_to_telegram_html(text) if parse_mode == "HTML" else text
        data = {
            "chat_id": str(chat_id),
            "message_id": str(message_id),
            "text": formatted,
            "reply_markup": reply_markup,
        }
        if parse_mode:
            data["parse_mode"] = parse_mode
        try:
            await self.call("editMessageText", data=data, timeout=20)
            return True
        except Exception as exc:
            return str(exc) == "Telegram API editMessageText: message is not modified."


_LOCALIZED_TEXTS = {
    "en": {
        "menu_title_strict": "🤖 **Ouroboros Control Panel**\nStrict mode is active. Commands are blocked.\n\nSelect action:",
        "menu_title": "🤖 **Ouroboros Control Centre**\nCommand Mode: `{command_mode}`\nLanguage: `{lang}`\n\nExplore and monitor the core using the buttons below:",
        "btn_metrics": "📉 Status & Metrics",
        "btn_mind": "🧠 Mind & BG",
        "btn_language": "🌐 Select language",
        "btn_refresh": "🔄 Update parameter card",
        "btn_back": "⬅️ Back to main panel",
        "btn_stop_bg": "🔴 Pause background thoughts",
        "btn_start_bg": "🟢 Resume background thoughts",
        "btn_thoughts": "💭 What are you thinking about?",
        "metrics_title": "📊 **Ouroboros live metrics**\n\n{info_text}\n---",
        "mind_title": "🧠 **Background Consciousness**\n\nCurrent state: {state_str}\n\nBackground thinking processes information between your chat queries.",
        "mind_thoughts": "\n\n**Recent thoughts catalog:**\n{thoughts_text}",
        "mind_state_active": "🟢 **Thinking** (running)",
        "mind_state_sleeping": "🔴 **Sleeping** (paused)",
        "lang_title": "🌐 Select chatbot bridge interface language:\nCurrently active: **English**",
        "lang_en": "🇬🇧 English",
        "lang_ru": "🇷🇺 Русский",
        "help_text": (
            "🤖 **Ouroboros Telegram Help**\n\n"
            "Available commands:\n"
            "• `/menu` — Show interactive control panel with active tabs\n"
            "• `/language` — Change bridge interface language\n"
            "• `/status` — Request live system status (if allowed)\n"
            "• `/help` — Show this friendly usage guide\n\n"
            "Modes description (changed in Web UI → Settings → Telegram):\n"
            "• **strict** — block all command injections (only `/menu`, `/help`, `/language`)\n"
            "• **safe_commands** — allow status monitoring\n"
            "• **full_access** — allow status + background loop start/stop"
        ),
        "slash_blocked_strict": "⛔ Slash commands are not allowed in strict mode. Use `/menu` to see available options, or change mode in Settings → Telegram.",
        "slash_blocked_mode": "⛔ This command is not allowed in the current mode. Use `/menu` to see available options.",
        "not_authorized": "Not authorized",
        "updating_status": "Updating status metrics...",
        "extracting_thoughts": "Extracting thoughts...",
        "injecting_consciousness": "Injecting consciousness signal...",
        "restricted_safe": "⛔ Restricted in safe mode",
        "lang_changed": "✅ Language changed to English",
        "unknown_command": "Unknown command",
        "metrics_budget_status": "• **Budget Status:**\n  Spent: `${spent_usd:.4f}`\n  Limit: `${total_budget:.2f}`\n  Remaining: `${rem:.4f}`\n\n• **System Environment:**\n  Branch: `{branch}`\n  BG Thoughts: `{bg_status}`",
        "bg_active_label": "ACTIVE",
        "bg_sleeping_label": "SLEEPING",
        "btn_settings": "⚙️ Settings",
        "settings_title": "⚙️ **Telegram Settings**\nConfigure the Telegram control panel and message display:",
        "btn_silent_on": "🔕 Silent Mode: ON",
        "btn_silent_off": "🔔 Silent Mode: OFF",
        "silent_toggled_on": "🔕 Silent mode enabled — new thoughts will replace the last message",
        "silent_toggled_off": "🔔 Silent mode disabled — each thought becomes a new message",
    },
    "ru": {
        "menu_title_strict": "🤖 **Панель управления Ouroboros**\nРежим Strict активен. Команды заблокированы.\n\nВыберите действие:",
        "menu_title": "🤖 **Центр управления Ouroboros**\nРежим команд: `{command_mode}`\nЯзык: `{lang}`\n\nУправляйте и следите за ядром с помощью кнопок:",
        "btn_metrics": "📉 Статус и метрики",
        "btn_mind": "🧠 Фоновое сознание",
        "btn_language": "🌐 Выбор языка",
        "btn_refresh": "🔄 Обновить показатели",
        "btn_back": "⬅️ Назад в меню",
        "btn_stop_bg": "🔴 Приостановить размышления",
        "btn_start_bg": "🟢 Продолжить размышления",
        "btn_thoughts": "💭 О чём ты думаешь сейчас?",
        "metrics_title": "📊 **Живые показатели Ouroboros**\n\n{info_text}\n---",
        "mind_title": "🧠 **Фоновое Сознание**\n\nТекущее состояние: {state_str}\n\nФоновое мышление анализирует информацию между вашими запросами.",
        "mind_thoughts": "\n\n**Последние мысли из лога:**\n{thoughts_text}",
        "mind_state_active": "🟢 **Думает** (активно)",
        "mind_state_sleeping": "🔴 **Спит** (на паузе)",
        "lang_title": "🌐 Выберите язык интерфейса бота-моста:\nАктивный язык: **Русский**",
        "lang_en": "🇬🇧 English",
        "lang_ru": "🇷🇺 Русский",
        "help_text": (
            "🤖 **Справка по Telegram в Ouroboros**\n\n"
            "Доступные команды:\n"
            "• `/menu` — Открыть интерактивную панель управления\n"
            "• `/language` — Изменить и настроить язык интерфейса\n"
            "• `/status` — Запросить текущий статус системы (если разрешено)\n"
            "• `/help` — Показать это руководство\n\n"
            "Описание режимов (меняется в Web UI → Settings → Telegram):\n"
            "• **strict** — блокировать ввод команд (доступны только `/menu`, `/help`, `/language`)\n"
            "• **safe_commands** — разрешить просмотр метрик и статуса\n"
            "• **full_access** — доступ ко всем кнопкам, включая запуск/паузу фонового сознания"
        ),
        "slash_blocked_strict": "⛔ Слэш-команды запрещены в режиме strict. Используйте `/menu` или измените режим в Settings → Telegram.",
        "slash_blocked_mode": "⛔ Эта команда не разрешена в текущем режиме. Используйте `/menu` для вызова управления.",
        "not_authorized": "Доступ ограничен",
        "updating_status": "Обновление метрик...",
        "extracting_thoughts": "Извлечение мыслей...",
        "injecting_consciousness": "Отправка сигнала сознания...",
        "restricted_safe": "⛔ Ограничено в режиме Safe",
        "lang_changed": "✅ Язык интерфейса изменен на Русский",
        "unknown_command": "Неизвестная команда",
        "metrics_budget_status": "• **Бюджетный статус:**\n  Потрачено: `${spent_usd:.4f}`\n  Лимит: `${total_budget:.2f}`\n  Осталось: `${rem:.4f}`\n\n• **Окружение системы:**\n  Ветка Git: `{branch}`\n  Фоновые мысли: `{bg_status}`",
        "bg_active_label": "АКТИВНЫ",
        "bg_sleeping_label": "СПЯТ",
        "btn_settings": "⚙️ Настройки",
        "settings_title": "⚙️ **Настройки Telegram**\nНастройте панель Telegram и отображение сообщений:",
        "btn_silent_on": "🔕 Тихий режим: ВКЛ",
        "btn_silent_off": "🔔 Тихий режим: ВЫКЛ",
        "silent_toggled_on": "🔕 Тихий режим включён — новые мысли будут заменять предыдущее сообщение",
        "silent_toggled_off": "🔔 Тихий режим выключен — каждая мысль становится отдельным сообщением",
    }
}
