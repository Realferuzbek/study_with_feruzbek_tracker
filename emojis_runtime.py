import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from telethon import types

# These keys map 1:1 with the Unicode glyphs currently sent by the tracker.
NORMAL_SET: Dict[str, str] = {
    "BAR_CHART": "📊",
    "BURST": "💥",
    "HEART_ON_FIRE": "❤️‍🔥",
    "CROWN": "👑",
    "FIRE": "🔥",
    "HIGH_VOLTAGE": "⚡",
    "GLOWING_STAR": "🌟",
    "CHEQUERED_FLAG": "🏁",
    "TARGET": "🎯",
    "DIZZY": "💫",
    "BRAIN": "🧠",
    "LION": "🦁",
    "WING": "🪽",
    "THREAD": "🧵",
    "SHIELD": "🛡️",
    "MOON": "🌙",
    "ROCKET": "🚀",
    "SPARKLES": "✨",
    "GEM_STONE": "💎",
    "CALENDAR": "📅",
    "TEAR_OFF_CALENDAR": "📆",
    "SPIRAL_CALENDAR": "🗓️",
    "FLEXED_BICEPS": "💪",
    "CHECK_MARK": "✅",
    "SLEEPING_FACE": "😴",
    "MEDAL_1": "🥇",
    "MEDAL_2": "🥈",
    "MEDAL_3": "🥉",
    "KEYCAP_1": "1️⃣",
    "KEYCAP_2": "2️⃣",
    "KEYCAP_3": "3️⃣",
    "KEYCAP_4": "4️⃣",
    "KEYCAP_5": "5️⃣",
    "KEYCAP_6": "6️⃣",
    "KEYCAP_7": "7️⃣",
    "KEYCAP_8": "8️⃣",
    "KEYCAP_9": "9️⃣",
    "KEYCAP_10": "🔟",
}


class PremiumEmojiResolver:
    """
    Resolves custom emoji document IDs for premium users by reading the pinned
    message in Saved Messages. Results are cached on disk.
    """

    _cache_path = Path(__file__).with_name("premium_emoji_cache.json")
    _lock = asyncio.Lock()
    _placeholder = "■"
    _cache_data: Dict[str, Any] = {"emojis": {}, "pinned_id": None, "content_hash": None}
    _cache_loaded: bool = False

    @classmethod
    def _load_cache(cls) -> None:
        if cls._cache_loaded:
            return
        try:
            data = json.loads(cls._cache_path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "emojis" in data:
                emojis = {
                    k: int(v)
                    for k, v in dict(data.get("emojis", {})).items()
                    if k in NORMAL_SET
                }
                cls._cache_data = {
                    "emojis": emojis,
                    "pinned_id": data.get("pinned_id"),
                    "content_hash": data.get("content_hash"),
                }
            elif isinstance(data, dict):
                emojis = {k: int(v) for k, v in data.items() if k in NORMAL_SET}
                cls._cache_data = {
                    "emojis": emojis,
                    "pinned_id": None,
                    "content_hash": None,
                }
            else:
                cls._cache_data = {"emojis": {}, "pinned_id": None, "content_hash": None}
        except FileNotFoundError:
            cls._cache_data = {"emojis": {}, "pinned_id": None, "content_hash": None}
        except (ValueError, TypeError):
            cls._cache_data = {"emojis": {}, "pinned_id": None, "content_hash": None}
        cls._cache_loaded = True

    @classmethod
    def _save_cache(cls) -> None:
        if not cls._cache_loaded:
            cls._load_cache()
        payload = {
            "emojis": {
                key: int(value)
                for key, value in cls._cache_data.get("emojis", {}).items()
                if key in NORMAL_SET
            },
            "pinned_id": cls._cache_data.get("pinned_id"),
            "content_hash": cls._cache_data.get("content_hash"),
        }
        try:
            cls._cache_path.write_text(json.dumps(payload), encoding="utf-8")
        except Exception:
            # Failing to persist the cache should not break runtime behaviour.
            pass

    @classmethod
    def is_ready(cls) -> bool:
        cls._load_cache()
        emojis = cls._cache_data.get("emojis", {})
        if not emojis:
            return False
        return all(key in emojis for key in NORMAL_SET)

    @classmethod
    async def hydrate(cls, client, pinned: Optional[types.Message] = None) -> None:
        """
        Fetch the pinned Saved Messages note and cache the custom emoji IDs.
        """
        cls._load_cache()
        if pinned is None and cls.is_ready():
            return

        async with cls._lock:
            # Re-check after obtaining the lock.
            if pinned is None and cls.is_ready():
                return

            if pinned is None:
                pinned = await cls._fetch_pinned_message(client)
            if pinned is None:
                return

            if not pinned or not getattr(pinned, "raw_text", None):
                return

            text = pinned.raw_text
            entities = list(getattr(pinned, "entities", []) or [])
            if not entities:
                return

            custom = [ent for ent in entities if isinstance(ent, types.MessageEntityCustomEmoji)]
            if not custom:
                return

            # Map offsets to (entity, snippet) for quick lookup.
            custom_by_offset: Dict[int, Tuple[types.MessageEntityCustomEmoji, str]] = {}
            for entity, snippet in pinned.get_entities_text(types.MessageEntityCustomEmoji):
                custom_by_offset[entity.offset] = (entity, snippet)

            new_entries: Dict[str, int] = {}
            line_start = 0
            for line in text.splitlines():
                line_length = len(line)
                colon = line.find(":")
                if colon == -1:
                    line_start += line_length + 1
                    continue
                key = line[:colon].strip()
                if not key or key not in NORMAL_SET:
                    line_start += line_length + 1
                    continue

                search_start = line_start + colon + 1
                while (
                    search_start < line_start + line_length
                    and text[search_start].isspace()
                ):
                    search_start += 1

                chosen: Optional[types.MessageEntityCustomEmoji] = None
                for offset, (entity, _) in custom_by_offset.items():
                    if offset < search_start:
                        continue
                    if offset >= line_start + line_length:
                        continue
                    chosen = entity
                    break

                if chosen is not None:
                    new_entries[key] = int(chosen.document_id)
                line_start += line_length + 1
                continue

            pinned_hash = cls._hash_pinned(pinned)
            cls._cache_data["emojis"] = new_entries
            cls._cache_data["pinned_id"] = getattr(pinned, "id", None)
            cls._cache_data["content_hash"] = pinned_hash
            cls._save_cache()

    @classmethod
    async def refresh_if_changed(cls, client) -> Optional[types.Message]:
        cls._load_cache()
        pinned = await cls._fetch_pinned_message(client)
        if pinned is None:
            return None

        current_hash = cls._hash_pinned(pinned)
        cached_id = cls._cache_data.get("pinned_id")
        cached_hash = cls._cache_data.get("content_hash")

        if cached_id != getattr(pinned, "id", None) or cached_hash != current_hash:
            await cls.hydrate(client, pinned=pinned)
        else:
            if cached_id is None or cached_hash is None:
                cls._cache_data["pinned_id"] = getattr(pinned, "id", None)
                cls._cache_data["content_hash"] = current_hash
                cls._save_cache()
        return pinned

    @classmethod
    async def _fetch_pinned_message(cls, client) -> Optional[types.Message]:
        try:
            async for msg in client.iter_messages("me", limit=50):
                if getattr(msg, "pinned", False):
                    return msg
        except Exception:
            return None
        return None

    @classmethod
    def _hash_pinned(cls, message: types.Message) -> str:
        text = (getattr(message, "raw_text", "") or "").replace("\r\n", "\n")
        parts = [text]
        entities = getattr(message, "entities", []) or []
        custom_entities = [
            ent
            for ent in entities
            if isinstance(ent, types.MessageEntityCustomEmoji)
        ]
        for ent in sorted(custom_entities, key=lambda e: (e.offset, e.length, getattr(e, "document_id", 0))):
            parts.append(f"{ent.offset}:{ent.length}:{getattr(ent, 'document_id', 0)}")
        payload = "\n".join(parts)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @classmethod
    def _emoji_cache(cls) -> Dict[str, int]:
        cls._load_cache()
        emojis = cls._cache_data.get("emojis", {})
        return {k: int(v) for k, v in emojis.items()}

    @classmethod
    def render(cls, text_with_tokens: str) -> Tuple[str, List[types.TypeMessageEntity]]:
        """
        Replace {KEY} tokens with placeholder glyphs and build custom emoji entities.
        """
        cache = cls._emoji_cache()
        rendered_parts: List[str] = []
        entities: List[types.TypeMessageEntity] = []
        i = 0
        current_len = 0

        while i < len(text_with_tokens):
            ch = text_with_tokens[i]
            if ch == "{":
                end = text_with_tokens.find("}", i + 1)
                if end == -1:
                    rendered_parts.append(ch)
                    current_len += 1
                    i += 1
                    continue
                key = text_with_tokens[i + 1 : end]
                token_value = cache.get(key)
                if token_value:
                    rendered_parts.append(cls._placeholder)
                    entities.append(
                        types.MessageEntityCustomEmoji(
                            offset=current_len, length=1, document_id=int(token_value)
                        )
                    )
                    current_len += 1
                else:
                    fallback = NORMAL_SET.get(key, "")
                    rendered_parts.append(fallback)
                    current_len += len(fallback)
                i = end + 1
            else:
                rendered_parts.append(ch)
                current_len += 1
                i += 1

        return "".join(rendered_parts), entities


async def has_premium(client) -> bool:
    """
    Check whether the current sender account has Telegram Premium.
    """
    me = await client.get_me()
    return bool(getattr(me, "premium", False))
