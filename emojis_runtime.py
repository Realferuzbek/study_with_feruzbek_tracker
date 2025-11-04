import asyncio
import copy
import hashlib
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from telethon import functions, types

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

    _SCHEMA_VERSION = 2
    _cache_path = Path(__file__).with_name("premium_emoji_cache.json")
    _lock = asyncio.Lock()
    _placeholder = "■"
    _key_prefix_chars = tuple("•*-—–▪·")
    _key_pattern = re.compile(r"^\s*([A-Za-z0-9_]+)")
    _cache_data: Dict[str, Any] = {
        "schema_version": _SCHEMA_VERSION,
        "updated_at": None,
        "fingerprint": None,
        "pinned_message_id": None,
        "items": {},
    }
    _cache_loaded: bool = False
    _cache_stale: bool = True
    _last_refresh_ts: float = 0.0
    _last_refresh_success: bool = True

    @classmethod
    def _default_cache(cls) -> Dict[str, Any]:
        return {
            "schema_version": cls._SCHEMA_VERSION,
            "updated_at": None,
            "fingerprint": None,
            "pinned_message_id": None,
            "items": {},
        }

    @classmethod
    def _load_cache(cls) -> None:
        if cls._cache_loaded:
            return
        try:
            data = json.loads(cls._cache_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("cache payload not a dict")
            schema_version = data.get("schema_version")
            if schema_version == cls._SCHEMA_VERSION:
                items = cls._parse_v4_entries(data.get("items"))
                cls._cache_data = {
                    "schema_version": cls._SCHEMA_VERSION,
                    "updated_at": data.get("updated_at"),
                    "fingerprint": data.get("fingerprint"),
                    "pinned_message_id": data.get("pinned_message_id"),
                    "items": items,
                }
                cls._cache_stale = False
            else:
                migrated_items = cls._migrate_legacy_entries(data)
                cls._cache_data = {
                    "schema_version": cls._SCHEMA_VERSION,
                    "updated_at": data.get("updated_at"),
                    "fingerprint": data.get("fingerprint"),
                    "pinned_message_id": data.get("pinned_message_id"),
                    "items": migrated_items,
                }
                cls._cache_stale = True
        except FileNotFoundError:
            cls._cache_data = cls._default_cache()
            cls._cache_stale = True
        except (ValueError, TypeError):
            cls._cache_data = cls._default_cache()
            cls._cache_stale = True
        cls._cache_loaded = True

    @classmethod
    def _save_cache(cls) -> None:
        if not cls._cache_loaded:
            cls._load_cache()
        entries_payload: Dict[str, Dict[str, Any]] = {}
        raw_entries = cls._cache_data.get("items", {})
        if isinstance(raw_entries, dict):
            for key, value in raw_entries.items():
                if key not in NORMAL_SET or not isinstance(value, dict):
                    continue
                entry_payload: Dict[str, Any] = {}
                premium_id = value.get("premium_id")
                if premium_id is not None:
                    try:
                        entry_payload["premium_id"] = int(premium_id)
                    except (TypeError, ValueError):
                        pass
                unicode_val = value.get("unicode")
                if isinstance(unicode_val, str) and unicode_val:
                    entry_payload["unicode"] = unicode_val
                if entry_payload:
                    entries_payload[key] = entry_payload
        payload = {
            "schema_version": cls._SCHEMA_VERSION,
            "updated_at": cls._cache_data.get("updated_at"),
            "fingerprint": cls._cache_data.get("fingerprint"),
            "pinned_message_id": cls._cache_data.get("pinned_message_id"),
            "items": entries_payload,
        }
        try:
            cls._atomic_write(payload)
        except Exception:
            # Failing to persist the cache should not break runtime behaviour.
            pass

    @classmethod
    def _atomic_write(cls, payload: Dict[str, Any]) -> None:
        tmp_path = cls._cache_path.with_suffix(".tmp")
        cls._cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.write_text(json.dumps(payload), encoding="utf-8")
        tmp_path.replace(cls._cache_path)

    @classmethod
    def _parse_v4_entries(cls, raw_entries: Any) -> Dict[str, Dict[str, Any]]:
        entries: Dict[str, Dict[str, Any]] = {}
        if not isinstance(raw_entries, dict):
            return entries
        for key, value in raw_entries.items():
            if key not in NORMAL_SET or not isinstance(value, dict):
                continue
            record: Dict[str, Any] = {}
            if "premium_id" in value:
                try:
                    record["premium_id"] = int(value["premium_id"])
                except (TypeError, ValueError):
                    pass
            unicode_val = value.get("unicode")
            if isinstance(unicode_val, str) and unicode_val:
                record["unicode"] = unicode_val
            if record:
                entries[key] = record
        return entries

    @classmethod
    def _migrate_legacy_entries(cls, data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        if not isinstance(data, dict):
            return {}
        entries: Dict[str, Dict[str, Any]] = {}
        raw_emojis = data.get("emojis", {})
        if isinstance(raw_emojis, dict):
            for key, value in raw_emojis.items():
                if key not in NORMAL_SET:
                    continue
                try:
                    premium_id = int(value)
                except (TypeError, ValueError):
                    continue
                entries[key] = {"premium_id": premium_id}
        raw_unicode = data.get("unicode_overrides")
        if not isinstance(raw_unicode, dict):
            raw_unicode = data.get("unicode")
        if isinstance(raw_unicode, dict):
            for key, value in raw_unicode.items():
                if key not in NORMAL_SET or not isinstance(value, str) or not value:
                    continue
                record = entries.get(key, {})
                record["unicode"] = value
                entries[key] = record
        return entries

    @classmethod
    def _normalise_key(cls, candidate: str) -> Optional[str]:
        if candidate is None:
            return None
        key = candidate.strip()
        if not key:
            return None
        key = key.upper()
        return key if key in NORMAL_SET else None

    @classmethod
    def _key_from_line(cls, line: str) -> Optional[str]:
        if not line:
            return None
        stripped = line.strip()
        while stripped and stripped[0] in cls._key_prefix_chars:
            stripped = stripped[1:].lstrip()
        for sep in (":", "-", "—", "–", "|"):
            if sep in stripped:
                candidate = stripped.split(sep, 1)[0].strip()
                key = cls._normalise_key(candidate)
                if key:
                    return key
        match = cls._key_pattern.match(stripped)
        if match:
            return cls._normalise_key(match.group(1))
        return None

    @classmethod
    def _extract_unicode_overrides(cls, text: str) -> Dict[str, str]:
        overrides: Dict[str, str] = {}
        if not text:
            return overrides
        for raw_line in text.splitlines():
            key = cls._key_from_line(raw_line)
            if not key:
                continue
            stripped = raw_line.strip()
            while stripped and stripped[0] in cls._key_prefix_chars:
                stripped = stripped[1:].lstrip()
            remainder = stripped
            for sep in (":", "-", "—", "–", "|"):
                if sep in remainder:
                    remainder = remainder.split(sep, 1)[1].strip()
                    break
            else:
                continue
            if not remainder:
                continue
            token = remainder.split()[0]
            token = token.rstrip(",.;:!?)]}\"'")
            if token:
                overrides[key] = token
        return overrides

    @classmethod
    def _extract_entries_from_pinned(cls, pinned: types.Message) -> Tuple[Dict[str, int], Dict[str, str]]:
        text = (getattr(pinned, "raw_text", "") or "").replace("\r\n", "\n")
        unicode_overrides = cls._extract_unicode_overrides(text)
        if not text:
            return {}, unicode_overrides

        entities = getattr(pinned, "entities", []) or []
        custom_entities = [
            ent
            for ent in entities
            if isinstance(ent, types.MessageEntityCustomEmoji)
        ]
        if not custom_entities:
            return {}, unicode_overrides

        new_entries: Dict[str, int] = {}
        for ent in sorted(custom_entities, key=lambda e: (e.offset, e.length, getattr(e, "document_id", 0))):
            line_start = text.rfind("\n", 0, ent.offset)
            if line_start == -1:
                line_start = 0
            else:
                line_start += 1
            line_end = text.find("\n", ent.offset)
            if line_end == -1:
                line_end = len(text)
            line = text[line_start:line_end]
            key = cls._key_from_line(line)
            if not key:
                continue
            doc_id = getattr(ent, "document_id", None)
            if doc_id is None:
                continue
            try:
                new_entries[key] = int(doc_id)
            except (TypeError, ValueError):
                continue
        return new_entries, unicode_overrides

    @classmethod
    async def _validate_document_ids(cls, client, entries: Dict[str, int]) -> Dict[str, int]:
        if not entries:
            return {}

        deduped_ids: List[int] = []
        seen: set[int] = set()
        for doc_id in entries.values():
            if doc_id not in seen:
                seen.add(doc_id)
                deduped_ids.append(doc_id)

        valid_ids: set[int] = set()
        try:
            for idx in range(0, len(deduped_ids), 100):
                chunk = deduped_ids[idx : idx + 100]
                docs = await client(functions.messages.GetCustomEmojiDocumentsRequest(document_id=chunk))
                docs_iter = getattr(docs, "documents", docs)
                if docs_iter is None:
                    continue
                for doc in docs_iter:
                    doc_id = getattr(doc, "id", None)
                    if doc_id is not None:
                        valid_ids.add(int(doc_id))
        except Exception:
            # If validation fails (network/API issue), keep the original entries
            return dict(entries)

        if not valid_ids:
            return {}
        return {key: value for key, value in entries.items() if value in valid_ids}

    @classmethod
    def _combine_items(cls, premium_map: Dict[str, int], unicode_map: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
        items: Dict[str, Dict[str, Any]] = {}
        for key, doc_id in premium_map.items():
            if key not in NORMAL_SET:
                continue
            try:
                items.setdefault(key, {})["premium_id"] = int(doc_id)
            except (TypeError, ValueError):
                continue
        for key, char in unicode_map.items():
            if key not in NORMAL_SET or not isinstance(char, str) or not char:
                continue
            record = items.setdefault(key, {})
            record["unicode"] = char
        return items

    @classmethod
    def is_ready(cls) -> bool:
        cls._load_cache()
        return True


    @classmethod
    async def hydrate(cls, client, pinned: Optional[types.Message] = None) -> None:
        """
        Deprecated shim retained for backward compatibility. Use refresh_if_needed.
        """
        await cls.refresh_if_needed(client, force=True)

    @classmethod
    async def refresh_if_needed(cls, client, *, force: bool = False) -> bool:
        cls._load_cache()
        async with cls._lock:
            pinned = await cls._fetch_pinned_message(client)
            if pinned is None:
                cls._last_refresh_success = False
                return False

            fingerprint = cls._compute_fingerprint(pinned)
            if not fingerprint:
                cls._last_refresh_success = False
                return False

            if not force and not cls._cache_stale and fingerprint == cls._cache_data.get("fingerprint"):
                cls._cache_data["pinned_message_id"] = getattr(pinned, "id", None)
                cls._cache_stale = False
                cls._last_refresh_success = True
                cls._last_refresh_ts = time.time()
                return False

            try:
                premium_map, unicode_map = cls._extract_entries_from_pinned(pinned)
                validated_premium = await cls._validate_document_ids(client, premium_map)
                items = cls._combine_items(validated_premium, unicode_map)
            except Exception:
                cls._last_refresh_success = False
                return False

            cls._cache_data = {
                "schema_version": cls._SCHEMA_VERSION,
                "updated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                "fingerprint": fingerprint,
                "pinned_message_id": getattr(pinned, "id", None),
                "items": items,
            }
            cls._cache_stale = False
            cls._last_refresh_ts = time.time()
            cls._last_refresh_success = True
            cls._save_cache()
            return True

    @classmethod
    async def refresh_if_changed(cls, client) -> Optional[types.Message]:
        """
        Deprecated shim; retained for callers expecting the old return signature.
        """
        changed = await cls.refresh_if_needed(client)
        if not changed:
            cls._load_cache()
        return await cls._fetch_pinned_message(client)

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
    def _compute_fingerprint(cls, message: types.Message) -> Optional[str]:
        if message is None:
            return None
        text = (getattr(message, "raw_text", "") or "").replace("\\r\\n", "\\n")
        message_id = getattr(message, "id", None) or 0
        entities = getattr(message, "entities", []) or []
        ordered_ids: List[str] = []
        for ent in entities:
            if isinstance(ent, types.MessageEntityCustomEmoji):
                try:
                    ordered_ids.append(str(int(getattr(ent, "document_id", 0))))
                except (TypeError, ValueError):
                    ordered_ids.append("0")
        payload = f"{message_id}\\n{text}\\n{','.join(ordered_ids)}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


    @classmethod
    def emoji_for_key(cls, key: str) -> Tuple[str, Optional[int], str]:
        """
        Resolve the glyph/document ID for a token.
        Returns (glyph, document_id, source).
        Source is one of: PREMIUM_ID, PINNED_UNICODE, NORMAL_SET, UNKNOWN.
        """
        cls._load_cache()
        entries = cls._cache_data.get("items", {})
        record = entries.get(key) if isinstance(entries, dict) else None

        if isinstance(record, dict):
            premium_id = record.get("premium_id")
            if premium_id is not None:
                try:
                    document_id = int(premium_id)
                except (TypeError, ValueError):
                    document_id = None
                else:
                    return cls._placeholder, document_id, "PREMIUM_ID"
            unicode_char = record.get("unicode")
            if isinstance(unicode_char, str) and unicode_char:
                return unicode_char, None, "PINNED_UNICODE"

        fallback = NORMAL_SET.get(key, "")
        source = "NORMAL_SET" if fallback else "UNKNOWN"
        return fallback, None, source

    @classmethod
    def render_with_sources(
        cls, text_with_tokens: str
    ) -> Tuple[str, List[types.TypeMessageEntity], List[int], List[Dict[str, Any]]]:
        """
        Replace {KEY} tokens with placeholder glyphs and build custom emoji entities.
        """
        cls._load_cache()
        rendered_parts: List[str] = []
        entities: List[types.TypeMessageEntity] = []
        final_lengths: List[int] = []
        token_metadata: List[Dict[str, Any]] = []
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
                glyph, document_id, source = cls.emoji_for_key(key)
                if document_id is not None:
                    glyph_to_use = glyph or cls._placeholder
                    rendered_parts.append(glyph_to_use)
                    entities.append(
                        types.MessageEntityCustomEmoji(
                            offset=current_len, length=len(glyph_to_use) or 1, document_id=int(document_id)
                        )
                    )
                    glyph_len = len(glyph_to_use) or 1
                    current_len += glyph_len
                    final_lengths.append(glyph_len)
                    token_metadata.append(
                        {
                            "key": key,
                            "source": source,
                            "glyph": glyph_to_use,
                            "document_id": int(document_id),
                        }
                    )
                else:
                    fallback = glyph
                    rendered_parts.append(fallback)
                    length = len(fallback)
                    current_len += length
                    final_lengths.append(length)
                    token_metadata.append(
                        {
                            "key": key,
                            "source": source,
                            "glyph": fallback,
                            "document_id": None,
                        }
                    )
                i = end + 1
            else:
                rendered_parts.append(ch)
                current_len += 1
                i += 1

        return "".join(rendered_parts), entities, final_lengths, token_metadata

    @classmethod
    def render(cls, text_with_tokens: str) -> Tuple[str, List[types.TypeMessageEntity], List[int]]:
        rendered, entities, final_lengths, _ = cls.render_with_sources(text_with_tokens)
        return rendered, entities, final_lengths

    @classmethod
    def per_key_sources(cls) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for key in NORMAL_SET.keys():
            _, _, source = cls.emoji_for_key(key)
            result[key] = source
        return result

    @classmethod
    def resolution_breakdown(cls) -> Dict[str, List[str]]:
        sources = cls.per_key_sources()
        breakdown: Dict[str, List[str]] = {
            "MAPPED_PREMIUM": [],
            "PINNED_UNICODE": [],
            "FALLING_BACK": [],
        }
        for key in sorted(NORMAL_SET.keys()):
            src = sources.get(key, "NORMAL_SET")
            if src == "PREMIUM_ID":
                breakdown["MAPPED_PREMIUM"].append(key)
            elif src == "PINNED_UNICODE":
                breakdown["PINNED_UNICODE"].append(key)
            else:
                breakdown["FALLING_BACK"].append(key)
        return breakdown

    @classmethod
    def resolution_mode(cls) -> str:
        breakdown = cls.resolution_breakdown()
        total = len(NORMAL_SET)
        premium_count = len(breakdown["MAPPED_PREMIUM"])
        unicode_count = len(breakdown["PINNED_UNICODE"])
        fallback_count = len(breakdown["FALLING_BACK"])
        if premium_count == total and unicode_count == 0 and fallback_count == 0:
            return "full-premium"
        if premium_count == 0 and unicode_count == 0:
            return "normal-only"
        return "mixed"

    @classmethod
    def counts(cls) -> Dict[str, int]:
        breakdown = cls.resolution_breakdown()
        return {
            "mapped_premium": len(breakdown["MAPPED_PREMIUM"]),
            "pinned_unicode": len(breakdown["PINNED_UNICODE"]),
            "normal_fallback": len(breakdown["FALLING_BACK"]),
        }

    @classmethod
    def missing_keys(cls) -> List[str]:
        return cls.resolution_breakdown().get("FALLING_BACK", [])

    @classmethod
    def current_fingerprint(cls) -> Optional[str]:
        cls._load_cache()
        return cls._cache_data.get("fingerprint")

    @classmethod
    def fingerprint_short(cls) -> str:
        fp = cls.current_fingerprint()
        if not fp:
            return "none"
        return fp[:10]

    @classmethod
    def last_updated(cls) -> Optional[str]:
        cls._load_cache()
        return cls._cache_data.get("updated_at")

    @classmethod
    def last_refresh_success(cls) -> bool:
        return cls._last_refresh_success


def _self_check_mixed_render() -> None:
    """
    Quick self-check to verify mixed premium and unicode fallbacks render per-key.
    """
    resolver = PremiumEmojiResolver
    snapshot_loaded = resolver._cache_loaded
    snapshot_cache = copy.deepcopy(resolver._cache_data)
    snapshot_stale = resolver._cache_stale
    try:
        resolver._cache_loaded = True
        resolver._cache_stale = False
        resolver._cache_data = {
            "schema_version": resolver._SCHEMA_VERSION,
            "updated_at": "self-check",
            "fingerprint": "self-check",
            "pinned_message_id": 1,
            "items": {
                "KEYCAP_1": {"premium_id": 1234567890123456789},
                "THREAD": {"unicode": "🧶"},
            },
        }
        rendered, entities, lengths = resolver.render("{KEYCAP_1} {THREAD} {KEYCAP_10}")
        premium_docs = [getattr(ent, "document_id", None) for ent in entities]
        assert resolver._placeholder in rendered, "Premium placeholder missing"
        assert "🧶" in rendered, "Unicode override not applied"
        assert NORMAL_SET["KEYCAP_10"] in rendered, "NORMAL_SET fallback missing"
        print("self_check_ok", {"rendered": rendered, "premium_ids": premium_docs, "lengths": lengths})
    finally:
        resolver._cache_loaded = snapshot_loaded
        resolver._cache_data = snapshot_cache
        resolver._cache_stale = snapshot_stale


async def has_premium(client) -> bool:
    """
    Check whether the current sender account has Telegram Premium.
    """
    me = await client.get_me()
    return bool(getattr(me, "premium", False))
