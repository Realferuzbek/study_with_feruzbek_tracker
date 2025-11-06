import asyncio
import copy
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from telethon import functions, types

logger = logging.getLogger(__name__)

# These keys map 1:1 with the Unicode glyphs currently sent by the tracker.
NORMAL_SET: Dict[str, str] = {
    "AIRPLANE": "✈️",
    "ALEMBIC": "⚗️",
    "ANCHOR": "⚓",
    "ARTIST_PALETTE": "🎨",
    "BAR_CHART": "📊",
    "BEATING_HEART": "💓",
    "BOOKS": "📚",
    "BOW_AND_ARROW": "🏹",
    "BRAIN": "🧠",
    "BRICK": "🧱",
    "BURST": "💥",
    "BULLET": "",
    "CALENDAR": "📅",
    "CASTLE": "🏰",
    "CHAINS": "⛓️",
    "CHART_UP": "📈",
    "CHEQUERED_FLAG": "🏁",
    "CHECK_MARK": "✅",
    "CHERRY_BLOSSOM": "🌸",
    "CLIMBER": "🧗",
    "COLLISION": "💥",
    "COMET": "☄️",
    "COMPASS": "🧭",
    "CONSTRUCTION_SIGN": "🚧",
    "CONTROL_KNOBS": "🎛️",
    "COWBOY_FACE": "🤠",
    "CROSSED_SWORDS": "⚔️",
    "CROWN": "👑",
    "DAGGER": "🗡️",
    "DANCER": "💃",
    "DIRECT_HIT": "🎯",
    "DIZZY": "💫",
    "DOLPHIN": "🐬",
    "DOOR": "🚪",
    "DOVE": "🕊️",
    "DRUM": "🥁",
    "EAGLE": "🦅",
    "EM_DASH": " \u2014 ",
    "EVERGREEN_TREE": "🌲",
    "FIRE": "🔥",
    "FLEXED_BICEPS": "💪",
    "GEAR": "⚙️",
    "GEM_STONE": "💎",
    "GLOWING_STAR": "🌟",
    "HAMMER": "🔨",
    "HAMMER_AND_WRENCH": "🛠️",
    "HEART_ON_FIRE": "❤️‍🔥",
    "HERB": "🌿",
    "HIGH_VOLTAGE": "⚡",
    "HINDU_TEMPLE": "🛕",
    "HORSE": "🐎",
    "HOURGLASS": "⏳",
    "KEYCAP_1": "1️⃣",
    "KEYCAP_10": "🔟",
    "KEYCAP_2": "2️⃣",
    "KEYCAP_3": "3️⃣",
    "KEYCAP_4": "4️⃣",
    "KEYCAP_5": "5️⃣",
    "KEYCAP_6": "6️⃣",
    "KEYCAP_7": "7️⃣",
    "KEYCAP_8": "8️⃣",
    "KEYCAP_9": "9️⃣",
    "LION": "🦁",
    "LOUDSPEAKER": "📢",
    "LOTUS_POSITION": "🧘",
    "MAGNET": "🧲",
    "MAN_RUNNING": "🏃‍♂️",
    "MANTEL_CLOCK": "🕰️",
    "MEDAL_1": "🥇",
    "MEDAL_2": "🥈",
    "MEDAL_3": "🥉",
    "MECHANICAL_ARM": "🦾",
    "MOAI": "🗿",
    "MOON": "🌙",
    "MUSICAL_NOTES": "🎶",
    "MUSICAL_SCORE": "🎼",
    "NAZAR": "🧿",
    "NINJA": "🥷",
    "NUT_AND_BOLT": "🔩",
    "OPEN_BOOK": "📖",
    "QUOTE_L": "\u201C",
    "QUOTE_R": "\u201D",
    "RACING_CAR": "🏎️",
    "RANGE_SEP": " - ",
    "REPEAT": "🔁",
    "ROCKET": "🚀",
    "ROCK": "🪨",
    "RUNNER": "🏃",
    "SURFER": "🏄",
    "SAILBOAT": "⛵",
    "SALUTING_FACE": "🫡",
    "SATELLITE": "🛰️",
    "SAW": "🪚",
    "SCREWDRIVER": "🪛",
    "SEWING_NEEDLE": "🪡",
    "SHIELD": "🛡️",
    "SHUSHING_FACE": "🤫",
    "SLEEPING_FACE": "😴",
    "SNOW_CAPPED_MOUNTAIN": "🏔️",
    "SPARKLES": "✨",
    "SPIRAL_CALENDAR": "🗓️",
    "STAR": "⭐",
    "SUNRISE": "🌅",
    "SUN_WITH_FACE": "🌞",
    "TARGET": "🎯",
    "TEAR_OFF_CALENDAR": "📆",
    "THREAD": "🧵",
    "THUNDER_CLOUD": "⛈️",
    "TOKYO_TOWER": "🗼",
    "TOOLBOX": "🧰",
    "TROPHY": "🏆",
    "VIOLIN": "🎻",
    "WATER_WAVE": "🌊",
    "WEIGHTLIFTER": "🏋️",
    "WIND_FACE": "🌬️",
    "WING": "🪽",
    "WOOD": "🪵",
    "WORLD_MAP": "🗺️",
    "WOTD_MARK": "🌟",
    "WRENCH": "🔧",
}


class PremiumEmojiResolver:
    """
    Resolves custom emoji document IDs for premium users by reading the pinned
    message in Saved Messages. Results are cached on disk.
    """
    _ALLOWED_POLICIES = {"pinned_strict", "pinned_prefer", "code_only"}
    _DEFAULT_POLICY = "pinned_prefer"  # use pinned set when available; fallback to Unicode for missing keys
    _SCHEMA_VERSION = 3
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
        "duplicates": {},
    }
    _cache_loaded: bool = False
    _cache_stale: bool = True
    _last_refresh_ts: float = 0.0
    _last_refresh_success: bool = True
    _policy: str = _DEFAULT_POLICY
    _policy_source: str = "default"  # default | env | runtime | auto
    _premium_detected: Optional[bool] = None

    @classmethod
    def _default_cache(cls) -> Dict[str, Any]:
        return {
            "schema_version": cls._SCHEMA_VERSION,
            "updated_at": None,
            "fingerprint": None,
            "pinned_message_id": None,
            "items": {},
            "duplicates": {},
        }

    @classmethod
    def _normalize_policy(cls, policy: str) -> str:
        normalized = (policy or "").strip().lower()
        if normalized not in cls._ALLOWED_POLICIES:
            raise ValueError(f"Invalid emoji policy: {policy}")
        return normalized

    @classmethod
    def set_policy(cls, policy: str, source: str = "runtime") -> str:
        normalized = cls._normalize_policy(policy)
        previous = cls._policy
        if normalized != previous or cls._policy_source != source:
            cls._policy = normalized
            cls._policy_source = source
            logger.info("emoji_policy_set policy=%s source=%s previous=%s", normalized, source, previous)
        return previous

    @classmethod
    def current_policy(cls) -> str:
        return cls._policy

    @classmethod
    def policy_source(cls) -> str:
        return cls._policy_source

    @classmethod
    def premium_status(cls) -> Optional[bool]:
        return cls._premium_detected

    @classmethod
    def register_premium_status(cls, premium: Optional[bool]) -> None:
        detected = None if premium is None else bool(premium)
        previous = cls._premium_detected
        cls._premium_detected = detected
        if detected != previous:
            logger.info(
                "emoji_premium_status premium=%s previous=%s policy=%s source=%s",
                detected,
                previous,
                cls._policy,
                cls._policy_source,
            )
        if cls._policy_source in ("default", "auto"):
            if detected is True:
                desired = "pinned_strict"
            elif detected is False:
                desired = "code_only"
            else:
                desired = cls._DEFAULT_POLICY
            if desired != cls._policy:
                cls._policy = desired
                cls._policy_source = "auto"
                logger.info(
                    "emoji_policy_auto policy=%s premium=%s",
                    desired,
                    detected,
                )

    @classmethod
    def _apply_env_policy(cls) -> None:
        env_policy = os.environ.get("EMOJI_POLICY")
        if not env_policy:
            return
        try:
            cls.set_policy(env_policy, source="env")
        except ValueError:
            logger.warning("Ignoring invalid EMOJI_POLICY value: %s", env_policy)

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
                items = cls._parse_schema_entries(data.get("items"))
                cls._cache_data = {
                    "schema_version": cls._SCHEMA_VERSION,
                    "updated_at": data.get("updated_at"),
                    "fingerprint": data.get("fingerprint"),
                    "pinned_message_id": data.get("pinned_message_id"),
                    "items": items,
                    "duplicates": cls._parse_duplicates(data.get("duplicates")),
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
                    "duplicates": {},
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
                        entry_payload["premium_id"] = str(int(premium_id))
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
            "duplicates": cls._cache_data.get("duplicates") or {},
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
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(cls._cache_path)

    @classmethod
    def _parse_schema_entries(cls, raw_entries: Any) -> Dict[str, Dict[str, Any]]:
        entries: Dict[str, Dict[str, Any]] = {}
        if not isinstance(raw_entries, dict):
            return entries
        for key, value in raw_entries.items():
            if key not in NORMAL_SET or not isinstance(value, dict):
                continue
            record: Dict[str, Any] = {}
            premium_val = value.get("premium_id")
            if premium_val is not None:
                try:
                    record["premium_id"] = int(premium_val)
                except (TypeError, ValueError):
                    pass
            unicode_val = value.get("unicode")
            if isinstance(unicode_val, str) and unicode_val:
                record["unicode"] = unicode_val
            if record:
                entries[key] = record
        return entries

    @staticmethod
    def _parse_duplicates(raw_duplicates: Any) -> Dict[str, int]:
        if not isinstance(raw_duplicates, dict):
            return {}
        parsed: Dict[str, int] = {}
        for key, count in raw_duplicates.items():
            if key in NORMAL_SET:
                try:
                    parsed[key] = int(count)
                except (TypeError, ValueError):
                    continue
        return parsed

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
    def _split_line_for_entry(cls, line: str) -> Tuple[Optional[str], Optional[int]]:
        if not line:
            return None, None
        length = len(line)
        cursor = 0
        while cursor < length and line[cursor].isspace():
            cursor += 1
        while cursor < length and line[cursor] in cls._key_prefix_chars:
            cursor += 1
        while cursor < length and line[cursor].isspace():
            cursor += 1
        for sep in (":", "-", "—", "–", "|"):
            sep_idx = line.find(sep, cursor)
            if sep_idx == -1:
                continue
            candidate = line[cursor:sep_idx].strip()
            key = cls._normalise_key(candidate)
            if not key:
                continue
            value_start = sep_idx + 1
            while value_start < length and line[value_start].isspace():
                value_start += 1
            return key, value_start
        stripped = line.strip()
        match = cls._key_pattern.match(stripped)
        if match:
            key = cls._normalise_key(match.group(1))
            if key:
                return key, len(line)
        return None, None

    @staticmethod
    def _first_token(text: str) -> Optional[str]:
        if not text:
            return None
        token = text.split()[0]
        token = token.strip(",.;:!?)]}\"'")
        return token or None

    @classmethod
    def _extract_entries_from_pinned(cls, pinned: types.Message) -> Tuple[Dict[str, int], Dict[str, str], Dict[str, int]]:
        text = (getattr(pinned, "raw_text", "") or "").replace("\r\n", "\n")
        premium_map: Dict[str, int] = {}
        unicode_map: Dict[str, str] = {}
        duplicates: Dict[str, int] = {}
        occurrences: Dict[str, int] = {}
        if not text:
            return premium_map, unicode_map, duplicates
        entities = getattr(pinned, "entities", []) or []
        custom_entities = [
            ent
            for ent in entities
            if isinstance(ent, types.MessageEntityCustomEmoji)
        ]
        sorted_entities = sorted(
            custom_entities,
            key=lambda e: (getattr(e, "offset", 0), getattr(e, "length", 0), getattr(e, "document_id", 0)),
        )

        offset = 0
        for raw_line in text.splitlines(keepends=True):
            line = raw_line.rstrip("\r\n")
            key, value_start = cls._split_line_for_entry(line)
            line_length = len(line)
            line_start_offset = offset
            line_end_offset = line_start_offset + line_length

            if key and value_start is not None and value_start <= line_length:
                count = occurrences.get(key, 0) + 1
                occurrences[key] = count
                if count > 1:
                    duplicates[key] = count
                value_offset = line_start_offset + value_start
                # Select the last custom emoji entity that sits within the value span for this line.
                matching_entities = [
                    ent
                    for ent in sorted_entities
                    if getattr(ent, "offset", 0) >= value_offset and getattr(ent, "offset", 0) < line_end_offset
                ]
                selected_entity = matching_entities[-1] if matching_entities else None
                if selected_entity is not None:
                    doc_id = getattr(selected_entity, "document_id", None)
                    try:
                        premium_map[key] = int(doc_id)
                        unicode_map.pop(key, None)
                        offset += len(raw_line)
                        continue
                    except (TypeError, ValueError):
                        pass
                value_text = line[value_start:].strip()
                token = cls._first_token(value_text)
                if token:
                    # Respect pinned Unicode verbatim (e.g. allow KEYCAP_10 -> 0️⃣ without auto-fixing).
                    unicode_map[key] = token
                    premium_map.pop(key, None)
            offset += len(raw_line)

        return premium_map, unicode_map, duplicates

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
            if cls.current_policy() == "code_only":
                logger.info("emoji_refresh_skipped policy=code_only force=%s", force)
                cls._last_refresh_success = True
                cls._cache_stale = False
                return False
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
                premium_map, unicode_map, duplicates = cls._extract_entries_from_pinned(pinned)
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
                "duplicates": duplicates,
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
        Resolve the glyph/document ID for a token according to the active policy.
        Returns (glyph, document_id, source).
        Source is one of: PREMIUM_ID, PINNED_UNICODE, NORMAL_SET, UNKNOWN.
        """
        policy = cls.current_policy()
        if policy == "code_only":
            fallback = NORMAL_SET.get(key, "")
            source = "NORMAL_SET" if key in NORMAL_SET else "UNKNOWN"
            return fallback, None, source

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
        source = "NORMAL_SET" if key in NORMAL_SET else "UNKNOWN"
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
    def selfcheck_medals(cls) -> Dict[str, Any]:
        """
        Acceptance self-check ensuring pinned premium/unicode medals win per-key.
        Returns a summary for pinned_prefer and pinned_strict policies.
        """
        snapshot_policy = cls._policy
        snapshot_policy_source = cls._policy_source
        snapshot_loaded = cls._cache_loaded
        snapshot_cache = copy.deepcopy(cls._cache_data)
        snapshot_stale = cls._cache_stale
        premium_doc_id = 987654321012345678
        unicode_char = "🏅"
        test_items = {
            "MEDAL_1": {"premium_id": premium_doc_id},
            "MEDAL_2": {"premium_id": premium_doc_id},
            "MEDAL_3": {"unicode": unicode_char},
        }

        def _collect(policy: str, text: str) -> Dict[str, Any]:
            cls._policy = policy
            cls._policy_source = "selfcheck"
            rendered, entities, _, metadata = cls.render_with_sources(text)
            tokens = [
                {
                    "key": entry.get("key"),
                    "source": entry.get("source"),
                    "document_id": entry.get("document_id"),
                    "glyph": entry.get("glyph"),
                }
                for entry in metadata
                if entry.get("key") in {"MEDAL_1", "MEDAL_2", "MEDAL_3"}
            ]
            entity_ids = [
                getattr(ent, "document_id", None)
                for ent in entities
                if isinstance(ent, types.MessageEntityCustomEmoji)
            ]
            return {
                "rendered": rendered,
                "tokens": tokens,
                "entity_ids": entity_ids,
            }

        try:
            cls._cache_loaded = True
            cls._cache_stale = False
            cls._cache_data = {
                "schema_version": cls._SCHEMA_VERSION,
                "updated_at": "selfcheck",
                "fingerprint": "selfcheck",
                "pinned_message_id": 1,
                "items": test_items,
                "duplicates": {},
            }
            two_text = "{MEDAL_1} First\n{MEDAL_2} Second"
            three_text = two_text + "\n{MEDAL_3} Third"
            results: Dict[str, Any] = {}
            for policy in ("pinned_prefer", "pinned_strict"):
                two = _collect(policy, two_text)
                three = _collect(policy, three_text)
                results[policy] = {
                    "two_entries": two,
                    "three_entries": three,
                }
            return results
        finally:
            cls._policy = snapshot_policy
            cls._policy_source = snapshot_policy_source
            cls._cache_loaded = snapshot_loaded
            cls._cache_stale = snapshot_stale
            cls._cache_data = snapshot_cache

    @classmethod
    def duplicate_keys(cls) -> Dict[str, int]:
        cls._load_cache()
        raw = cls._cache_data.get("duplicates")
        if isinstance(raw, dict):
            result: Dict[str, int] = {}
            for k, v in raw.items():
                if k not in NORMAL_SET:
                    continue
                try:
                    result[k] = int(v)
                except (TypeError, ValueError):
                    continue
            return result
        return {}

    @classmethod
    def known_keys(cls) -> List[str]:
        cls._load_cache()
        keys = set(NORMAL_SET.keys())
        items = cls._cache_data.get("items", {})
        if isinstance(items, dict):
            keys.update(items.keys())
        return sorted(keys)

    @classmethod
    def pinned_items(cls) -> Dict[str, Dict[str, Any]]:
        cls._load_cache()
        items = cls._cache_data.get("items", {})
        if isinstance(items, dict):
            return dict(items)
        return {}

    @classmethod
    def keys_missing_from_pinned(cls) -> List[str]:
        items = cls.pinned_items()
        missing: List[str] = []
        for key in NORMAL_SET.keys():
            record = items.get(key)
            if not isinstance(record, dict):
                missing.append(key)
                continue
            premium_val = record.get("premium_id")
            unicode_val = record.get("unicode")
            has_premium = False
            if premium_val is not None:
                try:
                    has_premium = int(premium_val) != 0
                except (TypeError, ValueError):
                    has_premium = False
            has_unicode = isinstance(unicode_val, str) and bool(unicode_val)
            if not has_premium and not has_unicode:
                missing.append(key)
        return sorted(missing)

    @classmethod
    def export_template(cls) -> Tuple[str, List[types.TypeMessageEntity]]:
        items = cls.pinned_items()
        keys = cls.known_keys()
        lines: List[str] = []
        entities: List[types.TypeMessageEntity] = []
        offset = 0
        total = len(keys)
        for idx, key in enumerate(keys):
            record = items.get(key, {})
            premium_id: Optional[int] = None
            unicode_val: Optional[str] = None
            if isinstance(record, dict):
                premium_val = record.get("premium_id")
                if premium_val is not None:
                    try:
                        premium_id = int(premium_val)
                    except (TypeError, ValueError):
                        premium_id = None
                unicode_candidate = record.get("unicode")
                if isinstance(unicode_candidate, str) and unicode_candidate:
                    unicode_val = unicode_candidate
            glyph: str
            if premium_id:
                glyph = cls._placeholder
            else:
                fallback = NORMAL_SET.get(key)
                if unicode_val is not None:
                    glyph = unicode_val
                elif fallback is not None:
                    glyph = fallback
                else:
                    glyph = "?"
            line = f"{key}: {glyph}"
            lines.append(line)
            if premium_id:
                entities.append(
                    types.MessageEntityCustomEmoji(
                        offset=offset + len(f"{key}: "),
                        length=len(glyph) or 1,
                        document_id=premium_id,
                    )
                )
            offset += len(line)
            if idx != total - 1:
                lines.append("\n")
                offset += 1
        text = "".join(lines)
        return text, entities

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


PremiumEmojiResolver._apply_env_policy()


def selfcheck_medals() -> Dict[str, Any]:
    return PremiumEmojiResolver.selfcheck_medals()


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
    premium = bool(getattr(me, "premium", False))
    try:
        PremiumEmojiResolver.register_premium_status(premium)
    except Exception:
        logger.exception("Failed to register premium status update")
    return premium
