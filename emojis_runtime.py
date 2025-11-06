from __future__ import annotations

import re
from typing import Dict, Mapping, Optional

# Canonical emoji mapping used throughout the tracker.
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
    "BULLET": "",
    "BURST": "💥",
    "CALENDAR": "📅",
    "CASTLE": "🏰",
    "CHAINS": "⛓️",
    "CHART_UP": "📈",
    "CHECK_MARK": "✅",
    "CHEQUERED_FLAG": "🏁",
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
    "EM_DASH": " — ",
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
    "LOTUS_POSITION": "🧘",
    "LOUDSPEAKER": "📢",
    "MAGNET": "🧲",
    "MANTEL_CLOCK": "🕰️",
    "MAN_RUNNING": "🏃‍♂️",
    "MECHANICAL_ARM": "🦾",
    "MEDAL_1": "🥇",
    "MEDAL_2": "🥈",
    "MEDAL_3": "🥉",
    "MOAI": "🗿",
    "MOON": "🌙",
    "MUSICAL_NOTES": "🎶",
    "MUSICAL_SCORE": "🎼",
    "NAZAR": "🧿",
    "NINJA": "🥷",
    "NUT_AND_BOLT": "🔩",
    "OPEN_BOOK": "📖",
    "QUOTE_L": "“",
    "QUOTE_R": "”",
    "RACING_CAR": "🏎️",
    "RANGE_SEP": " - ",
    "REPEAT": "🔁",
    "ROCK": "🪨",
    "ROCKET": "🚀",
    "RUNNER": "🏃",
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
    "SURFER": "🏄",
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

_TOKEN_PATTERN = re.compile(r"{([A-Z0-9_]+)}")


def resolve_tokens(text: str, mapping: Mapping[str, Optional[str]] | None = None) -> str:
    """
    Replace {TOKEN} placeholders with emoji strings from the provided mapping.
    When a mapping value is None or the key is unknown the token is left intact.
    """

    if mapping is None:
        mapping = NORMAL_SET

    def _replacement(match: re.Match[str]) -> str:
        key = match.group(1)
        value = mapping.get(key)
        if value is None:
            return match.group(0)
        return value

    return _TOKEN_PATTERN.sub(_replacement, text)


__all__ = ["NORMAL_SET", "resolve_tokens"]
