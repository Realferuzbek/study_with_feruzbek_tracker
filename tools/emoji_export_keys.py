#!/usr/bin/env python3
"""
Print all known emoji keys with the best available glyph so the output can be
copied into the pinned Saved Messages note.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from emojis_runtime import PremiumEmojiResolver


def main() -> None:
    text, _ = PremiumEmojiResolver.export_template()
    if text:
        print(text)


if __name__ == "__main__":
    main()
