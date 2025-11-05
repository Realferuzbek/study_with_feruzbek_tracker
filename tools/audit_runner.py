from __future__ import annotations

import contextlib
import hashlib
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from emojis_runtime import PremiumEmojiResolver, selfcheck_medals


TARGET_KEYS: Tuple[str, ...] = (
    *(f"KEYCAP_{n}" for n in range(1, 11)),
    "MEDAL_1",
    "MEDAL_2",
    "MEDAL_3",
    "FIRE",
    "BRAIN",
    "WING",
    "THREAD",
    "MOON",
    "TARGET",
    "CROWN",
    "FLEXED_BICEPS",
    "QUOTE_L",
    "QUOTE_R",
    "EM_DASH",
    "RANGE_SEP",
    "WOTD_MARK",
    "BULLET",
)

STATUS_EMOJI = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️"}

CHECK_LABELS = {
    "A": "Policy",
    "B": "Coverage",
    "C": "Sources",
    "D": "Layout",
    "E": "Send parity",
    "F": "DB rollups",
    "G": "Watchdog",
    "H": "Post log contract",
    "I": "Medals self-check",
}

TELEGRAM_SPLIT_THRESHOLD = 3800


@dataclass
class PreviewData:
    context: Dict[str, Any]
    html_text: str
    plain_text: str
    tokenized_text: str
    rendered_text: str
    tokens: Sequence[Any]
    markup_entities: Sequence[Any]
    emoji_entities: Sequence[Any]
    final_lengths: Sequence[int]
    metadata: Sequence[Dict[str, Any]]


@dataclass
class CheckResult:
    code: str
    status: str
    message: str
    detail: Optional[str] = None
    suggestion: Optional[str] = None

    @property
    def emoji(self) -> str:
        return STATUS_EMOJI.get(self.status, "❓")

    def summary_line(self) -> str:
        label = CHECK_LABELS.get(self.code, self.code)
        return f"{self.emoji} {label}: {self.message}"


@dataclass
class AuditReport:
    results: List[CheckResult]

    @property
    def has_fail(self) -> bool:
        return any(r.status == "FAIL" for r in self.results)

    def format_summary(self) -> str:
        lines: List[str] = ["=== Audit Summary ==="]
        for result in self.results:
            lines.append(result.summary_line())
            if result.suggestion and result.status == "FAIL":
                lines.append(f"Suggested fix: {result.suggestion}")
        detail_blocks: List[str] = []
        for result in self.results:
            if result.detail:
                label = CHECK_LABELS.get(result.code, result.code)
                detail_blocks.append(f"--- {label} Detail ---\n{result.detail}")
        if detail_blocks:
            lines.append("")
            lines.extend(detail_blocks)
        return "\n".join(lines).rstrip()


def _sum_duplicate_counts(duplicates: Dict[str, int]) -> int:
    total = 0
    for count in duplicates.values():
        if isinstance(count, int) and count > 1:
            total += count - 1
    return total


@contextlib.contextmanager
def _patch_attributes(pairs: Iterable[Tuple[Any, str, Any]]):
    originals: List[Tuple[Any, str, Any]] = []
    try:
        for obj, name, replacement in pairs:
            originals.append((obj, name, getattr(obj, name)))
            setattr(obj, name, replacement)
        yield
    finally:
        for obj, name, original in reversed(originals):
            setattr(obj, name, original)


class AuditRunner:
    def __init__(self, tracker_module):
        self.tracker = tracker_module
        self._preview_cache: Optional[PreviewData] = None
        self._stub_preview_cache: Optional[PreviewData] = None

    async def run(self, quick: bool) -> AuditReport:
        results: List[CheckResult] = []
        if quick:
            results.append(await self._check_policy())
            results.append(await self._check_coverage(counts_only=True))
            results.append(await self._check_layout(use_stub=True))
            results.append(await self._check_watchdog())
        else:
            results.append(await self._check_policy())
            results.append(await self._check_coverage(counts_only=False))
            results.append(await self._check_sources())
            results.append(await self._check_layout(use_stub=False))
            results.append(await self._check_send_parity())
            results.append(await self._check_rollups())
            results.append(await self._check_watchdog())
            results.append(await self._check_log_contract())
            results.append(await self._check_medals())
        return AuditReport(results)

    async def _check_policy(self) -> CheckResult:
        policy = PremiumEmojiResolver.current_policy()
        source = PremiumEmojiResolver.policy_source()
        env_policy = os.getenv("EMOJI_POLICY") or ""
        premium_flag = PremiumEmojiResolver.premium_status()
        premium_display = (
            "unknown" if premium_flag is None else ("True" if premium_flag else "False")
        )
        if premium_flag and not env_policy and policy != "pinned_strict":
            message = f"expected pinned_strict for premium account (got {policy}, source={source}, premium=True)"
            return CheckResult(
                code="A",
                status="FAIL",
                message=message,
                suggestion=".emoji policy pinned_strict",
            )
        message = f"{policy} (source={source}, premium={premium_display})"
        return CheckResult(code="A", status="PASS", message=message)

    async def _check_coverage(self, *, counts_only: bool) -> CheckResult:
        counts = PremiumEmojiResolver.counts()
        breakdown = PremiumEmojiResolver.resolution_breakdown()
        missing = breakdown.get("FALLING_BACK", [])
        duplicates = PremiumEmojiResolver.duplicate_keys()
        dup_total = _sum_duplicate_counts(duplicates)
        fingerprint = PremiumEmojiResolver.current_fingerprint() or "none"
        detail_lines: List[str] = [
            f"missing_keys: {missing}",
            f"duplicate_keys: {duplicates}",
        ]
        status = "PASS"
        message_suffix = f"premium={counts.get('mapped_premium', 0)} unicode={counts.get('pinned_unicode', 0)} fallback={counts.get('normal_fallback', 0)} dupes={dup_total} fp={fingerprint[:10]}"
        recomputed_fp = None
        missing_used_keys: List[str] = []
        suggestion = None
        if not counts_only:
            recomputed_fp = self._recompute_cache_fingerprint()
            if recomputed_fp and recomputed_fp != fingerprint:
                status = "FAIL"
                detail_lines.append(f"recomputed_fingerprint: {recomputed_fp}")
                detail_lines.append(f"cached_fingerprint: {fingerprint}")
                suggestion = ".emoji refresh"
            preview = await self._get_preview_data(use_stub=False)
            missing_used_keys = sorted(
                {
                    item.get("key")
                    for item in preview.metadata
                    if item.get("source") == "NORMAL_SET"
                }
            )
            if PremiumEmojiResolver.current_policy() == "pinned_strict" and missing_used_keys:
                status = "FAIL"
                detail_lines.append(f"missing_used_keys: {missing_used_keys}")
                suggestion = ".emoji export_template → paste → .emoji refresh"
        else:
            detail_lines = []
        message = f"{message_suffix}"
        if counts_only:
            message += " (counts-only)"
        detail = "\n".join(detail_lines) if detail_lines else None
        return CheckResult(
            code="B",
            status=status,
            message=message,
            detail=detail,
            suggestion=suggestion,
        )

    async def _check_sources(self) -> CheckResult:
        policy = PremiumEmojiResolver.current_policy()
        rows: List[str] = []
        normal_fallback_keys: List[str] = []
        resolved = 0
        for key in TARGET_KEYS:
            glyph, _, source = PremiumEmojiResolver.emoji_for_key(key)
            display = glyph if glyph else "(blank)"
            line = f"{key} -> {display} (source: {source})"
            if policy == "pinned_strict" and source == "NORMAL_SET":
                normal_fallback_keys.append(key)
                line += " ⚠️"
            rows.append(line)
            resolved += 1
        warn = bool(normal_fallback_keys)
        status = "WARN" if warn else "PASS"
        message = f"{resolved}/{len(TARGET_KEYS)} keys resolvable"
        if policy == "pinned_strict":
            message += f" ({len(normal_fallback_keys)} NORMAL_SET under pinned_strict)"
        detail = "\n".join(rows)
        suggestion = None
        if warn:
            suggestion = ".emoji explain <KEY> → adjust pinned note"
        return CheckResult(
            code="C",
            status=status,
            message=message,
            detail=detail,
            suggestion=suggestion,
        )

    async def _check_layout(self, *, use_stub: bool) -> CheckResult:
        preview = await self._get_preview_data(use_stub=use_stub)
        rendered = preview.rendered_text.rstrip("\n")
        lines = rendered.split("\n")
        if len(lines) < 2:
            return CheckResult(
                code="D",
                status="FAIL",
                message="rendered preview missing header lines",
            )
        violations = self._layout_violations(lines)
        if violations:
            rule, offending = violations[0]
            message = f"{rule} (line='{offending}')"
            return CheckResult(code="D", status="FAIL", message=message)
        suffix = " (stub preview)" if use_stub else ""
        return CheckResult(code="D", status="PASS", message=f"Day-3 spec PASS{suffix}")

    def _layout_violations(self, lines: Sequence[str]) -> List[Tuple[str, str]]:
        tracker = self.tracker
        nb_space = getattr(tracker, "NBSP", "\u00A0")
        em_dash = getattr(tracker, "EM_DASH", "\u2014")
        quote_l = getattr(tracker, "QUOTE_L", "\u201C")
        quote_r = getattr(tracker, "QUOTE_R", "\u201D")
        range_sep = getattr(tracker, "RANGE_SEP", " - ")
        violations: List[Tuple[str, str]] = []
        title = lines[1]
        if not re.fullmatch(rf"📊 LEADERBOARD{em_dash}DAY \d+ 👑", title):
            violations.append(("title format mismatch", title))
        header_lines = [ln for ln in lines if ln.startswith(quote_l)]
        expected_fragments = ["📅 Today", "📆 This", "🗓️ This"]
        for frag in expected_fragments:
            match = next((ln for ln in header_lines if frag in ln), None)
            if not match:
                violations.append((f"missing header for {frag}", frag))
                continue
            if not match.endswith(quote_r):
                violations.append(("header missing smart closing quote", match))
        if any("This Week" in ln or "This Month" in ln for ln in lines):
            violations.append(("section labels missing NBSP", "This Week/Month"))
        for ln in lines:
            if ("(WEEK" in ln or "(MONTH" in ln) and range_sep not in ln:
                violations.append(("date range missing hyphen separator", ln))
        joined = "\n".join(lines)
        if "■" in joined:
            violations.append(("found placeholder glyph '■'", "■"))
        if "  " in joined:
            violations.append(("double spaces detected", "  "))
        return violations

    async def _check_send_parity(self) -> CheckResult:
        preview = await self._get_preview_data(use_stub=False)
        context = preview.context
        tracker = self.tracker
        html_text = context["msg"]
        plain_text, _ = tracker.tele_html.parse(html_text)
        if not plain_text.endswith("\n"):
            plain_text = f"{plain_text}\n"
        tokenized, _ = tracker._tokenize_plain_text(plain_text)
        post_text, *_ = PremiumEmojiResolver.render_with_sources(preview.tokenized_text)
        preview_hash = hashlib.sha256(preview.rendered_text.encode("utf-8")).hexdigest()
        post_hash = hashlib.sha256(post_text.encode("utf-8")).hexdigest()
        if preview.rendered_text != post_text:
            detail = [
                f"preview_hash={preview_hash}",
                f"post_hash={post_hash}",
                f"preview_text={preview.rendered_text[:80]!r}",
                f"post_text={post_text[:80]!r}",
            ]
            return CheckResult(
                code="E",
                status="FAIL",
                message="preview text differs from send path",
                detail="\n".join(detail),
            )
        message = f"preview_hash == post_hash ({preview_hash[:10]})"
        return CheckResult(code="E", status="PASS", message=message)

    async def _check_rollups(self) -> CheckResult:
        preview = await self._get_preview_data(use_stub=False)
        context = preview.context
        tracker = self.tracker
        day_entries = context["day_entries"]
        week_entries = context["week_entries"]
        month_entries = context["month_entries"]
        alias_to_canon, _ = tracker._alias_maps_from_cache()

        def _minutes_ok(entries: Sequence[Dict[str, Any]]) -> bool:
            return all(entry.get("minutes", 0) >= 0 for entry in entries)

        if not _minutes_ok(day_entries):
            return CheckResult(
                code="F",
                status="FAIL",
                message="negative minute value detected",
            )

        week_ok, week_detail = self._verify_period_totals(
            tracker,
            alias_to_canon,
            context["w_start"],
            context["w_end"],
            week_entries,
        )
        month_ok, month_detail = self._verify_period_totals(
            tracker,
            alias_to_canon,
            context["m_start"],
            context["m_end"],
            month_entries,
        )
        if not week_ok or not month_ok:
            detail_lines = week_detail + month_detail
            return CheckResult(
                code="F",
                status="FAIL",
                message="rollup totals mismatch",
                detail="\n".join(detail_lines),
            )

        top_lines = self._format_top_five(day_entries, week_entries, month_entries)
        message = "consistent (top5 listed)"
        return CheckResult(
            code="F",
            status="PASS",
            message=message,
            detail="\n".join(top_lines),
        )

    def _verify_period_totals(
        self,
        tracker,
        alias_to_canon: Dict[int, int],
        period_start: datetime,
        period_end: datetime,
        entries: Sequence[Dict[str, Any]],
    ) -> Tuple[bool, List[str]]:
        seconds_by_user = self._folded_period_totals(
            tracker, alias_to_canon, period_start, period_end
        )
        stats: List[str] = []
        ok = True
        for entry in entries:
            uid = entry["user_id"]
            listed_secs = int(entry["seconds"])
            computed_secs = seconds_by_user.get(uid, 0)
            if listed_secs < computed_secs:
                ok = False
                stats.append(
                    f"user {uid}: listed={listed_secs}s < computed={computed_secs}s"
                )
        return ok, stats

    def _folded_period_totals(
        self,
        tracker,
        alias_to_canon: Dict[int, int],
        period_start: datetime,
        period_end: datetime,
    ) -> Dict[int, int]:
        duration_days = (period_end.date() - period_start.date()).days
        totals: Dict[int, int] = {}
        for day_offset in range(duration_days + 1):
            current = period_start + timedelta(days=day_offset)
            day_start = datetime(
                current.year,
                current.month,
                current.day,
                0,
                0,
                tzinfo=tracker.TZ,
            )
            day_end = datetime(
                current.year,
                current.month,
                current.day,
                23,
                59,
                59,
                tzinfo=tracker.TZ,
            )
            rows = tracker.db_fetch_period_seconds(day_start, day_end, tracker.MIN_DAILY_SECONDS)
            merged: Dict[int, int] = {}
            for uid, secs in rows:
                canon = alias_to_canon.get(uid, uid)
                merged[canon] = merged.get(canon, 0) + int(secs)
            for canon, secs in merged.items():
                totals[canon] = totals.get(canon, 0) + secs
        return totals

    def _format_top_five(
        self,
        day_entries: Sequence[Dict[str, Any]],
        week_entries: Sequence[Dict[str, Any]],
        month_entries: Sequence[Dict[str, Any]],
    ) -> List[str]:
        combined: List[int] = []
        for seq in (day_entries, week_entries, month_entries):
            for entry in seq:
                uid = entry["user_id"]
                if uid not in combined:
                    combined.append(uid)
                if len(combined) >= 5:
                    break
            if len(combined) >= 5:
                break
        lines: List[str] = []
        for uid in combined[:5]:
            day = next((e for e in day_entries if e["user_id"] == uid), None)
            week = next((e for e in week_entries if e["user_id"] == uid), None)
            month = next((e for e in month_entries if e["user_id"] == uid), None)
            display = (day or week or month or {}).get("display", f"user {uid}")
            dm = day["minutes"] if day else 0
            wm = week["minutes"] if week else 0
            mm = month["minutes"] if month else 0
            lines.append(f"{display}: today={dm}m week={wm}m month={mm}m")
        return lines

    async def _check_watchdog(self) -> CheckResult:
        tracker = self.tracker
        lock_path = tracker.HEARTBEAT_FILE
        if not lock_path.exists():
            return CheckResult(
                code="G",
                status="FAIL",
                message=f"{lock_path} missing",
            )
        try:
            raw = lock_path.read_text(encoding="utf-8").strip()
            lock_ts = float(raw)
        except Exception:
            return CheckResult(
                code="G",
                status="FAIL",
                message="tracker.lock unreadable",
            )
        age = int(time.time() - lock_ts)
        log_path = tracker.LOG_FILE
        last_heartbeat = "unknown"
        if log_path.exists():
            try:
                lines = log_path.read_text(encoding="utf-8").splitlines()
                for line in reversed(lines):
                    if "[heartbeat] alive" in line:
                        last_heartbeat = line.split(" ", 1)[0]
                        break
            except Exception:
                last_heartbeat = "error"
        message = f"tracker.lock age={age}s, last heartbeat={last_heartbeat}"
        status = "PASS" if age <= 120 else "FAIL"
        suggestion = None
        if status == "FAIL":
            suggestion = "check tracker heartbeat process"
        return CheckResult(
            code="G",
            status=status,
            message=message,
            suggestion=suggestion,
        )

    async def _check_log_contract(self) -> CheckResult:
        target = (
            "post_sent emoji_mode=%s mapped_premium=%d pinned_unicode=%d "
            "normal_fallback=%d missing_keys=%s fingerprint=%s"
        )
        try:
            contents = Path("study_tracker.py").read_text(encoding="utf-8")
        except Exception:
            return CheckResult(
                code="H",
                status="FAIL",
                message="unable to read study_tracker.py",
            )
        if target not in contents:
            return CheckResult(
                code="H",
                status="FAIL",
                message="post_sent contract log missing",
            )
        return CheckResult(code="H", status="PASS", message="string present")

    async def _check_medals(self) -> CheckResult:
        results = selfcheck_medals()
        strict = results.get("pinned_strict", {})
        three = strict.get("three_entries", {})
        tokens = three.get("tokens", [])
        order = {"MEDAL_1": 0, "MEDAL_2": 1, "MEDAL_3": 2}
        tokens_sorted = sorted(tokens, key=lambda t: order.get(t.get("key", ""), 99))
        sources = [token.get("source") for token in tokens_sorted]
        expected = ["PREMIUM_ID", "PREMIUM_ID", "PINNED_UNICODE"]
        if sources != expected:
            detail = f"observed={sources}, expected={expected}"
            return CheckResult(
                code="I",
                status="FAIL",
                message="unexpected medal sources",
                detail=detail,
                suggestion=".emoji test medals",
            )
        return CheckResult(
            code="I",
            status="PASS",
            message=f"{sources}",
        )

    def _recompute_cache_fingerprint(self) -> Optional[str]:
        text, entities = PremiumEmojiResolver.export_template()
        cache = getattr(PremiumEmojiResolver, "_cache_data", {})
        msg_id = cache.get("pinned_message_id") or 0
        fake_msg = SimpleNamespace(raw_text=text, id=msg_id, entities=entities)
        return PremiumEmojiResolver._compute_fingerprint(fake_msg)

    async def _get_preview_data(self, *, use_stub: bool) -> PreviewData:
        if use_stub:
            if self._stub_preview_cache is None:
                self._stub_preview_cache = await self._build_preview(use_stub=True)
            return self._stub_preview_cache
        if self._preview_cache is None:
            self._preview_cache = await self._build_preview(use_stub=False)
        return self._preview_cache

    async def _build_preview(self, *, use_stub: bool) -> PreviewData:
        tracker = self.tracker

        async def _noop_refresh(cls, client, *, force: bool = False) -> bool:
            return False

        async def _noop_ensure(*args, **kwargs):
            return True

        def _noop_save(*args, **kwargs):
            return None

        def _stub_get_meta(key: str) -> Optional[str]:
            if key == "anchor_date":
                return datetime.now(tracker.TZ).date().isoformat()
            return None

        def _safe_ensure_anchor() -> datetime:
            raw = tracker.db_get_meta("anchor_date")
            if raw:
                try:
                    return datetime.fromisoformat(raw).replace(tzinfo=tracker.TZ)
                except Exception:
                    pass
            return datetime.now(tracker.TZ).replace(hour=0, minute=0, second=0, microsecond=0)

        def _stub_alias():
            return {}, {}

        def _stub_fetch(*args, **kwargs):
            return []

        def _stub_get_day(*args, **kwargs):
            return 0

        def _stub_get_compliment(*args, **kwargs):
            return None

        def _stub_all_used(*args, **kwargs):
            return set()

        def _stub_ensure_anchor() -> datetime:
            return datetime.now(tracker.TZ).replace(hour=0, minute=0, second=0, microsecond=0)

        patches: List[Tuple[Any, str, Any]] = [
            (tracker, "ensure_connected", _noop_ensure),
            (PremiumEmojiResolver, "refresh_if_needed", _noop_refresh),
            (tracker, "_save_compliment", _noop_save),
            (tracker, "_ensure_anchor", _safe_ensure_anchor),
        ]
        if use_stub:
            patches.extend(
                [
                    (tracker, "db_fetch_period_seconds", _stub_fetch),
                    (tracker, "db_get_day_seconds", _stub_get_day),
                    (tracker, "_alias_maps_from_cache", _stub_alias),
                    (tracker, "db_get_meta", _stub_get_meta),
                    (tracker, "_get_saved_compliment", _stub_get_compliment),
                    (tracker, "_all_used_for_scope", _stub_all_used),
                ]
            )

        async with _async_patch_context(patches):
            context = await tracker._build_leaderboard_context()
        html_text = context["msg"]
        plain_text, markup_entities = tracker.tele_html.parse(html_text)
        if not plain_text.endswith("\n"):
            plain_text = f"{plain_text}\n"
        tokenized_text, tokens = tracker._tokenize_plain_text(plain_text)
        rendered_text, emoji_entities, final_lengths, metadata = PremiumEmojiResolver.render_with_sources(
            tokenized_text
        )
        return PreviewData(
            context=context,
            html_text=html_text,
            plain_text=plain_text,
            tokenized_text=tokenized_text,
            rendered_text=rendered_text,
            tokens=tokens,
            markup_entities=markup_entities,
            emoji_entities=emoji_entities,
            final_lengths=final_lengths,
            metadata=metadata,
        )


@contextlib.asynccontextmanager
async def _async_patch_context(pairs: Iterable[Tuple[Any, str, Any]]):
    with _patch_attributes(pairs):
        yield


def split_for_telegram(text: str, limit: int = TELEGRAM_SPLIT_THRESHOLD) -> List[str]:
    if len(text) <= limit:
        return [text]
    parts: List[str] = []
    buffer: List[str] = []
    length = 0
    for line in text.splitlines():
        candidate = line if not buffer else "\n".join(buffer + [line])
        if len(candidate) > limit and buffer:
            parts.append("\n".join(buffer))
            buffer = [line]
        else:
            buffer.append(line)
        length += len(line) + 1
    if buffer:
        parts.append("\n".join(buffer))
    return parts
