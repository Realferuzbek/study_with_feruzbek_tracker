from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


STATUS_EMOJI = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️"}


@dataclass
class CheckResult:
    code: str
    status: str
    message: str
    detail: Optional[str] = None

    @property
    def emoji(self) -> str:
        return STATUS_EMOJI.get(self.status, "ℹ️")

    def summary_line(self) -> str:
        return f"{self.emoji} {self.code}: {self.message}"


@dataclass
class AuditReport:
    results: List[CheckResult]

    @property
    def has_fail(self) -> bool:
        return any(result.status == "FAIL" for result in self.results)

    def format_summary(self) -> str:
        lines: List[str] = ["=== Audit Summary ==="]
        for result in self.results:
            lines.append(result.summary_line())
            if result.detail and result.status == "FAIL":
                lines.append(f"Detail: {result.detail}")
        return "\n".join(lines).rstrip()


def split_for_telegram(text: str, limit: int = 3800) -> List[str]:
    if len(text) <= limit:
        return [text]
    parts: List[str] = []
    buffer: List[str] = []
    for line in text.splitlines():
        candidate = "\n".join(buffer + [line]) if buffer else line
        if len(candidate) > limit and buffer:
            parts.append("\n".join(buffer))
            buffer = [line]
        else:
            buffer.append(line)
    if buffer:
        parts.append("\n".join(buffer))
    return parts


class AuditRunner:
    """
    Minimal audit used by admin commands to confirm the leaderboard layout stays canonical.
    """

    def __init__(self, tracker_module):
        self.tracker = tracker_module

    async def run(self, quick: bool = False) -> AuditReport:
        layout = await self.tracker.render_preview_layout()
        ok, detail = self.tracker._audit_layout_text(layout.plain_text)
        if ok:
            result = CheckResult(code="LAYOUT", status="PASS", message="Layout format looks good.")
        else:
            result = CheckResult(
                code="LAYOUT",
                status="FAIL",
                message="Layout format issues detected.",
                detail=detail,
            )
        return AuditReport(results=[result])
