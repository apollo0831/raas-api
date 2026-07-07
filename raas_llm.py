# -*- coding: utf-8 -*-
"""RAAS 공용 LLM 클라이언트 — Claude API 호출 단일 지점.

grounding·storyline_router·server가 import한다.
(구 raas_query_engine → raas_fallback_engine 에 얹혀 있던 call_claude를 이관.)
"""
import os
import json
import urllib.request
from dotenv import load_dotenv
load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")
HAIKU_MODEL       = os.getenv("HAIKU_MODEL",  "claude-haiku-4-5-20251001")

_CACHE_HEADERS = {
    "Content-Type": "application/json",
    "anthropic-version": "2023-06-01",
    "anthropic-beta": "prompt-caching-2024-07-31",
    "x-api-key": ANTHROPIC_API_KEY,
}


def call_claude(system, user: str, max_tokens: int = 1000, model: str = None) -> tuple:
    """(answer_text, usage_dict) 반환. system은 str 또는 (static_str, dynamic_str) 튜플.
       튜플이면 static_part만 prompt-caching(ephemeral) 대상으로 표시한다."""
    if isinstance(system, tuple):
        static_part, dynamic_part = system
        system_payload = [{"type": "text", "text": static_part, "cache_control": {"type": "ephemeral"}}]
        if dynamic_part:
            system_payload.append({"type": "text", "text": dynamic_part})
    else:
        system_payload = [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]
    payload = json.dumps({
        "model": model or CLAUDE_MODEL,
        "max_tokens": max_tokens,
        "system": system_payload,
        "messages": [{"role": "user", "content": user}]
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=payload,
        headers=_CACHE_HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        body = json.loads(r.read())
        return body["content"][0]["text"], body.get("usage", {})
