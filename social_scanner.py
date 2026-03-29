#!/usr/bin/env python3
"""Scan the last N days of Reddit and X into normalized CSV/JSONL outputs.

Features
--------
- Reddit: scan one or more subreddits, or run a broader search across r/all.
- X: scan either:
  1) platform-wide search via full-archive search, or
  2) specific public accounts via user timelines.
- Normalized outputs: CSV, JSONL, and a metadata summary JSON.
- Safe defaults and explicit warnings for API truncation / access limits.

Environment variables
---------------------
REDDIT_CLIENT_ID
REDDIT_CLIENT_SECRET
REDDIT_USER_AGENT
X_BEARER_TOKEN

Examples
--------
# Reddit only: search two subreddits for the last 30 days
python social_scanner.py \
  --days 30 \
  --reddit-subreddits MachineLearning,OpenAI \
  --reddit-query 'openai OR gpt' \
  --outdir ./out

# X only: full-archive search for the last 30 days (requires paid / full-archive access)
python social_scanner.py \
  --days 30 \
  --x-query '"OpenAI" lang:en -is:retweet' \
  --outdir ./out

# X only: scan specific accounts for the last 30 days using timelines
python social_scanner.py \
  --days 30 \
  --x-usernames openai,sama \
  --x-exclude-replies \
  --x-exclude-retweets \
  --outdir ./out
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests


def eprint(*args: object, **kwargs: object) -> None:
    print(*args, file=sys.stderr, **kwargs)


UTC = timezone.utc


@dataclass
class ScanRecord:
    platform: str
    source_mode: str
    source_label: str
    platform_id: str
    created_at: str
    author: str = ""
    title: str = ""
    text: str = ""
    url: str = ""
    permalink: str = ""
    subreddit: str = ""
    lang: str = ""
    score: Optional[int] = None
    num_comments: Optional[int] = None
    like_count: Optional[int] = None
    reply_count: Optional[int] = None
    repost_count: Optional[int] = None
    quote_count: Optional[int] = None
    bookmark_count: Optional[int] = None
    impression_count: Optional[int] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    def as_csv_row(self) -> Dict[str, Any]:
        def _clean(s: str) -> str:
            return re.sub(r"\s+", " ", s).strip()

        return {
            "platform": self.platform,
            "source_mode": self.source_mode,
            "source_label": self.source_label,
            "platform_id": self.platform_id,
            "created_at": self.created_at,
            "author": self.author,
            "title": _clean(self.title),
            "text": _clean(self.text),
            "url": self.url,
            "permalink": self.permalink,
            "subreddit": self.subreddit,
            "lang": self.lang,
            "score": self.score,
            "num_comments": self.num_comments,
            "like_count": self.like_count,
            "reply_count": self.reply_count,
            "repost_count": self.repost_count,
            "quote_count": self.quote_count,
            "bookmark_count": self.bookmark_count,
            "impression_count": self.impression_count,
        }

    def as_json(self) -> Dict[str, Any]:
        row = self.as_csv_row()
        row["raw"] = self.raw
        return row


@dataclass
class ScanResult:
    records: List[ScanRecord] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def extend(self, other: "ScanResult") -> None:
        self.records.extend(other.records)
        self.warnings.extend(other.warnings)
        self.errors.extend(other.errors)


class ToolError(RuntimeError):
    """Raised for user-facing tool errors."""


class XAPIError(ToolError):
    """Raised for X API-specific failures."""


class RedditScanner:
    def __init__(self, client_id: str, client_secret: str, user_agent: str) -> None:
        try:
            import praw  # type: ignore
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise ToolError(
                "Reddit scanning requires the 'praw' package. Install dependencies from requirements_social_scanner.txt first."
            ) from exc

        self._praw = praw
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )
        self.reddit.read_only = True

    @classmethod
    def from_env(cls) -> "RedditScanner":
        client_id = os.getenv("REDDIT_CLIENT_ID")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        user_agent = os.getenv("REDDIT_USER_AGENT")
        missing = [
            name
            for name, value in [
                ("REDDIT_CLIENT_ID", client_id),
                ("REDDIT_CLIENT_SECRET", client_secret),
                ("REDDIT_USER_AGENT", user_agent),
            ]
            if not value
        ]
        if missing:
            raise ToolError(
                f"Missing Reddit credentials in environment: {', '.join(missing)}"
            )
        return cls(client_id=client_id or "", client_secret=client_secret or "", user_agent=user_agent or "")

    def scan(
        self,
        *,
        days: int,
        subreddits: Sequence[str],
        query: Optional[str],
        max_per_subreddit: int,
        max_global_results: int,
    ) -> ScanResult:
        result = ScanResult()
        cutoff = datetime.now(UTC) - timedelta(days=days)

        if subreddits:
            for subreddit in subreddits:
                result.extend(
                    self._scan_single_subreddit(
                        subreddit=subreddit,
                        query=query,
                        cutoff=cutoff,
                        max_items=max_per_subreddit,
                    )
                )
            return result

        if not query:
            raise ToolError(
                "For Reddit, provide either --reddit-subreddits or --reddit-query (or both)."
            )

        eprint(f"[reddit] searching across r/all for: {query!r}")
        try:
            generator = self.reddit.subreddit("all").search(
                query,
                sort="new",
                time_filter="month",
                limit=max_global_results,
            )
            seen = 0
            last_created: Optional[datetime] = None
            for submission in generator:
                seen += 1
                created = datetime.fromtimestamp(submission.created_utc, tz=UTC)
                last_created = created
                if created < cutoff:
                    break
                result.records.append(self._normalize_submission(submission, source_label="r/all"))

            if seen >= max_global_results and last_created and last_created >= cutoff:
                result.warnings.append(
                    "Reddit global search hit the configured result cap while still inside the requested window. "
                    "Results may be truncated; try narrower queries or explicit subreddits."
                )
        except Exception as exc:
            result.errors.append(f"Reddit global scan failed: {exc}")

        return result

    def _scan_single_subreddit(
        self,
        *,
        subreddit: str,
        query: Optional[str],
        cutoff: datetime,
        max_items: int,
    ) -> ScanResult:
        result = ScanResult()
        eprint(f"[reddit] scanning r/{subreddit}")
        try:
            sr = self.reddit.subreddit(subreddit)
            if query:
                generator = sr.search(query, sort="new", time_filter="month", limit=max_items)
                mode = "search"
            else:
                generator = sr.new(limit=max_items)
                mode = "listing"

            seen = 0
            last_created: Optional[datetime] = None
            for submission in generator:
                seen += 1
                created = datetime.fromtimestamp(submission.created_utc, tz=UTC)
                last_created = created
                if created < cutoff:
                    break
                result.records.append(
                    self._normalize_submission(submission, source_label=f"r/{subreddit}")
                )

            if seen >= max_items and last_created and last_created >= cutoff:
                result.warnings.append(
                    f"Reddit r/{subreddit} {mode} hit {max_items} items while still inside the requested window. "
                    "This usually means the API listing/search cap was reached and older in-window posts may be missing."
                )
        except Exception as exc:
            result.errors.append(f"Reddit r/{subreddit} scan failed: {exc}")
        return result

    def _normalize_submission(self, submission: Any, *, source_label: str) -> ScanRecord:
        author_name = ""
        try:
            author_name = submission.author.name if submission.author else ""
        except Exception:
            author_name = ""

        title = submission.title or ""
        body = submission.selftext or ""
        text = title if not body else f"{title}\n\n{body}"
        permalink = f"https://www.reddit.com{submission.permalink}"

        raw = {
            "id": submission.id,
            "subreddit": getattr(submission.subreddit, "display_name", ""),
            "title": title,
            "selftext": body,
            "created_utc": submission.created_utc,
            "score": getattr(submission, "score", None),
            "num_comments": getattr(submission, "num_comments", None),
            "url": getattr(submission, "url", ""),
            "permalink": getattr(submission, "permalink", ""),
            "author": author_name,
            "over_18": getattr(submission, "over_18", None),
        }

        return ScanRecord(
            platform="reddit",
            source_mode="subreddit_scan",
            source_label=source_label,
            platform_id=submission.id,
            created_at=datetime.fromtimestamp(submission.created_utc, tz=UTC).isoformat(),
            author=author_name,
            title=title,
            text=text,
            url=getattr(submission, "url", "") or permalink,
            permalink=permalink,
            subreddit=getattr(submission.subreddit, "display_name", ""),
            score=getattr(submission, "score", None),
            num_comments=getattr(submission, "num_comments", None),
            raw=raw,
        )


class XScanner:
    BASE_URL = "https://api.x.com/2"

    def __init__(self, bearer_token: str, timeout: int = 30) -> None:
        if not bearer_token:
            raise ToolError("Missing X_BEARER_TOKEN in environment.")
        self.bearer_token = bearer_token
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {bearer_token}",
                "User-Agent": "social-scanner/1.0",
                "Accept": "application/json",
            }
        )

    @classmethod
    def from_env(cls) -> "XScanner":
        return cls(os.getenv("X_BEARER_TOKEN", ""))

    def scan(
        self,
        *,
        days: int,
        search_query: Optional[str],
        usernames: Sequence[str],
        max_search_posts: int,
        max_posts_per_user: int,
        exclude_replies: bool,
        exclude_retweets: bool,
    ) -> ScanResult:
        result = ScanResult()
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(days=days)

        if search_query:
            result.extend(
                self._scan_search(
                    query=search_query,
                    start_time=start_time,
                    end_time=end_time,
                    max_posts=max_search_posts,
                    exclude_replies=exclude_replies,
                    exclude_retweets=exclude_retweets,
                )
            )

        for username in usernames:
            result.extend(
                self._scan_user_timeline(
                    username=username,
                    start_time=start_time,
                    end_time=end_time,
                    max_posts=max_posts_per_user,
                    exclude_replies=exclude_replies,
                    exclude_retweets=exclude_retweets,
                )
            )

        if not search_query and not usernames:
            raise ToolError("For X, provide --x-query and/or --x-usernames.")

        return result

    def _request(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 5,
    ) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{path}"
        backoff = 1.0
        last_error: Optional[str] = None

        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt == max_retries - 1:
                    break
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue

            if response.status_code == 429:
                reset_header = response.headers.get("x-rate-limit-reset")
                sleep_for = backoff
                if reset_header:
                    try:
                        reset_epoch = int(reset_header)
                        sleep_for = max(1, reset_epoch - int(time.time()))
                    except ValueError:
                        pass
                if attempt == max_retries - 1:
                    raise XAPIError("X API rate limit exceeded and retries were exhausted.")
                eprint(f"[x] rate limited, sleeping {sleep_for}s")
                time.sleep(sleep_for)
                backoff = min(backoff * 2, 32)
                continue

            if response.status_code in {500, 502, 503, 504}:
                if attempt == max_retries - 1:
                    break
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue

            try:
                payload = response.json()
            except ValueError:
                payload = {"raw_text": response.text}

            if not response.ok:
                detail = self._extract_error_detail(payload) or response.text
                if response.status_code == 403 and path == "/tweets/search/all":
                    raise XAPIError(
                        "X full-archive search was rejected. This endpoint requires full-archive access "
                        "(pay-per-use or Enterprise) for 30-day platform-wide search."
                    )
                raise XAPIError(
                    f"X API request failed ({response.status_code}) for {path}: {detail}"
                )

            return payload

        raise XAPIError(f"X API request failed for {path}: {last_error or 'server error'}")

    @staticmethod
    def _extract_error_detail(payload: Dict[str, Any]) -> str:
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            parts = []
            for err in errors:
                if isinstance(err, dict):
                    title = err.get("title")
                    detail = err.get("detail")
                    status = err.get("status")
                    text = " - ".join(str(x) for x in [title, detail] if x)
                    if status:
                        text = f"[{status}] {text}" if text else f"[{status}]"
                    if text:
                        parts.append(text)
            if parts:
                return "; ".join(parts)
        return payload.get("detail", "") if isinstance(payload, dict) else ""

    def _scan_search(
        self,
        *,
        query: str,
        start_time: datetime,
        end_time: datetime,
        max_posts: int,
        exclude_replies: bool,
        exclude_retweets: bool,
    ) -> ScanResult:
        result = ScanResult()
        q = self._augment_x_query(
            query,
            exclude_replies=exclude_replies,
            exclude_retweets=exclude_retweets,
        )
        eprint(f"[x] full-archive search: {q!r}")

        next_token: Optional[str] = None
        total = 0
        while total < max_posts:
            remaining = max_posts - total
            params: Dict[str, Any] = {
                "query": q,
                "start_time": start_time.isoformat().replace("+00:00", "Z"),
                "end_time": end_time.isoformat().replace("+00:00", "Z"),
                "max_results": min(500, max(10, remaining)),
                "tweet.fields": "created_at,lang,public_metrics,author_id,conversation_id,possibly_sensitive",
                "expansions": "author_id",
                "user.fields": "username,name,verified",
            }
            if next_token:
                params["next_token"] = next_token

            try:
                payload = self._request("/tweets/search/all", params=params)
            except XAPIError as exc:
                result.errors.append(str(exc))
                return result

            users_by_id = {
                user["id"]: user
                for user in payload.get("includes", {}).get("users", [])
                if isinstance(user, dict) and user.get("id")
            }
            data = payload.get("data", []) or []
            for post in data:
                user = users_by_id.get(post.get("author_id", ""), {})
                result.records.append(
                    self._normalize_post(
                        post,
                        source_mode="search",
                        source_label="x:search",
                        author_username=str(user.get("username", "")),
                        raw_user=user,
                    )
                )
                total += 1
                if total >= max_posts:
                    break

            meta = payload.get("meta", {}) or {}
            next_token = meta.get("next_token")
            if not next_token or not data:
                break
            time.sleep(1.05)  # documented 1/sec cadence for full-archive search

        return result

    def _scan_user_timeline(
        self,
        *,
        username: str,
        start_time: datetime,
        end_time: datetime,
        max_posts: int,
        exclude_replies: bool,
        exclude_retweets: bool,
    ) -> ScanResult:
        result = ScanResult()
        username = username.lstrip("@").strip()
        if not username:
            return result

        eprint(f"[x] timeline scan for @{username}")
        try:
            user_payload = self._request(f"/users/by/username/{username}")
        except XAPIError as exc:
            result.errors.append(f"X user lookup failed for @{username}: {exc}")
            return result

        user_data = user_payload.get("data") or {}
        user_id = user_data.get("id")
        if not user_id:
            result.errors.append(f"X user lookup returned no user id for @{username}.")
            return result

        excludes: List[str] = []
        if exclude_replies:
            excludes.append("replies")
        if exclude_retweets:
            excludes.append("retweets")

        next_token: Optional[str] = None
        total = 0
        while total < max_posts:
            remaining = max_posts - total
            params: Dict[str, Any] = {
                "start_time": start_time.isoformat().replace("+00:00", "Z"),
                "end_time": end_time.isoformat().replace("+00:00", "Z"),
                "max_results": min(100, max(5, remaining)),
                "tweet.fields": "created_at,lang,public_metrics,author_id,conversation_id,possibly_sensitive",
            }
            if excludes:
                params["exclude"] = ",".join(excludes)
            if next_token:
                params["pagination_token"] = next_token

            try:
                payload = self._request(f"/users/{user_id}/tweets", params=params)
            except XAPIError as exc:
                result.errors.append(f"X timeline scan failed for @{username}: {exc}")
                return result

            data = payload.get("data", []) or []
            for post in data:
                result.records.append(
                    self._normalize_post(
                        post,
                        source_mode="timeline",
                        source_label=f"x:user:{username}",
                        author_username=username,
                        raw_user=user_data,
                    )
                )
                total += 1
                if total >= max_posts:
                    break

            meta = payload.get("meta", {}) or {}
            next_token = meta.get("next_token")
            if not next_token or not data:
                break

        return result

    @staticmethod
    def _augment_x_query(
        query: str,
        *,
        exclude_replies: bool,
        exclude_retweets: bool,
    ) -> str:
        q = query.strip()
        suffixes = []
        if exclude_replies and "-is:reply" not in q:
            suffixes.append("-is:reply")
        if exclude_retweets and "-is:retweet" not in q:
            suffixes.append("-is:retweet")
        if suffixes:
            q = f"{q} {' '.join(suffixes)}"
        return q

    def _normalize_post(
        self,
        post: Dict[str, Any],
        *,
        source_mode: str,
        source_label: str,
        author_username: str,
        raw_user: Dict[str, Any],
    ) -> ScanRecord:
        metrics = post.get("public_metrics") or {}
        post_id = str(post.get("id", ""))
        profile = f"https://x.com/{author_username}" if author_username else "https://x.com"
        url = f"{profile}/status/{post_id}" if post_id else profile

        return ScanRecord(
            platform="x",
            source_mode=source_mode,
            source_label=source_label,
            platform_id=post_id,
            created_at=str(post.get("created_at", "")),
            author=author_username,
            text=str(post.get("text", "")),
            url=url,
            permalink=url,
            lang=str(post.get("lang", "")),
            like_count=_int_or_none(metrics.get("like_count")),
            reply_count=_int_or_none(metrics.get("reply_count")),
            repost_count=_int_or_none(metrics.get("repost_count", metrics.get("retweet_count"))),
            quote_count=_int_or_none(metrics.get("quote_count")),
            bookmark_count=_int_or_none(metrics.get("bookmark_count")),
            impression_count=_int_or_none(metrics.get("impression_count")),
            raw={"post": post, "user": raw_user},
        )


def _int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_csv_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def write_outputs(records: Sequence[ScanRecord], outdir: Path) -> Dict[str, str]:
    outdir.mkdir(parents=True, exist_ok=True)
    csv_path = outdir / "records.csv"
    jsonl_path = outdir / "records.jsonl"

    sorted_records = sorted(records, key=lambda r: r.created_at, reverse=True)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = list(sorted_records[0].as_csv_row().keys()) if sorted_records else [
            "platform",
            "source_mode",
            "source_label",
            "platform_id",
            "created_at",
            "author",
            "title",
            "text",
            "url",
            "permalink",
            "subreddit",
            "lang",
            "score",
            "num_comments",
            "like_count",
            "reply_count",
            "repost_count",
            "quote_count",
            "bookmark_count",
            "impression_count",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in sorted_records:
            writer.writerow(record.as_csv_row())

    with jsonl_path.open("w", encoding="utf-8") as f:
        for record in sorted_records:
            f.write(json.dumps(record.as_json(), ensure_ascii=False) + "\n")

    return {"csv": str(csv_path), "jsonl": str(jsonl_path)}


def write_meta(
    *,
    outdir: Path,
    args: argparse.Namespace,
    result: ScanResult,
    output_paths: Dict[str, str],
) -> str:
    meta = {
        "generated_at": datetime.now(UTC).isoformat(),
        "parameters": vars(args),
        "record_count": len(result.records),
        "counts_by_platform": _count_by_platform(result.records),
        "warnings": result.warnings,
        "errors": result.errors,
        "outputs": output_paths,
    }
    meta_path = outdir / "scan_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return str(meta_path)


def _count_by_platform(records: Sequence[ScanRecord]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for record in records:
        counts[record.platform] = counts.get(record.platform, 0) + 1
    return counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan the last N days of Reddit and/or X and export normalized results.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days.")
    parser.add_argument("--outdir", default="./scan_output", help="Directory for CSV / JSONL / meta outputs.")

    reddit = parser.add_argument_group("Reddit")
    reddit.add_argument(
        "--reddit-subreddits",
        default="",
        help="Comma-separated subreddits to scan, e.g. MachineLearning,OpenAI.",
    )
    reddit.add_argument(
        "--reddit-query",
        default=None,
        help="Optional Reddit search query. If omitted and subreddits are provided, the tool scans recent posts in those subreddits.",
    )
    reddit.add_argument(
        "--reddit-max-per-subreddit",
        type=int,
        default=1000,
        help="Configured cap per subreddit scan. Reddit listings/searches usually top out around 1000 items anyway.",
    )
    reddit.add_argument(
        "--reddit-max-global-results",
        type=int,
        default=1000,
        help="Configured cap for global Reddit search across r/all.",
    )

    x_group = parser.add_argument_group("X")
    x_group.add_argument(
        "--x-query",
        default=None,
        help="X search query for platform-wide search. 30-day search requires full-archive access.",
    )
    x_group.add_argument(
        "--x-usernames",
        default="",
        help="Comma-separated X usernames to scan via timelines, e.g. openai,sama.",
    )
    x_group.add_argument(
        "--x-max-search-posts",
        type=int,
        default=5000,
        help="Hard cap for total posts pulled from X search/all.",
    )
    x_group.add_argument(
        "--x-max-posts-per-user",
        type=int,
        default=1000,
        help="Hard cap per X user timeline scan.",
    )
    x_group.add_argument(
        "--x-exclude-replies",
        action="store_true",
        help="Exclude replies from X results.",
    )
    x_group.add_argument(
        "--x-exclude-retweets",
        action="store_true",
        help="Exclude retweets/reposts from X results.",
    )

    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.days <= 0:
        raise ToolError("--days must be a positive integer.")
    if args.reddit_max_per_subreddit <= 0 or args.reddit_max_global_results <= 0:
        raise ToolError("Reddit max result caps must be positive integers.")
    if args.x_max_search_posts <= 0 or args.x_max_posts_per_user <= 0:
        raise ToolError("X max result caps must be positive integers.")
    if not any(
        [
            args.reddit_query,
            args.reddit_subreddits,
            args.x_query,
            args.x_usernames,
        ]
    ):
        raise ToolError(
            "Nothing to scan. Provide Reddit and/or X options such as --reddit-subreddits, --reddit-query, --x-query, or --x-usernames."
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_args(args)
    except ToolError as exc:
        parser.error(str(exc))

    outdir = Path(args.outdir)
    result = ScanResult()

    reddit_srs = parse_csv_list(args.reddit_subreddits)
    x_usernames = parse_csv_list(args.x_usernames)

    if args.reddit_query or reddit_srs:
        try:
            reddit_scanner = RedditScanner.from_env()
            result.extend(
                reddit_scanner.scan(
                    days=args.days,
                    subreddits=reddit_srs,
                    query=args.reddit_query,
                    max_per_subreddit=args.reddit_max_per_subreddit,
                    max_global_results=args.reddit_max_global_results,
                )
            )
        except ToolError as exc:
            result.errors.append(f"Reddit setup failed: {exc}")

    if args.x_query or x_usernames:
        try:
            x_scanner = XScanner.from_env()
            result.extend(
                x_scanner.scan(
                    days=args.days,
                    search_query=args.x_query,
                    usernames=x_usernames,
                    max_search_posts=args.x_max_search_posts,
                    max_posts_per_user=args.x_max_posts_per_user,
                    exclude_replies=args.x_exclude_replies,
                    exclude_retweets=args.x_exclude_retweets,
                )
            )
        except ToolError as exc:
            result.errors.append(f"X setup failed: {exc}")

    # Deduplicate by platform + id while preserving most recent ordering later.
    deduped: Dict[Tuple[str, str], ScanRecord] = {}
    for record in result.records:
        deduped[(record.platform, record.platform_id)] = record
    result.records = list(deduped.values())

    output_paths = write_outputs(result.records, outdir)
    meta_path = write_meta(outdir=outdir, args=args, result=result, output_paths=output_paths)

    eprint(f"[done] wrote {len(result.records)} records to {outdir}")
    if result.warnings:
        eprint("[warnings]")
        for warning in result.warnings:
            eprint(f"- {warning}")
    if result.errors:
        eprint("[errors]")
        for error in result.errors:
            eprint(f"- {error}")

    print(json.dumps({
        "records": len(result.records),
        "warnings": len(result.warnings),
        "errors": len(result.errors),
        "outputs": output_paths,
        "meta": meta_path,
    }, indent=2))

    return 0 if result.records or not result.errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
