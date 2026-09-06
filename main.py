#!/usr/bin/env python3
"""Mirror Navidrome playback to Discord and Stats.fm from one process."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiohttp
import discord
from dotenv import load_dotenv
from spotify_scraper import SpotifyClient


BASE_DIR = Path(__file__).resolve().parent
SKIP_THRESHOLD = 0.70
POSITION_RESET_TOLERANCE_MS = 10_000
PRESENCE_SEEK_TOLERANCE_MS = 1_000
PRESENCE_STALE_AFTER_SECONDS = 20
SPOTIFY_RETRY_DELAYS_SECONDS = (5, 10, 30, 60)
STATSFM_IMPORT_URL = "https://import.stats.fm/api/v1/me/imports/SPOTIFY"
SPOTIFY_ACTIVITY_ID = "spotify:1"
TERMINAL_STATES = {"stopped", "skipped"}
logger = logging.getLogger("navidrome-discord-scrobbler")


class ConfigurationError(ValueError):
    pass


class ServiceError(RuntimeError):
    pass


@dataclass(frozen=True)
class Settings:
    discord_token: str
    navidrome_url: str
    navidrome_username: str
    navidrome_password: str
    poll_interval: float
    statsfm_token: str | None
    statsfm_batch_size: int
    history_file: Path

    @classmethod
    def from_env(cls) -> Settings:
        required = {
            "DISCORD_TOKEN": os.getenv("DISCORD_TOKEN", "").strip(),
            "NAVIDROME_URL": os.getenv("NAVIDROME_URL", "").strip(),
            "NAVIDROME_USERNAME": os.getenv("NAVIDROME_USERNAME", "").strip(),
            "NAVIDROME_PASSWORD": os.getenv("NAVIDROME_PASSWORD", "").strip(),
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            raise ConfigurationError(f"Missing .env settings: {', '.join(missing)}")

        try:
            interval = float(os.getenv("POLL_INTERVAL_SECONDS", "5"))
            batch_size = int(os.getenv("STATS_FM_BATCH_SIZE", "1"))
        except ValueError as exc:
            raise ConfigurationError(
                "POLL_INTERVAL_SECONDS and STATS_FM_BATCH_SIZE must be numbers"
            ) from exc
        if interval <= 0 or batch_size <= 0:
            raise ConfigurationError(
                "POLL_INTERVAL_SECONDS and STATS_FM_BATCH_SIZE must be positive"
            )

        token = os.getenv("STATSFM_TOKEN", "").strip()
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        history_value = os.getenv("HISTORY_FILE", "spotify_history.json").strip()
        history_path = Path(history_value)
        if not history_path.is_absolute():
            history_path = BASE_DIR / history_path
        return cls(
            discord_token=required["DISCORD_TOKEN"],
            navidrome_url=required["NAVIDROME_URL"].rstrip("/").removesuffix("/rest"),
            navidrome_username=required["NAVIDROME_USERNAME"],
            navidrome_password=required["NAVIDROME_PASSWORD"],
            poll_interval=interval,
            statsfm_token=token or None,
            statsfm_batch_size=batch_size,
            history_file=history_path,
        )


@dataclass(frozen=True)
class Track:
    name: str
    artist: str
    album: str
    duration_ms: int
    navidrome_id: str
    position_ms: int | None
    state: str = "playing"
    spotify_id: str | None = None
    image_key: str | None = None


@dataclass(frozen=True)
class PlaybackSession:
    track: Track
    max_position_ms: int | None
    first_seen: float
    last_seen: float
    missing_since: float | None = None


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        body = b"Presence and scrobbler are ready!"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *args: object) -> None:
        return


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] > %(message)s",
        datefmt="%H:%M:%S",
    )


def start_health_server() -> ThreadingHTTPServer:
    host = os.getenv("HOST", "0.0.0.0")
    try:
        port = int(os.getenv("PORT", "3000"))
    except ValueError as exc:
        raise ConfigurationError("PORT must be an integer") from exc
    server = ThreadingHTTPServer((host, port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info("Health endpoint listening on http://%s:%s", host, port)
    return server


def nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        result = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None


def parse_now_playing(payload: dict[str, Any], username: str) -> dict[str, Track]:
    root = payload.get("subsonic-response", payload)
    if not isinstance(root, dict) or root.get("status") != "ok":
        detail = root.get("error", {}) if isinstance(root, dict) else {}
        message = detail.get("message", "invalid response") if isinstance(detail, dict) else detail
        raise ServiceError(f"Navidrome getNowPlaying failed: {message}")
    container = root.get("nowPlaying", {})
    entries = container.get("entry", []) if isinstance(container, dict) else []
    if isinstance(entries, dict):
        entries = [entries]
    if not isinstance(entries, list):
        raise ServiceError("Navidrome returned invalid now-playing entries")

    tracks: dict[str, Track] = {}
    for item in entries:
        if not isinstance(item, dict) or item.get("username") != username or not item.get("id"):
            continue
        try:
            duration_ms = int(float(item.get("duration", 0)) * 1000)
        except (TypeError, ValueError):
            duration_ms = 0
        position_ms = nonnegative_int(item.get("positionMs"))
        if position_ms is None:
            seconds = nonnegative_int(item.get("position"))
            position_ms = seconds * 1000 if seconds is not None else None
        track = Track(
            name=str(item.get("title", "")),
            artist=str(item.get("artist", "")),
            album=str(item.get("album", "")),
            duration_ms=max(0, duration_ms),
            navidrome_id=str(item["id"]),
            position_ms=position_ms,
            state=str(item.get("state", "playing")).lower(),
        )
        tracks[str(item.get("playerId") or track.navidrome_id)] = track
    return tracks


class NavidromeClient:
    def __init__(self, settings: Settings, session: aiohttp.ClientSession) -> None:
        self.settings = settings
        self.session = session

    async def now_playing(self) -> tuple[dict[str, Track], int]:
        salt = secrets.token_hex(8)
        token = hashlib.md5(
            f"{self.settings.navidrome_password}{salt}".encode(), usedforsecurity=False
        ).hexdigest()
        query = urlencode(
            {
                "u": self.settings.navidrome_username,
                "t": token,
                "s": salt,
                "v": "1.16.1",
                "c": "navidrome-discord-scrobbler",
                "f": "json",
            }
        )
        url = f"{self.settings.navidrome_url}/rest/getNowPlaying.view?{query}"
        request_started_ms = int(time.time() * 1000)
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    raise ServiceError(f"Navidrome returned HTTP {response.status}")
                payload = await response.json()
                response_received_ms = int(time.time() * 1000)
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
            raise ServiceError(f"Navidrome request failed: {exc}") from exc
        # Navidrome generated the reported position during this request. Using
        # the request midpoint compensates for network latency, while capturing
        # it here prevents the later Spotify lookup from shifting the clock.
        observed_at_ms = (request_started_ms + response_received_ms) // 2
        return (
            parse_now_playing(payload, self.settings.navidrome_username),
            observed_at_ms,
        )


def normalize(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", value.casefold()))


def match_score(query: str, track: Track) -> float:
    wanted = normalize(query)
    title = normalize(track.name)
    combined = normalize(f"{track.name} {track.artist}")
    sequence = max(
        SequenceMatcher(None, wanted, title).ratio(),
        SequenceMatcher(None, wanted, combined).ratio(),
    )
    wanted_tokens, result_tokens = set(wanted.split()), set(combined.split())
    overlap = (
        len(wanted_tokens & result_tokens) / len(wanted_tokens | result_tokens)
        if wanted_tokens and result_tokens
        else 0.0
    )
    return 0.75 * sequence + 0.25 * overlap


def spotify_image_key(images: object) -> str | None:
    candidates = list(images) if images else []  # type: ignore[arg-type]
    if not candidates:
        return None
    image = max(candidates, key=lambda item: int(getattr(item, "width", 0) or 0))
    image_id = str(getattr(image, "url", "")).rstrip("/").rsplit("/", 1)[-1]
    return f"spotify:{image_id}" if image_id else None


def search_spotify_sync(track: Track) -> Track:
    query = f"{track.name} {track.artist}".strip()
    try:
        with SpotifyClient() as client:
            results = client.search(query, types=("track",), limit=10)
    except Exception as exc:
        raise ServiceError(f"Spotify search failed: {exc}") from exc
    candidates = []
    for item in results.tracks:
        artists = ", ".join(artist.name for artist in item.artists)
        candidates.append(
            Track(
                name=item.name,
                artist=artists,
                album=item.album.name if item.album else track.album,
                duration_ms=int(item.duration_ms),
                navidrome_id=track.navidrome_id,
                position_ms=track.position_ms,
                state=track.state,
                spotify_id=str(item.id),
                image_key=spotify_image_key(
                    getattr(item, "images", None)
                    or getattr(getattr(item, "album", None), "images", None)
                ),
            )
        )
    if not candidates:
        raise ServiceError(f"Spotify found no match for {query}")
    return max(candidates, key=lambda item: match_score(query, item))


def start_session(track: Track, now: float) -> PlaybackSession:
    return PlaybackSession(track, track.position_ms, now, now)


def update_session(session: PlaybackSession, track: Track, now: float) -> PlaybackSession:
    positions = [p for p in (session.max_position_ms, track.position_ms) if p is not None]
    return PlaybackSession(track, max(positions) if positions else None, session.first_seen, now)


def mark_missing(session: PlaybackSession, now: float) -> PlaybackSession:
    return PlaybackSession(
        session.track,
        session.max_position_ms,
        session.first_seen,
        session.last_seen,
        session.missing_since if session.missing_since is not None else now,
    )


def is_restart(session: PlaybackSession, track: Track) -> bool:
    if session.track.navidrome_id != track.navidrome_id:
        return True
    if session.max_position_ms is None or track.position_ms is None:
        return False
    return track.position_ms + POSITION_RESET_TOLERANCE_MS < session.max_position_ms


def played_ms(session: PlaybackSession, now: float) -> int:
    if session.max_position_ms is None:
        observed = int(max(0, session.last_seen - session.first_seen) * 1000)
    else:
        observed = session.max_position_ms
        if session.track.state == "playing":
            until = session.missing_since if session.missing_since is not None else now
            observed += int(max(0, until - session.last_seen) * 1000)
    return min(max(observed, 0), max(session.track.duration_ms, 0))


def load_history(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ServiceError(f"History file is not valid JSON: {path}") from exc
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ServiceError(f"History file must contain a JSON array: {path}")
    return value


def history_entry(track: Track, actual_ms: int, template: dict[str, Any]) -> dict[str, Any]:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    ended = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return {
        "ts": timestamp,
        "username": template.get("username", ""),
        "platform": template.get("platform", "navidrome"),
        "ms_played": actual_ms,
        "conn_country": template.get("conn_country", "US"),
        "ip_addr_decrypted": template.get("ip_addr_decrypted"),
        "user_agent_decrypted": template.get("user_agent_decrypted"),
        "master_metadata_track_name": track.name,
        "master_metadata_album_artist_name": track.artist,
        "master_metadata_album_album_name": track.album,
        "spotify_track_uri": f"spotify:track:{track.spotify_id}",
        "episode_name": None,
        "episode_show_name": None,
        "spotify_episode_uri": None,
        "audiobook_title": None,
        "audiobook_uri": None,
        "audiobook_chapter_uri": None,
        "audiobook_chapter_title": None,
        "reason_start": "clickrow",
        "reason_end": "trackdone",
        "shuffle": False,
        "skipped": False,
        "offline": False,
        "offline_timestamp": int(ended.timestamp() - actual_ms / 1000),
        "incognito_mode": False,
    }


def build_spotify_activity(
    track: Track, *, start_ms: int, duration_ms: int, user_id: int
) -> discord.Spotify:
    """Build the same activity fields used by Discord's Spotify integration."""
    activity = discord.Spotify(
        title=track.name[:128],
        track_id=track.spotify_id,
        track_type="track" if track.spotify_id else None,
        artists=[track.artist[:128]],
        album=track.album[:128] or None,
        album_cover_url=track.image_key,
        start_time=datetime.fromtimestamp(start_ms / 1000, timezone.utc),
        duration=timedelta(milliseconds=duration_ms),
        party_owner_id=user_id,
    )
    activity.id = SPOTIFY_ACTIVITY_ID
    return activity


class UnifiedClient(discord.Client):
    def __init__(self, settings: Settings) -> None:
        super().__init__()
        # discord.Client already owns a read-only property named ``settings``.
        self.app_settings = settings
        self.http_session: aiohttp.ClientSession | None = None
        self.watcher_task: asyncio.Task[None] | None = None
        self.active: dict[str, PlaybackSession] = {}
        self.spotify_cache: dict[tuple[str, str], Track] = {}
        self.spotify_retry_after: dict[tuple[str, str], tuple[int, float]] = {}
        self.presence_key: tuple[str, str] | None = None
        self.presence_anchor_ms: int | None = None
        self.presence_signature: tuple[object, ...] | None = None
        self.presence_last_seen_at: float | None = None

    async def on_ready(self) -> None:
        logger.info("Logged in as %s", self.user)
        # A new gateway session has no knowledge of the previous activity.
        # Force the watcher to publish the current track again on its next poll.
        self.presence_signature = None
        if self.watcher_task is None or self.watcher_task.done():
            self.http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
            self.watcher_task = asyncio.create_task(self.watch(), name="navidrome-watcher")

    async def on_resumed(self) -> None:
        # A resumed gateway connection may have dropped the activity update.
        self.presence_signature = None
        logger.info("Discord gateway resumed; presence will be republished")

    async def close(self) -> None:
        if self.watcher_task is not None:
            self.watcher_task.cancel()
        if self.http_session is not None:
            await self.http_session.close()
        await super().close()

    async def spotify_match(self, track: Track) -> Track:
        key = (normalize(track.name), normalize(track.artist))
        if key not in self.spotify_cache:
            self.spotify_cache[key] = await asyncio.to_thread(search_spotify_sync, track)
        matched = self.spotify_cache[key]
        return Track(
            matched.name,
            matched.artist,
            matched.album,
            matched.duration_ms,
            track.navidrome_id,
            track.position_ms,
            track.state,
            matched.spotify_id,
            matched.image_key,
        )

    async def spotify_match_for_presence(self, track: Track) -> Track | None:
        """Resolve Spotify without letting a temporary failure remove the RPC."""
        key = (normalize(track.name), normalize(track.artist))
        now = time.monotonic()
        failure = self.spotify_retry_after.get(key)
        if failure is not None and now < failure[1]:
            return None
        try:
            matched = await self.spotify_match(track)
        except ServiceError as error:
            attempts = 1 if failure is None else failure[0] + 1
            delay = SPOTIFY_RETRY_DELAYS_SECONDS[
                min(attempts - 1, len(SPOTIFY_RETRY_DELAYS_SECONDS) - 1)
            ]
            self.spotify_retry_after[key] = (attempts, now + delay)
            logger.warning(
                "Spotify match unavailable for %s - %s; using fallback and retrying in %ss: %s",
                track.artist,
                track.name,
                delay,
                error,
            )
            return None
        self.spotify_retry_after.pop(key, None)
        return matched

    async def clear_presence(self, reason: str) -> bool:
        """Remove the Discord activity without affecting the user's status."""
        if self.presence_signature is None:
            self.presence_key = None
            self.presence_anchor_ms = None
            self.presence_last_seen_at = None
            return True
        try:
            await self.change_presence(activity=None)
        except Exception as error:
            # Keep the local state intact so the following polling cycle retries
            # the clear instead of forgetting an activity Discord still shows.
            logger.warning("Could not clear Discord presence: %s", error)
            return False
        self.presence_signature = None
        self.presence_key = None
        self.presence_anchor_ms = None
        self.presence_last_seen_at = None
        logger.info("Discord presence cleared (%s)", reason)
        return True

    async def expire_stale_presence(self) -> None:
        if self.presence_signature is None or self.presence_last_seen_at is None:
            return
        age = time.monotonic() - self.presence_last_seen_at
        if age >= PRESENCE_STALE_AFTER_SECONDS:
            await self.clear_presence(
                f"no Navidrome playback confirmation for {age:.0f} seconds"
            )

    async def show_presence(
        self, tracks: dict[str, Track], observed_at_ms: int
    ) -> None:
        playing = next((item for item in tracks.items() if item[1].state == "playing"), None)
        if playing is None:
            await self.clear_presence("no active Navidrome track")
            return

        player_id, source = playing
        key = (player_id, source.navidrome_id)
        # Do not leave the previous track visible while a fresh Spotify lookup
        # is in progress or fails for the new one.
        if self.presence_key is not None and self.presence_key != key:
            await self.clear_presence("Navidrome track changed")
        self.presence_last_seen_at = time.monotonic()

        position = source.position_ms or 0
        if self.presence_key != key or self.presence_anchor_ms is None:
            self.presence_key = key
            self.presence_anchor_ms = observed_at_ms - position
        else:
            predicted = observed_at_ms - self.presence_anchor_ms
            if abs(predicted - position) > PRESENCE_SEEK_TOLERANCE_MS:
                self.presence_anchor_ms = observed_at_ms - position
        matched = await self.spotify_match_for_presence(source)
        display_track = matched or source
        # sync_id identifies the Spotify result, so its exact duration must
        # define the Discord end time. Navidrome commonly reports whole seconds.
        duration_ms = matched.duration_ms if matched is not None else source.duration_ms
        end_ms = self.presence_anchor_ms + duration_ms
        signature = (
            key,
            display_track.spotify_id,
            display_track.name,
            display_track.artist,
            display_track.image_key,
            self.presence_anchor_ms // 1000,
            end_ms // 1000,
        )
        if signature == self.presence_signature:
            return
        if self.user is None:
            return
        activity = build_spotify_activity(
            display_track,
            start_ms=self.presence_anchor_ms,
            duration_ms=duration_ms,
            user_id=self.user.id,
        )
        try:
            await self.change_presence(activity=activity)
        except Exception as error:
            # Do not record the signature on failure. This causes a retry on
            # the next poll instead of letting an old Discord activity persist.
            logger.warning("Could not update Discord presence: %s", error)
            return
        self.presence_signature = signature
        if matched is None:
            logger.info(
                "Discord fallback presence: %s - %s (awaiting Spotify match)",
                source.artist,
                source.name,
            )
        else:
            logger.info("Discord now playing: %s - %s", matched.artist, matched.name)

    async def record(self, session: PlaybackSession, now: float) -> None:
        actual = played_ms(session, now)
        duration = max(session.track.duration_ms, 0)
        if duration <= 0 or actual <= duration * SKIP_THRESHOLD:
            logger.info(
                "Ignored skipped: %s - %s (%s/%s ms)",
                session.track.artist,
                session.track.name,
                actual,
                duration,
            )
            return
        matched = await self.spotify_match(session.track)
        actual = min(actual, matched.duration_ms)
        entries = load_history(self.app_settings.history_file)
        template = entries[-1] if entries else {}
        entries.append(history_entry(matched, actual, template))
        self.app_settings.history_file.parent.mkdir(parents=True, exist_ok=True)
        self.app_settings.history_file.write_text(
            json.dumps(entries, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        logger.info("Recorded completed: %s - %s (%s ms)", matched.artist, matched.name, actual)
        await self.upload_pending(entries)

    async def upload_pending(self, entries: list[dict[str, Any]]) -> None:
        if not self.app_settings.statsfm_token or self.http_session is None:
            return
        state_path = self.app_settings.history_file.with_name(
            f"{self.app_settings.history_file.stem}.statsfm-state.json"
        )
        uploaded = 0
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
                uploaded = int(state.get("uploaded_count", 0))
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                uploaded = 0
        if not 0 <= uploaded <= len(entries):
            uploaded = 0
        pending = entries[uploaded:]
        if len(pending) < self.app_settings.statsfm_batch_size:
            return
        compact = [
            {
                "ts": item["ts"],
                "ms": item["ms_played"],
                "tn": item["master_metadata_track_name"],
                "ti": str(item["spotify_track_uri"]).removeprefix("spotify:track:"),
                "platform": item.get("platform", "navidrome"),
            }
            for item in pending
        ]
        form = aiohttp.FormData()
        form.add_field(
            "files",
            json.dumps(compact, separators=(",", ":")).encode(),
            filename="spotify_history.json",
            content_type="application/json",
        )
        headers = {
            "Authorization": f"Bearer {self.app_settings.statsfm_token}",
            "User-Agent": "NavidromeDiscordScrobbler/2.0",
        }
        try:
            async with self.http_session.post(STATSFM_IMPORT_URL, data=form, headers=headers) as response:
                body = (await response.text()).strip()
                if response.status not in {200, 201, 202}:
                    raise ServiceError(f"Stats.fm returned HTTP {response.status}: {body[:300]}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise ServiceError(f"Stats.fm upload failed: {exc}") from exc
        # Only discard local history after Stats.fm accepted the upload. Reset
        # the cursor as well so the next recorded listen starts a fresh batch.
        self.app_settings.history_file.write_text("[]\n", encoding="utf-8")
        state_path.write_text(
            json.dumps({"uploaded_count": 0}, indent=2) + "\n", encoding="utf-8"
        )
        logger.info(
            "Stats.fm accepted %s listens; local Spotify history cleared",
            len(pending),
        )

    async def watch(self) -> None:
        assert self.http_session is not None
        navidrome = NavidromeClient(self.app_settings, self.http_session)
        logger.info("Watching Navidrome for %s", self.app_settings.navidrome_username)
        while not self.is_closed():
            try:
                current, observed_at_ms = await navidrome.now_playing()
                await self.show_presence(current, observed_at_ms)
                now = time.monotonic()
                remaining = dict(self.active)
                next_active: dict[str, PlaybackSession] = {}
                finished: list[PlaybackSession] = []
                for player_id, track in current.items():
                    previous = remaining.pop(player_id, None)
                    if previous is None:
                        if track.state not in TERMINAL_STATES:
                            next_active[player_id] = start_session(track, now)
                    elif track.state in TERMINAL_STATES:
                        finished.append(update_session(previous, track, now))
                    elif is_restart(previous, track):
                        finished.append(previous)
                        next_active[player_id] = start_session(track, now)
                    else:
                        next_active[player_id] = update_session(previous, track, now)
                for player_id, session in remaining.items():
                    next_active[player_id] = mark_missing(session, now)
                self.active = next_active
                for session in finished:
                    try:
                        await self.record(session, now)
                    except ServiceError as exc:
                        logger.error("Could not process completed listen: %s", exc)
            except ServiceError as exc:
                logger.warning("%s; retrying in %s seconds", exc, self.app_settings.poll_interval)
                await self.expire_stale_presence()
            except Exception:
                # A Discord gateway exception must not terminate the watcher.
                logger.exception(
                    "Unexpected watcher failure; retrying in %s seconds",
                    self.app_settings.poll_interval,
                )
                await self.expire_stale_presence()
            await asyncio.sleep(self.app_settings.poll_interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mirror Navidrome playback to Discord and Stats.fm"
    )
    parser.add_argument("--check", action="store_true", help="validate .env without connecting")
    parser.add_argument("--no-health", action="store_true", help="disable the health endpoint")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    load_dotenv(BASE_DIR / ".env")
    args = parse_args()
    try:
        settings = Settings.from_env()
        if args.check:
            logger.info("Configuration is valid")
            return 0
        client = UnifiedClient(settings)
        health = None if args.no_health else start_health_server()
        try:
            client.run(settings.discord_token, log_handler=None)
        finally:
            if health is not None:
                health.shutdown()
                health.server_close()
    except (ConfigurationError, ServiceError, OSError) as exc:
        logger.error("%s", exc)
        return 1
    except KeyboardInterrupt:
        logger.info("Stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
