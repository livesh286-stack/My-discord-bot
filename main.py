from __future__ import annotations

import ast
import asyncio
import io
import json
import logging
import os
import random
import re
import shlex
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

import aiosqlite
import discord
import yt_dlp
from discord import app_commands
from discord.ext import commands, tasks


BOT_NAME = "Sentinel"
DATABASE_PATH = Path(os.getenv("BOT_DB_PATH", "data/discord_bot.sqlite3"))
MAX_TIMEOUT_MINUTES = 28 * 24 * 60
INVITE_RE = re.compile(r"(?:discord\.gg/|discord(?:app)?\.com/invite/)[\w-]+", re.I)
URL_RE = re.compile(r"https?://\S+", re.I)
log = logging.getLogger("sentinel")


def utc_now() -> datetime:
    return discord.utils.utcnow()


def clean_reason(reason: str | None) -> str:
    text = (reason or "No reason provided").strip()
    return text[:500] or "No reason provided"


def mention_suppressed() -> discord.AllowedMentions:
    return discord.AllowedMentions.none()


def config_key(key: str) -> str:
    return key


async def db_execute(query: str, values: tuple[Any, ...] = ()) -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(query, values)
        await db.commit()
        return cursor.lastrowid or cursor.rowcount


async def db_fetchone(query: str, values: tuple[Any, ...] = ()) -> aiosqlite.Row | None:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, values) as cursor:
            return await cursor.fetchone()


async def db_fetchall(query: str, values: tuple[Any, ...] = ()) -> list[aiosqlite.Row]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, values) as cursor:
            return await cursor.fetchall()


async def get_setting(guild_id: int, key: str, default: str | None = None) -> str | None:
    row = await db_fetchone(
        "SELECT value FROM guild_settings WHERE guild_id = ? AND key = ?",
        (guild_id, config_key(key)),
    )
    return str(row["value"]) if row else default


async def set_setting(guild_id: int, key: str, value: str | None) -> None:
    if value is None:
        await db_execute(
            "DELETE FROM guild_settings WHERE guild_id = ? AND key = ?",
            (guild_id, config_key(key)),
        )
        return
    await db_execute(
        """
        INSERT INTO guild_settings (guild_id, key, value) VALUES (?, ?, ?)
        ON CONFLICT(guild_id, key) DO UPDATE SET value = excluded.value
        """,
        (guild_id, config_key(key), value),
    )


async def initialize_database() -> None:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (guild_id, key)
            );

            CREATE TABLE IF NOT EXISTS warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                moderator_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS warnings_lookup
                ON warnings (guild_id, user_id, active, id);

            CREATE TABLE IF NOT EXISTS automod_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS automod_lookup
                ON automod_events (guild_id, user_id, id);

            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL UNIQUE,
                user_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                claimed_by INTEGER,
                panel_message_id INTEGER,
                created_at TEXT NOT NULL,
                closed_at TEXT
            );
            CREATE INDEX IF NOT EXISTS tickets_open_user
                ON tickets (guild_id, user_id, status);

            CREATE TABLE IF NOT EXISTS minecraft_monitors (
                guild_id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                address TEXT NOT NULL,
                bedrock INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS voice_settings (
                guild_id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                afk_247 INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        await db.commit()


def make_embed(
    title: str,
    description: str | None = None,
    *,
    color: discord.Colour = discord.Colour.blurple(),
) -> discord.Embed:
    embed = discord.Embed(title=title[:256], description=(description or "")[:4000], color=color)
    embed.timestamp = utc_now()
    return embed


async def respond(
    interaction: discord.Interaction,
    content: str | None = None,
    *,
    embed: discord.Embed | None = None,
    ephemeral: bool = True,
) -> None:
    kwargs: dict[str, Any] = {
        "content": content,
        "embed": embed,
        "ephemeral": ephemeral,
        "allowed_mentions": mention_suppressed(),
    }
    if interaction.response.is_done():
        await interaction.followup.send(**kwargs)
    else:
        await interaction.response.send_message(**kwargs)


async def log_action(
    guild: discord.Guild,
    title: str,
    description: str,
    *,
    moderator: discord.abc.User | None = None,
    color: discord.Colour = discord.Colour.orange(),
    file: discord.File | None = None,
    setting_key: str = "log_channel",
) -> None:
    channel_id = await get_setting(guild.id, setting_key)
    if not channel_id:
        return
    channel = guild.get_channel(int(channel_id))
    if not isinstance(channel, discord.TextChannel):
        return
    embed = make_embed(title, description, color=color)
    if moderator:
        embed.set_footer(text=f"By {moderator} • ID {moderator.id}")
    try:
        await channel.send(
            embed=embed,
            file=file,
            allowed_mentions=mention_suppressed(),
        )
    except (discord.Forbidden, discord.HTTPException):
        log.warning("Could not post a moderation log in guild %s", guild.id)


def moderation_block_reason(
    interaction: discord.Interaction,
    target: discord.Member,
) -> str | None:
    guild = interaction.guild
    actor = interaction.user
    if guild is None or not isinstance(actor, discord.Member):
        return "This command can only be used in a server."
    if target.id == guild.owner_id:
        return "The server owner cannot be moderated."
    if target.id == actor.id:
        return "You cannot use this action on yourself."
    if target.id == interaction.client.user.id:
        return "I cannot moderate myself."
    if actor.id != guild.owner_id and target.top_role >= actor.top_role:
        return "Your highest role must be above the target's highest role."
    bot_member = guild.me
    if bot_member and target.top_role >= bot_member.top_role:
        return "My highest role must be above the target's highest role."
    return None


def safe_calc(expression: str) -> float | int:
    if len(expression) > 100:
        raise ValueError("Expression is too long.")
    tree = ast.parse(expression, mode="eval")
    operators: dict[type[ast.operator], Any] = {
        ast.Add: lambda a, b: a + b,
        ast.Sub: lambda a, b: a - b,
        ast.Mult: lambda a, b: a * b,
        ast.Div: lambda a, b: a / b,
        ast.FloorDiv: lambda a, b: a // b,
        ast.Mod: lambda a, b: a % b,
        ast.Pow: lambda a, b: a**b,
    }
    unary: dict[type[ast.unaryop], Any] = {
        ast.UAdd: lambda value: value,
        ast.USub: lambda value: -value,
    }

    def evaluate(node: ast.AST) -> float | int:
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            if abs(node.value) > 1e100:
                raise ValueError("Number is too large.")
            return node.value
        if isinstance(node, ast.UnaryOp) and type(node.op) in unary:
            return unary[type(node.op)](evaluate(node.operand))
        if isinstance(node, ast.BinOp) and type(node.op) in operators:
            left, right = evaluate(node.left), evaluate(node.right)
            if isinstance(node.op, ast.Pow) and abs(right) > 100:
                raise ValueError("Power must be between -100 and 100.")
            value = operators[type(node.op)](left, right)
            if abs(value) > 1e100:
                raise ValueError("Result is too large.")
            return value
        raise ValueError("Only basic arithmetic is allowed.")

    result = evaluate(tree)
    if isinstance(result, float) and (result != result or abs(result) == float("inf")):
        raise ValueError("That calculation does not produce a finite result.")
    return result


@dataclass(slots=True)
class MusicTrack:
    title: str
    webpage_url: str
    duration: int | None
    requester_id: int
    text_channel_id: int


@dataclass
class GuildMusicState:
    queue: deque[MusicTrack] = field(default_factory=deque)
    current: MusicTrack | None = None
    volume: float = 1.0
    loop_mode: str = "off"
    afk_247: bool = False
    skip_current: bool = False
    generation: int = 0
    start_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class SentinelBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.moderation = True
        intents.guild_messages = True
        intents.message_content = True
        intents.voice_states = True
        super().__init__(
            command_prefix=commands.when_mentioned_or("$"),
            intents=intents,
            help_command=None,
            case_insensitive=True,
            allowed_mentions=mention_suppressed(),
        )
        self.spam_cache: defaultdict[tuple[int, int], deque[float]] = defaultdict(deque)
        self.ticket_locks: defaultdict[tuple[int, int], asyncio.Lock] = defaultdict(asyncio.Lock)
        self.music_states: dict[int, GuildMusicState] = {}

    async def setup_hook(self) -> None:
        await initialize_database()
        self.add_view(TicketPanelView(self))
        open_tickets = await db_fetchall(
            "SELECT panel_message_id FROM tickets WHERE status = 'open' AND panel_message_id IS NOT NULL"
        )
        for row in open_tickets:
            self.add_view(TicketControlsView(self), message_id=int(row["panel_message_id"]))

        guild_id = os.getenv("DEV_GUILD_ID")
        try:
            if guild_id:
                dev_guild = discord.Object(id=int(guild_id))
                self.tree.copy_global_to(guild=dev_guild)
                synced = await self.tree.sync(guild=dev_guild)
                log.info("Synced %s commands to development server %s", len(synced), guild_id)
            else:
                synced = await self.tree.sync()
                log.info("Synced %s global commands", len(synced))
        except (ValueError, discord.HTTPException) as exc:
            log.exception("Slash-command sync failed: %s", exc)

        if not update_presence.is_running():
            update_presence.start()
        if not minecraft_status_refresh.is_running():
            minecraft_status_refresh.start()
        if not voice_reconnect.is_running():
            voice_reconnect.start()

    async def create_ticket(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        member = interaction.user
        if guild is None or not isinstance(member, discord.Member):
            await respond(interaction, "Tickets can only be opened inside a server.")
            return

        lock = self.ticket_locks[(guild.id, member.id)]
        async with lock:
            existing = await db_fetchone(
                "SELECT channel_id FROM tickets WHERE guild_id = ? AND user_id = ? AND status = 'open'",
                (guild.id, member.id),
            )
            if existing:
                channel = guild.get_channel(int(existing["channel_id"]))
                if channel:
                    await respond(interaction, f"You already have an open ticket: {channel.mention}")
                    return
                await db_execute(
                    "UPDATE tickets SET status = 'closed', closed_at = ? WHERE channel_id = ?",
                    (utc_now().isoformat(), int(existing["channel_id"])),
                )

            category_id = await get_setting(guild.id, "ticket_category")
            if not category_id:
                await respond(interaction, "Tickets are not configured yet. Ask a server admin to run `/ticket setup`.")
                return
            category = guild.get_channel(int(category_id))
            if not isinstance(category, discord.CategoryChannel):
                await respond(interaction, "The saved ticket category is missing. Ask an admin to run `/ticket setup` again.")
                return

            overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False)
            }
            overwrites[member] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
            )
            bot_member = guild.me
            if bot_member:
                overwrites[bot_member] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                    manage_channels=True,
                    attach_files=True,
                    embed_links=True,
                )
            staff_id = await get_setting(guild.id, "ticket_staff_role")
            if staff_id:
                staff_role = guild.get_role(int(staff_id))
                if staff_role:
                    overwrites[staff_role] = discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                        read_message_history=True,
                        attach_files=True,
                        embed_links=True,
                    )

            base_name = re.sub(r"[^a-z0-9-]", "-", member.name.lower()).strip("-")[:75] or "member"
            try:
                channel = await guild.create_text_channel(
                    name=f"ticket-{base_name}",
                    category=category,
                    overwrites=overwrites,
                    topic=f"Support ticket for {member} ({member.id})",
                    reason=f"Ticket opened by {member}",
                )
                first_message = await channel.send(
                    content=f"{member.mention} Thanks for contacting the team. Share the details here; use the close button when finished.",
                    embed=make_embed("Support ticket", "Only you and the support team can see this channel."),
                    view=TicketControlsView(self),
                    allowed_mentions=mention_suppressed(),
                )
                await db_execute(
                    """
                    INSERT INTO tickets (guild_id, channel_id, user_id, status, panel_message_id, created_at)
                    VALUES (?, ?, ?, 'open', ?, ?)
                    """,
                    (guild.id, channel.id, member.id, first_message.id, utc_now().isoformat()),
                )
                await respond(interaction, f"Your private ticket is ready: {channel.mention}")
                await log_action(
                    guild,
                    "Ticket opened",
                    f"Channel: {channel.mention}\nUser: {member.mention} (`{member.id}`)",
                    moderator=member,
                    color=discord.Colour.green(),
                )
            except (discord.Forbidden, discord.HTTPException, aiosqlite.Error):
                log.exception("Could not create a ticket for user %s in guild %s", member.id, guild.id)
                await respond(interaction, "I could not create the ticket. Check my Manage Channels and View Channel permissions.")

    async def close_ticket(
        self,
        interaction: discord.Interaction,
        reason: str | None = None,
    ) -> str:
        guild, channel, user = interaction.guild, interaction.channel, interaction.user
        if guild is None or not isinstance(channel, discord.TextChannel):
            return "This can only be used inside a ticket channel."
        ticket = await db_fetchone(
            "SELECT * FROM tickets WHERE guild_id = ? AND channel_id = ? AND status = 'open'",
            (guild.id, channel.id),
        )
        if not ticket:
            return "This channel is not an open ticket."
        staff_id = await get_setting(guild.id, "ticket_staff_role")
        is_staff = isinstance(user, discord.Member) and (
            user.guild_permissions.manage_channels
            or (staff_id is not None and any(role.id == int(staff_id) for role in user.roles))
        )
        if user.id != int(ticket["user_id"]) and not is_staff:
            return "Only the ticket owner or support staff can close this ticket."

        transcript_lines: list[str] = [
            f"Ticket transcript — #{channel.name}",
            f"Server: {guild.name} ({guild.id})",
            f"Opened by: {ticket['user_id']}",
            f"Closed by: {user} ({user.id})",
            f"Reason: {clean_reason(reason)}",
            "",
        ]
        try:
            async for message in channel.history(limit=1000, oldest_first=True):
                stamp = message.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
                body = message.content.replace("\n", " ")
                if message.attachments:
                    body += " " + " ".join(item.url for item in message.attachments)
                if message.embeds:
                    body += " [embed]"
                transcript_lines.append(f"[{stamp}] {message.author} ({message.author.id}): {body or '[no text]'}")
            transcript = "\n".join(transcript_lines)
            transcript_file = discord.File(
                io.BytesIO(transcript.encode("utf-8", errors="replace")),
                filename=f"ticket-{channel.id}-transcript.txt",
            )
            await log_action(
                guild,
                "Ticket closed",
                f"Channel: #{channel.name} (`{channel.id}`)\nClosed by: {user.mention}\nReason: {clean_reason(reason)}",
                moderator=user,
                color=discord.Colour.dark_grey(),
                file=transcript_file,
                setting_key="ticket_log_channel",
            )
            owner = guild.get_member(int(ticket["user_id"]))
            if owner:
                overwrite = channel.overwrites_for(owner)
                overwrite.view_channel = False
                await channel.set_permissions(owner, overwrite=overwrite, reason="Ticket closed")
            await channel.edit(name=f"closed-{channel.id}", topic=f"Closed by {user} • {clean_reason(reason)}")
            await db_execute(
                "UPDATE tickets SET status = 'closed', closed_at = ? WHERE channel_id = ?",
                (utc_now().isoformat(), channel.id),
            )
            return "Ticket closed. The transcript was sent to the configured log channel when one is set."
        except (discord.Forbidden, discord.HTTPException):
            log.exception("Could not close ticket channel %s", channel.id)
            return "I could not close this ticket. Check my channel permissions and try again."


bot = SentinelBot()


YOUTUBE_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com", "youtu.be"}
MAX_MUSIC_QUEUE = 50


def youtube_source_url(query: str) -> str:
    value = query.strip().removeprefix("<").removesuffix(">")
    parsed = urlparse(value)
    if parsed.scheme or parsed.netloc:
        if parsed.scheme != "https" or (parsed.hostname or "").casefold() not in YOUTUBE_HOSTS:
            raise ValueError("For links, use an HTTPS YouTube or youtu.be URL. Search terms are also supported.")
        return value
    if not value:
        raise ValueError("Enter a song name or a YouTube URL.")
    return f"ytsearch1:{value}"


def extract_music_track(query: str, requester_id: int, text_channel_id: int) -> MusicTrack:
    source = youtube_source_url(query)
    options: dict[str, Any] = {
        "format": "bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 15,
        "retries": 1,
    }
    with yt_dlp.YoutubeDL(options) as downloader:
        info = downloader.extract_info(source, download=False)
    if not isinstance(info, dict):
        raise ValueError("YouTube did not return a usable song.")
    if info.get("entries") is not None:
        info = next((entry for entry in info["entries"] if isinstance(entry, dict)), None)
    if not isinstance(info, dict):
        raise ValueError("No matching song was found.")
    webpage_url = str(info.get("webpage_url") or info.get("original_url") or "")
    if not webpage_url or youtube_source_url(webpage_url) != webpage_url:
        raise ValueError("The song source did not provide a valid YouTube link.")
    duration_value = info.get("duration")
    duration = int(duration_value) if isinstance(duration_value, (int, float)) and duration_value >= 0 else None
    return MusicTrack(
        title=str(info.get("title") or "Untitled track")[:250],
        webpage_url=webpage_url,
        duration=duration,
        requester_id=requester_id,
        text_channel_id=text_channel_id,
    )


def resolve_music_stream(webpage_url: str) -> str:
    if youtube_source_url(webpage_url) != webpage_url:
        raise ValueError("The saved song URL is not a valid YouTube link.")
    options: dict[str, Any] = {
        "format": "bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 15,
        "retries": 1,
    }
    with yt_dlp.YoutubeDL(options) as downloader:
        info = downloader.extract_info(webpage_url, download=False)
    if not isinstance(info, dict) or not info.get("url"):
        raise ValueError("YouTube did not provide a playable audio stream.")
    return str(info["url"])


async def get_music_state(guild_id: int) -> GuildMusicState:
    state = bot.music_states.get(guild_id)
    if state is None:
        try:
            volume = int(await get_setting(guild_id, "music_default_volume", "100") or "100")
        except ValueError:
            volume = 100
        state = GuildMusicState(volume=max(0, min(200, volume)) / 100)
        bot.music_states[guild_id] = state
    return state


def format_duration(seconds: int | None) -> str:
    if seconds is None:
        return "Live"
    minutes, remaining = divmod(max(seconds, 0), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}:{minutes:02}:{remaining:02}" if hours else f"{minutes}:{remaining:02}"


async def send_music_notice(track: MusicTrack, content: str | None = None) -> None:
    channel = bot.get_channel(track.text_channel_id)
    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        return
    try:
        await channel.send(
            content=content,
            embed=None if content else make_embed(
                "Now playing",
                f"**[{track.title}]({track.webpage_url})**\n"
                f"Duration: `{format_duration(track.duration)}`\n"
                f"Requested by <@{track.requester_id}>",
                color=discord.Colour.green(),
            ),
            allowed_mentions=mention_suppressed(),
        )
    except (discord.Forbidden, discord.HTTPException):
        log.warning("Could not post music status in channel %s", track.text_channel_id)


async def start_next_music_track(guild_id: int) -> None:
    state = bot.music_states.get(guild_id)
    guild = bot.get_guild(guild_id)
    if state is None or guild is None:
        return
    async with state.start_lock:
        voice = guild.voice_client
        if voice is None or not voice.is_connected():
            return
        if voice.is_playing() or voice.is_paused():
            return
        while state.current is not None or state.queue:
            if state.current is None:
                state.current = state.queue.popleft()
            track = state.current
            generation = state.generation
            try:
                stream_url = await asyncio.to_thread(resolve_music_stream, track.webpage_url)
                if state.generation != generation or state.current is not track:
                    return
                source = discord.FFmpegPCMAudio(
                    stream_url,
                    before_options="-nostdin -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5",
                    options="-vn -loglevel error",
                )
                audio = discord.PCMVolumeTransformer(source, volume=state.volume)
                event_loop = asyncio.get_running_loop()
                voice.play(
                    audio,
                    after=lambda error, current_guild_id=guild_id: asyncio.run_coroutine_threadsafe(
                        finish_music_track(current_guild_id, error),
                        event_loop,
                    ),
                )
            except Exception as exc:
                log.warning("Could not start music track in guild %s: %s", guild_id, exc)
                failed_track = state.current
                state.current = None
                await send_music_notice(failed_track, f"Could not play **{failed_track.title}**; skipping it.")
                continue
            await send_music_notice(track)
            return


async def finish_music_track(guild_id: int, error: Exception | None) -> None:
    state = bot.music_states.get(guild_id)
    if state is None:
        return
    if error:
        log.warning("Voice playback ended with an error in guild %s: %s", guild_id, error)
    if state.skip_current:
        state.skip_current = False
        state.current = None
    elif state.current and state.loop_mode == "track":
        pass
    elif state.current and state.loop_mode == "queue":
        state.queue.append(state.current)
        state.current = None
    else:
        state.current = None
    await start_next_music_track(guild_id)


async def connect_music_to_member(
    ctx: commands.Context[SentinelBot],
    *,
    move: bool = False,
    require_speak: bool = True,
) -> discord.VoiceClient | None:
    if ctx.guild is None or not isinstance(ctx.author, discord.Member):
        await ctx.send("Voice commands can only be used inside a server.")
        return None
    if not ctx.author.voice or not isinstance(ctx.author.voice.channel, discord.VoiceChannel):
        await ctx.send("Join a voice channel first.")
        return None
    channel = ctx.author.voice.channel
    bot_member = ctx.guild.me
    permissions = channel.permissions_for(bot_member) if bot_member else None
    if permissions is None or not permissions.connect or (require_speak and not permissions.speak):
        requirement = "Connect and Speak" if require_speak else "Connect"
        await ctx.send(f"I need {requirement} permission in that voice channel.")
        return None
    voice = ctx.guild.voice_client
    try:
        if voice and voice.is_connected():
            if voice.channel.id != channel.id:
                if not move:
                    await ctx.send(f"I'm already playing in {voice.channel.mention}. Join that channel first.")
                    return None
                await voice.move_to(channel)
        else:
            if voice:
                await voice.disconnect(force=True)
            voice = await channel.connect(timeout=20, reconnect=True, self_deaf=True)
    except (discord.ClientException, discord.Forbidden, discord.HTTPException, asyncio.TimeoutError) as exc:
        log.warning("Could not connect to voice channel %s: %s", channel.id, exc)
        await ctx.send("I could not join that voice channel. Check my Connect and Speak permissions.")
        return None
    state = await get_music_state(ctx.guild.id)
    if state.afk_247:
        await db_execute(
            """
            INSERT INTO voice_settings (guild_id, channel_id, afk_247, created_at, updated_at)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET channel_id = excluded.channel_id, updated_at = excluded.updated_at
            """,
            (ctx.guild.id, channel.id, utc_now().isoformat(), utc_now().isoformat()),
        )
    return voice


async def set_voice_247(
    guild: discord.Guild,
    channel: discord.VoiceChannel | None,
    enabled: bool,
) -> None:
    state = await get_music_state(guild.id)
    if enabled:
        if channel is None:
            raise ValueError("Join a voice channel before enabling 24/7 mode.")
        bot_member = guild.me
        permissions = channel.permissions_for(bot_member) if bot_member else None
        if permissions is None or not permissions.connect or not permissions.speak:
            raise ValueError("I need Connect and Speak permissions in that voice channel.")
        voice = guild.voice_client
        if voice and voice.is_connected():
            if voice.channel.id != channel.id:
                await voice.move_to(channel)
                await start_next_music_track(guild.id)
        else:
            if voice:
                await voice.disconnect(force=True)
            await channel.connect(timeout=20, reconnect=True, self_deaf=True)
        now = utc_now().isoformat()
        await db_execute(
            """
            INSERT INTO voice_settings (guild_id, channel_id, afk_247, created_at, updated_at)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                channel_id = excluded.channel_id,
                afk_247 = 1,
                updated_at = excluded.updated_at
            """,
            (guild.id, channel.id, now, now),
        )
        state.afk_247 = True
        await start_next_music_track(guild.id)
    else:
        state.afk_247 = False
        await db_execute("DELETE FROM voice_settings WHERE guild_id = ?", (guild.id,))


@tasks.loop(seconds=45)
async def voice_reconnect() -> None:
    rows = await db_fetchall("SELECT guild_id, channel_id FROM voice_settings WHERE afk_247 = 1")
    for row in rows:
        guild = bot.get_guild(int(row["guild_id"]))
        channel_id = int(row["channel_id"])
        channel = guild.get_channel(channel_id) if guild else None
        if guild is None or not isinstance(channel, discord.VoiceChannel):
            continue
        state = await get_music_state(guild.id)
        state.afk_247 = True
        voice = guild.voice_client
        try:
            if voice and voice.is_connected():
                if voice.channel.id != channel.id and not voice.is_playing():
                    await voice.move_to(channel)
                    await start_next_music_track(guild.id)
                continue
            if voice:
                await voice.disconnect(force=True)
            bot_member = guild.me
            permissions = channel.permissions_for(bot_member) if bot_member else None
            if permissions is None or not permissions.connect:
                continue
            await channel.connect(timeout=20, reconnect=True, self_deaf=True)
            log.info("Restored 24/7 voice connection in guild %s", guild.id)
            await start_next_music_track(guild.id)
        except (discord.ClientException, discord.Forbidden, discord.HTTPException, asyncio.TimeoutError) as exc:
            log.warning("Could not restore 24/7 voice connection in guild %s: %s", guild.id, exc)


@voice_reconnect.before_loop
async def before_voice_reconnect() -> None:
    await bot.wait_until_ready()


async def update_presence_once() -> None:
    activity = discord.Activity(
        type=discord.ActivityType.watching,
        name=f"over {len(bot.guilds)} servers • /help",
    )
    await bot.change_presence(status=discord.Status.online, activity=activity)


@tasks.loop(minutes=5)
async def update_presence() -> None:
    await update_presence_once()


@update_presence.before_loop
async def before_presence_update() -> None:
    await bot.wait_until_ready()


def minecraft_api_address(address: str) -> str:
    value = address.strip()
    if not value or len(value) > 253 or any(char.isspace() for char in value):
        raise ValueError("Enter a valid Minecraft hostname or IP with an optional port.")
    if value.startswith("[") and "]" in value:
        return value
    if value.count(":") > 1:
        raise ValueError("IPv6 addresses must be written in brackets, for example `[::1]:25565`.")
    return value


def fetch_minecraft_status(address: str, bedrock: bool = False) -> dict[str, Any]:
    api_kind = "bedrock/3" if bedrock else "3"
    encoded_address = quote(address, safe=":[],-._")
    request = Request(
        f"https://api.mcsrvstat.us/{api_kind}/{encoded_address}",
        headers={"User-Agent": "SentinelDiscordBot/1.0 (Minecraft status monitor)"},
    )
    with urlopen(request, timeout=15) as response:
        payload = response.read(1_000_000)
    data = json.loads(payload.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Minecraft status API returned an invalid response.")
    return data


async def get_minecraft_status(address: str, bedrock: bool = False) -> dict[str, Any]:
    return await asyncio.to_thread(fetch_minecraft_status, minecraft_api_address(address), bedrock)


def minecraft_text(value: Any, fallback: str = "Unknown") -> str:
    if value is None:
        return fallback
    if isinstance(value, list):
        return "\n".join(minecraft_text(item, "") for item in value if item is not None).strip() or fallback
    if isinstance(value, dict):
        for key in ("clean", "raw", "name", "value"):
            if value.get(key):
                return minecraft_text(value[key], fallback)
        return fallback
    text = re.sub(r"§[0-9A-FK-ORa-fk-or]", "", str(value)).strip()
    return text or fallback


def minecraft_status_embed(
    address: str,
    data: dict[str, Any],
    *,
    bedrock: bool = False,
    checked_at: datetime | None = None,
) -> discord.Embed:
    online = bool(data.get("online"))
    color = discord.Colour.green() if online else discord.Colour.red()
    state = "🟢 ONLINE" if online else "🔴 OFFLINE"
    title = f"Minecraft {'Bedrock' if bedrock else 'Java'} Server Status"
    embed = make_embed(title, f"**{state}**\n`{address}`", color=color)
    if online:
        players = data.get("players") or {}
        protocol = data.get("protocol") or {}
        version = minecraft_text(data.get("version") or protocol.get("name"))
        player_line = f"{players.get('online', 0)} / {players.get('max', '?')}"
        player_list = players.get("list") or []
        names = [
            minecraft_text(item.get("name") if isinstance(item, dict) else item, "")
            for item in player_list[:20]
        ]
        motd = minecraft_text((data.get("motd") or {}).get("clean") if isinstance(data.get("motd"), dict) else data.get("motd"))
        embed.add_field(name="Version", value=f"`{version}`", inline=True)
        embed.add_field(name="Players", value=f"`{player_line}`", inline=True)
        embed.add_field(name="Port", value=f"`{data.get('port', 'Unknown')}`", inline=True)
        embed.add_field(name="MOTD", value=motd[:1024], inline=False)
        if names:
            embed.add_field(name="Online players", value=", ".join(names)[:1024], inline=False)
        for label, key in (
            ("Software", "software"),
            ("Map", "map"),
            ("Gamemode", "gamemode"),
        ):
            if data.get(key):
                embed.add_field(name=label, value=minecraft_text(data[key])[:1024], inline=True)
        plugins = data.get("plugins") or []
        mods = data.get("mods") or []
        if plugins:
            embed.add_field(
                name=f"Plugins ({len(plugins)})",
                value=", ".join(minecraft_text(item.get("name") if isinstance(item, dict) else item, "") for item in plugins[:20])[:1024],
                inline=False,
            )
        if mods:
            embed.add_field(
                name=f"Mods ({len(mods)})",
                value=", ".join(minecraft_text(item.get("name") if isinstance(item, dict) else item, "") for item in mods[:20])[:1024],
                inline=False,
            )
        if data.get("ip"):
            embed.add_field(name="Resolved IP", value=f"`{data['ip']}`", inline=True)
    else:
        error_text = minecraft_text((data.get("debug") or {}).get("error"), "The server did not respond.")
        embed.add_field(name="Details", value=error_text[:1024], inline=False)
    embed.set_footer(text=f"Updates every 3 minutes • Last checked {checked_at or utc_now():%Y-%m-%d %H:%M UTC}")
    return embed


async def minecraft_monitor_update(
    guild: discord.Guild,
    *,
    channel_id: int,
    message_id: int,
    address: str,
    bedrock: bool,
) -> tuple[int, int] | None:
    channel = guild.get_channel(channel_id)
    if not isinstance(channel, discord.TextChannel):
        return None
    try:
        data = await get_minecraft_status(address, bedrock)
        embed = minecraft_status_embed(address, data, bedrock=bedrock)
    except Exception as exc:
        log.warning("Minecraft status lookup failed for %s: %s", address, exc)
        embed = make_embed(
            "Minecraft server status",
            f"🔴 **OFFLINE / UNREACHABLE**\n`{address}`\n\nCould not fetch a response right now. The next check will retry.",
            color=discord.Colour.red(),
        )
    try:
        message = await channel.fetch_message(message_id)
        await message.edit(embed=embed)
        return channel.id, message.id
    except discord.NotFound:
        try:
            message = await channel.send(embed=embed, allowed_mentions=mention_suppressed())
            return channel.id, message.id
        except (discord.Forbidden, discord.HTTPException):
            return None
    except (discord.Forbidden, discord.HTTPException):
        return None


async def save_minecraft_monitor(
    guild: discord.Guild,
    channel: discord.TextChannel,
    address: str,
    bedrock: bool,
) -> discord.Message:
    existing = await db_fetchone(
        "SELECT channel_id, message_id FROM minecraft_monitors WHERE guild_id = ?",
        (guild.id,),
    )
    try:
        data = await get_minecraft_status(address, bedrock)
    except Exception as exc:
        log.warning("Minecraft monitor lookup failed for %s: %s", address, exc)
        data = {
            "online": False,
            "debug": {"error": "The status API could not be reached; monitoring will retry automatically."},
        }
    embed = minecraft_status_embed(address, data, bedrock=bedrock)
    message: discord.Message | None = None
    if existing and int(existing["channel_id"]) == channel.id:
        try:
            message = await channel.fetch_message(int(existing["message_id"]))
            await message.edit(embed=embed)
        except discord.NotFound:
            message = None
    if message is None:
        message = await channel.send(embed=embed, allowed_mentions=mention_suppressed())
    now = utc_now().isoformat()
    await db_execute(
        """
        INSERT INTO minecraft_monitors
            (guild_id, channel_id, message_id, address, bedrock, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET
            channel_id = excluded.channel_id,
            message_id = excluded.message_id,
            address = excluded.address,
            bedrock = excluded.bedrock,
            updated_at = excluded.updated_at
        """,
        (guild.id, channel.id, message.id, address, int(bedrock), now, now),
    )
    return message


@tasks.loop(minutes=3)
async def minecraft_status_refresh() -> None:
    rows = await db_fetchall("SELECT guild_id, channel_id, message_id, address, bedrock FROM minecraft_monitors")
    for row in rows:
        guild = bot.get_guild(int(row["guild_id"]))
        if guild is None:
            continue
        result = await minecraft_monitor_update(
            guild,
            channel_id=int(row["channel_id"]),
            message_id=int(row["message_id"]),
            address=str(row["address"]),
            bedrock=bool(row["bedrock"]),
        )
        if result:
            await db_execute(
                "UPDATE minecraft_monitors SET channel_id = ?, message_id = ?, updated_at = ? WHERE guild_id = ?",
                (*result, utc_now().isoformat(), guild.id),
            )


@minecraft_status_refresh.before_loop
async def before_minecraft_status_refresh() -> None:
    await bot.wait_until_ready()


@bot.event
async def on_ready() -> None:
    log.info("Connected as %s (ID %s)", bot.user, bot.user.id if bot.user else "unknown")
    await update_presence_once()


@bot.event
async def on_member_join(member: discord.Member) -> None:
    channel_id = await get_setting(member.guild.id, "welcome_channel")
    if channel_id:
        channel = member.guild.get_channel(int(channel_id))
        if isinstance(channel, discord.TextChannel):
            template = await get_setting(
                member.guild.id,
                "welcome_message",
                "Welcome {user} to **{server}**!",
            )
            message = template.replace("{user}", member.mention).replace("{server}", member.guild.name)
            try:
                await channel.send(
                    message[:1900],
                    allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                )
            except (discord.Forbidden, discord.HTTPException):
                log.warning("Could not send a welcome message in guild %s", member.guild.id)
    await log_action(
        member.guild,
        "Member joined",
        f"{member.mention} (`{member.id}`)\nAccount created: <t:{int(member.created_at.timestamp())}:R>",
        color=discord.Colour.green(),
    )


@bot.event
async def on_member_remove(member: discord.Member) -> None:
    await log_action(
        member.guild,
        "Member left",
        f"{member} (`{member.id}`)",
        color=discord.Colour.dark_grey(),
    )


async def record_automod_event(guild_id: int, user_id: int, reason: str) -> int:
    await db_execute(
        "INSERT INTO automod_events (guild_id, user_id, reason, created_at) VALUES (?, ?, ?, ?)",
        (guild_id, user_id, reason, utc_now().isoformat()),
    )
    cutoff = (utc_now() - timedelta(minutes=10)).isoformat()
    row = await db_fetchone(
        """
        SELECT COUNT(*) AS count FROM automod_events
        WHERE guild_id = ? AND user_id = ? AND created_at >= ?
        """,
        (guild_id, user_id, cutoff),
    )
    await db_execute(
        "DELETE FROM automod_events WHERE created_at < ?",
        ((utc_now() - timedelta(days=7)).isoformat(),),
    )
    return int(row["count"]) if row else 1


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.bot:
        return
    if message.guild and isinstance(message.author, discord.Member):
        is_staff = (
            message.author.guild_permissions.manage_messages
            or message.author.guild_permissions.administrator
        )
        automod_enabled = await get_setting(message.guild.id, "automod_enabled", "false") == "true"
        if automod_enabled and not is_staff:
            reason: str | None = None
            words_json = await get_setting(message.guild.id, "automod_words", "[]")
            try:
                blocked_words = [str(word).casefold() for word in json.loads(words_json or "[]")]
            except (TypeError, json.JSONDecodeError):
                blocked_words = []
            folded = message.content.casefold()
            if any(word and re.search(rf"(?<!\w){re.escape(word)}(?!\w)", folded) for word in blocked_words):
                reason = "blocked word"
            elif (
                await get_setting(message.guild.id, "filter_invites", "true") == "true"
                and INVITE_RE.search(message.content)
            ):
                reason = "Discord invite link"
            elif (
                await get_setting(message.guild.id, "filter_links", "false") == "true"
                and URL_RE.search(message.content)
            ):
                reason = "link filter"
            else:
                limit = int(await get_setting(message.guild.id, "antispam_limit", "5") or "5")
                window = int(await get_setting(message.guild.id, "antispam_window", "8") or "8")
                key = (message.guild.id, message.author.id)
                now = time.monotonic()
                recent = bot.spam_cache[key]
                while recent and now - recent[0] > window:
                    recent.popleft()
                recent.append(now)
                if len(recent) >= limit:
                    reason = f"message spam ({limit} messages/{window}s)"
                    recent.clear()

            if reason:
                try:
                    await message.delete()
                except (discord.Forbidden, discord.HTTPException):
                    log.warning("AutoMod could not delete a message in guild %s", message.guild.id)
                    return
                else:
                    strikes = await record_automod_event(message.guild.id, message.author.id, reason)
                    timeout_text = ""
                    if strikes >= 3:
                        try:
                            await message.author.timeout(
                                timedelta(minutes=10),
                                reason=f"AutoMod: {reason} (strike {strikes})",
                            )
                            timeout_text = "\nAction: 10-minute timeout (3+ AutoMod strikes in 10 minutes)"
                        except (discord.Forbidden, discord.HTTPException):
                            timeout_text = "\nA timeout was not applied; check my Moderation permission and role position."
                    try:
                        await message.channel.send(
                            f"{message.author.mention}, that message was removed by AutoMod ({reason}).{timeout_text}",
                            delete_after=8,
                            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                        )
                    except (discord.Forbidden, discord.HTTPException):
                        pass
                    await log_action(
                        message.guild,
                        "AutoMod action",
                        f"User: {message.author.mention} (`{message.author.id}`)\nTrigger: {reason}\nStrikes in 10 minutes: {strikes}",
                        color=discord.Colour.red(),
                    )
                    return

    await bot.process_commands(message)


class TicketPanelView(discord.ui.View):
    def __init__(self, client: SentinelBot) -> None:
        super().__init__(timeout=None)
        self.client = client

    @discord.ui.button(
        label="Open a ticket",
        style=discord.ButtonStyle.primary,
        emoji="🎫",
        custom_id="sentinel:ticket:open",
    )
    async def open_ticket(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button[TicketPanelView],
    ) -> None:
        await self.client.create_ticket(interaction)


class TicketControlsView(discord.ui.View):
    def __init__(self, client: SentinelBot) -> None:
        super().__init__(timeout=None)
        self.client = client

    @discord.ui.button(
        label="Close ticket",
        style=discord.ButtonStyle.danger,
        emoji="🔒",
        custom_id="sentinel:ticket:close",
    )
    async def close_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button[TicketControlsView],
    ) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)
        result = await self.client.close_ticket(interaction, "Closed with the ticket button")
        await interaction.followup.send(result, ephemeral=True)


class HelpMenuView(discord.ui.View):
    PAGE_SIZE = 8
    CATEGORIES = (
        ("all", "All commands", "Browse every prefix command."),
        ("moderation", "Moderation", "Warnings, member actions, channel controls, and voice moderation."),
        ("music", "Music", "Join voice, play songs, manage the queue, and enable 24/7 reconnect."),
        ("configuration", "Configuration", "AutoMod, welcome/log settings, ticket setup, and other server configuration."),
        ("tickets", "Tickets", "Ticket setup, panels, closing, claiming, and member access."),
        ("utilities", "Utilities", "Polls, math, random choices, announcements, and more."),
        ("information", "Information", "Server, member, role, channel, and avatar details."),
        ("general", "General", "Help, status, latency, and bot information."),
        ("minecraft", "Minecraft", "Live Minecraft status, monitor setup, players, version, MOTD, plugins, and more."),
    )

    def __init__(self, owner_id: int, category: str = "all", page: int = 0) -> None:
        super().__init__(timeout=180)
        self.owner_id = owner_id
        self.category = category if category in {item[0] for item in self.CATEGORIES} else "all"
        self.page = max(0, page)
        self.message: discord.Message | None = None

        self.category_select = discord.ui.Select(
            placeholder="Choose a help category...",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(
                    label=label,
                    value=value,
                    description=description,
                    default=value == self.category,
                )
                for value, label, description in self.CATEGORIES
            ],
            row=0,
        )
        self.category_select.callback = self.select_category
        self.add_item(self.category_select)

        self.previous_button = discord.ui.Button(
            label="Previous",
            emoji="◀️",
            style=discord.ButtonStyle.secondary,
            row=1,
        )
        self.previous_button.callback = self.previous_page
        self.add_item(self.previous_button)

        self.next_button = discord.ui.Button(
            label="Next",
            emoji="▶️",
            style=discord.ButtonStyle.primary,
            row=1,
        )
        self.next_button.callback = self.next_page
        self.add_item(self.next_button)
        self.sync_controls()

    def get_commands(self) -> list[commands.Command[Any, ..., Any]]:
        commands_list = sorted(bot.commands, key=lambda item: (item.brief or "", item.name))
        if self.category == "all":
            return commands_list
        category_names = {
            "moderation": {"Moderation"},
            "configuration": {"Settings", "Tickets"},
            "tickets": {"Tickets"},
            "utilities": {"Utilities"},
            "information": {"Information"},
            "general": {"General"},
            "minecraft": {"Minecraft"},
            "music": {"Music"},
        }.get(self.category, set())
        return [command for command in commands_list if (command.brief or "General") in category_names]

    def sync_controls(self) -> None:
        command_count = len(self.get_commands())
        page_count = max(1, (command_count + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        self.page = min(self.page, page_count - 1)
        self.previous_button.disabled = self.page <= 0
        self.next_button.disabled = self.page >= page_count - 1
        for option in self.category_select.options:
            option.default = option.value == self.category
        label = next((item[1] for item in self.CATEGORIES if item[0] == self.category), "All commands")
        self.category_select.placeholder = f"Category: {label}"

    def make_embed(self) -> discord.Embed:
        commands_list = self.get_commands()
        page_count = max(1, (len(commands_list) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        self.sync_controls()
        start = self.page * self.PAGE_SIZE
        selected = commands_list[start : start + self.PAGE_SIZE]
        lines = [
            f"**`${command.usage or command.name}`** · {command.brief or 'General'}\n"
            f"{command.help or 'No description.'}"
            for command in selected
        ]
        title = next((item[1] for item in self.CATEGORIES if item[0] == self.category), "All commands")
        description = "\n\n".join(lines) or "No commands are available in this category."
        embed = make_embed(f"Sentinel Help • {title}", description)
        embed.set_footer(
            text=f"Page {self.page + 1}/{page_count} • {len(commands_list)} commands • Use $help <command> for details"
        )
        return embed

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.owner_id:
            return True
        await interaction.response.send_message(
            "Only the person who opened this help menu can use these controls.",
            ephemeral=True,
        )
        return False

    async def select_category(self, interaction: discord.Interaction) -> None:
        self.category = self.category_select.values[0]
        self.page = 0
        self.sync_controls()
        await interaction.response.edit_message(embed=self.make_embed(), view=self)

    async def previous_page(self, interaction: discord.Interaction) -> None:
        self.page = max(0, self.page - 1)
        self.sync_controls()
        await interaction.response.edit_message(embed=self.make_embed(), view=self)

    async def next_page(self, interaction: discord.Interaction) -> None:
        self.page += 1
        self.sync_controls()
        await interaction.response.edit_message(embed=self.make_embed(), view=self)

    async def on_timeout(self) -> None:
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass


def setup_snowflake(value: str) -> int | None:
    match = re.fullmatch(r"(?:<#|<@&)?(\d+)>?", value.strip())
    return int(match.group(1)) if match else None


async def setup_embed(guild: discord.Guild, section: str) -> discord.Embed:
    if section == "ticket":
        category_id = await get_setting(guild.id, "ticket_category")
        staff_id = await get_setting(guild.id, "ticket_staff_role")
        log_id = await get_setting(guild.id, "ticket_log_channel")
        category = guild.get_channel(int(category_id)) if category_id and category_id.isdigit() else None
        role = guild.get_role(int(staff_id)) if staff_id and staff_id.isdigit() else None
        log_channel = guild.get_channel(int(log_id)) if log_id and log_id.isdigit() else None
        return make_embed(
            "Ticket setup",
            f"Category: {category.name if isinstance(category, discord.CategoryChannel) else 'Not configured'}\n"
            f"Staff role: {role.mention if role else 'Not configured'}\n"
            f"Transcript logs: {log_channel.mention if isinstance(log_channel, discord.TextChannel) else 'Not configured'}\n\n"
            "Use **Configure** to save the category, support role, and transcript channel. "
            "Use **Post Panel** to send the ticket button.",
        )
    if section == "automod":
        enabled = await get_setting(guild.id, "automod_enabled", "false") == "true"
        links = await get_setting(guild.id, "filter_links", "false") == "true"
        invites = await get_setting(guild.id, "filter_invites", "true") == "true"
        words = await get_setting(guild.id, "automod_words", "[]")
        limit = await get_setting(guild.id, "antispam_limit", "5")
        window = await get_setting(guild.id, "antispam_window", "8")
        try:
            word_count = len(json.loads(words or "[]"))
        except (TypeError, json.JSONDecodeError):
            word_count = 0
        return make_embed(
            "AutoMod setup",
            f"Status: {'🟢 Enabled' if enabled else '🔴 Disabled'}\n"
            f"Invite filter: {'On' if invites else 'Off'}\n"
            f"Link filter: {'On' if links else 'Off'}\n"
            f"Blocked words: `{word_count}`\n"
            f"Spam limit: `{limit}` messages in `{window}` seconds\n\n"
            "Use **Configure** to update blocked words and spam limits. Use **Toggle** to enable or disable AutoMod.",
            color=discord.Colour.green() if enabled else discord.Colour.orange(),
        )
    if section == "welcome":
        channel_id = await get_setting(guild.id, "welcome_channel")
        message = await get_setting(guild.id, "welcome_message", "Welcome {user} to **{server}**!")
        channel = guild.get_channel(int(channel_id)) if channel_id and channel_id.isdigit() else None
        return make_embed(
            "Welcome setup",
            f"Channel: {channel.mention if isinstance(channel, discord.TextChannel) else 'Not configured'}\n"
            f"Message: `{message[:500]}`\n\n"
            "Placeholders: `{user}` and `{server}`. Use **Configure** to change the channel and message.",
        )
    if section == "logs":
        channel_id = await get_setting(guild.id, "log_channel")
        channel = guild.get_channel(int(channel_id)) if channel_id and channel_id.isdigit() else None
        return make_embed(
            "Moderation logs setup",
            f"Log channel: {channel.mention if isinstance(channel, discord.TextChannel) else 'Not configured'}\n\n"
            "Warnings, moderation actions, AutoMod events, and member changes are sent there when configured.",
        )
    if section == "minecraft":
        row = await db_fetchone(
            "SELECT channel_id, address, bedrock, updated_at FROM minecraft_monitors WHERE guild_id = ?",
            (guild.id,),
        )
        if not row:
            description = "Status monitor: 🔴 Not configured\n\nUse **Configure** to add an IP/hostname and port."
        else:
            channel = guild.get_channel(int(row["channel_id"]))
            description = (
                "Status monitor: 🟢 Enabled\n"
                f"Address: `{row['address']}`\n"
                f"Edition: `{'Bedrock' if row['bedrock'] else 'Java'}`\n"
                f"Channel: {channel.mention if channel else row['channel_id']}\n"
                f"Last saved: `{row['updated_at']}`\n\n"
                "The status message refreshes every 3 minutes."
            )
        return make_embed("Minecraft setup", description)
    if section == "voice":
        row = await db_fetchone(
            "SELECT channel_id FROM voice_settings WHERE guild_id = ? AND afk_247 = 1",
            (guild.id,),
        )
        channel = guild.get_channel(int(row["channel_id"])) if row else None
        volume = await get_setting(guild.id, "music_default_volume", "100")
        state = bot.music_states.get(guild.id)
        voice = guild.voice_client
        connection = voice.channel.mention if voice and voice.is_connected() else "Disconnected"
        return make_embed(
            "Voice and music setup",
            f"Voice connection: {connection}\n"
            f"24/7 reconnect: {'🟢 On' if row else '🔴 Off'}"
            f"{f' • {channel.mention}' if isinstance(channel, discord.VoiceChannel) else ''}\n"
            f"Default volume: `{volume}%`\n"
            f"Upcoming queue: `{len(state.queue) if state else 0}`\n\n"
            "Use **Configure** to set the default music volume. Use **Toggle 24/7** to stay connected "
            "after the bot restarts or temporarily disconnects. `$join`, `$play`, and `$help music` "
            "show the voice commands.",
        )
    return make_embed(
        "Moderation setup",
        "Moderation commands use Discord permissions and role hierarchy automatically.\n\n"
        "Configure logs and AutoMod from this menu, then use the Moderation category in `$help` for actions.",
    )


class SetupModal(discord.ui.Modal):
    def __init__(self, parent: SetupHubView, title: str) -> None:
        super().__init__(title=title, timeout=300)
        self.parent = parent

    async def finish(self, interaction: discord.Interaction, message: str) -> None:
        if not interaction.response.is_done():
            await interaction.response.send_message(message, ephemeral=True)
        else:
            await interaction.followup.send(message, ephemeral=True)
        await self.parent.refresh_message()


class TicketSetupModal(SetupModal):
    def __init__(self, parent: SetupHubView) -> None:
        super().__init__(parent, "Configure ticket system")
        self.category = discord.ui.TextInput(
            label="Ticket category ID or mention",
            placeholder="123456789012345678",
            required=True,
            max_length=30,
        )
        self.staff_role = discord.ui.TextInput(
            label="Support staff role ID or mention",
            placeholder="@Support or 123456789012345678",
            required=True,
            max_length=40,
        )
        self.log_channel = discord.ui.TextInput(
            label="Transcript log channel ID or mention",
            placeholder="#ticket-logs or 123456789012345678",
            required=True,
            max_length=40,
        )
        self.add_item(self.category)
        self.add_item(self.staff_role)
        self.add_item(self.log_channel)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        category_id = setup_snowflake(str(self.category.value))
        role_id = setup_snowflake(str(self.staff_role.value))
        log_id = setup_snowflake(str(self.log_channel.value))
        category = guild.get_channel(category_id) if guild and category_id else None
        role = guild.get_role(role_id) if guild and role_id else None
        log_channel = guild.get_channel(log_id) if guild and log_id else None
        if not guild or not isinstance(category, discord.CategoryChannel):
            await interaction.response.send_message("That ticket category was not found.", ephemeral=True)
            return
        if not role or role.is_default() or role.managed:
            await interaction.response.send_message("That support staff role was not found or cannot be used.", ephemeral=True)
            return
        if not isinstance(log_channel, discord.TextChannel):
            await interaction.response.send_message("That transcript log channel was not found.", ephemeral=True)
            return
        await set_setting(guild.id, "ticket_category", str(category.id))
        await set_setting(guild.id, "ticket_staff_role", str(role.id))
        await set_setting(guild.id, "ticket_log_channel", str(log_channel.id))
        await self.finish(interaction, f"Ticket system saved: **{category.name}**, {role.mention}, logs {log_channel.mention}.")


class AutoModSetupModal(SetupModal):
    def __init__(self, parent: SetupHubView) -> None:
        super().__init__(parent, "Configure AutoMod")
        self.words = discord.ui.TextInput(
            label="Blocked words or phrases (comma separated)",
            placeholder="word one, phrase two",
            required=False,
            max_length=900,
            style=discord.TextStyle.paragraph,
        )
        self.limit = discord.ui.TextInput(
            label="Spam message limit",
            placeholder="5",
            required=True,
            max_length=2,
        )
        self.window = discord.ui.TextInput(
            label="Spam window in seconds",
            placeholder="8",
            required=True,
            max_length=2,
        )
        self.add_item(self.words)
        self.add_item(self.limit)
        self.add_item(self.window)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        try:
            limit = int(str(self.limit.value).strip())
            window = int(str(self.window.value).strip())
        except ValueError:
            await interaction.response.send_message("Spam limit and window must be numbers.", ephemeral=True)
            return
        if not guild or not 3 <= limit <= 10 or not 3 <= window <= 30:
            await interaction.response.send_message("Use a limit from 3-10 and a window from 3-30 seconds.", ephemeral=True)
            return
        words = [word.strip()[:64] for word in str(self.words.value).split(",") if 2 <= len(word.strip()) <= 64]
        await set_setting(guild.id, "automod_words", json.dumps(words[:50], ensure_ascii=False))
        await set_setting(guild.id, "antispam_limit", str(limit))
        await set_setting(guild.id, "antispam_window", str(window))
        await self.finish(interaction, f"AutoMod configuration saved with `{len(words[:50])}` blocked word(s).")


class WelcomeSetupModal(SetupModal):
    def __init__(self, parent: SetupHubView) -> None:
        super().__init__(parent, "Configure welcome messages")
        self.channel = discord.ui.TextInput(
            label="Welcome channel ID or mention",
            placeholder="#welcome or 123456789012345678",
            required=True,
            max_length=40,
        )
        self.message = discord.ui.TextInput(
            label="Welcome message",
            placeholder="Welcome {user} to **{server}**!",
            required=True,
            max_length=1800,
            style=discord.TextStyle.paragraph,
        )
        self.add_item(self.channel)
        self.add_item(self.message)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        channel_id = setup_snowflake(str(self.channel.value))
        channel = guild.get_channel(channel_id) if guild and channel_id else None
        if not guild or not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("That welcome text channel was not found.", ephemeral=True)
            return
        await set_setting(guild.id, "welcome_channel", str(channel.id))
        await set_setting(guild.id, "welcome_message", str(self.message.value).strip())
        await self.finish(interaction, f"Welcome messages will be sent to {channel.mention}.")


class LogsSetupModal(SetupModal):
    def __init__(self, parent: SetupHubView) -> None:
        super().__init__(parent, "Configure moderation logs")
        self.channel = discord.ui.TextInput(
            label="Log channel ID or mention",
            placeholder="#mod-logs or 123456789012345678",
            required=True,
            max_length=40,
        )
        self.add_item(self.channel)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        channel_id = setup_snowflake(str(self.channel.value))
        channel = guild.get_channel(channel_id) if guild and channel_id else None
        if not guild or not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("That log text channel was not found.", ephemeral=True)
            return
        await set_setting(guild.id, "log_channel", str(channel.id))
        await self.finish(interaction, f"Moderation logs will be sent to {channel.mention}.")


class MinecraftSetupModal(SetupModal):
    def __init__(self, parent: SetupHubView) -> None:
        super().__init__(parent, "Configure Minecraft monitor")
        self.address = discord.ui.TextInput(
            label="Minecraft IP or hostname",
            placeholder="play.example.com",
            required=True,
            max_length=253,
        )
        self.port = discord.ui.TextInput(
            label="Port",
            placeholder="25565 for Java or 19132 for Bedrock",
            required=False,
            max_length=5,
        )
        self.edition = discord.ui.TextInput(
            label="Edition",
            placeholder="java or bedrock",
            required=True,
            max_length=7,
        )
        self.add_item(self.address)
        self.add_item(self.port)
        self.add_item(self.edition)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        channel = interaction.channel
        if not guild or not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Use the setup menu in a server text channel.", ephemeral=True)
            return
        address = str(self.address.value).strip()
        port = str(self.port.value).strip()
        bedrock = str(self.edition.value).strip().casefold() in {"bedrock", "be", "pe"}
        if port:
            if not port.isdigit() or not 1 <= int(port) <= 65535:
                await interaction.response.send_message("Port must be between 1 and 65535.", ephemeral=True)
                return
            if ":" not in address:
                address = f"{address}:{port}"
        try:
            address = minecraft_api_address(address)
        except ValueError as exc:
            await interaction.response.send_message(str(exc), ephemeral=True)
            return
        try:
            await save_minecraft_monitor(guild, channel, address, bedrock)
        except (discord.Forbidden, discord.HTTPException):
            await interaction.response.send_message("I need permission to send and embed messages in this channel.", ephemeral=True)
            return
        await self.finish(interaction, f"Minecraft monitor saved for `{address}` and will refresh every 3 minutes.")


class VoiceSetupModal(SetupModal):
    def __init__(self, parent: SetupHubView) -> None:
        super().__init__(parent, "Configure voice and music")
        self.volume = discord.ui.TextInput(
            label="Default music volume (0-200%)",
            placeholder="100",
            required=True,
            max_length=3,
        )
        self.add_item(self.volume)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        try:
            volume = int(str(self.volume.value).strip())
        except ValueError:
            await interaction.response.send_message("Volume must be a number from 0 to 200.", ephemeral=True)
            return
        if not guild or not 0 <= volume <= 200:
            await interaction.response.send_message("Volume must be between 0 and 200.", ephemeral=True)
            return
        await set_setting(guild.id, "music_default_volume", str(volume))
        state = bot.music_states.get(guild.id)
        if state:
            state.volume = volume / 100
            voice = guild.voice_client
            if voice and isinstance(voice.source, discord.PCMVolumeTransformer):
                voice.source.volume = state.volume
        await self.finish(interaction, f"Default music volume saved at **{volume}%**.")


class SetupHubView(discord.ui.View):
    SECTIONS = (
        ("ticket", "Tickets", "Configure panel category, staff role, and logs."),
        ("automod", "AutoMod", "Set up blocked words, spam thresholds, and filters."),
        ("welcome", "Welcome", "Set welcome message channel and text."),
        ("logs", "Logs", "Choose moderation log channel."),
        ("minecraft", "Minecraft", "Configure live Minecraft status monitor."),
        ("voice", "Voice", "Set default music volume and 24/7 settings."),
        ("moderation", "Moderation", "View moderation system overview."),
    )

    def __init__(self, guild: discord.Guild, owner_id: int, section: str = "ticket") -> None:
        super().__init__(timeout=300)
        self.guild = guild
        self.owner_id = owner_id
        self.section = section
        self.message: discord.Message | None = None

        self.section_select = discord.ui.Select(
            placeholder="Choose a module...",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label=label, value=val, description=desc, default=val == self.section)
                for val, label, desc in self.SECTIONS
            ],
            row=0,
        )
        self.section_select.callback = self.select_section
        self.add_item(self.section_select)
        self.build_action_buttons()

    def build_action_buttons(self) -> None:
        self.children = [item for item in self.children if item.row != 1]

        if self.section in {"ticket", "automod", "welcome", "logs", "minecraft", "voice"}:
            btn_config = discord.ui.Button(label="Configure", style=discord.ButtonStyle.primary, emoji="⚙️", row=1)
            btn_config.callback = self.open_modal
            self.add_item(btn_config)

        if self.section == "ticket":
            btn_panel = discord.ui.Button(label="Post Panel", style=discord.ButtonStyle.success, emoji="📩", row=1)
            btn_panel.callback = self.post_ticket_panel
            self.add_item(btn_panel)

        elif self.section == "automod":
            btn_toggle = discord.ui.Button(label="Toggle AutoMod", style=discord.ButtonStyle.secondary, emoji="🛡️", row=1)
            btn_toggle.callback = self.toggle_automod
            self.add_item(btn_toggle)

        elif self.section == "voice":
            btn_247 = discord.ui.Button(label="Toggle 24/7", style=discord.ButtonStyle.secondary, emoji="📻", row=1)
            btn_247.callback = self.toggle_247
            self.add_item(btn_247)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.owner_id:
            return True
        await interaction.response.send_message("Only the person who opened this dashboard can use it.", ephemeral=True)
        return False

    async def refresh_message(self) -> None:
        if self.message:
            self.build_action_buttons()
            embed = await setup_embed(self.guild, self.section)
            try:
                await self.message.edit(embed=embed, view=self)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

    async def select_section(self, interaction: discord.Interaction) -> None:
        self.section = self.section_select.values[0]
        for opt in self.section_select.options:
            opt.default = opt.value == self.section
        self.build_action_buttons()
        embed = await setup_embed(self.guild, self.section)
        await interaction.response.edit_message(embed=embed, view=self)

    async def open_modal(self, interaction: discord.Interaction) -> None:
        modals = {
            "ticket": TicketSetupModal,
            "automod": AutoModSetupModal,
            "welcome": WelcomeSetupModal,
            "logs": LogsSetupModal,
            "minecraft": MinecraftSetupModal,
            "voice": VoiceSetupModal,
        }
        modal_cls = modals.get(self.section)
        if modal_cls:
            await interaction.response.send_modal(modal_cls(self))

    async def post_ticket_panel(self, interaction: discord.Interaction) -> None:
        category_id = await get_setting(self.guild.id, "ticket_category")
        if not category_id:
            await interaction.response.send_message("Please configure the ticket category first.", ephemeral=True)
            return
        channel = interaction.channel
        if isinstance(channel, discord.TextChannel):
            embed = make_embed("Support Tickets", "Click the button below to open a private support ticket.")
            await channel.send(embed=embed, view=TicketPanelView(bot))
            await interaction.response.send_message("Ticket panel posted successfully!", ephemeral=True)

    async def toggle_automod(self, interaction: discord.Interaction) -> None:
        current = await get_setting(self.guild.id, "automod_enabled", "false") == "true"
        new_val = "false" if current else "true"
        await set_setting(self.guild.id, "automod_enabled", new_val)
        status_text = "enabled" if new_val == "true" else "disabled"
        await interaction.response.send_message(f"AutoMod is now **{status_text}**.", ephemeral=True)
        await self.refresh_message()

    async def toggle_247(self, interaction: discord.Interaction) -> None:
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return
        voice_state = member.voice
        channel = voice_state.channel if voice_state and isinstance(voice_state.channel, discord.VoiceChannel) else None
        state = await get_music_state(self.guild.id)
        try:
            await set_voice_247(self.guild, channel, not state.afk_247)
            status_text = "enabled" if state.afk_247 else "disabled"
            await interaction.response.send_message(f"24/7 mode is now **{status_text}**.", ephemeral=True)
        except ValueError as exc:
            await interaction.response.send_message(str(exc), ephemeral=True)
        await self.refresh_message()


# --- Prefix Commands ---

@bot.command(name="help", brief="General", usage="help [command_or_category]")
async def help_cmd(ctx: commands.Context[SentinelBot], *, query: str | None = None) -> None:
    if query:
        clean_query = query.strip().casefold()
        cat_match = next((val for val, label, _ in HelpMenuView.CATEGORIES if val == clean_query or label.casefold() == clean_query), None)
        if cat_match:
            view = HelpMenuView(ctx.author.id, category=cat_match)
            view.message = await ctx.send(embed=view.make_embed(), view=view)
            return

        cmd = bot.get_command(clean_query)
        if cmd:
            embed = make_embed(
                f"Command: ${cmd.name}",
                f"**Usage:** `${cmd.usage or cmd.name}`\n"
                f"**Category:** {cmd.brief or 'General'}\n\n"
                f"{cmd.help or 'No description available.'}"
            )
            await ctx.send(embed=embed)
            return

        await ctx.send(f"No command or category found matching `{query}`.")
        return

    view = HelpMenuView(ctx.author.id)
    view.message = await ctx.send(embed=view.make_embed(), view=view)


@bot.command(name="setup", brief="Settings", usage="setup")
@commands.has_permissions(administrator=True)
async def setup_cmd(ctx: commands.Context[SentinelBot]) -> None:
    if not ctx.guild:
        return
    view = SetupHubView(ctx.guild, ctx.author.id)
    embed = await setup_embed(ctx.guild, view.section)
    view.message = await ctx.send(embed=embed, view=view)


@bot.command(name="ping", brief="General", usage="ping")
async def ping_cmd(ctx: commands.Context[SentinelBot]) -> None:
    latency = round(bot.latency * 1000)
    await ctx.send(embed=make_embed("Pong! 🏓", f"WebSocket Latency: `{latency}ms`"))


# --- Music Prefix Commands ---

@bot.command(name="join", brief="Music", usage="join")
async def join_cmd(ctx: commands.Context[SentinelBot]) -> None:
    voice = await connect_music_to_member(ctx, move=True)
    if voice:
        await ctx.send(embed=make_embed("Voice Connected", f"Connected to {voice.channel.mention}."))


@bot.command(name="play", brief="Music", usage="play <song name or link>")
async def play_cmd(ctx: commands.Context[SentinelBot], *, query: str) -> None:
    if not ctx.guild:
        return
    voice = await connect_music_to_member(ctx, move=False)
    if not voice:
        return

    state = await get_music_state(ctx.guild.id)
    if len(state.queue) >= MAX_MUSIC_QUEUE:
        await ctx.send(f"The queue is full (max {MAX_MUSIC_QUEUE} tracks).")
        return

    msg = await ctx.send("🔎 Searching for track...")
    try:
        track = await asyncio.to_thread(extract_music_track, query, ctx.author.id, ctx.channel.id)
        state.queue.append(track)
        await msg.delete()
        if voice.is_playing() or voice.is_paused():
            embed = make_embed(
                "Track Queued",
                f"**[{track.title}]({track.webpage_url})**\nPosition in queue: `{len(state.queue)}`",
                color=discord.Colour.blue(),
            )
            await ctx.send(embed=embed)
        else:
            await start_next_music_track(ctx.guild.id)
    except Exception as exc:
        await msg.edit(content=f"❌ Could not add song: {exc}")


@bot.command(name="skip", brief="Music", usage="skip")
async def skip_cmd(ctx: commands.Context[SentinelBot]) -> None:
    if not ctx.guild or not ctx.guild.voice_client:
        await ctx.send("I am not connected to a voice channel.")
        return
    voice = ctx.guild.voice_client
    if not voice.is_playing():
        await ctx.send("Nothing is currently playing.")
        return

    state = await get_music_state(ctx.guild.id)
    state.skip_current = True
    voice.stop()
    await ctx.send("⏭️ Skipped current track.")


@bot.command(name="stop", brief="Music", usage="stop")
async def stop_cmd(ctx: commands.Context[SentinelBot]) -> None:
    if not ctx.guild or not ctx.guild.voice_client:
        await ctx.send("I am not connected to a voice channel.")
        return

    state = await get_music_state(ctx.guild.id)
    state.queue.clear()
    state.current = None
    ctx.guild.voice_client.stop()
    await ctx.send("⏹️ Stopped playback and cleared the queue.")


@bot.command(name="leave", brief="Music", usage="leave")
async def leave_cmd(ctx: commands.Context[SentinelBot]) -> None:
    if not ctx.guild or not ctx.guild.voice_client:
        await ctx.send("I am not connected to a voice channel.")
        return

    state = await get_music_state(ctx.guild.id)
    state.queue.clear()
    state.current = None
    state.afk_247 = False
    await db_execute("DELETE FROM voice_settings WHERE guild_id = ?", (ctx.guild.id,))
    await ctx.guild.voice_client.disconnect(force=True)
    await ctx.send("Disconnected from voice and cleared 24/7 settings.")


@bot.command(name="queue", brief="Music", usage="queue")
async def queue_cmd(ctx: commands.Context[SentinelBot]) -> None:
    if not ctx.guild:
        return
    state = await get_music_state(ctx.guild.id)
    lines = []
    if state.current:
        lines.append(f"**Now Playing:** [{state.current.title}]({state.current.webpage_url})")
    else:
        lines.append("Nothing is currently playing.")

    if state.queue:
        lines.append("\n**Up Next:**")
        for i, track in enumerate(list(state.queue)[:10], start=1):
            lines.append(f"`{i}.` [{track.title}]({track.webpage_url})")
        if len(state.queue) > 10:
            lines.append(f"*...and {len(state.queue) - 10} more.*")

    embed = make_embed("Music Queue", "\n".join(lines))
    await ctx.send(embed=embed)


# --- Slash Commands ---

@bot.tree.command(name="ping", description="Check bot response latency.")
async def ping_slash(interaction: discord.Interaction) -> None:
    latency = round(bot.latency * 1000)
    await respond(interaction, embed=make_embed("Pong! 🏓", f"WebSocket Latency: `{latency}ms`"))


@bot.tree.command(name="calc", description="Perform basic arithmetic calculations.")
@app_commands.describe(expression="Arithmetic expression (e.g. 12 + 4 * 2)")
async def calc_slash(interaction: discord.Interaction, expression: str) -> None:
    try:
        result = safe_calc(expression)
        await respond(interaction, embed=make_embed("Calculator", f"**Input:** `{expression}`\n**Result:** `{result}`"))
    except Exception as exc:
        await respond(interaction, f"❌ Calculation error: {exc}", ephemeral=True)


if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        log.critical("DISCORD_TOKEN environment variable is missing!")
    else:
        logging.basicConfig(level=logging.INFO)
        bot.run(token)
