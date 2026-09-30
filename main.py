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

    # This overrides discord.py's default listener, so prefix parsing is explicit.
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
    def __init__(self, parent: "SetupHubView", title: str) -> None:
        super().__init__(title=title, timeout=300)
        self.parent = parent

    async def finish(self, interaction: discord.Interaction, message: str) -> None:
        if not interaction.response.is_done():
            await interaction.response.send_message(message, ephemeral=True)
        else:
            await interaction.followup.send(message, ephemeral=True)
        await self.parent.refresh_message()


class TicketSetupModal(SetupModal):
    def __init__(self, parent: "SetupHubView") -> None:
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
    def __init__(self, parent: "SetupHubView") -> None:
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
    def __init__(self, parent: "SetupHubView") -> None:
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
    def __init__(self, parent: "SetupHubView") -> None:
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
    def __init__(self, parent: "SetupHubView") -> None:
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
    def __init__(self, parent: "SetupHubView") -> None:
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
        ("ticket", "Tickets", "Ticket category, support role, transcripts, and panel."),
        ("automod", "AutoMod", "Blocked words, links, invites, and anti-spam."),
        ("welcome", "Welcome", "Welcome channel, copy, and placeholders."),
        ("logs", "Logs", "Moderation and member event log channel."),
        ("minecraft", "Minecraft", "IP, port, edition, and live 3-minute monitor."),
        ("voice", "Voice", "Music playback, default volume, and 24/7 voice reconnect."),
        ("moderation", "Moderation", "Permission and role-hierarchy overview."),
    )

    def __init__(self, owner_id: int) -> None:
        super().__init__(timeout=300)
        self.owner_id = owner_id
        self.section = "ticket"
        self.message: discord.Message | None = None
        self.select = discord.ui.Select(
            placeholder="Choose a feature to configure...",
            options=[
                discord.SelectOption(label=label, value=value, description=description, default=value == self.section)
                for value, label, description in self.SECTIONS
            ],
            row=0,
        )
        self.select.callback = self.select_section
        self.add_item(self.select)
        self.configure_button = discord.ui.Button(label="Configure", style=discord.ButtonStyle.primary, row=1)
        self.configure_button.callback = self.configure
        self.add_item(self.configure_button)
        self.action_button = discord.ui.Button(label="Post Panel", style=discord.ButtonStyle.secondary, row=1)
        self.action_button.callback = self.action
        self.add_item(self.action_button)
        self.refresh_button = discord.ui.Button(label="Refresh", style=discord.ButtonStyle.success, row=2)
        self.refresh_button.callback = self.refresh
        self.add_item(self.refresh_button)
        self.close_button = discord.ui.Button(label="Close", style=discord.ButtonStyle.danger, row=2)
        self.close_button.callback = self.close
        self.add_item(self.close_button)
        self.update_labels()

    def update_labels(self) -> None:
        labels = {
            "ticket": ("Configure Ticket", "Post Panel"),
            "automod": ("Configure AutoMod", "Toggle AutoMod"),
            "welcome": ("Configure Welcome", "Disable Welcome"),
            "logs": ("Configure Logs", "Clear Logs"),
            "minecraft": ("Configure Monitor", "Stop Monitor"),
            "voice": ("Configure Voice", "Toggle 24/7"),
            "moderation": ("View Permissions", "Refresh Settings"),
        }
        configure, action = labels[self.section]
        self.configure_button.label = configure
        self.action_button.label = action
        self.action_button.disabled = self.section == "moderation"
        for option in self.select.options:
            option.default = option.value == self.section
        self.select.placeholder = f"Configure: {dict((value, label) for value, label, _ in self.SECTIONS)[self.section]}"

    async def refresh_message(self) -> None:
        if self.message and self.message.guild:
            self.update_labels()
            try:
                await self.message.edit(embed=await setup_embed(self.message.guild, self.section), view=self)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Only the admin who opened this setup menu can use it.", ephemeral=True)
            return False
        if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need Manage Server permission to use setup controls.", ephemeral=True)
            return False
        return True

    async def select_section(self, interaction: discord.Interaction) -> None:
        self.section = self.select.values[0]
        self.update_labels()
        await interaction.response.edit_message(embed=await setup_embed(interaction.guild, self.section), view=self)

    async def configure(self, interaction: discord.Interaction) -> None:
        modals: dict[str, type[SetupModal]] = {
            "ticket": TicketSetupModal,
            "automod": AutoModSetupModal,
            "welcome": WelcomeSetupModal,
            "logs": LogsSetupModal,
            "minecraft": MinecraftSetupModal,
            "voice": VoiceSetupModal,
        }
        modal_type = modals.get(self.section)
        if modal_type:
            await interaction.response.send_modal(modal_type(self))
            return
        await interaction.response.send_message("Moderation uses Discord permissions and role hierarchy automatically.", ephemeral=True)

    async def action(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("This setup menu only works in a server.", ephemeral=True)
            return
        if self.section == "ticket":
            if not isinstance(interaction.channel, discord.TextChannel):
                await interaction.response.send_message("Open the setup menu in a text channel.", ephemeral=True)
                return
            await interaction.channel.send(
                embed=make_embed("Need help?", "Press the button below to create a private support ticket."),
                view=TicketPanelView(bot),
                allowed_mentions=mention_suppressed(),
            )
            await interaction.response.send_message("Ticket panel posted in this channel.", ephemeral=True)
        elif self.section == "automod":
            current = await get_setting(guild.id, "automod_enabled", "false") == "true"
            await set_setting(guild.id, "automod_enabled", "false" if current else "true")
            await interaction.response.send_message(f"AutoMod {'disabled' if current else 'enabled'}.", ephemeral=True)
        elif self.section == "welcome":
            await set_setting(guild.id, "welcome_channel", None)
            await interaction.response.send_message("Welcome messages disabled.", ephemeral=True)
        elif self.section == "logs":
            await set_setting(guild.id, "log_channel", None)
            await interaction.response.send_message("Moderation logs cleared.", ephemeral=True)
        elif self.section == "minecraft":
            await db_execute("DELETE FROM minecraft_monitors WHERE guild_id = ?", (guild.id,))
            await interaction.response.send_message("Minecraft monitor stopped.", ephemeral=True)
        elif self.section == "voice":
            row = await db_fetchone(
                "SELECT channel_id FROM voice_settings WHERE guild_id = ? AND afk_247 = 1",
                (guild.id,),
            )
            if row:
                await set_voice_247(guild, None, False)
                await interaction.response.send_message("24/7 reconnect disabled. Use Leave to disconnect.", ephemeral=True)
            else:
                member = interaction.user
                if not isinstance(member, discord.Member) or not member.voice or not isinstance(member.voice.channel, discord.VoiceChannel):
                    await interaction.response.send_message("Join the voice channel where the bot should stay, then toggle 24/7.", ephemeral=True)
                    return
                try:
                    await set_voice_247(guild, member.voice.channel, True)
                except (discord.Forbidden, discord.HTTPException, asyncio.TimeoutError, ValueError) as exc:
                    log.warning("Could not enable voice 24/7 from setup in guild %s: %s", guild.id, exc)
                    await interaction.response.send_message(
                        str(exc) if isinstance(exc, ValueError) else "I could not join. Check Connect and Speak permissions.",
                        ephemeral=True,
                    )
                    return
                await interaction.response.send_message(
                    f"24/7 reconnect enabled in {member.voice.channel.mention}.",
                    ephemeral=True,
                )
        await self.refresh_message()

    async def refresh(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(embed=await setup_embed(interaction.guild, self.section), view=self)

    async def close(self, interaction: discord.Interaction) -> None:
        self.stop()
        await interaction.response.edit_message(view=None)

    async def on_timeout(self) -> None:
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass


PrefixHandler = Any
PREFIX_COMMANDS: list[tuple[str, str, str, str, PrefixHandler, tuple[str, ...], tuple[str, ...]]] = []


def prefix_permission_check(*permissions: str) -> Any:
    async def predicate(ctx: commands.Context[SentinelBot]) -> bool:
        if ctx.guild is None:
            raise commands.NoPrivateMessage("This command can only be used in a server.")
        current = ctx.author.guild_permissions
        missing = [permission for permission in permissions if not getattr(current, permission, False)]
        if missing:
            raise commands.MissingPermissions(missing)
        return True

    return predicate


def register_prefix(
    name: str,
    category: str,
    usage: str,
    description: str,
    handler: PrefixHandler,
    *,
    aliases: tuple[str, ...] = (),
    permissions: tuple[str, ...] = (),
) -> None:
    async def callback(ctx: commands.Context[SentinelBot], *, args: str = "") -> None:
        await handler(ctx, args.strip())

    callback.__name__ = f"prefix_{name.replace('-', '_')}"
    command_aliases = list(aliases)
    # Common short forms make the growing command set easy to invoke with either style.
    command_aliases.extend((f"s{name}", f"server{name}", f"guild{name}", f"bot{name}", f"cmd{name}"))
    clean_aliases: list[str] = []
    for alias in command_aliases:
        normalized = alias.casefold().strip()
        if normalized and normalized != name and normalized not in bot.all_commands and normalized not in clean_aliases:
            clean_aliases.append(normalized)

    checks: list[Any] = []
    if permissions:
        checks.append(prefix_permission_check(*permissions))
    command = commands.Command(
        callback,
        name=name,
        aliases=clean_aliases,
        help=description,
        brief=category,
        usage=f"{name} {usage}".strip(),
        checks=checks,
    )
    bot.add_command(command)
    PREFIX_COMMANDS.append((name, category, usage, description, handler, tuple(clean_aliases), permissions))


def parse_prefix_args(args: str) -> list[str]:
    try:
        return shlex.split(args)
    except ValueError as exc:
        raise commands.BadArgument("Check your quotes and try again.") from exc


async def prefix_member(ctx: commands.Context[SentinelBot], value: str) -> discord.Member:
    if ctx.guild is None:
        raise commands.NoPrivateMessage()
    try:
        return await commands.MemberConverter().convert(ctx, value)
    except commands.BadArgument as exc:
        raise commands.BadArgument(f"Could not find server member `{value}`. Mention them or use their ID.") from exc


async def prefix_role(ctx: commands.Context[SentinelBot], value: str) -> discord.Role:
    if ctx.guild is None:
        raise commands.NoPrivateMessage()
    try:
        return await commands.RoleConverter().convert(ctx, value)
    except commands.BadArgument as exc:
        raise commands.BadArgument(f"Could not find role `{value}`. Mention it or use its ID.") from exc


def prefix_target_error(ctx: commands.Context[SentinelBot], member: discord.Member) -> str | None:
    guild = ctx.guild
    actor = ctx.author
    if guild is None or not isinstance(actor, discord.Member):
        return "This command can only be used in a server."
    if member.id == guild.owner_id:
        return "The server owner cannot be moderated."
    if member.id == actor.id:
        return "You cannot use this action on yourself."
    if bot.user and member.id == bot.user.id:
        return "I cannot moderate myself."
    if actor.id != guild.owner_id and member.top_role >= actor.top_role:
        return "Your highest role must be above the target's highest role."
    if guild.me and member.top_role >= guild.me.top_role:
        return "Move my highest role above the target's highest role first."
    return None


async def prefix_setup(ctx: commands.Context[SentinelBot], args: str) -> None:
    view = SetupHubView(ctx.author.id)
    message = await ctx.send(
        embed=await setup_embed(ctx.guild, view.section),
        view=view,
        allowed_mentions=mention_suppressed(),
    )
    view.message = message


async def prefix_help(ctx: commands.Context[SentinelBot], args: str) -> None:
    query = args.strip()
    category_aliases = {
        "all": "all",
        "all commands": "all",
        "moderation": "moderation",
        "mod": "moderation",
        "configuration": "configuration",
        "config": "configuration",
        "settings": "configuration",
        "tickets": "tickets",
        "ticket": "tickets",
        "utilities": "utilities",
        "utility": "utilities",
        "information": "information",
        "info": "information",
        "general": "general",
    }
    category = category_aliases.get(query.casefold())
    if not query or query.isdigit() or category:
        selected_category = category or "all"
        page = max(0, int(query) - 1) if query.isdigit() else 0
        view = HelpMenuView(ctx.author.id, category=selected_category, page=page)
        embed = view.make_embed()
        message = await ctx.send(embed=embed, view=view)
        view.message = message
        return

    if query:
        exact = bot.get_command(query.casefold())
        if exact:
            aliases = [alias for alias in exact.aliases if not alias.startswith(("s", "server", "guild", "bot", "cmd"))]
            embed = make_embed(f"${exact.name}", exact.help or "No description.")
            embed.add_field(name="Usage", value=f"`${exact.usage or exact.name}`", inline=False)
            embed.add_field(name="Category", value=exact.brief or "General", inline=True)
            if aliases:
                embed.add_field(name="Shortcuts", value=", ".join(f"`{alias}`" for alias in aliases[:15]), inline=False)
            await ctx.send(embed=embed)
            return
        matches = [
            command
            for command in bot.commands
            if query.casefold() in command.name.casefold()
            or query.casefold() in (command.brief or "").casefold()
        ]
        if not matches:
            await ctx.send(f"No `$` command matches `{query}`. Try `$help`.")
            return
        shown = matches[:20]
        lines = [f"`${command.usage or command.name}` — {command.help or ''}" for command in shown]
        await ctx.send(embed=make_embed(f"Commands matching {query}", "\n".join(lines)))


async def prefix_ping(ctx: commands.Context[SentinelBot], args: str) -> None:
    await ctx.send(f"Pong — `{round(bot.latency * 1000)} ms` gateway latency.")


async def prefix_status(ctx: commands.Context[SentinelBot], args: str) -> None:
    await ctx.send(
        embed=make_embed(
            "Sentinel status",
            f"State: online\nLatency: `{round(bot.latency * 1000)} ms`\n"
            f"Servers: `{len(bot.guilds)}`\nPrefix: `$`",
            color=discord.Colour.green(),
        )
    )


async def prefix_about(ctx: commands.Context[SentinelBot], args: str) -> None:
    await ctx.send(
        embed=make_embed(
            "Sentinel",
            f"Moderation, AutoMod, tickets, logs, and utilities.\n"
            f"Servers: `{len(bot.guilds)}` • Prefix: `$` • Library: `discord.py {discord.__version__}`",
            color=discord.Colour.green(),
        )
    )


async def prefix_serverinfo(ctx: commands.Context[SentinelBot], args: str) -> None:
    guild = ctx.guild
    if guild is None:
        await ctx.send("Use this in a server.")
        return
    embed = make_embed(guild.name, color=discord.Colour.blurple())
    embed.add_field(name="Owner", value=str(guild.owner or "Unknown"), inline=True)
    embed.add_field(name="Members", value=str(guild.member_count or 0), inline=True)
    embed.add_field(name="Channels", value=str(len(guild.channels)), inline=True)
    embed.add_field(name="Roles", value=str(len(guild.roles)), inline=True)
    embed.add_field(name="Created", value=f"<t:{int(guild.created_at.timestamp())}:D>", inline=True)
    embed.add_field(name="Server ID", value=f"`{guild.id}`", inline=True)
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    await ctx.send(embed=embed)


async def prefix_userinfo(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    member = await prefix_member(ctx, tokens[0]) if tokens else ctx.author
    if not isinstance(member, discord.Member):
        await ctx.send("Use this in a server.")
        return
    embed = make_embed(str(member), color=member.top_role.color)
    embed.add_field(name="User ID", value=f"`{member.id}`", inline=True)
    embed.add_field(name="Joined", value=f"<t:{int(member.joined_at.timestamp())}:R>" if member.joined_at else "Unknown", inline=True)
    embed.add_field(name="Created", value=f"<t:{int(member.created_at.timestamp())}:R>", inline=True)
    roles = [role.mention for role in reversed(member.roles) if not role.is_default()]
    embed.add_field(name=f"Roles ({len(roles)})", value=", ".join(roles[:15]) or "None", inline=False)
    embed.set_thumbnail(url=member.display_avatar.url)
    await ctx.send(embed=embed)


async def prefix_avatar(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if tokens:
        try:
            user = await commands.UserConverter().convert(ctx, tokens[0])
        except commands.BadArgument as exc:
            raise commands.BadArgument("Mention a user or use their ID.") from exc
    else:
        user = ctx.author
    embed = make_embed(f"{user}'s avatar")
    embed.set_image(url=user.display_avatar.with_size(1024).url)
    await ctx.send(embed=embed)


async def prefix_membercount(ctx: commands.Context[SentinelBot], args: str) -> None:
    if ctx.guild is None:
        await ctx.send("Use this in a server.")
        return
    humans = sum(not member.bot for member in ctx.guild.members)
    bots = (ctx.guild.member_count or 0) - humans
    await ctx.send(f"Members: **{ctx.guild.member_count or 0}** • People: **{humans}** • Bots: **{bots}**")


async def prefix_rolelist(ctx: commands.Context[SentinelBot], args: str) -> None:
    if ctx.guild is None:
        await ctx.send("Use this in a server.")
        return
    roles = [role for role in reversed(ctx.guild.roles) if not role.is_default()]
    text = ", ".join(f"{role.name} ({len(role.members)})" for role in roles[:50]) or "No roles."
    await ctx.send(embed=make_embed(f"Roles in {ctx.guild.name} ({len(roles)})", text[:4000]))


async def prefix_channelcount(ctx: commands.Context[SentinelBot], args: str) -> None:
    if ctx.guild is None:
        await ctx.send("Use this in a server.")
        return
    counts = defaultdict(int)
    for channel in ctx.guild.channels:
        counts[channel.type.name] += 1
    summary = " • ".join(f"{name}: {count}" for name, count in sorted(counts.items()))
    await ctx.send(f"Channels ({len(ctx.guild.channels)}): {summary or 'none'}")


async def prefix_warn(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$warn @member reason`")
    member = await prefix_member(ctx, tokens[0])
    blocked = prefix_target_error(ctx, member)
    if blocked:
        await ctx.send(blocked)
        return
    reason = clean_reason(" ".join(tokens[1:]))
    warning_id = await db_execute(
        "INSERT INTO warnings (guild_id, user_id, moderator_id, reason, created_at) VALUES (?, ?, ?, ?, ?)",
        (ctx.guild.id, member.id, ctx.author.id, reason, utc_now().isoformat()),
    )
    try:
        await member.send(f"You received a warning in **{ctx.guild.name}**: {reason}")
    except (discord.Forbidden, discord.HTTPException):
        pass
    await ctx.send(f"Warned {member.mention}. Warning ID: `{warning_id}`.")
    await log_action(
        ctx.guild,
        "Member warned",
        f"Member: {member.mention} (`{member.id}`)\nWarning ID: `{warning_id}`\nReason: {reason}",
        moderator=ctx.author,
        color=discord.Colour.yellow(),
    )


async def prefix_warnings(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$warnings @member`")
    member = await prefix_member(ctx, tokens[0])
    rows = await db_fetchall(
        "SELECT id, moderator_id, reason, created_at FROM warnings WHERE guild_id = ? AND user_id = ? AND active = 1 ORDER BY id DESC LIMIT 10",
        (ctx.guild.id, member.id),
    )
    if not rows:
        await ctx.send(f"{member.mention} has no active warnings.")
        return
    lines = [f"`#{row['id']}` {row['reason'][:160]} • moderator `{row['moderator_id']}`" for row in rows]
    await ctx.send(embed=make_embed(f"Warnings for {member}", "\n".join(lines), color=discord.Colour.yellow()))


async def prefix_unwarn(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if len(tokens) != 1 or not tokens[0].isdigit():
        raise commands.BadArgument("Usage: `$unwarn <warning-id>`")
    warning_id = int(tokens[0])
    cursor = await db_execute(
        "UPDATE warnings SET active = 0 WHERE guild_id = ? AND id = ? AND active = 1",
        (ctx.guild.id, warning_id),
    )
    if cursor:
        await ctx.send(f"Removed warning `{warning_id}`.")
    else:
        await ctx.send("No active warning with that ID was found in this server.")


async def prefix_clearwarnings(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$clearwarnings @member`")
    member = await prefix_member(ctx, tokens[0])
    cursor = await db_execute(
        "UPDATE warnings SET active = 0 WHERE guild_id = ? AND user_id = ? AND active = 1",
        (ctx.guild.id, member.id),
    )
    await ctx.send(f"Cleared {cursor} active warning(s) for {member.mention}.")


async def prefix_kick(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$kick @member reason`")
    member = await prefix_member(ctx, tokens[0])
    blocked = prefix_target_error(ctx, member)
    if blocked:
        await ctx.send(blocked)
        return
    reason = clean_reason(" ".join(tokens[1:]))
    try:
        await member.send(f"You were kicked from **{ctx.guild.name}**. Reason: {reason}")
    except (discord.Forbidden, discord.HTTPException):
        pass
    await member.kick(reason=f"{ctx.author}: {reason}")
    await ctx.send(f"Kicked {member}.")
    await log_action(ctx.guild, "Member kicked", f"{member} (`{member.id}`)\nReason: {reason}", moderator=ctx.author)


async def prefix_ban(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$ban @member reason`")
    member = await prefix_member(ctx, tokens[0])
    blocked = prefix_target_error(ctx, member)
    if blocked:
        await ctx.send(blocked)
        return
    reason = clean_reason(" ".join(tokens[1:]))
    try:
        await member.send(f"You were banned from **{ctx.guild.name}**. Reason: {reason}")
    except (discord.Forbidden, discord.HTTPException):
        pass
    await ctx.guild.ban(member, reason=f"{ctx.author}: {reason}", delete_message_seconds=0)
    await ctx.send(f"Banned {member}.")
    await log_action(
        ctx.guild,
        "Member banned",
        f"{member} (`{member.id}`)\nReason: {reason}",
        moderator=ctx.author,
        color=discord.Colour.red(),
    )


async def prefix_unban(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens or not tokens[0].isdigit():
        raise commands.BadArgument("Usage: `$unban <user-id> [reason]`")
    user_id = int(tokens[0])
    reason = clean_reason(" ".join(tokens[1:]))
    user = discord.Object(id=user_id)
    try:
        await ctx.guild.unban(user, reason=f"{ctx.author}: {reason}")
    except discord.NotFound:
        await ctx.send("That user is not banned from this server.")
        return
    await ctx.send(f"Unbanned user `{user_id}`.")
    await log_action(ctx.guild, "User unbanned", f"User ID: `{user_id}`\nReason: {reason}", moderator=ctx.author)


async def prefix_softban(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$softban @member reason`")
    member = await prefix_member(ctx, tokens[0])
    blocked = prefix_target_error(ctx, member)
    if blocked:
        await ctx.send(blocked)
        return
    reason = clean_reason(" ".join(tokens[1:]))
    await ctx.guild.ban(member, reason=f"Softban by {ctx.author}: {reason}", delete_message_seconds=86400)
    await ctx.guild.unban(member, reason=f"Softban complete by {ctx.author}")
    await ctx.send(f"Softbanned {member}; their recent messages were removed.")


async def prefix_timeout(ctx: commands.Context[SentinelBot], args: str, *, default_minutes: int | None = None) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$timeout @member minutes reason`")
    member = await prefix_member(ctx, tokens[0])
    blocked = prefix_target_error(ctx, member)
    if blocked:
        await ctx.send(blocked)
        return
    if default_minutes is None:
        if len(tokens) < 2 or not tokens[1].isdigit():
            raise commands.BadArgument("Usage: `$timeout @member minutes reason`")
        minutes = int(tokens[1])
        reason = clean_reason(" ".join(tokens[2:]))
    else:
        minutes = default_minutes
        reason = clean_reason(" ".join(tokens[1:]))
    if not 1 <= minutes <= MAX_TIMEOUT_MINUTES:
        await ctx.send(f"Timeout must be between 1 and {MAX_TIMEOUT_MINUTES} minutes.")
        return
    await member.timeout(timedelta(minutes=minutes), reason=f"{ctx.author}: {reason}")
    await ctx.send(f"Timed out {member.mention} for {minutes} minute(s).")
    await log_action(
        ctx.guild,
        "Member timed out",
        f"{member.mention} (`{member.id}`)\nDuration: {minutes} minutes\nReason: {reason}",
        moderator=ctx.author,
        color=discord.Colour.red(),
    )


async def prefix_untimeout(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$untimeout @member [reason]`")
    member = await prefix_member(ctx, tokens[0])
    blocked = prefix_target_error(ctx, member)
    if blocked:
        await ctx.send(blocked)
        return
    await member.timeout(None, reason=f"{ctx.author}: {clean_reason(' '.join(tokens[1:]))}")
    await ctx.send(f"Removed {member.mention}'s timeout.")


async def prefix_purge(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens or not tokens[0].isdigit():
        raise commands.BadArgument("Usage: `$purge <1-100>`")
    amount = int(tokens[0])
    if not 1 <= amount <= 100:
        await ctx.send("Amount must be from 1 to 100.")
        return
    if not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("Use this in a text channel.")
        return
    await ctx.message.delete()
    deleted = await ctx.channel.purge(limit=amount)
    await ctx.send(f"Deleted {len(deleted)} message(s).", delete_after=5)


async def prefix_purgeuser(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if len(tokens) < 2 or not tokens[1].isdigit():
        raise commands.BadArgument("Usage: `$purgeuser @member <1-100>`")
    member = await prefix_member(ctx, tokens[0])
    amount = int(tokens[1])
    if not 1 <= amount <= 100 or not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("Choose 1-100 messages in a text channel.")
        return
    deleted = await ctx.channel.purge(
        limit=amount,
        check=lambda message: message.author.id == member.id,
    )
    await ctx.send(f"Deleted {len(deleted)} message(s) from {member.display_name}.", delete_after=5)


async def prefix_clearbots(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    amount = int(tokens[0]) if tokens and tokens[0].isdigit() else 50
    if not 1 <= amount <= 100 or not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("Usage: `$clearbots [1-100]` in a text channel.")
        return
    deleted = await ctx.channel.purge(limit=amount, check=lambda message: message.author.bot)
    await ctx.send(f"Deleted {len(deleted)} bot message(s).", delete_after=5)


async def prefix_purge_kind(ctx: commands.Context[SentinelBot], args: str, kind: str) -> None:
    tokens = parse_prefix_args(args)
    amount = int(tokens[0]) if tokens and tokens[0].isdigit() else 50
    if not 1 <= amount <= 100 or not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("Choose 1-100 messages in a text channel.")
        return
    def matches(message: discord.Message) -> bool:
        if kind == "links":
            return bool(URL_RE.search(message.content))
        if kind == "invites":
            return bool(INVITE_RE.search(message.content))
        if kind == "attachments":
            return bool(message.attachments)
        if kind == "embeds":
            return bool(message.embeds)
        return False

    deleted = await ctx.channel.purge(limit=amount, check=matches)
    await ctx.send(f"Deleted {len(deleted)} {kind} message(s).", delete_after=5)


async def prefix_lock(ctx: commands.Context[SentinelBot], args: str, locked: bool) -> None:
    if not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("Use this in a text channel.")
        return
    overwrite = ctx.channel.overwrites_for(ctx.guild.default_role)
    overwrite.send_messages = False if locked else None
    await ctx.channel.set_permissions(
        ctx.guild.default_role,
        overwrite=overwrite,
        reason=f"{'Locked' if locked else 'Unlocked'} by {ctx.author}",
    )
    await ctx.send(f"{'Locked' if locked else 'Unlocked'} {ctx.channel.mention}.")


async def prefix_slowmode(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens or not tokens[0].isdigit():
        raise commands.BadArgument("Usage: `$slowmode <0-21600>`")
    seconds = int(tokens[0])
    if not 0 <= seconds <= 21600 or not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("Slowmode must be 0-21,600 seconds, in a text channel.")
        return
    await ctx.channel.edit(slowmode_delay=seconds, reason=f"Slowmode changed by {ctx.author}")
    await ctx.send(f"Slowmode set to {seconds} second(s).")


async def prefix_nick(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if len(tokens) < 2:
        raise commands.BadArgument('Usage: `$nick @member "New nickname"`')
    member = await prefix_member(ctx, tokens[0])
    blocked = prefix_target_error(ctx, member)
    if blocked:
        await ctx.send(blocked)
        return
    nickname = " ".join(tokens[1:])
    if len(nickname) > 32:
        await ctx.send("Nickname must be 32 characters or fewer.")
        return
    await member.edit(nick=nickname, reason=f"Nickname changed by {ctx.author}")
    await ctx.send(f"Updated {member.mention}'s nickname.")


async def prefix_clearnick(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$clearnick @member`")
    member = await prefix_member(ctx, tokens[0])
    blocked = prefix_target_error(ctx, member)
    if blocked:
        await ctx.send(blocked)
        return
    await member.edit(nick=None, reason=f"Nickname cleared by {ctx.author}")
    await ctx.send(f"Cleared {member.mention}'s nickname.")


async def prefix_role_change(ctx: commands.Context[SentinelBot], args: str, add: bool) -> None:
    tokens = parse_prefix_args(args)
    if len(tokens) < 2:
        raise commands.BadArgument("Usage: `$roleadd @member @role`")
    member = await prefix_member(ctx, tokens[0])
    role = await prefix_role(ctx, tokens[1])
    if role.is_default() or role.managed or (ctx.guild.me and role >= ctx.guild.me.top_role):
        await ctx.send("Move my highest role above that role first.")
        return
    actor = ctx.author
    if isinstance(actor, discord.Member) and actor.id != ctx.guild.owner_id and role >= actor.top_role:
        await ctx.send("You can only manage roles below your highest role.")
        return
    if add:
        await member.add_roles(role, reason=f"Role added by {actor}")
        await ctx.send(f"Added {role.mention} to {member.mention}.")
    else:
        await member.remove_roles(role, reason=f"Role removed by {actor}")
        await ctx.send(f"Removed {role.mention} from {member.mention}.")


async def prefix_voice_action(ctx: commands.Context[SentinelBot], args: str, action: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument(f"Usage: `${action} @member`")
    member = await prefix_member(ctx, tokens[0])
    blocked = prefix_target_error(ctx, member)
    if blocked:
        await ctx.send(blocked)
        return
    if action == "voicekick":
        if not member.voice or not member.voice.channel:
            await ctx.send("That member is not in a voice channel.")
            return
        await member.move_to(None, reason=f"Disconnected by {ctx.author}")
        await ctx.send(f"Disconnected {member.mention} from voice.")
    elif action == "voicemute":
        if not member.voice:
            await ctx.send("That member is not in a voice channel.")
            return
        await member.edit(mute=True, reason=f"Server-muted by {ctx.author}")
        await ctx.send(f"Server-muted {member.mention} in voice.")
    elif action == "voiceunmute":
        if not member.voice:
            await ctx.send("That member is not in a voice channel.")
            return
        await member.edit(mute=False, reason=f"Server-unmuted by {ctx.author}")
        await ctx.send(f"Server-unmuted {member.mention} in voice.")
    elif action == "deafen":
        if not member.voice:
            await ctx.send("That member is not in a voice channel.")
            return
        await member.edit(deafen=True, reason=f"Server-deafened by {ctx.author}")
        await ctx.send(f"Server-deafened {member.mention} in voice.")
    elif action == "undeafen":
        if not member.voice:
            await ctx.send("That member is not in a voice channel.")
            return
        await member.edit(deafen=False, reason=f"Server-undeafened by {ctx.author}")
        await ctx.send(f"Server-undeafened {member.mention} in voice.")


async def music_voice_for_context(ctx: commands.Context[SentinelBot]) -> discord.VoiceClient | None:
    if ctx.guild is None or not isinstance(ctx.author, discord.Member):
        await ctx.send("Voice commands can only be used inside a server.")
        return None
    if not ctx.author.voice or not isinstance(ctx.author.voice.channel, discord.VoiceChannel):
        await ctx.send("Join the bot's voice channel first.")
        return None
    voice = ctx.guild.voice_client
    if not voice or not voice.is_connected():
        await ctx.send("The bot is not connected to a voice channel. Use `$join` first.")
        return None
    if voice.channel.id != ctx.author.voice.channel.id:
        await ctx.send("Join the same voice channel as the bot to control music.")
        return None
    return voice


async def prefix_music_join(ctx: commands.Context[SentinelBot], args: str) -> None:
    voice = await connect_music_to_member(ctx, move=True, require_speak=False)
    if voice:
        await ctx.send(f"Joined {voice.channel.mention}. Use `$play <song name or YouTube URL>`.")


async def prefix_music_leave(ctx: commands.Context[SentinelBot], args: str) -> None:
    voice = await music_voice_for_context(ctx)
    if not voice or not ctx.guild:
        return
    state = await get_music_state(ctx.guild.id)
    state.queue.clear()
    state.loop_mode = "off"
    state.afk_247 = False
    state.skip_current = True
    state.generation += 1
    await db_execute("DELETE FROM voice_settings WHERE guild_id = ?", (ctx.guild.id,))
    if voice.is_playing() or voice.is_paused():
        voice.stop()
    await voice.disconnect(force=True)
    bot.music_states.pop(ctx.guild.id, None)
    await ctx.send("Left voice, cleared the music queue, and disabled 24/7 reconnect.")


async def prefix_music_play(ctx: commands.Context[SentinelBot], args: str) -> None:
    query = args.strip()
    if not query:
        raise commands.BadArgument("Usage: `$play <song name or YouTube URL>`")
    voice = await connect_music_to_member(ctx)
    if not voice or not ctx.guild:
        return
    state = await get_music_state(ctx.guild.id)
    if len(state.queue) >= MAX_MUSIC_QUEUE:
        await ctx.send(f"The upcoming queue is full (maximum {MAX_MUSIC_QUEUE} songs).")
        return
    try:
        async with ctx.typing():
            track = await asyncio.to_thread(
                extract_music_track,
                query,
                ctx.author.id,
                ctx.channel.id,
            )
    except Exception as exc:
        log.info("Music search failed for guild %s: %s", ctx.guild.id, exc)
        await ctx.send(f"I couldn't find that song or load its details. Try another title or YouTube URL.")
        return
    state.queue.append(track)
    position = len(state.queue) + (1 if state.current else 0)
    await ctx.send(
        embed=make_embed(
            "Added to queue",
            f"**[{track.title}]({track.webpage_url})**\n"
            f"Duration: `{format_duration(track.duration)}`\n"
            f"Queue position: `{position}`",
            color=discord.Colour.blurple(),
        ),
        allowed_mentions=mention_suppressed(),
    )
    if not voice.is_playing() and not voice.is_paused():
        await start_next_music_track(ctx.guild.id)


async def prefix_music_skip(ctx: commands.Context[SentinelBot], args: str) -> None:
    voice = await music_voice_for_context(ctx)
    if not voice or not ctx.guild:
        return
    state = await get_music_state(ctx.guild.id)
    if state.current is None:
        await ctx.send("Nothing is playing right now.")
        return
    if voice.is_playing() or voice.is_paused():
        state.skip_current = True
        voice.stop()
        await ctx.send("Skipped the current song.")
    else:
        state.generation += 1
        state.current = None
        await start_next_music_track(ctx.guild.id)
        await ctx.send("Skipped to the next song.")


async def prefix_music_stop(ctx: commands.Context[SentinelBot], args: str) -> None:
    voice = await music_voice_for_context(ctx)
    if not voice or not ctx.guild:
        return
    state = await get_music_state(ctx.guild.id)
    state.queue.clear()
    state.loop_mode = "off"
    state.generation += 1
    if voice.is_playing() or voice.is_paused():
        state.skip_current = True
        voice.stop()
    else:
        state.skip_current = False
        state.current = None
    await ctx.send("Playback stopped and the queue was cleared. I will stay in voice; use `$leave` to disconnect.")


async def prefix_music_pause(ctx: commands.Context[SentinelBot], args: str) -> None:
    voice = await music_voice_for_context(ctx)
    if voice and voice.is_playing():
        voice.pause()
        await ctx.send("Playback paused.")
    elif voice:
        await ctx.send("Nothing is playing right now.")


async def prefix_music_resume(ctx: commands.Context[SentinelBot], args: str) -> None:
    voice = await music_voice_for_context(ctx)
    if voice and voice.is_paused():
        voice.resume()
        await ctx.send("Playback resumed.")
    elif voice:
        await ctx.send("Playback is not paused.")


async def prefix_music_queue(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not await music_voice_for_context(ctx) or not ctx.guild:
        return
    state = await get_music_state(ctx.guild.id)
    lines: list[str] = []
    if state.current:
        lines.append(f"**Now:** [{state.current.title}]({state.current.webpage_url}) · `{format_duration(state.current.duration)}`")
    upcoming = list(state.queue)[:15]
    lines.extend(
        f"`{index}.` [{track.title}]({track.webpage_url}) · `{format_duration(track.duration)}`"
        for index, track in enumerate(upcoming, 1)
    )
    if not lines:
        await ctx.send("The music queue is empty.")
        return
    remaining = max(0, len(state.queue) - len(upcoming))
    if remaining:
        lines.append(f"...and {remaining} more song(s).")
    await ctx.send(
        embed=make_embed(
            f"Music queue • {len(state.queue)} upcoming",
            "\n".join(lines)[:3900],
        ),
        allowed_mentions=mention_suppressed(),
    )


async def prefix_music_nowplaying(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not await music_voice_for_context(ctx) or not ctx.guild:
        return
    state = await get_music_state(ctx.guild.id)
    if not state.current:
        await ctx.send("Nothing is playing right now.")
        return
    track = state.current
    await ctx.send(
        embed=make_embed(
            "Now playing",
            f"**[{track.title}]({track.webpage_url})**\n"
            f"Duration: `{format_duration(track.duration)}`\n"
            f"Requested by <@{track.requester_id}>",
            color=discord.Colour.green(),
        ),
        allowed_mentions=mention_suppressed(),
    )


async def prefix_music_volume(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not await music_voice_for_context(ctx) or not ctx.guild:
        return
    state = await get_music_state(ctx.guild.id)
    value = args.strip()
    if not value:
        await ctx.send(f"Current volume: **{round(state.volume * 100)}%**. Use `$volume <0-200>`.")
        return
    if not value.isdigit() or not 0 <= int(value) <= 200:
        raise commands.BadArgument("Volume must be a number from 0 to 200.")
    volume = int(value)
    state.volume = volume / 100
    voice = ctx.guild.voice_client
    if voice and isinstance(voice.source, discord.PCMVolumeTransformer):
        voice.source.volume = state.volume
    await ctx.send(f"Playback volume set to **{volume}%**.")


async def prefix_music_loop(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not await music_voice_for_context(ctx) or not ctx.guild:
        return
    state = await get_music_state(ctx.guild.id)
    value = (args.strip().casefold() or "status")
    modes = {"off": "off", "none": "off", "track": "track", "song": "track", "queue": "queue"}
    if value == "status":
        await ctx.send(f"Loop mode: **{state.loop_mode}**.")
        return
    if value not in modes:
        raise commands.BadArgument("Choose `$loop off`, `$loop track`, or `$loop queue`.")
    state.loop_mode = modes[value]
    await ctx.send(f"Loop mode set to **{state.loop_mode}**.")


async def prefix_music_shuffle(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not await music_voice_for_context(ctx) or not ctx.guild:
        return
    state = await get_music_state(ctx.guild.id)
    tracks = list(state.queue)
    if len(tracks) < 2:
        await ctx.send("Add at least two upcoming songs before shuffling.")
        return
    random.shuffle(tracks)
    state.queue = deque(tracks)
    await ctx.send(f"Shuffled **{len(tracks)}** upcoming songs.")


async def prefix_music_remove(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not await music_voice_for_context(ctx) or not ctx.guild:
        return
    tokens = parse_prefix_args(args)
    if len(tokens) != 1 or not tokens[0].isdigit():
        raise commands.BadArgument("Usage: `$remove <queue-position>`")
    state = await get_music_state(ctx.guild.id)
    position = int(tokens[0])
    if not 1 <= position <= len(state.queue):
        await ctx.send(f"Choose a queue position from 1 to {len(state.queue)}.")
        return
    tracks = list(state.queue)
    removed = tracks.pop(position - 1)
    state.queue = deque(tracks)
    await ctx.send(f"Removed **{removed.title}** from the queue.")


async def prefix_music_clear(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not await music_voice_for_context(ctx) or not ctx.guild:
        return
    state = await get_music_state(ctx.guild.id)
    count = len(state.queue)
    state.queue.clear()
    await ctx.send(f"Removed **{count}** upcoming song(s) from the queue.")


async def prefix_music_status(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not ctx.guild:
        await ctx.send("This command can only be used in a server.")
        return
    state = await get_music_state(ctx.guild.id)
    voice = ctx.guild.voice_client
    channel = voice.channel.mention if voice and voice.is_connected() else "Disconnected"
    current = f"[{state.current.title}]({state.current.webpage_url})" if state.current else "Nothing playing"
    await ctx.send(
        embed=make_embed(
            "Voice and music status",
            f"Channel: {channel}\n"
            f"Playback: {current}\n"
            f"Upcoming songs: `{len(state.queue)}`\n"
            f"Volume: `{round(state.volume * 100)}%`\n"
            f"Loop: `{state.loop_mode}`\n"
            f"24/7 reconnect: `{'On' if state.afk_247 else 'Off'}`",
        )
    )


async def prefix_music_247(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not ctx.guild:
        await ctx.send("This command can only be used in a server.")
        return
    action = (args.strip().casefold() or "status")
    state = await get_music_state(ctx.guild.id)
    if action in {"status", "info"}:
        row = await db_fetchone(
            "SELECT channel_id FROM voice_settings WHERE guild_id = ? AND afk_247 = 1",
            (ctx.guild.id,),
        )
        channel = ctx.guild.get_channel(int(row["channel_id"])) if row else None
        await ctx.send(
            f"24/7 reconnect is **{'on' if state.afk_247 else 'off'}**"
            + (f" for {channel.mention}." if isinstance(channel, discord.VoiceChannel) else ".")
        )
        return
    if action in {"off", "disable", "false"}:
        await set_voice_247(ctx.guild, None, False)
        await ctx.send("24/7 reconnect is off. The bot stays connected until `$leave`.")
        return
    if action not in {"on", "enable", "true"}:
        raise commands.BadArgument("Usage: `$247 on|off|status`")
    if not isinstance(ctx.author, discord.Member) or not ctx.author.voice or not isinstance(ctx.author.voice.channel, discord.VoiceChannel):
        await ctx.send("Join the voice channel where the bot should stay, then run `$247 on`.")
        return
    try:
        await set_voice_247(ctx.guild, ctx.author.voice.channel, True)
    except (discord.Forbidden, discord.HTTPException, asyncio.TimeoutError, ValueError) as exc:
        log.warning("Could not enable 24/7 voice in guild %s: %s", ctx.guild.id, exc)
        await ctx.send(str(exc) if isinstance(exc, ValueError) else "I could not enable 24/7 mode. Check voice-channel permissions.")
        return
    await ctx.send(f"24/7 reconnect is **on** in {ctx.author.voice.channel.mention}. Use `$247 off` to disable it.")


async def prefix_message_pin(ctx: commands.Context[SentinelBot], args: str, pin: bool) -> None:
    tokens = parse_prefix_args(args)
    if not tokens or not tokens[0].isdigit():
        raise commands.BadArgument("Usage: `$pin <message-id>`")
    if not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("Use this in a text channel.")
        return
    message = await ctx.channel.fetch_message(int(tokens[0]))
    if pin:
        await message.pin(reason=f"Pinned by {ctx.author}")
        await ctx.send("Message pinned.")
    else:
        await message.unpin(reason=f"Unpinned by {ctx.author}")
        await ctx.send("Message unpinned.")


async def prefix_warn_list(ctx: commands.Context[SentinelBot], args: str) -> None:
    await prefix_warnings(ctx, args)


async def prefix_timeout_default(ctx: commands.Context[SentinelBot], args: str) -> None:
    await prefix_timeout(ctx, args, default_minutes=60)


async def prefix_execute_callback(ctx: commands.Context[SentinelBot], args: str) -> None:
    await ctx.send("This command requires a specific action. Use `$help` to see the correct syntax.")


def ticket_prefix_check() -> Any:
    async def predicate(ctx: commands.Context[SentinelBot]) -> bool:
        if ctx.guild is None or not isinstance(ctx.author, discord.Member):
            raise commands.NoPrivateMessage()
        if ctx.author.guild_permissions.manage_channels:
            return True
        staff_id = await get_setting(ctx.guild.id, "ticket_staff_role")
        if staff_id and any(role.id == int(staff_id) for role in ctx.author.roles):
            return True
        raise commands.MissingPermissions(["ticket staff role or Manage Channels"])

    return predicate


async def prefix_logs(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens or tokens[0].lower() in {"off", "none", "clear"}:
        await set_setting(ctx.guild.id, "log_channel", None)
        await ctx.send("Moderation log channel cleared.")
        return
    try:
        channel = await commands.TextChannelConverter().convert(ctx, tokens[0])
    except commands.BadArgument as exc:
        raise commands.BadArgument("Mention a text channel, or use `$logs off`.") from exc
    await set_setting(ctx.guild.id, "log_channel", str(channel.id))
    await ctx.send(f"Moderation logs will go to {channel.mention}.")


async def prefix_welcome_channel(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens or tokens[0].lower() in {"off", "none", "clear"}:
        await set_setting(ctx.guild.id, "welcome_channel", None)
        await ctx.send("Welcome channel cleared.")
        return
    try:
        channel = await commands.TextChannelConverter().convert(ctx, tokens[0])
    except commands.BadArgument as exc:
        raise commands.BadArgument("Mention a text channel, or use `$welcomechannel off`.") from exc
    await set_setting(ctx.guild.id, "welcome_channel", str(channel.id))
    await ctx.send(f"Welcome messages will go to {channel.mention}.")


async def prefix_welcome_message(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not args:
        raise commands.BadArgument("Usage: `$welcomemsg Welcome {user} to {server}!`")
    if len(args) > 1800:
        await ctx.send("Welcome message must be 1,800 characters or fewer.")
        return
    await set_setting(ctx.guild.id, "welcome_message", args)
    await ctx.send("Welcome message saved. Placeholders: `{user}` and `{server}`.")


async def prefix_toggle_setting(ctx: commands.Context[SentinelBot], args: str, key: str, label: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens or tokens[0].lower() not in {"on", "off", "true", "false", "enable", "disable"}:
        raise commands.BadArgument(f"Usage: `${ctx.command.name} on|off`")
    enabled = tokens[0].lower() in {"on", "true", "enable"}
    await set_setting(ctx.guild.id, key, "true" if enabled else "false")
    await ctx.send(f"{label} {'enabled' if enabled else 'disabled'}.")


async def prefix_blockword(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$blockword add <word>`, `$blockword remove <word>`, or `$blockword list`")
    action = tokens[0].lower()
    words_json = await get_setting(ctx.guild.id, "automod_words", "[]")
    try:
        words = [str(item) for item in json.loads(words_json or "[]")]
    except (TypeError, json.JSONDecodeError):
        words = []
    if action == "list":
        await ctx.send("Blocked words: " + (", ".join(f"`{word}`" for word in words) or "none"))
        return
    word = " ".join(tokens[1:]).strip()
    if action not in {"add", "remove"} or not 2 <= len(word) <= 64:
        raise commands.BadArgument("Usage: `$blockword add|remove <2-64 character word>`")
    if action == "add":
        if any(item.casefold() == word.casefold() for item in words):
            await ctx.send("That word is already blocked.")
            return
        if len(words) >= 50:
            await ctx.send("The block list is full (50 entries).")
            return
        words.append(word)
    else:
        words = [item for item in words if item.casefold() != word.casefold()]
    await set_setting(ctx.guild.id, "automod_words", json.dumps(words, ensure_ascii=False))
    await ctx.send(f"Updated AutoMod block list ({len(words)} entries).")


async def prefix_antispam(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if len(tokens) != 2 or not all(token.isdigit() for token in tokens):
        raise commands.BadArgument("Usage: `$antispam <3-10 messages> <3-30 seconds>`")
    limit, window = map(int, tokens)
    if not 3 <= limit <= 10 or not 3 <= window <= 30:
        await ctx.send("Choose 3-10 messages and a 3-30 second window.")
        return
    await set_setting(ctx.guild.id, "antispam_limit", str(limit))
    await set_setting(ctx.guild.id, "antispam_window", str(window))
    await ctx.send(f"AutoMod spam threshold: {limit} messages in {window} seconds.")


async def prefix_ticket_setup(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if len(tokens) != 3:
        raise commands.BadArgument("Usage: `$ticketsetup <category> <staff-role> <log-channel>`")
    try:
        category = await commands.CategoryChannelConverter().convert(ctx, tokens[0])
        role = await prefix_role(ctx, tokens[1])
        channel = await commands.TextChannelConverter().convert(ctx, tokens[2])
    except commands.BadArgument as exc:
        raise commands.BadArgument("Use a category, a staff role, and a transcript log channel.") from exc
    await set_setting(ctx.guild.id, "ticket_category", str(category.id))
    await set_setting(ctx.guild.id, "ticket_staff_role", str(role.id))
    await set_setting(ctx.guild.id, "ticket_log_channel", str(channel.id))
    await ctx.send(f"Tickets set up: category **{category.name}**, staff {role.mention}, logs {channel.mention}.")


async def prefix_ticket_panel(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("Use this in a text channel.")
        return
    embed = make_embed(
        "Need help?",
        "Press the button below to create a private support ticket. Only you and the support team can view it.",
    )
    await ctx.send(embed=embed, view=TicketPanelView(bot))


async def prefix_ticket_close(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("Use this inside a ticket channel.")
        return
    ticket = await db_fetchone(
        "SELECT * FROM tickets WHERE guild_id = ? AND channel_id = ? AND status = 'open'",
        (ctx.guild.id, ctx.channel.id),
    )
    if not ticket:
        await ctx.send("This is not an open ticket.")
        return
    staff_id = await get_setting(ctx.guild.id, "ticket_staff_role")
    is_staff = ctx.author.guild_permissions.manage_channels or (
        staff_id and any(role.id == int(staff_id) for role in ctx.author.roles)
    )
    if ctx.author.id != int(ticket["user_id"]) and not is_staff:
        await ctx.send("Only the ticket owner or configured ticket staff can close this ticket.")
        return
    reason = clean_reason(args or "Closed with prefix command")
    lines = [
        f"Ticket transcript — #{ctx.channel.name}",
        f"Server: {ctx.guild.name} ({ctx.guild.id})",
        f"Opened by: {ticket['user_id']}",
        f"Closed by: {ctx.author} ({ctx.author.id})",
        f"Reason: {reason}",
        "",
    ]
    async for message in ctx.channel.history(limit=1000, oldest_first=True):
        body = message.content.replace("\n", " ")
        if message.attachments:
            body += " " + " ".join(attachment.url for attachment in message.attachments)
        lines.append(f"[{message.created_at:%Y-%m-%d %H:%M:%S UTC}] {message.author} ({message.author.id}): {body or '[no text]'}")
    transcript = discord.File(
        io.BytesIO("\n".join(lines).encode("utf-8", errors="replace")),
        filename=f"ticket-{ctx.channel.id}-transcript.txt",
    )
    await log_action(
        ctx.guild,
        "Ticket closed",
        f"Channel: {ctx.channel.mention}\nClosed by: {ctx.author.mention}\nReason: {reason}",
        moderator=ctx.author,
        color=discord.Colour.dark_grey(),
        file=transcript,
        setting_key="ticket_log_channel",
    )
    owner = ctx.guild.get_member(int(ticket["user_id"]))
    if owner:
        overwrite = ctx.channel.overwrites_for(owner)
        overwrite.view_channel = False
        await ctx.channel.set_permissions(owner, overwrite=overwrite, reason="Ticket closed")
    await ctx.channel.edit(name=f"closed-{ctx.channel.id}", topic=f"Closed by {ctx.author} • {reason}")
    await db_execute(
        "UPDATE tickets SET status = 'closed', closed_at = ? WHERE channel_id = ?",
        (utc_now().isoformat(), ctx.channel.id),
    )
    await ctx.send("Ticket closed and transcript saved.")


async def prefix_ticket_claim(ctx: commands.Context[SentinelBot], args: str) -> None:
    await ticket_prefix_check()(ctx)
    if not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("Use this inside a ticket channel.")
        return
    cursor = await db_execute(
        "UPDATE tickets SET claimed_by = ? WHERE guild_id = ? AND channel_id = ? AND status = 'open'",
        (ctx.author.id, ctx.guild.id, ctx.channel.id),
    )
    await ctx.send(f"Ticket assigned to {ctx.author.mention}." if cursor else "This is not an open ticket.")


async def prefix_ticket_member(ctx: commands.Context[SentinelBot], args: str, add: bool) -> None:
    await ticket_prefix_check()(ctx)
    tokens = parse_prefix_args(args)
    if not tokens or not isinstance(ctx.channel, discord.TextChannel):
        raise commands.BadArgument(f"Usage: `${ctx.command.name} @member` in an open ticket.")
    ticket = await db_fetchone(
        "SELECT user_id FROM tickets WHERE guild_id = ? AND channel_id = ? AND status = 'open'",
        (ctx.guild.id, ctx.channel.id),
    )
    if not ticket:
        await ctx.send("This is not an open ticket.")
        return
    member = await prefix_member(ctx, tokens[0])
    if not add and member.id == int(ticket["user_id"]):
        await ctx.send("The ticket owner cannot be removed from their ticket.")
        return
    if add:
        await ctx.channel.set_permissions(
            member,
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            reason=f"Added to ticket by {ctx.author}",
        )
        await ctx.send(f"Added {member.mention} to this ticket.")
    else:
        await ctx.channel.set_permissions(member, overwrite=None, reason=f"Removed from ticket by {ctx.author}")
        await ctx.send(f"Removed {member.mention} from this ticket.")


async def prefix_poll(ctx: commands.Context[SentinelBot], args: str) -> None:
    parts = [part.strip() for part in args.split("|", 2)]
    if len(parts) != 3 or not all(parts):
        raise commands.BadArgument("Usage: `$poll Question | Option A | Option B`")
    if any(len(part) > 180 for part in parts):
        await ctx.send("Keep the question and options under 180 characters each.")
        return
    message = await ctx.send(
        embed=make_embed(f"Poll: {parts[0]}", f"🇦 {parts[1]}\n🇧 {parts[2]}"),
        allowed_mentions=mention_suppressed(),
    )
    await message.add_reaction("🇦")
    await message.add_reaction("🇧")


async def prefix_coinflip(ctx: commands.Context[SentinelBot], args: str) -> None:
    await ctx.send(f"Coin flip: **{random.choice(['Heads', 'Tails'])}**")


async def prefix_roll(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    sides = int(tokens[0]) if tokens and tokens[0].isdigit() else 6
    if not 2 <= sides <= 100000:
        await ctx.send("Choose a die from 2 to 100,000 sides.")
        return
    await ctx.send(f"{ctx.author.mention} rolled **{random.randint(1, sides)}** (d{sides}).")


async def prefix_choose(ctx: commands.Context[SentinelBot], args: str) -> None:
    choices = [item.strip() for item in re.split(r"[,\n|]", args) if item.strip()]
    if len(choices) < 2:
        raise commands.BadArgument("Usage: `$choose option one | option two | option three`")
    await ctx.send(f"I choose: **{random.choice(choices)[:200]}**")


async def prefix_calculate(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not args:
        raise commands.BadArgument("Usage: `$calc (2 + 3) * 4`")
    try:
        result = safe_calc(args)
    except (ValueError, SyntaxError, ZeroDivisionError, OverflowError):
        await ctx.send("Invalid arithmetic. Only basic math and powers up to 100 are supported.")
        return
    await ctx.send(f"`{args[:100]}` = **{result}**")


async def prefix_eightball(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not args:
        raise commands.BadArgument("Usage: `$8ball your question`")
    answer = random.choice(
        [
            "Yes.", "No.", "Ask again later.", "Most likely.", "It is uncertain.",
            "Absolutely.", "I would not count on it.", "The outlook is good.",
        ]
    )
    await ctx.send(f"Question: {args[:300]}\n**{answer}**")


async def prefix_say(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not args:
        raise commands.BadArgument("Usage: `$say message`")
    await ctx.message.delete()
    await ctx.send(args[:1900], allowed_mentions=mention_suppressed())


async def prefix_announce(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not args:
        raise commands.BadArgument("Usage: `$announce title | message`")
    parts = [part.strip() for part in args.split("|", 1)]
    title = parts[0][:200] or "Announcement"
    body = (parts[1] if len(parts) > 1 else parts[0])[:3500]
    await ctx.send(embed=make_embed(title, body, color=discord.Colour.gold()), allowed_mentions=mention_suppressed())


async def prefix_embed(ctx: commands.Context[SentinelBot], args: str) -> None:
    parts = [part.strip() for part in args.split("|", 1)]
    if len(parts) != 2 or not all(parts):
        raise commands.BadArgument("Usage: `$embed title | message`")
    await ctx.send(
        embed=make_embed(parts[0][:200], parts[1][:3500]),
        allowed_mentions=mention_suppressed(),
    )


async def prefix_randomcolor(ctx: commands.Context[SentinelBot], args: str) -> None:
    color = discord.Colour(random.randint(0, 0xFFFFFF))
    await ctx.send(embed=make_embed("Random color", f"Hex: `{color}`", color=color))


async def prefix_roleinfo(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$roleinfo @role`")
    role = await prefix_role(ctx, tokens[0])
    embed = make_embed(role.name, color=role.color)
    embed.add_field(name="ID", value=f"`{role.id}`", inline=True)
    embed.add_field(name="Members", value=str(len(role.members)), inline=True)
    embed.add_field(name="Position", value=str(role.position), inline=True)
    embed.add_field(name="Mentionable", value=str(role.mentionable), inline=True)
    embed.add_field(name="Hoisted", value=str(role.hoist), inline=True)
    await ctx.send(embed=embed)


async def prefix_channelinfo(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    channel: discord.abc.GuildChannel = ctx.channel
    if tokens:
        try:
            channel = await commands.GuildChannelConverter().convert(ctx, tokens[0])
        except commands.BadArgument as exc:
            raise commands.BadArgument("Mention a server channel or use its ID.") from exc
    embed = make_embed(f"#{channel.name}")
    embed.add_field(name="ID", value=f"`{channel.id}`", inline=True)
    embed.add_field(name="Type", value=channel.type.name, inline=True)
    embed.add_field(name="Category", value=channel.category.name if channel.category else "None", inline=True)
    await ctx.send(embed=embed)


async def prefix_servericon(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not ctx.guild or not ctx.guild.icon:
        await ctx.send("This server does not have an icon.")
        return
    embed = make_embed(f"{ctx.guild.name} icon")
    embed.set_image(url=ctx.guild.icon.with_size(1024).url)
    await ctx.send(embed=embed)


async def prefix_purge_action(ctx: commands.Context[SentinelBot], args: str) -> None:
    await prefix_purge_kind(ctx, args, ctx.command.name.removeprefix("purge"))


async def prefix_lock_action(ctx: commands.Context[SentinelBot], args: str) -> None:
    await prefix_lock(ctx, args, ctx.command.name == "lock")


async def prefix_role_add_action(ctx: commands.Context[SentinelBot], args: str) -> None:
    await prefix_role_change(ctx, args, True)


async def prefix_role_remove_action(ctx: commands.Context[SentinelBot], args: str) -> None:
    await prefix_role_change(ctx, args, False)


async def prefix_voice_action_dispatch(ctx: commands.Context[SentinelBot], args: str) -> None:
    await prefix_voice_action(ctx, args, ctx.command.name)


async def prefix_pin_action(ctx: commands.Context[SentinelBot], args: str) -> None:
    await prefix_message_pin(ctx, args, ctx.command.name == "pin")


async def prefix_invite(ctx: commands.Context[SentinelBot], args: str) -> None:
    if not bot.user:
        await ctx.send("Bot information is not available yet.")
        return
    invite_url = discord.utils.oauth_url(bot.user.id, scopes=("bot", "applications.commands"))
    await ctx.send(f"Invite Sentinel: <{invite_url}>")


async def mc_target(ctx: commands.Context[SentinelBot], args: str) -> tuple[str, bool]:
    tokens = parse_prefix_args(args)
    if tokens:
        address = tokens[0]
        if len(tokens) >= 2 and tokens[1].isdigit() and ":" not in address:
            address = f"{address}:{tokens[1]}"
        bedrock = any(token.casefold() in {"bedrock", "be", "pe"} for token in tokens[1:])
        return minecraft_api_address(address), bedrock
    row = await db_fetchone(
        "SELECT address, bedrock FROM minecraft_monitors WHERE guild_id = ?",
        (ctx.guild.id,),
    )
    if not row:
        raise commands.BadArgument("Add an address first with `$mcsetup <ip-or-host> [port] [bedrock]`.")
    return str(row["address"]), bool(row["bedrock"])


async def minecraft_lookup_for_context(
    ctx: commands.Context[SentinelBot],
    args: str,
) -> tuple[str, bool, dict[str, Any]]:
    address, bedrock = await mc_target(ctx, args)
    try:
        data = await get_minecraft_status(address, bedrock)
    except Exception as exc:
        log.warning("Minecraft command lookup failed for %s: %s", address, exc)
        data = {"online": False, "debug": {"error": "The status API could not be reached."}}
    return address, bedrock, data


async def prefix_mcstatus(ctx: commands.Context[SentinelBot], args: str) -> None:
    address, bedrock, data = await minecraft_lookup_for_context(ctx, args)
    await ctx.send(
        embed=minecraft_status_embed(address, data, bedrock=bedrock),
        allowed_mentions=mention_suppressed(),
    )


async def prefix_mcsetup(ctx: commands.Context[SentinelBot], args: str) -> None:
    tokens = parse_prefix_args(args)
    if not tokens:
        raise commands.BadArgument("Usage: `$mcsetup <ip-or-host> [port] [bedrock]`")
    address = tokens[0]
    bedrock = any(token.casefold() in {"bedrock", "be", "pe"} for token in tokens[1:])
    numeric_port = next((token for token in tokens[1:] if token.isdigit()), None)
    if numeric_port and ":" not in address:
        address = f"{address}:{numeric_port}"
    address = minecraft_api_address(address)
    existing = await db_fetchone(
        "SELECT channel_id, message_id FROM minecraft_monitors WHERE guild_id = ?",
        (ctx.guild.id,),
    )
    try:
        data = await get_minecraft_status(address, bedrock)
    except Exception as exc:
        log.warning("Minecraft setup lookup failed for %s: %s", address, exc)
        data = {
            "online": False,
            "debug": {"error": "The status API could not be reached; monitoring will retry automatically."},
        }
    embed = minecraft_status_embed(address, data, bedrock=bedrock)
    if existing and int(existing["channel_id"]) == ctx.channel.id:
        try:
            message = await ctx.channel.fetch_message(int(existing["message_id"]))
            await message.edit(embed=embed)
        except discord.NotFound:
            message = await ctx.send(embed=embed, allowed_mentions=mention_suppressed())
    else:
        message = await ctx.send(embed=embed, allowed_mentions=mention_suppressed())
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
        (
            ctx.guild.id,
            ctx.channel.id,
            message.id,
            address,
            int(bedrock),
            utc_now().isoformat(),
            utc_now().isoformat(),
        ),
    )
    await ctx.send(
        f"✅ Minecraft monitor saved for `{address}`. This message refreshes every **3 minutes**. "
        f"Use `$mcstop` to stop it.",
        allowed_mentions=mention_suppressed(),
    )


async def prefix_mcstop(ctx: commands.Context[SentinelBot], args: str) -> None:
    row = await db_fetchone(
        "SELECT channel_id, message_id FROM minecraft_monitors WHERE guild_id = ?",
        (ctx.guild.id,),
    )
    if not row:
        await ctx.send("Minecraft monitoring is not configured in this server.")
        return
    await db_execute("DELETE FROM minecraft_monitors WHERE guild_id = ?", (ctx.guild.id,))
    channel = ctx.guild.get_channel(int(row["channel_id"]))
    if isinstance(channel, discord.TextChannel):
        try:
            message = await channel.fetch_message(int(row["message_id"]))
            await message.edit(
                embed=make_embed(
                    "Minecraft status monitor stopped",
                    "This monitor was stopped by a server administrator.",
                    color=discord.Colour.dark_grey(),
                )
            )
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
    await ctx.send("Minecraft status monitoring stopped for this server.")


async def prefix_mcrefresh(ctx: commands.Context[SentinelBot], args: str) -> None:
    row = await db_fetchone(
        "SELECT channel_id, message_id, address, bedrock FROM minecraft_monitors WHERE guild_id = ?",
        (ctx.guild.id,),
    )
    if not row:
        raise commands.BadArgument("No monitor is configured. Use `$mcsetup <ip-or-host> [port] [bedrock]` first.")
    result = await minecraft_monitor_update(
        ctx.guild,
        channel_id=int(row["channel_id"]),
        message_id=int(row["message_id"]),
        address=str(row["address"]),
        bedrock=bool(row["bedrock"]),
    )
    if result:
        await db_execute(
            "UPDATE minecraft_monitors SET channel_id = ?, message_id = ?, updated_at = ? WHERE guild_id = ?",
            (*result, utc_now().isoformat(), ctx.guild.id),
        )
        await ctx.send("Minecraft status refreshed.")
    else:
        await ctx.send("I could not update the monitor message. Check my Send Messages and Embed Links permissions.")


async def prefix_mc_fullstatus(ctx: commands.Context[SentinelBot], args: str) -> None:
    await prefix_mcstatus(ctx, args)


async def prefix_mc_players(ctx: commands.Context[SentinelBot], args: str) -> None:
    address, bedrock, data = await minecraft_lookup_for_context(ctx, args)
    if not data.get("online"):
        await ctx.send(embed=minecraft_status_embed(address, data, bedrock=bedrock))
        return
    players = data.get("players") or {}
    names = [
        minecraft_text(item.get("name") if isinstance(item, dict) else item, "")
        for item in (players.get("list") or [])[:100]
    ]
    text = ", ".join(names) if names else "The server did not provide a player list."
    await ctx.send(
        embed=make_embed(
            f"Players on {address}",
            f"Online: **{players.get('online', 0)} / {players.get('max', '?')}**\n{text[:3500]}",
            color=discord.Colour.green(),
        )
    )


async def prefix_mc_field(ctx: commands.Context[SentinelBot], args: str, field: str, label: str) -> None:
    address, bedrock, data = await minecraft_lookup_for_context(ctx, args)
    if field == "online":
        value = "ONLINE" if data.get("online") else "OFFLINE"
    elif field == "version":
        protocol = data.get("protocol") or {}
        value = data.get("version") or protocol.get("name")
    elif field == "motd":
        value = data.get("motd")
    elif field == "players":
        players = data.get("players") or {}
        value = f"{players.get('online', 0)} / {players.get('max', '?')}"
    elif field == "playernames":
        players = data.get("players") or {}
        value = ", ".join(
            minecraft_text(item.get("name") if isinstance(item, dict) else item, "")
            for item in (players.get("list") or [])
        )
    elif field in {"plugins", "mods"}:
        items = data.get(field) or []
        value = ", ".join(minecraft_text(item.get("name") if isinstance(item, dict) else item, "") for item in items)
    else:
        value = data.get(field)
    if not data.get("online"):
        value = "Offline / unavailable"
    await ctx.send(
        embed=make_embed(
            f"Minecraft {label}",
            f"Server: `{address}`\n{minecraft_text(value, 'No data reported.')[:3500]}",
            color=discord.Colour.green() if data.get("online") else discord.Colour.red(),
        )
    )


async def prefix_mc_icon(ctx: commands.Context[SentinelBot], args: str) -> None:
    address, _, data = await minecraft_lookup_for_context(ctx, args)
    encoded = quote(address, safe=":[],-._")
    embed = make_embed(f"Minecraft icon • {address}", "Server icon from the status service.")
    embed.set_image(url=f"https://api.mcsrvstat.us/icon/{encoded}")
    if not data.get("online"):
        embed.description = "The server is currently offline or unavailable."
    await ctx.send(embed=embed)


async def prefix_mc_monitor_info(ctx: commands.Context[SentinelBot], args: str) -> None:
    row = await db_fetchone(
        "SELECT channel_id, message_id, address, bedrock, updated_at FROM minecraft_monitors WHERE guild_id = ?",
        (ctx.guild.id,),
    )
    if not row:
        await ctx.send("No Minecraft status monitor is configured in this server.")
        return
    channel = ctx.guild.get_channel(int(row["channel_id"]))
    await ctx.send(
        embed=make_embed(
            "Minecraft monitor configuration",
            f"Address: `{row['address']}`\n"
            f"Edition: `{'Bedrock' if row['bedrock'] else 'Java'}`\n"
            f"Channel: {channel.mention if channel else row['channel_id']}\n"
            f"Message ID: `{row['message_id']}`\n"
            f"Last update: `{row['updated_at']}`\n"
            "Refresh interval: `3 minutes`",
        )
    )


async def prefix_mc_query(ctx: commands.Context[SentinelBot], args: str) -> None:
    address, bedrock, data = await minecraft_lookup_for_context(ctx, args)
    debug = data.get("debug") or {}
    fields = "\n".join(f"`{key}`: `{value}`" for key, value in debug.items())
    await ctx.send(
        embed=make_embed(
            f"Minecraft diagnostics • {address}",
            f"Edition: `{'Bedrock' if bedrock else 'Java'}`\nOnline: `{bool(data.get('online'))}`\n{fields or 'No diagnostic details returned.'}",
        )
    )


async def prefix_mc_json(ctx: commands.Context[SentinelBot], args: str) -> None:
    address, bedrock, data = await minecraft_lookup_for_context(ctx, args)
    safe_payload = json.dumps(
        {key: value for key, value in data.items() if key != "icon"},
        ensure_ascii=False,
        indent=2,
    )
    await ctx.send(
        f"```json\n{safe_payload[:1900]}\n```",
        allowed_mentions=mention_suppressed(),
    )


async def prefix_mc_unsupported_action(ctx: commands.Context[SentinelBot], args: str) -> None:
    await ctx.send(
        "This Minecraft command is an information/status command. "
        "For live server control, configure RCON separately; Sentinel will not fake server actions."
    )


async def prefix_mc_register_status_commands() -> None:
    return


async def prefix_ticket_send_panel(ctx: commands.Context[SentinelBot], args: str) -> None:
    await prefix_ticket_panel(ctx, args)


def register_prefix_commands() -> None:
    specs: list[tuple[str, str, str, str, PrefixHandler, tuple[str, ...], tuple[str, ...]]] = [
        ("help", "General", "[page|search]", "Search commands or browse the paginated command list.", prefix_help, ("h", "commands", "cmds", "guide", "menu"), ()),
        ("setup", "Settings", "", "Open the interactive setup center for tickets, AutoMod, welcome, logs, Minecraft, voice, and moderation.", prefix_setup, ("configure", "config", "settings", "dashboard", "setupmenu"), ("manage_guild",)),
        ("join", "Music", "", "Join or move to your current voice channel.", prefix_music_join, ("connect", "vcjoin", "summon"), ()),
        ("leave", "Music", "", "Leave voice, clear the queue, and turn off 24/7 reconnect.", prefix_music_leave, ("disconnectbot", "vcleave", "musicleave"), ()),
        ("play", "Music", "<song name or YouTube URL>", "Play a YouTube song or add it to the voice queue.", prefix_music_play, ("p", "song", "ytplay", "musicplay"), ()),
        ("skip", "Music", "", "Skip the current song.", prefix_music_skip, ("next", "skipsong", "musicnext"), ()),
        ("stop", "Music", "", "Stop playback and clear upcoming songs without leaving voice.", prefix_music_stop, ("stopsong", "musicstop"), ()),
        ("pause", "Music", "", "Pause the current song.", prefix_music_pause, ("musicpause",), ()),
        ("resume", "Music", "", "Resume paused music.", prefix_music_resume, ("unpause", "musicresume"), ()),
        ("queue", "Music", "", "Show the current song and upcoming queue.", prefix_music_queue, ("q", "songqueue", "playlist"), ()),
        ("nowplaying", "Music", "", "Show the currently playing song.", prefix_music_nowplaying, ("np", "current", "playing"), ()),
        ("volume", "Music", "[0-200]", "Get or change playback volume.", prefix_music_volume, ("vol", "setvolume"), ()),
        ("loop", "Music", "[off|track|queue]", "Repeat the current track or the whole queue.", prefix_music_loop, ("loopmode", "repeat"), ()),
        ("shuffle", "Music", "", "Shuffle upcoming songs.", prefix_music_shuffle, ("shufflequeue",), ()),
        ("remove", "Music", "<queue-position>", "Remove one upcoming song by its position.", prefix_music_remove, ("removequeue", "rmqueue"), ()),
        ("clearqueue", "Music", "", "Clear upcoming songs without stopping the current track.", prefix_music_clear, ("emptyqueue", "queueclear"), ()),
        ("voice", "Music", "", "Show voice channel, playback, queue, volume, loop, and 24/7 status.", prefix_music_status, ("voiceinfo", "musicstatus"), ()),
        ("247", "Music", "<on|off|status>", "Keep the bot connected to your voice channel and reconnect after restarts.", prefix_music_247, ("voice247", "stayconnected", "twentyfourseven"), ("manage_guild",)),
        ("ping", "General", "", "Check the bot's gateway latency.", prefix_ping, ("latency", "botping", "pong", "ms"), ()),
        ("status", "General", "", "Show online state, latency, server count, and prefix.", prefix_status, ("botstatus", "online", "health"), ()),
        ("about", "General", "", "Show bot version, prefix, and server count.", prefix_about, ("botinfo", "info", "sentinel"), ()),
        ("serverinfo", "Information", "", "Show this server's owner, member count, roles, and channels.", prefix_serverinfo, ("guildinfo", "server", "guild"), ()),
        ("userinfo", "Information", "[@member]", "Show a member's account, join date, and roles.", prefix_userinfo, ("whois", "memberinfo", "user"), ()),
        ("avatar", "Information", "[@user]", "Show a user's avatar.", prefix_avatar, ("pfp", "profilepic", "useravatar"), ()),
        ("membercount", "Information", "", "Show total members, people, and bots.", prefix_membercount, ("members", "member_count", "countmembers"), ()),
        ("rolelist", "Information", "", "List server roles and member counts.", prefix_rolelist, ("roles", "listroles", "serverroles"), ()),
        ("channelcount", "Information", "", "Show channel counts by type.", prefix_channelcount, ("channels", "countchannels", "serverchannels"), ()),
        ("roleinfo", "Information", "@role", "Show role settings and member count.", prefix_roleinfo, ("role", "aboutrole", "role_info"), ()),
        ("channelinfo", "Information", "[#channel]", "Show channel type and category.", prefix_channelinfo, ("channel", "aboutchannel", "channel_info"), ()),
        ("servericon", "Information", "", "Show the server icon.", prefix_servericon, ("guildicon", "icon", "server_icon"), ()),
        ("warn", "Moderation", "@member [reason]", "Add a persistent warning and record it in the mod log.", prefix_warn, ("warnuser", "warning", "issuewarn"), ("moderate_members",)),
        ("warnings", "Moderation", "@member", "View a member's active warnings.", prefix_warnings, ("warns", "warninglist", "checkwarns"), ("moderate_members",)),
        ("unwarn", "Moderation", "<warning-id>", "Remove one active warning.", prefix_unwarn, ("removewarn", "deletewarn", "clearwarn"), ("moderate_members",)),
        ("clearwarnings", "Moderation", "@member", "Clear all active warnings for a member.", prefix_clearwarnings, ("clearwarns", "resetwarns", "warnreset"), ("moderate_members",)),
        ("kick", "Moderation", "@member [reason]", "Kick a member after checking role hierarchy.", prefix_kick, ("kickuser", "kickmember", "removeuser"), ("kick_members",)),
        ("ban", "Moderation", "@member [reason]", "Ban a member and log the action.", prefix_ban, ("banuser", "banmember", "serverban"), ("ban_members",)),
        ("unban", "Moderation", "<user-id> [reason]", "Unban a user by their numeric Discord ID.", prefix_unban, ("pardon", "unbanuser", "removeban"), ("ban_members",)),
        ("softban", "Moderation", "@member [reason]", "Ban and unban a member to clear their recent messages.", prefix_softban, ("softbanuser", "tempkick", "cleanban"), ("ban_members",)),
        ("timeout", "Moderation", "@member <minutes> [reason]", "Timeout a member for up to 28 days.", prefix_timeout, ("timeoutuser", "silence", "muteuser"), ("moderate_members",)),
        ("mute", "Moderation", "@member [reason]", "Apply a 60-minute timeout.", prefix_timeout_default, ("quickmute", "mutemember", "silenceuser"), ("moderate_members",)),
        ("untimeout", "Moderation", "@member [reason]", "Remove a member's timeout.", prefix_untimeout, ("unmute", "untimeoutuser", "unsilence"), ("moderate_members",)),
        ("purge", "Moderation", "<1-100>", "Delete up to 100 recent channel messages.", prefix_purge, ("clear", "clean", "prune"), ("manage_messages",)),
        ("purgeuser", "Moderation", "@member <1-100>", "Delete recent messages by one member.", prefix_purgeuser, ("cleanuser", "clearuser", "purge_member"), ("manage_messages",)),
        ("clearbots", "Moderation", "[1-100]", "Delete recent bot messages.", prefix_clearbots, ("purgebots", "cleanbots", "clear_bots"), ("manage_messages",)),
        ("purgelinks", "Moderation", "[1-100]", "Delete recent messages containing URLs.", prefix_purge_action, ("cleanlinks", "clearlinks", "purge_links"), ("manage_messages",)),
        ("purgeinvites", "Moderation", "[1-100]", "Delete recent Discord invite links.", prefix_purge_action, ("cleaninvites", "clearinvites", "purge_invites"), ("manage_messages",)),
        ("purgeattachments", "Moderation", "[1-100]", "Delete recent messages with attachments.", prefix_purge_action, ("cleanattachments", "clearattachments", "purgefiles"), ("manage_messages",)),
        ("purgeembeds", "Moderation", "[1-100]", "Delete recent messages with embeds.", prefix_purge_action, ("cleanembeds", "clearembeds", "purge_embeds"), ("manage_messages",)),
        ("lock", "Moderation", "", "Prevent @everyone from sending messages here.", prefix_lock_action, ("lockdown", "lockchannel", "channellock"), ("manage_channels",)),
        ("unlock", "Moderation", "", "Allow @everyone to send messages here again.", prefix_lock_action, ("unlockchannel", "channelunlock", "openchat"), ("manage_channels",)),
        ("slowmode", "Moderation", "<0-21600 seconds>", "Set channel slowmode.", prefix_slowmode, ("slow", "setslowmode", "chatdelay"), ("manage_channels",)),
        ("nick", "Moderation", "@member <nickname>", "Change a member's server nickname.", prefix_nick, ("nickname", "setnick", "changenick"), ("manage_nicknames",)),
        ("clearnick", "Moderation", "@member", "Clear a member's server nickname.", prefix_clearnick, ("resetnick", "removenick", "clearnickname"), ("manage_nicknames",)),
        ("roleadd", "Moderation", "@member @role", "Give a member a role after checking role hierarchy.", prefix_role_add_action, ("addrole", "giverole", "assignrole"), ("manage_roles",)),
        ("roleremove", "Moderation", "@member @role", "Remove a role from a member.", prefix_role_remove_action, ("removerole", "takerole", "unassignrole"), ("manage_roles",)),
        ("voicekick", "Moderation", "@member", "Disconnect a member from voice.", prefix_voice_action_dispatch, ("disconnect", "voiceleave", "kickvoice"), ("move_members",)),
        ("voicemute", "Moderation", "@member", "Server-mute a member in voice.", prefix_voice_action_dispatch, ("servermute", "mutevoice", "voicemuteuser"), ("mute_members",)),
        ("voiceunmute", "Moderation", "@member", "Remove a server voice mute.", prefix_voice_action_dispatch, ("serverunmute", "unmutevoice", "voiceunmuteuser"), ("mute_members",)),
        ("deafen", "Moderation", "@member", "Server-deafen a member in voice.", prefix_voice_action_dispatch, ("serverdeafen", "deafenuser", "deafenvoice"), ("deafen_members",)),
        ("undeafen", "Moderation", "@member", "Remove a server voice deafen.", prefix_voice_action_dispatch, ("serverundeafen", "undeafenuser", "undeafenvoice"), ("deafen_members",)),
        ("pin", "Moderation", "<message-id>", "Pin a message in this channel.", prefix_pin_action, ("pinmessage", "savepin", "messagepin"), ("manage_messages",)),
        ("unpin", "Moderation", "<message-id>", "Unpin a message in this channel.", prefix_pin_action, ("unpinmessage", "removepin", "messageunpin"), ("manage_messages",)),
        ("logs", "Settings", "[#channel|off]", "Set or clear the moderation log channel.", prefix_logs, ("setlogs", "logchannel", "modlogs"), ("manage_guild",)),
        ("welcomechannel", "Settings", "[#channel|off]", "Set or clear the welcome channel.", prefix_welcome_channel, ("setwelcome", "welcomeset", "welcomechan"), ("manage_guild",)),
        ("welcomemsg", "Settings", "<message>", "Set welcome text using {user} and {server}.", prefix_welcome_message, ("setwelcomemsg", "welcometext", "welcome_message"), ("manage_guild",)),
        ("automod", "Settings", "<on|off>", "Enable or disable AutoMod.", lambda ctx, args: prefix_toggle_setting(ctx, args, "automod_enabled", "AutoMod"), ("auto_mod", "antiraid", "automodtoggle"), ("manage_guild",)),
        ("filterlinks", "Settings", "<on|off>", "Enable or disable URL filtering.", lambda ctx, args: prefix_toggle_setting(ctx, args, "filter_links", "Link filtering"), ("antilinks", "linkfilter", "linksfilter"), ("manage_guild",)),
        ("filterinvites", "Settings", "<on|off>", "Enable or disable Discord invite filtering.", lambda ctx, args: prefix_toggle_setting(ctx, args, "filter_invites", "Invite filtering"), ("antiinvites", "invitefilter", "invitesfilter"), ("manage_guild",)),
        ("blockword", "Settings", "add|remove <word> | list", "Manage AutoMod blocked words.", prefix_blockword, ("badword", "wordfilter", "blockedwords"), ("manage_guild",)),
        ("antispam", "Settings", "<3-10 messages> <3-30 seconds>", "Configure AutoMod's spam threshold.", prefix_antispam, ("spamfilter", "setantispam", "antispamconfig"), ("manage_guild",)),
        ("ticketsetup", "Tickets", "<category> <staff-role> <log-channel>", "Configure the ticket category, staff role, and transcript channel.", prefix_ticket_setup, ("setticket", "ticketconfig", "setuptickets"), ("manage_guild",)),
        ("ticketpanel", "Tickets", "", "Post the persistent ticket-open button.", prefix_ticket_send_panel, ("newticketpanel", "supportpanel", "createpanel"), ("manage_guild",)),
        ("ticketclose", "Tickets", "[reason]", "Close the current ticket and export its transcript.", prefix_ticket_close, ("closeticket", "closecase", "endticket"), ()),
        ("ticketclaim", "Tickets", "", "Assign the current open ticket to yourself.", prefix_ticket_claim, ("claimticket", "claimcase", "assignticket"), ()),
        ("ticketadd", "Tickets", "@member", "Give a member access to the current ticket.", lambda ctx, args: prefix_ticket_member(ctx, args, True), ("addtoticket", "ticketuseradd", "grantticket"), ()),
        ("ticketremove", "Tickets", "@member", "Remove a member's access to this ticket.", lambda ctx, args: prefix_ticket_member(ctx, args, False), ("removefromticket", "ticketuserremove", "revoketicket"), ()),
        ("poll", "Utilities", "Question | Option A | Option B", "Create a two-option reaction poll.", prefix_poll, ("vote", "quickpoll", "createpoll"), ()),
        ("coinflip", "Utilities", "", "Flip a coin.", prefix_coinflip, ("coin", "flip", "headsortails"), ()),
        ("roll", "Utilities", "[sides]", "Roll a die (default d6).", prefix_roll, ("dice", "diceroll", "randomnumber"), ()),
        ("choose", "Utilities", "option one | option two", "Choose one item from a list.", prefix_choose, ("pick", "randomchoice", "decide"), ()),
        ("calc", "Utilities", "<expression>", "Safely calculate basic arithmetic.", prefix_calculate, ("calculate", "math", "calculator"), ()),
        ("8ball", "Utilities", "<question>", "Ask the eight ball a question.", prefix_eightball, ("eightball", "magic8ball", "askball"), ()),
        ("say", "Utilities", "<message>", "Send a message without triggering mentions.", prefix_say, ("repeat", "echo", "speak"), ("manage_messages",)),
        ("announce", "Utilities", "title | message", "Post a formatted announcement.", prefix_announce, ("announcement", "broadcast", "news"), ("manage_guild",)),
        ("embed", "Utilities", "title | message", "Post a formatted embed in this channel.", prefix_embed, ("sendembed", "makeembed", "postembed"), ("manage_messages",)),
        ("randomcolor", "Utilities", "", "Generate a random color embed and hex code.", prefix_randomcolor, ("randomcolour", "color", "colour"), ()),
        ("invite", "Utilities", "", "Create an OAuth invite URL for the bot.", prefix_invite, ("botinvite", "invitelink", "addbot"), ()),
        ("mcstatus", "Minecraft", "[ip[:port] [bedrock]]", "Show live Minecraft online/offline status, version, players, MOTD, software, and more.", prefix_mcstatus, ("minecraftstatus", "mcserverstatus", "mcserver", "serverstatusmc"), ()),
        ("mcsetup", "Minecraft", "<ip> [port] [bedrock]", "Create a 3-minute auto-refreshing Minecraft status monitor in this channel.", prefix_mcsetup, ("minecraftsetup", "mcconfig", "mcconfigure", "setupmc"), ("manage_guild",)),
        ("mcstop", "Minecraft", "", "Stop this server's Minecraft status monitor.", prefix_mcstop, ("minecraftstop", "mcdisable", "mcmonitoroff", "stopmc"), ("manage_guild",)),
        ("mcrefresh", "Minecraft", "", "Refresh the saved Minecraft status monitor immediately.", prefix_mcrefresh, ("minecraftrefresh", "mcupdate", "refreshmc", "mcnow"), ("manage_guild",)),
        ("mcplayers", "Minecraft", "[ip[:port]]", "Show the current Minecraft player count and available player list.", prefix_mc_players, ("minecraftplayers", "mcplayerlist", "mcusers", "mcplist"), ()),
        ("mcversion", "Minecraft", "[ip[:port]]", "Show the Minecraft server version.", lambda ctx, args: prefix_mc_field(ctx, args, "version", "version"), ("minecraftversion", "mcver", "mcversioninfo", "mcgameversion"), ()),
        ("mcmotd", "Minecraft", "[ip[:port]]", "Show the Minecraft server MOTD.", lambda ctx, args: prefix_mc_field(ctx, args, "motd", "MOTD"), ("minecraftmotd", "mcmotdinfo", "servermotd", "mctext"), ()),
        ("mcaddress", "Minecraft", "[ip[:port]]", "Show the resolved Minecraft server address and IP.", lambda ctx, args: prefix_mc_field(ctx, args, "ip", "address"), ("minecraftaddress", "mcip", "mcipaddress", "serverip"), ()),
        ("mcport", "Minecraft", "[ip[:port]]", "Show the Minecraft server port.", lambda ctx, args: prefix_mc_field(ctx, args, "port", "port"), ("minecraftport", "serverport", "mcserverport", "gameport"), ()),
        ("mcsoftware", "Minecraft", "[ip[:port]]", "Show detected Minecraft server software.", lambda ctx, args: prefix_mc_field(ctx, args, "software", "software"), ("minecraftsoftware", "mcengine", "serversoftware", "mctype"), ()),
        ("mcplugins", "Minecraft", "[ip[:port]]", "List detected Minecraft plugins.", lambda ctx, args: prefix_mc_field(ctx, args, "plugins", "plugins"), ("minecraftplugins", "pluginlist", "mcpluginlist", "serverplugins"), ()),
        ("mcmods", "Minecraft", "[ip[:port]]", "List detected Minecraft mods.", lambda ctx, args: prefix_mc_field(ctx, args, "mods", "mods"), ("minecraftmods", "modlist", "mcmodlist", "servermods"), ()),
        ("mcmap", "Minecraft", "[ip[:port]]", "Show the detected Minecraft world/map name.", lambda ctx, args: prefix_mc_field(ctx, args, "map", "map"), ("minecraftmap", "worldname", "mcworld", "servermap"), ()),
        ("mcgamemode", "Minecraft", "[ip[:port]]", "Show the detected Bedrock gamemode.", lambda ctx, args: prefix_mc_field(ctx, args, "gamemode", "gamemode"), ("minecraftgamemode", "mcgamemodeinfo", "servergamemode", "mcgameplay"), ()),
        ("mcicon", "Minecraft", "[ip[:port]]", "Show the Minecraft server icon.", prefix_mc_icon, ("minecrafticon", "mcservericon", "servericonmc", "mcimage"), ()),
        ("mcdebug", "Minecraft", "[ip[:port]]", "Show Minecraft status diagnostics and API debug flags.", prefix_mc_query, ("minecraftdebug", "mcdiagnostics", "mcdiagnose", "mcdebuginfo"), ()),
        ("mconline", "Minecraft", "[ip[:port]]", "Check whether a Minecraft server is online.", lambda ctx, args: prefix_mc_field(ctx, args, "online", "online"), ("minecraftonline", "mcuptime", "mcreachability", "mcisonline"), ()),
        ("mcoffline", "Minecraft", "[ip[:port]]", "Check whether a Minecraft server is offline or unreachable.", lambda ctx, args: prefix_mc_field(ctx, args, "online", "availability"), ("minecraftoffline", "mcdown", "mcunavailable", "mcofflinecheck"), ()),
        ("mcping", "Minecraft", "[ip[:port]]", "Ping a Minecraft server through the status service.", lambda ctx, args: prefix_mc_field(ctx, args, "online", "ping"), ("minecraftping", "mclatency", "mcliveping", "mcreach"), ()),
        ("mcmonitor", "Minecraft", "", "Show the saved live Minecraft monitor and its refresh state.", prefix_mc_monitor_info, ("minecraftmonitor", "mcwatch", "mclive", "mcautorefresh"), ()),
        ("mcmonitorinfo", "Minecraft", "", "Show Minecraft monitor channel, message, edition, and refresh details.", prefix_mc_monitor_info, ("minecraftmonitorinfo", "mcmonitorstatus", "mcwatchstatus", "mctracker"), ()),
        ("mcplayercount", "Minecraft", "[ip[:port]]", "Show only the live online/max Minecraft player count.", lambda ctx, args: prefix_mc_field(ctx, args, "players", "players"), ("minecraftplayercount", "mcplayercount", "mccount", "onlineplayers"), ()),
        ("mcplayernames", "Minecraft", "[ip[:port]]", "Show names returned by the Minecraft player sample.", lambda ctx, args: prefix_mc_field(ctx, args, "playernames", "player names"), ("minecraftplayernames", "mconlineplayers", "mcwho", "mcplayernames"), ()),
        ("mcfullstatus", "Minecraft", "[ip[:port]]", "Show the complete Minecraft status embed.", prefix_mc_fullstatus, ("minecraftfullstatus", "mcdetails", "mcserverdetails", "mcallstatus"), ()),
        ("mcjson", "Minecraft", "[ip[:port]]", "Show the raw Minecraft status response without the server icon payload.", prefix_mc_json, ("minecraftjson", "mcraw", "mcapi", "mcresponse"), ()),
    ]
    for name, category, usage, description, handler, aliases, permissions in specs:
        register_prefix(
            name,
            category,
            usage,
            description,
            handler,
            aliases=aliases,
            permissions=permissions,
        )


register_prefix_commands()


@bot.check
async def prefix_commands_require_guild(ctx: commands.Context[SentinelBot]) -> bool:
    if ctx.guild is None:
        raise commands.NoPrivateMessage("Prefix commands can only be used inside a server.")
    return True


moderation = app_commands.Group(name="mod", description="Moderation tools")
tickets = app_commands.Group(name="ticket", description="Private support tickets")
settings = app_commands.Group(name="settings", description="Server logs, welcome messages, and AutoMod")
utility = app_commands.Group(name="utility", description="Useful server utilities")


async def is_ticket_staff(interaction: discord.Interaction) -> bool:
    if isinstance(interaction.user, discord.Member) and interaction.user.guild_permissions.manage_channels:
        return True
    if interaction.guild_id is None or not isinstance(interaction.user, discord.Member):
        return False
    staff_id = await get_setting(interaction.guild_id, "ticket_staff_role")
    return bool(staff_id and any(role.id == int(staff_id) for role in interaction.user.roles))


@moderation.command(name="warn", description="Issue a stored warning to a member")
@app_commands.checks.has_permissions(moderate_members=True)
@app_commands.checks.bot_has_permissions(moderate_members=True)
@app_commands.guild_only()
async def mod_warn(
    interaction: discord.Interaction,
    member: discord.Member,
    reason: str = "No reason provided",
) -> None:
    blocked = moderation_block_reason(interaction, member)
    if blocked:
        await respond(interaction, blocked)
        return
    reason = clean_reason(reason)
    warning_id = await db_execute(
        """
        INSERT INTO warnings (guild_id, user_id, moderator_id, reason, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (interaction.guild_id, member.id, interaction.user.id, reason, utc_now().isoformat()),
    )
    try:
        await member.send(
            f"You received a warning in **{interaction.guild.name}**: {reason}",
            allowed_mentions=mention_suppressed(),
        )
    except (discord.Forbidden, discord.HTTPException):
        pass
    await respond(interaction, f"Warned {member.mention}. Warning ID: `{warning_id}`.")
    await log_action(
        interaction.guild,
        "Member warned",
        f"Member: {member.mention} (`{member.id}`)\nWarning ID: `{warning_id}`\nReason: {reason}",
        moderator=interaction.user,
        color=discord.Colour.yellow(),
    )


@moderation.command(name="warnings", description="Show a member's recent active warnings")
@app_commands.checks.has_permissions(moderate_members=True)
@app_commands.guild_only()
async def mod_warnings(interaction: discord.Interaction, member: discord.Member) -> None:
    rows = await db_fetchall(
        """
        SELECT id, moderator_id, reason, created_at FROM warnings
        WHERE guild_id = ? AND user_id = ? AND active = 1 ORDER BY id DESC LIMIT 10
        """,
        (interaction.guild_id, member.id),
    )
    if not rows:
        await respond(interaction, f"{member.mention} has no active warnings.")
        return
    embed = make_embed(f"Warnings for {member}", color=discord.Colour.yellow())
    for row in rows:
        embed.add_field(
            name=f"#{row['id']} • <t:{int(discord.utils.parse_time(row['created_at']).timestamp())}:R>",
            value=f"{str(row['reason'])[:500]}\nModerator ID: `{row['moderator_id']}`",
            inline=False,
        )
    await respond(interaction, embed=embed)


@moderation.command(name="unwarn", description="Remove one warning using its ID")
@app_commands.checks.has_permissions(moderate_members=True)
@app_commands.guild_only()
async def mod_unwarn(interaction: discord.Interaction, warning_id: int) -> None:
    if warning_id < 1:
        await respond(interaction, "Warning ID must be a positive number.")
        return
    row = await db_fetchone(
        "SELECT id FROM warnings WHERE id = ? AND guild_id = ? AND active = 1",
        (warning_id, interaction.guild_id),
    )
    if not row:
        await respond(interaction, "No active warning with that ID was found in this server.")
        return
    await db_execute("UPDATE warnings SET active = 0 WHERE id = ?", (warning_id,))
    await respond(interaction, f"Warning `{warning_id}` removed.")
    await log_action(
        interaction.guild,
        "Warning removed",
        f"Warning ID: `{warning_id}`",
        moderator=interaction.user,
        color=discord.Colour.green(),
    )


@moderation.command(name="clear-warnings", description="Clear a member's active warning history")
@app_commands.checks.has_permissions(moderate_members=True)
@app_commands.guild_only()
async def mod_clear_warnings(interaction: discord.Interaction, member: discord.Member) -> None:
    cursor = await db_execute(
        "UPDATE warnings SET active = 0 WHERE guild_id = ? AND user_id = ? AND active = 1",
        (interaction.guild_id, member.id),
    )
    await respond(interaction, f"Cleared {cursor} active warning(s) for {member.mention}.")
    await log_action(
        interaction.guild,
        "Warnings cleared",
        f"Member: {member.mention} (`{member.id}`)\nWarnings cleared: {cursor}",
        moderator=interaction.user,
        color=discord.Colour.green(),
    )


@moderation.command(name="kick", description="Kick a member from this server")
@app_commands.checks.has_permissions(kick_members=True)
@app_commands.checks.bot_has_permissions(kick_members=True)
@app_commands.guild_only()
async def mod_kick(
    interaction: discord.Interaction,
    member: discord.Member,
    reason: str = "No reason provided",
) -> None:
    blocked = moderation_block_reason(interaction, member)
    if blocked:
        await respond(interaction, blocked)
        return
    reason = clean_reason(reason)
    try:
        await member.send(f"You were kicked from **{interaction.guild.name}**. Reason: {reason}")
    except (discord.Forbidden, discord.HTTPException):
        pass
    await member.kick(reason=f"{interaction.user}: {reason}")
    await respond(interaction, f"Kicked {member}.")
    await log_action(
        interaction.guild,
        "Member kicked",
        f"Member: {member} (`{member.id}`)\nReason: {reason}",
        moderator=interaction.user,
    )


@moderation.command(name="ban", description="Ban a member from this server")
@app_commands.checks.has_permissions(ban_members=True)
@app_commands.checks.bot_has_permissions(ban_members=True)
@app_commands.guild_only()
async def mod_ban(
    interaction: discord.Interaction,
    member: discord.Member,
    delete_message_days: app_commands.Range[int, 0, 7] = 0,
    reason: str = "No reason provided",
) -> None:
    blocked = moderation_block_reason(interaction, member)
    if blocked:
        await respond(interaction, blocked)
        return
    reason = clean_reason(reason)
    try:
        await member.send(f"You were banned from **{interaction.guild.name}**. Reason: {reason}")
    except (discord.Forbidden, discord.HTTPException):
        pass
    await interaction.guild.ban(
        member,
        reason=f"{interaction.user}: {reason}",
        delete_message_seconds=delete_message_days * 86400,
    )
    await respond(interaction, f"Banned {member}.")
    await log_action(
        interaction.guild,
        "Member banned",
        f"Member: {member} (`{member.id}`)\nDelete message history: {delete_message_days} day(s)\nReason: {reason}",
        moderator=interaction.user,
        color=discord.Colour.red(),
    )


@moderation.command(name="unban", description="Unban a user by their Discord ID")
@app_commands.checks.has_permissions(ban_members=True)
@app_commands.checks.bot_has_permissions(ban_members=True)
@app_commands.guild_only()
async def mod_unban(
    interaction: discord.Interaction,
    user_id: str,
    reason: str = "No reason provided",
) -> None:
    try:
        parsed_id = int(user_id)
        user = await bot.fetch_user(parsed_id)
        await interaction.guild.unban(user, reason=f"{interaction.user}: {clean_reason(reason)}")
    except ValueError:
        await respond(interaction, "Enter a valid numeric Discord user ID.")
        return
    except discord.NotFound:
        await respond(interaction, "That user is not banned from this server.")
        return
    await respond(interaction, f"Unbanned {user}.")
    await log_action(
        interaction.guild,
        "User unbanned",
        f"User: {user} (`{user.id}`)\nReason: {clean_reason(reason)}",
        moderator=interaction.user,
        color=discord.Colour.green(),
    )


@moderation.command(name="softban", description="Ban and immediately unban to clear recent messages")
@app_commands.checks.has_permissions(ban_members=True)
@app_commands.checks.bot_has_permissions(ban_members=True)
@app_commands.guild_only()
async def mod_softban(
    interaction: discord.Interaction,
    member: discord.Member,
    reason: str = "No reason provided",
) -> None:
    blocked = moderation_block_reason(interaction, member)
    if blocked:
        await respond(interaction, blocked)
        return
    reason = clean_reason(reason)
    await interaction.guild.ban(
        member,
        reason=f"Softban by {interaction.user}: {reason}",
        delete_message_seconds=86400,
    )
    await interaction.guild.unban(member, reason=f"Softban complete by {interaction.user}")
    await respond(interaction, f"Softbanned {member}; their recent messages were deleted.")
    await log_action(
        interaction.guild,
        "Member softbanned",
        f"Member: {member} (`{member.id}`)\nReason: {reason}",
        moderator=interaction.user,
        color=discord.Colour.red(),
    )


@moderation.command(name="timeout", description="Timeout a member for up to 28 days")
@app_commands.checks.has_permissions(moderate_members=True)
@app_commands.checks.bot_has_permissions(moderate_members=True)
@app_commands.guild_only()
async def mod_timeout(
    interaction: discord.Interaction,
    member: discord.Member,
    minutes: app_commands.Range[int, 1, MAX_TIMEOUT_MINUTES],
    reason: str = "No reason provided",
) -> None:
    blocked = moderation_block_reason(interaction, member)
    if blocked:
        await respond(interaction, blocked)
        return
    reason = clean_reason(reason)
    try:
        await member.send(f"You were timed out in **{interaction.guild.name}** for {minutes} minute(s). Reason: {reason}")
    except (discord.Forbidden, discord.HTTPException):
        pass
    await member.timeout(timedelta(minutes=minutes), reason=f"{interaction.user}: {reason}")
    await respond(interaction, f"Timed out {member.mention} for {minutes} minute(s).")
    await log_action(
        interaction.guild,
        "Member timed out",
        f"Member: {member.mention} (`{member.id}`)\nDuration: {minutes} minute(s)\nReason: {reason}",
        moderator=interaction.user,
        color=discord.Colour.red(),
    )


@moderation.command(name="untimeout", description="Remove a member's timeout")
@app_commands.checks.has_permissions(moderate_members=True)
@app_commands.checks.bot_has_permissions(moderate_members=True)
@app_commands.guild_only()
async def mod_untimeout(
    interaction: discord.Interaction,
    member: discord.Member,
    reason: str = "No reason provided",
) -> None:
    blocked = moderation_block_reason(interaction, member)
    if blocked:
        await respond(interaction, blocked)
        return
    await member.timeout(None, reason=f"{interaction.user}: {clean_reason(reason)}")
    await respond(interaction, f"Removed {member.mention}'s timeout.")
    await log_action(
        interaction.guild,
        "Timeout removed",
        f"Member: {member.mention} (`{member.id}`)\nReason: {clean_reason(reason)}",
        moderator=interaction.user,
        color=discord.Colour.green(),
    )


@moderation.command(name="purge", description="Delete up to 100 recent messages in this channel")
@app_commands.checks.has_permissions(manage_messages=True)
@app_commands.checks.bot_has_permissions(manage_messages=True, read_message_history=True)
@app_commands.guild_only()
async def mod_purge(
    interaction: discord.Interaction,
    amount: app_commands.Range[int, 1, 100],
    member: discord.Member | None = None,
) -> None:
    if not isinstance(interaction.channel, discord.TextChannel):
        await respond(interaction, "Use this command in a text channel.")
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    predicate = (lambda message: message.author.id == member.id) if member else None
    deleted = await interaction.channel.purge(limit=amount, check=predicate)
    await interaction.followup.send(
        f"Deleted {len(deleted)} message(s)" + (f" from {member.mention}." if member else "."),
        ephemeral=True,
        allowed_mentions=mention_suppressed(),
    )
    await log_action(
        interaction.guild,
        "Messages purged",
        f"Channel: {interaction.channel.mention}\nDeleted: {len(deleted)}"
        + (f"\nAuthor filter: {member.mention}" if member else ""),
        moderator=interaction.user,
    )


@moderation.command(name="purge-user", description="Delete recent messages by one member")
@app_commands.checks.has_permissions(manage_messages=True)
@app_commands.checks.bot_has_permissions(manage_messages=True, read_message_history=True)
@app_commands.guild_only()
async def mod_purge_user(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: app_commands.Range[int, 1, 100] = 50,
) -> None:
    if not isinstance(interaction.channel, discord.TextChannel):
        await respond(interaction, "Use this command in a text channel.")
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    deleted = await interaction.channel.purge(
        limit=amount,
        check=lambda message: message.author.id == member.id,
    )
    await interaction.followup.send(
        f"Deleted {len(deleted)} message(s) from {member.mention}.",
        ephemeral=True,
        allowed_mentions=mention_suppressed(),
    )
    await log_action(
        interaction.guild,
        "Member messages purged",
        f"Channel: {interaction.channel.mention}\nMember: {member.mention}\nDeleted: {len(deleted)}",
        moderator=interaction.user,
    )


@moderation.command(name="clear-bots", description="Delete recent bot messages in this channel")
@app_commands.checks.has_permissions(manage_messages=True)
@app_commands.checks.bot_has_permissions(manage_messages=True, read_message_history=True)
@app_commands.guild_only()
async def mod_clear_bots(
    interaction: discord.Interaction,
    amount: app_commands.Range[int, 1, 100] = 50,
) -> None:
    if not isinstance(interaction.channel, discord.TextChannel):
        await respond(interaction, "Use this command in a text channel.")
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    deleted = await interaction.channel.purge(
        limit=amount,
        check=lambda message: message.author.bot,
    )
    await interaction.followup.send(f"Deleted {len(deleted)} bot message(s).", ephemeral=True)
    await log_action(
        interaction.guild,
        "Bot messages cleared",
        f"Channel: {interaction.channel.mention}\nDeleted: {len(deleted)}",
        moderator=interaction.user,
    )


async def set_channel_lock(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    locked: bool,
    reason: str,
) -> None:
    guild = interaction.guild
    if guild is None:
        await respond(interaction, "This command can only be used in a server.")
        return
    overwrite = channel.overwrites_for(guild.default_role)
    overwrite.send_messages = False if locked else None
    try:
        await channel.set_permissions(guild.default_role, overwrite=overwrite, reason=reason)
    except discord.Forbidden:
        await respond(interaction, "I need Manage Roles permission to change channel permissions.")
        return
    await respond(interaction, f"{'Locked' if locked else 'Unlocked'} {channel.mention}.")
    await log_action(
        guild,
        f"Channel {'locked' if locked else 'unlocked'}",
        f"Channel: {channel.mention}",
        moderator=interaction.user,
    )


@moderation.command(name="lock", description="Prevent @everyone from sending messages in a channel")
@app_commands.checks.has_permissions(manage_channels=True)
@app_commands.checks.bot_has_permissions(manage_roles=True)
@app_commands.guild_only()
async def mod_lock(
    interaction: discord.Interaction,
    channel: discord.TextChannel | None = None,
) -> None:
    selected = channel or interaction.channel
    if not isinstance(selected, discord.TextChannel):
        await respond(interaction, "Choose a text channel.")
        return
    await set_channel_lock(interaction, selected, True, f"Locked by {interaction.user}")


@moderation.command(name="unlock", description="Allow @everyone to send messages in a channel")
@app_commands.checks.has_permissions(manage_channels=True)
@app_commands.checks.bot_has_permissions(manage_roles=True)
@app_commands.guild_only()
async def mod_unlock(
    interaction: discord.Interaction,
    channel: discord.TextChannel | None = None,
) -> None:
    selected = channel or interaction.channel
    if not isinstance(selected, discord.TextChannel):
        await respond(interaction, "Choose a text channel.")
        return
    await set_channel_lock(interaction, selected, False, f"Unlocked by {interaction.user}")


@moderation.command(name="slowmode", description="Set a channel slowmode from 0 to 6 hours")
@app_commands.checks.has_permissions(manage_channels=True)
@app_commands.checks.bot_has_permissions(manage_channels=True)
@app_commands.guild_only()
async def mod_slowmode(
    interaction: discord.Interaction,
    seconds: app_commands.Range[int, 0, 21600],
    channel: discord.TextChannel | None = None,
) -> None:
    selected = channel or interaction.channel
    if not isinstance(selected, discord.TextChannel):
        await respond(interaction, "Choose a text channel.")
        return
    await selected.edit(slowmode_delay=seconds, reason=f"Slowmode changed by {interaction.user}")
    await respond(interaction, f"Set {selected.mention} slowmode to {seconds} second(s).")
    await log_action(
        interaction.guild,
        "Slowmode changed",
        f"Channel: {selected.mention}\nDelay: {seconds}s",
        moderator=interaction.user,
    )


@moderation.command(name="nickname", description="Change a member's server nickname")
@app_commands.checks.has_permissions(manage_nicknames=True)
@app_commands.checks.bot_has_permissions(manage_nicknames=True)
@app_commands.guild_only()
async def mod_nickname(
    interaction: discord.Interaction,
    member: discord.Member,
    nickname: str,
) -> None:
    if not nickname or len(nickname) > 32:
        await respond(interaction, "A nickname must contain 1 to 32 characters.")
        return
    blocked = moderation_block_reason(interaction, member)
    if blocked:
        await respond(interaction, blocked)
        return
    previous = member.display_name
    await member.edit(nick=nickname, reason=f"Nickname changed by {interaction.user}")
    await respond(interaction, f"Changed {member.mention}'s nickname to **{nickname}**.")
    await log_action(
        interaction.guild,
        "Nickname changed",
        f"Member: {member.mention}\nBefore: {previous}\nAfter: {nickname}",
        moderator=interaction.user,
    )


@moderation.command(name="clear-nickname", description="Remove a member's server nickname")
@app_commands.checks.has_permissions(manage_nicknames=True)
@app_commands.checks.bot_has_permissions(manage_nicknames=True)
@app_commands.guild_only()
async def mod_clear_nickname(interaction: discord.Interaction, member: discord.Member) -> None:
    blocked = moderation_block_reason(interaction, member)
    if blocked:
        await respond(interaction, blocked)
        return
    await member.edit(nick=None, reason=f"Nickname cleared by {interaction.user}")
    await respond(interaction, f"Cleared {member.mention}'s server nickname.")
    await log_action(
        interaction.guild,
        "Nickname cleared",
        f"Member: {member.mention} (`{member.id}`)",
        moderator=interaction.user,
    )


@moderation.command(name="role-add", description="Give a member a role")
@app_commands.checks.has_permissions(manage_roles=True)
@app_commands.checks.bot_has_permissions(manage_roles=True)
@app_commands.guild_only()
async def mod_role_add(
    interaction: discord.Interaction,
    member: discord.Member,
    role: discord.Role,
) -> None:
    if role.is_default() or role.managed or role >= interaction.guild.me.top_role:
        await respond(interaction, "I cannot assign that role. Move my highest role above it first.")
        return
    if isinstance(interaction.user, discord.Member) and interaction.user.id != interaction.guild.owner_id:
        if role >= interaction.user.top_role:
            await respond(interaction, "You can only assign roles below your highest role.")
            return
    await member.add_roles(role, reason=f"Role assigned by {interaction.user}")
    await respond(interaction, f"Added {role.mention} to {member.mention}.")
    await log_action(
        interaction.guild,
        "Role added",
        f"Member: {member.mention}\nRole: {role.mention}",
        moderator=interaction.user,
    )


@moderation.command(name="role-remove", description="Remove a role from a member")
@app_commands.checks.has_permissions(manage_roles=True)
@app_commands.checks.bot_has_permissions(manage_roles=True)
@app_commands.guild_only()
async def mod_role_remove(
    interaction: discord.Interaction,
    member: discord.Member,
    role: discord.Role,
) -> None:
    if role.is_default() or role.managed or role >= interaction.guild.me.top_role:
        await respond(interaction, "I cannot remove that role. Move my highest role above it first.")
        return
    if isinstance(interaction.user, discord.Member) and interaction.user.id != interaction.guild.owner_id:
        if role >= interaction.user.top_role:
            await respond(interaction, "You can only remove roles below your highest role.")
            return
    await member.remove_roles(role, reason=f"Role removed by {interaction.user}")
    await respond(interaction, f"Removed {role.mention} from {member.mention}.")
    await log_action(
        interaction.guild,
        "Role removed",
        f"Member: {member.mention}\nRole: {role.mention}",
        moderator=interaction.user,
    )


@settings.command(name="log-channel", description="Choose or clear the moderation log channel")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def settings_log_channel(
    interaction: discord.Interaction,
    channel: discord.TextChannel | None = None,
) -> None:
    await set_setting(interaction.guild_id, "log_channel", str(channel.id) if channel else None)
    await respond(
        interaction,
        f"Moderation logs will go to {channel.mention}." if channel else "Moderation log channel cleared.",
    )


@settings.command(name="welcome-channel", description="Choose or clear the new-member welcome channel")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def settings_welcome_channel(
    interaction: discord.Interaction,
    channel: discord.TextChannel | None = None,
) -> None:
    await set_setting(interaction.guild_id, "welcome_channel", str(channel.id) if channel else None)
    await respond(
        interaction,
        f"Welcome messages will go to {channel.mention}." if channel else "Welcome channel cleared.",
    )


@settings.command(name="welcome-message", description="Set welcome copy; use {user} and {server}")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def settings_welcome_message(
    interaction: discord.Interaction,
    message: str,
) -> None:
    if not message or len(message) > 1800:
        await respond(interaction, "The welcome message must contain 1 to 1,800 characters.")
        return
    await set_setting(interaction.guild_id, "welcome_message", message)
    await respond(interaction, "Welcome message saved. Available placeholders: `{user}` and `{server}`.")


@settings.command(name="automod", description="Turn AutoMod protection on or off")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def settings_automod(interaction: discord.Interaction, enabled: bool) -> None:
    await set_setting(interaction.guild_id, "automod_enabled", "true" if enabled else "false")
    await respond(
        interaction,
        "AutoMod enabled." if enabled else "AutoMod disabled. Existing rules are saved.",
    )


@settings.command(name="filter-invites", description="Turn Discord invite-link filtering on or off")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def settings_filter_invites(interaction: discord.Interaction, enabled: bool) -> None:
    await set_setting(interaction.guild_id, "filter_invites", "true" if enabled else "false")
    await respond(interaction, f"Discord invite filtering {'enabled' if enabled else 'disabled'}.")


@settings.command(name="filter-links", description="Turn URL filtering on or off")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def settings_filter_links(interaction: discord.Interaction, enabled: bool) -> None:
    await set_setting(interaction.guild_id, "filter_links", "true" if enabled else "false")
    await respond(interaction, f"Link filtering {'enabled' if enabled else 'disabled'}.")


@settings.command(name="word-add", description="Add a word or phrase to AutoMod's block list")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def settings_word_add(
    interaction: discord.Interaction,
    word: str,
) -> None:
    if not 2 <= len(word.strip()) <= 64:
        await respond(interaction, "A blocked word or phrase must contain 2 to 64 characters.")
        return
    words_json = await get_setting(interaction.guild_id, "automod_words", "[]")
    try:
        words = [str(item) for item in json.loads(words_json or "[]")]
    except (TypeError, json.JSONDecodeError):
        words = []
    normalized = word.strip().casefold()
    if not normalized:
        await respond(interaction, "Enter a non-empty word or phrase.")
        return
    if any(item.casefold() == normalized for item in words):
        await respond(interaction, "That word or phrase is already blocked.")
        return
    if len(words) >= 50:
        await respond(interaction, "The block list is full (50 entries).")
        return
    words.append(word.strip())
    await set_setting(interaction.guild_id, "automod_words", json.dumps(words, ensure_ascii=False))
    await respond(interaction, f"Added `{word[:64]}` to the AutoMod block list.")


@settings.command(name="word-remove", description="Remove a word or phrase from AutoMod's block list")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def settings_word_remove(
    interaction: discord.Interaction,
    word: str,
) -> None:
    if not 2 <= len(word.strip()) <= 64:
        await respond(interaction, "Enter a blocked word or phrase between 2 and 64 characters.")
        return
    words_json = await get_setting(interaction.guild_id, "automod_words", "[]")
    try:
        words = [str(item) for item in json.loads(words_json or "[]")]
    except (TypeError, json.JSONDecodeError):
        words = []
    updated = [item for item in words if item.casefold() != word.strip().casefold()]
    if len(updated) == len(words):
        await respond(interaction, "That word or phrase was not on the block list.")
        return
    await set_setting(interaction.guild_id, "automod_words", json.dumps(updated, ensure_ascii=False))
    await respond(interaction, f"Removed `{word[:64]}` from the AutoMod block list.")


@settings.command(name="antispam", description="Set the AutoMod message burst threshold")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def settings_antispam(
    interaction: discord.Interaction,
    message_limit: app_commands.Range[int, 3, 10],
    window_seconds: app_commands.Range[int, 3, 30],
) -> None:
    await set_setting(interaction.guild_id, "antispam_limit", str(message_limit))
    await set_setting(interaction.guild_id, "antispam_window", str(window_seconds))
    await respond(interaction, f"AutoMod will flag {message_limit} messages within {window_seconds} seconds.")


@tickets.command(name="setup", description="Configure the ticket category, staff role, and transcript log channel")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def ticket_setup(
    interaction: discord.Interaction,
    category: discord.CategoryChannel,
    staff_role: discord.Role,
    log_channel: discord.TextChannel,
) -> None:
    await set_setting(interaction.guild_id, "ticket_category", str(category.id))
    await set_setting(interaction.guild_id, "ticket_staff_role", str(staff_role.id))
    await set_setting(interaction.guild_id, "ticket_log_channel", str(log_channel.id))
    await respond(
        interaction,
        f"Tickets configured: category {category.mention}, staff {staff_role.mention}, logs {log_channel.mention}.",
    )


@tickets.command(name="panel", description="Post the persistent Open a ticket button")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def ticket_panel(
    interaction: discord.Interaction,
    channel: discord.TextChannel | None = None,
) -> None:
    destination = channel or interaction.channel
    if not isinstance(destination, discord.TextChannel):
        await respond(interaction, "Choose a text channel for the ticket panel.")
        return
    embed = make_embed(
        "Need help?",
        "Press the button below to create a private support ticket. Only you and the support team can view it.",
        color=discord.Colour.blurple(),
    )
    try:
        await destination.send(embed=embed, view=TicketPanelView(bot))
    except discord.Forbidden:
        await respond(interaction, "I need permission to send messages and use buttons in that channel.")
        return
    await respond(interaction, f"Ticket panel posted in {destination.mention}.")


@tickets.command(name="close", description="Close this ticket and export a transcript")
@app_commands.guild_only()
async def ticket_close(
    interaction: discord.Interaction,
    reason: str = "Closed by command",
) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)
    result = await bot.close_ticket(interaction, reason)
    await interaction.followup.send(result, ephemeral=True)


@tickets.command(name="claim", description="Assign this ticket to yourself")
@app_commands.check(is_ticket_staff)
@app_commands.guild_only()
async def ticket_claim(interaction: discord.Interaction) -> None:
    if interaction.guild_id is None or interaction.channel_id is None:
        await respond(interaction, "Use this inside a ticket channel.")
        return
    cursor = await db_execute(
        """
        UPDATE tickets SET claimed_by = ?
        WHERE guild_id = ? AND channel_id = ? AND status = 'open'
        """,
        (interaction.user.id, interaction.guild_id, interaction.channel_id),
    )
    if cursor == 0:
        await respond(interaction, "This is not an open ticket.")
        return
    await respond(interaction, f"This ticket is now assigned to {interaction.user.mention}.")
    await interaction.channel.send(
        f"{interaction.user.mention} is handling this ticket.",
        allowed_mentions=mention_suppressed(),
    )


@tickets.command(name="add", description="Give a member access to this ticket")
@app_commands.check(is_ticket_staff)
@app_commands.checks.bot_has_permissions(manage_roles=True)
@app_commands.guild_only()
async def ticket_add(interaction: discord.Interaction, member: discord.Member) -> None:
    row = await db_fetchone(
        "SELECT channel_id FROM tickets WHERE guild_id = ? AND channel_id = ? AND status = 'open'",
        (interaction.guild_id, interaction.channel_id),
    )
    if not row or not isinstance(interaction.channel, discord.TextChannel):
        await respond(interaction, "Use this inside an open ticket.")
        return
    await interaction.channel.set_permissions(
        member,
        view_channel=True,
        send_messages=True,
        read_message_history=True,
        attach_files=True,
        reason=f"Added to ticket by {interaction.user}",
    )
    await respond(interaction, f"Added {member.mention} to this ticket.")


@tickets.command(name="remove", description="Remove a member's access to this ticket")
@app_commands.check(is_ticket_staff)
@app_commands.checks.bot_has_permissions(manage_roles=True)
@app_commands.guild_only()
async def ticket_remove(interaction: discord.Interaction, member: discord.Member) -> None:
    row = await db_fetchone(
        "SELECT channel_id, user_id FROM tickets WHERE guild_id = ? AND channel_id = ? AND status = 'open'",
        (interaction.guild_id, interaction.channel_id),
    )
    if not row or not isinstance(interaction.channel, discord.TextChannel):
        await respond(interaction, "Use this inside an open ticket.")
        return
    if member.id == int(row["user_id"]):
        await respond(interaction, "The ticket owner cannot be removed from their own ticket.")
        return
    await interaction.channel.set_permissions(member, overwrite=None, reason=f"Removed from ticket by {interaction.user}")
    await respond(interaction, f"Removed {member.mention} from this ticket.")


@bot.tree.command(name="help", description="Show Sentinel's commands and setup guide")
async def help_command(interaction: discord.Interaction) -> None:
    embed = make_embed(
        "Sentinel command guide",
        "Moderation: `/mod warn`, `/mod kick`, `/mod ban`, `/mod timeout`, `/mod purge`, `/mod lock`.\n"
        "Tickets: an admin runs `/ticket setup`, then `/ticket panel`.\n"
        "Server settings: `/settings log-channel`, `/settings welcome-channel`, `/settings automod`.\n"
        "Utilities: `/utility poll`, `/utility calculate`, `/utility roll`.\n\n"
        "Commands appear in Discord's `/` menu. Moderation and settings commands require the matching server permission.",
    )
    embed.set_footer(text="Tip: start with /ticket setup and /settings log-channel.")
    await respond(interaction, embed=embed)


@bot.tree.command(name="ping", description="Check bot latency and service status")
async def ping_command(interaction: discord.Interaction) -> None:
    milliseconds = round(bot.latency * 1000)
    await respond(
        interaction,
        embed=make_embed(
            "Bot status",
            f"Status: online\nGateway latency: `{milliseconds} ms`\nServers: `{len(bot.guilds)}`",
            color=discord.Colour.green(),
        ),
    )


@bot.tree.command(name="about", description="Show bot version and uptime information")
async def about_command(interaction: discord.Interaction) -> None:
    embed = make_embed(
        "Sentinel",
        "Discord server moderation, persistent warnings, configurable AutoMod, audit logs, welcome messages, and private support tickets.",
    )
    embed.add_field(name="Library", value=f"discord.py {discord.__version__}", inline=True)
    embed.add_field(name="Servers", value=str(len(bot.guilds)), inline=True)
    embed.add_field(name="Status", value="Online", inline=True)
    await respond(interaction, embed=embed)


@bot.tree.command(name="serverinfo", description="Show information about this server")
@app_commands.guild_only()
async def serverinfo_command(interaction: discord.Interaction) -> None:
    guild = interaction.guild
    if guild is None:
        await respond(interaction, "This command can only be used in a server.")
        return
    embed = make_embed(guild.name, color=guild.owner.top_role.color if guild.owner else discord.Colour.blurple())
    embed.add_field(name="Owner", value=str(guild.owner or "Unknown"), inline=True)
    embed.add_field(name="Members", value=str(guild.member_count or 0), inline=True)
    embed.add_field(name="Channels", value=str(len(guild.channels)), inline=True)
    embed.add_field(name="Roles", value=str(len(guild.roles)), inline=True)
    embed.add_field(name="Created", value=f"<t:{int(guild.created_at.timestamp())}:D>", inline=True)
    embed.add_field(name="Server ID", value=f"`{guild.id}`", inline=True)
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    await respond(interaction, embed=embed)


@bot.tree.command(name="userinfo", description="Show information about a server member")
@app_commands.guild_only()
async def userinfo_command(
    interaction: discord.Interaction,
    member: discord.Member | None = None,
) -> None:
    target = member or interaction.user
    embed = make_embed(str(target), color=target.top_role.color)
    embed.add_field(name="Account created", value=f"<t:{int(target.created_at.timestamp())}:R>", inline=True)
    embed.add_field(name="Joined server", value=f"<t:{int(target.joined_at.timestamp())}:R>" if target.joined_at else "Unknown", inline=True)
    embed.add_field(name="User ID", value=f"`{target.id}`", inline=True)
    roles = [role.mention for role in reversed(target.roles) if not role.is_default()]
    embed.add_field(name=f"Roles ({len(roles)})", value=", ".join(roles[:15]) or "None", inline=False)
    embed.set_thumbnail(url=target.display_avatar.url)
    await respond(interaction, embed=embed)


@bot.tree.command(name="avatar", description="Show a user's profile picture")
async def avatar_command(
    interaction: discord.Interaction,
    user: discord.User | None = None,
) -> None:
    target = user or interaction.user
    embed = make_embed(f"{target}'s avatar")
    embed.set_image(url=target.display_avatar.with_size(1024).url)
    await respond(interaction, embed=embed)


@utility.command(name="poll", description="Create a two-option poll")
async def utility_poll(
    interaction: discord.Interaction,
    question: str,
    option_one: str,
    option_two: str,
) -> None:
    if not question.strip() or len(question) > 180 or not option_one.strip() or not option_two.strip():
        await respond(interaction, "Enter a question (up to 180 characters) and two non-empty options.")
        return
    if len(option_one) > 60 or len(option_two) > 60:
        await respond(interaction, "Poll options must be 60 characters or fewer.")
        return
    embed = make_embed(f"Poll: {question}", f"React to vote:\n\n🇦 {option_one}\n🇧 {option_two}")
    await interaction.response.send_message(embed=embed, allowed_mentions=mention_suppressed())
    message = await interaction.original_response()
    try:
        await message.add_reaction("🇦")
        await message.add_reaction("🇧")
    except discord.HTTPException:
        pass


@utility.command(name="coinflip", description="Flip a coin")
async def utility_coinflip(interaction: discord.Interaction) -> None:
    await respond(interaction, f"Coin flip: **{random.choice(['Heads', 'Tails'])}**", ephemeral=False)


@utility.command(name="roll", description="Roll an n-sided die")
async def utility_roll(
    interaction: discord.Interaction,
    sides: app_commands.Range[int, 2, 100000] = 6,
) -> None:
    await respond(interaction, f"🎲 {interaction.user.mention} rolled **{random.randint(1, sides)}** (d{sides}).", ephemeral=False)


@utility.command(name="choose", description="Choose one option at random")
async def utility_choose(
    interaction: discord.Interaction,
    options: str,
) -> None:
    if len(options) > 500:
        await respond(interaction, "Keep the full list of options under 500 characters.")
        return
    choices = [item.strip() for item in re.split(r"[,\n]", options) if item.strip()]
    if len(choices) < 2:
        await respond(interaction, "Give at least two options separated by commas.")
        return
    await respond(interaction, f"I choose: **{random.choice(choices)[:200]}**", ephemeral=False)


@utility.command(name="calculate", description="Safely calculate basic arithmetic")
async def utility_calculate(
    interaction: discord.Interaction,
    expression: str,
) -> None:
    if not expression or len(expression) > 100:
        await respond(interaction, "Enter an arithmetic expression up to 100 characters.")
        return
    try:
        result = safe_calc(expression)
    except (ValueError, SyntaxError, ZeroDivisionError, OverflowError):
        await respond(interaction, "That expression is invalid. I only support basic arithmetic and powers up to 100.")
        return
    await respond(interaction, f"`{expression}` = **{result}**")


@utility.command(name="embed", description="Post a formatted announcement embed")
@app_commands.checks.has_permissions(manage_messages=True)
@app_commands.guild_only()
async def utility_embed(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    title: str,
    message: str,
) -> None:
    if not title.strip() or len(title) > 200 or not message.strip() or len(message) > 3500:
        await respond(interaction, "Enter a title up to 200 characters and message up to 3,500 characters.")
        return
    embed = make_embed(title, message)
    embed.set_author(name=str(interaction.user), icon_url=interaction.user.display_avatar.url)
    try:
        await channel.send(embed=embed, allowed_mentions=mention_suppressed())
    except discord.Forbidden:
        await respond(interaction, "I cannot send messages in that channel.")
        return
    await respond(interaction, f"Embed posted in {channel.mention}.")


@utility.command(name="announce", description="Post an announcement in a server channel")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.guild_only()
async def utility_announce(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    message: str,
    title: str = "Announcement",
) -> None:
    if not message.strip() or len(message) > 3500 or not title.strip() or len(title) > 200:
        await respond(interaction, "Enter a title up to 200 characters and message up to 3,500 characters.")
        return
    embed = make_embed(title, message, color=discord.Colour.gold())
    embed.set_footer(text=f"Posted by {interaction.user}")
    try:
        await channel.send(embed=embed, allowed_mentions=mention_suppressed())
    except discord.Forbidden:
        await respond(interaction, "I cannot send messages in that channel.")
        return
    await respond(interaction, f"Announcement posted in {channel.mention}.")


bot.tree.add_command(moderation)
bot.tree.add_command(tickets)
bot.tree.add_command(settings)
bot.tree.add_command(utility)


@bot.event
async def on_command_error(
    ctx: commands.Context[SentinelBot],
    error: commands.CommandError,
) -> None:
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.CommandOnCooldown):
        message = f"Try again in {error.retry_after:.1f} seconds."
    elif isinstance(error, commands.MissingPermissions):
        message = "You do not have the server permission required for that command."
    elif isinstance(error, commands.BotMissingPermissions):
        message = "I am missing a Discord permission needed for that action. Check my role and channel permissions."
    elif isinstance(error, commands.NoPrivateMessage):
        message = "This command can only be used in a server."
    elif isinstance(error, commands.CheckFailure):
        message = "You cannot use that command here or with your current permissions."
    elif isinstance(error, (commands.BadArgument, commands.MissingRequiredArgument)):
        message = str(error)
        if ctx.command:
            message += f"\nUsage: `${ctx.command.usage or ctx.command.name}`"
    elif isinstance(error, commands.CommandInvokeError):
        original = error.original
        if isinstance(original, discord.Forbidden):
            message = "Discord denied that action. Check my permissions and role position."
        elif isinstance(original, discord.NotFound):
            message = "I could not find that Discord item. Check the ID or mention and try again."
        else:
            log.error(
                "Prefix command %s failed",
                ctx.command.qualified_name if ctx.command else "unknown",
                exc_info=(type(original), original, original.__traceback__),
            )
            message = "That command failed. Check the bot's Discord permissions and try again."
    else:
        log.error(
            "Prefix command error in %s",
            ctx.command.qualified_name if ctx.command else "unknown",
            exc_info=(type(error), error, error.__traceback__),
        )
        message = "That command failed. Use `$help` to check its usage."
    try:
        await ctx.send(message)
    except (discord.Forbidden, discord.HTTPException):
        log.warning("Could not send a prefix-command error response")


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: app_commands.AppCommandError,
) -> None:
    original = getattr(error, "original", error)
    if isinstance(original, app_commands.MissingPermissions):
        message = "You do not have the server permission required for this command."
    elif isinstance(original, app_commands.BotMissingPermissions):
        message = "I am missing a Discord permission needed for that action. Check my role and channel permissions."
    elif isinstance(original, app_commands.CommandOnCooldown):
        message = f"Try again in {original.retry_after:.1f} seconds."
    elif isinstance(original, app_commands.CheckFailure):
        message = "This command is not available in this channel or for your current permissions."
    else:
        log.exception("Unhandled slash command error", exc_info=original)
        message = "The command failed. Check the bot logs and Discord permissions."
    await respond(interaction, message)


async def run() -> None:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise SystemExit(
            "DISCORD_TOKEN is missing. Set it in your hosting provider's environment variables; never put it in main.py."
        )
    async with bot:
        await bot.start(token)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Bot stopped.")
