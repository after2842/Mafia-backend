import asyncio
import json
import random
import string
import time
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

app = FastAPI()

# ── Data stores ──────────────────────────────────────────────────────────────

rooms: dict[str, "GameRoom"] = {}

# Load quiz bank
QUIZ_BANK = []
quiz_path = Path(__file__).parent / "quiz_bank.json"
if quiz_path.exists():
    with open(quiz_path) as f:
        QUIZ_BANK = json.load(f)


# ── Models ───────────────────────────────────────────────────────────────────

class Player:
    def __init__(self, ws: WebSocket, nickname: str, player_id: str):
        self.ws = ws
        self.nickname = nickname
        self.id = player_id
        self.role: Optional[str] = None
        self.alive = True
        self.is_host = False
        self.has_lifeguard = False
        self.quiz_score = 0

    def to_dict(self, reveal_role=False):
        d = {"id": self.id, "nickname": self.nickname, "alive": self.alive,
             "is_host": self.is_host, "has_lifeguard": self.has_lifeguard}
        if reveal_role:
            d["role"] = self.role
        return d

    async def send(self, data: dict):
        try:
            await self.ws.send_json(data)
        except Exception:
            pass


class GameRoom:
    def __init__(self, room_id: str):
        self.room_id = room_id
        self.players: dict[str, Player] = {}
        self.phase = "lobby"  # lobby | night | day | vote | last_defense | final_vote
        self.round = 0
        self.role_config = {"mafia": 1, "journalist": 1, "citizen": 2}
        self.host_id: Optional[str] = None

        # Night state
        self.mafia_target: Optional[str] = None
        self.journalist_target: Optional[str] = None
        self.quiz_index = 0
        self.quiz_answered = False
        self.quiz_scores: dict[str, int] = {}

        # Day/vote state
        self.votes: dict[str, str] = {}
        self.vote_target_id: Optional[str] = None
        self.final_votes: dict[str, str] = {}
        self.morning_report: list[str] = []
        self.journalist_result: dict[str, str] = {}  # journalist_id -> result string
        self.can_extend = False

        # Timer
        self._timer_task: Optional[asyncio.Task] = None
        self.time_left = 0
        self.initial_player_count = 0

    # ── Player management ──

    def add_player(self, player: Player):
        self.players[player.id] = player
        if not self.host_id:
            self.host_id = player.id
            player.is_host = True

    def remove_player(self, player_id: str):
        if player_id in self.players:
            del self.players[player_id]
        if self.host_id == player_id and self.players:
            new_host_id = next(iter(self.players))
            self.host_id = new_host_id
            self.players[new_host_id].is_host = True

    def alive_players(self) -> list[Player]:
        return [p for p in self.players.values() if p.alive]

    def alive_players_by_role(self, role: str) -> list[Player]:
        return [p for p in self.players.values() if p.alive and p.role == role]

    # ── State serialization ──

    def game_state(self, for_player_id: Optional[str] = None) -> dict:
        player = self.players.get(for_player_id)
        reveal = self.phase == "lobby" or (player and not player.alive) or self.phase == "game_over"

        alive_list = []
        for p in self.alive_players():
            d = p.to_dict(reveal_role=reveal)
            if for_player_id and p.id == for_player_id:
                d["role"] = p.role  # Always show own role
            alive_list.append(d)

        state = {
            "room_id": self.room_id,
            "phase": self.phase,
            "round": self.round,
            "players": [p.to_dict(reveal_role=reveal) for p in self.players.values()],
            "alive_players": alive_list,
            "time_left": self.time_left,
            "role_config": self.role_config,
            "morning_report": self.morning_report,
            "can_extend": self.can_extend,
        }

        if player and player.role:
            state["my_role"] = player.role

        # Mafia sees current target
        if player and player.role == "mafia" and self.phase == "night":
            target = self.players.get(self.mafia_target)
            state["mafia_current_target"] = target.nickname if target else None

        # Quiz state
        if self.phase == "night":
            if self.quiz_index < len(QUIZ_BANK):
                state["current_quiz"] = {"question": QUIZ_BANK[self.quiz_index]["question"]}
            state["quiz_taken"] = self.quiz_answered
            state["my_quiz_score"] = self.quiz_scores.get(for_player_id, 0)

        # Journalist result (only for the journalist, during day)
        if player and player.role == "journalist" and for_player_id in self.journalist_result:
            state["journalist_result"] = self.journalist_result[for_player_id]

        # Vote target
        if self.vote_target_id:
            target = self.players.get(self.vote_target_id)
            if target:
                state["vote_target"] = {"id": target.id, "nickname": target.nickname}

        # Game over
        if self.phase == "game_over":
            state["winner"] = self._check_winner()
            state["all_players"] = [p.to_dict(reveal_role=True) for p in self.players.values()]

        return state

    # ── Broadcasting ──

    async def broadcast(self, data: dict, exclude: set = None):
        exclude = exclude or set()
        for pid, player in self.players.items():
            if pid not in exclude:
                await player.send(data)

    async def broadcast_state(self):
        for pid, player in self.players.items():
            await player.send({
                "type": "game_state_update",
                "game_state": self.game_state(for_player_id=pid),
            })

    async def broadcast_to_role(self, role: str, data: dict):
        for p in self.alive_players_by_role(role):
            await p.send(data)

    async def announce(self, text: str, exclude: set = None):
        await self.broadcast({
            "type": "announcement",
            "text": text,
        }, exclude=exclude)

    # ── Game flow ──

    async def start_game(self, role_config: dict):
        self.role_config = role_config
        self.initial_player_count = len(self.players)
        self.round = 1  # First day will be round 1

        # Assign roles
        roles = []
        for role, count in role_config.items():
            roles.extend([role] * count)
        random.shuffle(roles)

        for player, role in zip(self.players.values(), roles):
            player.role = role
            player.alive = True
            player.has_lifeguard = False
            player.quiz_score = 0

        self.quiz_scores = {pid: 0 for pid in self.players}

        # Private role reveal before the first day begins
        for pid, player in self.players.items():
            await player.send({
                "type": "role_assigned",
                "role": player.role,
                "player_id": player.id,
            })

        # Send game_started with personalized state
        for pid, player in self.players.items():
            await player.send({
                "type": "game_started",
                "game_state": self.game_state(for_player_id=pid),
            })

        await asyncio.sleep(1)
        await self._start_day(initial=True)

    async def _start_night(self):
        self.phase = "night"
        self.mafia_target = None
        self.journalist_target = None
        self.quiz_answered = False
        self.quiz_index = random.randint(0, max(0, len(QUIZ_BANK) - 1)) if QUIZ_BANK else 0
        self.morning_report = []
        self.journalist_result = {}
        self.can_extend = False

        await self._broadcast_phase_change()
        self._start_timer(35, self._end_night)

    async def _end_night(self):
        # Determine who dies
        killed_player = None
        saved_by_quiz = False

        if self.mafia_target:
            target = self.players.get(self.mafia_target)
            if target and target.alive:
                # Check lifeguard
                if target.has_lifeguard:
                    saved_by_quiz = True
                    target.has_lifeguard = False
                    self.morning_report.append(f"🛡️ {target.nickname} was attacked but survived with a lifeguard!")
                else:
                    killed_player = target

        if killed_player:
            killed_player.alive = False
            self.morning_report.append(f"💀 {killed_player.nickname} was killed during the night.")
            await self.broadcast({
                "type": "player_eliminated",
                "player_id": killed_player.id,
                "nickname": killed_player.nickname,
                "role": killed_player.role,
            })
        elif not self.mafia_target:
            self.morning_report.append("It was a peaceful night. Nobody was attacked.")
        elif saved_by_quiz:
            pass  # Already reported above

        # Journalist result
        ROLE_EN = {"mafia": "Mafia", "journalist": "Journalist", "citizen": "Citizen"}
        if self.journalist_target:
            target = self.players.get(self.journalist_target)
            for p in self.alive_players_by_role("journalist"):
                if target:
                    role_en = ROLE_EN.get(target.role, target.role)
                    self.journalist_result[p.id] = f"{target.nickname}'s role is {role_en}."

        # Quiz life guard: highest scorer who doesn't already have one
        if self.quiz_scores:
            best_score = max(self.quiz_scores.values())
            if best_score > 0:
                best_players = [pid for pid, s in self.quiz_scores.items()
                              if s == best_score and self.players.get(pid)
                              and self.players[pid].alive and not self.players[pid].has_lifeguard]
                if best_players:
                    winner_id = random.choice(best_players)
                    winner = self.players[winner_id]
                    winner.has_lifeguard = True
                    # Only the winner knows (sent privately via game state)

        # Check win condition
        winner = self._check_winner()
        if winner:
            self.phase = "game_over"
            await self.broadcast_state()
            await self.broadcast({"type": "game_over", "game_state": {
                "winner": winner,
                "all_players": [p.to_dict(reveal_role=True) for p in self.players.values()],
            }})
            return

        await self._start_day()

    async def _start_day(self, initial: bool = False):
        self.phase = "day"
        self.votes = {}
        self.vote_target_id = None
        self.final_votes = {}

        if not initial:
            self.round += 1

        duration = 240  # 4 minutes for all day phases

        await self._broadcast_phase_change()
        self.can_extend = False
        self._start_timer(duration, self._start_vote, on_alarm=self._day_alarm)

    async def _day_alarm(self):
        """Called when day timer runs out - show extend button briefly."""
        self.can_extend = True
        await self.broadcast_state()
        # Give 10 seconds to extend
        await asyncio.sleep(10)
        if self.can_extend:
            self.can_extend = False
            await self._start_vote()

    async def extend_day(self):
        if self.can_extend:
            self.can_extend = False
            self._start_timer(120, self._start_vote, on_alarm=self._day_alarm)
            await self.broadcast_state()

    async def skip_day(self):
        if self.phase == "day":
            self.can_extend = False
            if self._timer_task:
                self._timer_task.cancel()
            await self._start_vote()

    async def _start_vote(self):
        self.phase = "vote"
        self.votes = {}
        self.can_extend = False
        await self._broadcast_phase_change()
        self._start_timer(60, self._tally_votes)

    async def _tally_votes(self):
        if not self.votes:
            await self.announce("No votes were cast. Moving to night.")
            await self._start_night()
            return

        # Count votes
        vote_counts: dict[str, int] = {}
        for target_id in self.votes.values():
            vote_counts[target_id] = vote_counts.get(target_id, 0) + 1

        max_votes = max(vote_counts.values())
        top = [pid for pid, count in vote_counts.items() if count == max_votes]

        if len(top) > 1:
            # Tie - no elimination
            await self.announce("The vote is tied. Nobody is eliminated.")
            await asyncio.sleep(6)
            await self._start_night()
            return

        self.vote_target_id = top[0]
        target = self.players.get(self.vote_target_id)
        if target:
            await self.announce(f"{target.nickname} received the most votes.")

        # Last defense phase
        self.phase = "last_defense"
        await self._broadcast_phase_change()
        self._start_timer(30, self._start_final_vote)

    async def _start_final_vote(self):
        self.phase = "final_vote"
        self.final_votes = {}
        await self._broadcast_phase_change()
        self._start_timer(30, self._tally_final_vote)

    async def _tally_final_vote(self):
        kills = sum(1 for v in self.final_votes.values() if v == "kill")
        saves = sum(1 for v in self.final_votes.values() if v == "save")

        target = self.players.get(self.vote_target_id)
        if not target:
            await self._start_night()
            return

        if kills > saves:
            target.alive = False
            await self.announce(f"⚖️ {target.nickname} has been executed by vote.")
            await self.broadcast({
                "type": "player_eliminated",
                "player_id": target.id,
                "nickname": target.nickname,
                "role": target.role,
            })
        else:
            await self.announce(f"⚖️ {target.nickname} has been spared by vote.")

        # Check win
        winner = self._check_winner()
        if winner:
            self.phase = "game_over"
            await self.broadcast_state()
            await self.broadcast({"type": "game_over", "game_state": {
                "winner": winner,
                "all_players": [p.to_dict(reveal_role=True) for p in self.players.values()],
            }})
            return

        await asyncio.sleep(6)
        await self._start_night()

    # ── Win condition ──

    def _check_winner(self) -> Optional[str]:
        alive = self.alive_players()
        mafia_alive = sum(1 for p in alive if p.role == "mafia")
        non_mafia_alive = sum(1 for p in alive if p.role != "mafia")

        if mafia_alive == 0:
            return "citizens"
        if mafia_alive >= non_mafia_alive:
            return "mafia"
        return None

    # ── Timer ──

    def _start_timer(self, seconds: int, callback, on_alarm=None):
        if self._timer_task:
            self._timer_task.cancel()
        self.time_left = seconds

        async def run():
            try:
                while self.time_left > 0:
                    await asyncio.sleep(1)
                    self.time_left -= 1
                    # Broadcast time update every 5 seconds (or when low)
                    if self.time_left % 5 == 0 or self.time_left <= 10:
                        await self.broadcast_state()
                if on_alarm:
                    await on_alarm()
                else:
                    await callback()
            except asyncio.CancelledError:
                pass

        self._timer_task = asyncio.create_task(run())

    # ── Phase change broadcast ──

    async def _broadcast_phase_change(self):
        for pid, player in self.players.items():
            await player.send({
                "type": "phase_change",
                "game_state": self.game_state(for_player_id=pid),
            })

    # ── Handle quiz answer ──

    async def handle_quiz_answer(self, player_id: str, answer: str):
        if self.phase != "night" or self.quiz_index >= len(QUIZ_BANK):
            return

        # Broadcast the attempt anonymously to all alive players
        for p in self.alive_players():
            await p.send({"type": "night_answer", "text": answer.strip()})

        correct = QUIZ_BANK[self.quiz_index].get("answer", "").strip().lower()
        if answer.strip().lower() == correct and not self.quiz_answered:
            self.quiz_answered = True
            self.quiz_scores[player_id] = self.quiz_scores.get(player_id, 0) + 1

            # Move to next question
            await asyncio.sleep(0.5)
            self.quiz_index += 1
            self.quiz_answered = False
            if self.quiz_index < len(QUIZ_BANK):
                # Broadcast new quiz to all alive
                await self.broadcast_state()
            else:
                await self.announce("All quiz questions have been used up!")


# ── Room helpers ─────────────────────────────────────────────────────────────

def generate_room_id() -> str:
    while True:
        code = ''.join(random.choices(string.digits, k=4))
        if code not in rooms:
            return code


# ── WebSocket endpoint ───────────────────────────────────────────────────────

@app.websocket("/ws/{room_id}")
async def websocket_endpoint(ws: WebSocket, room_id: str, nickname: str = Query("")):
    await ws.accept()
    player_id = str(uuid.uuid4())[:8]
    player = Player(ws, nickname or f"Player-{player_id}", player_id)

    current_room: Optional[GameRoom] = None

    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            if msg_type == "create_room":
                rid = generate_room_id()
                room = GameRoom(rid)
                player.nickname = data.get("nickname", player.nickname)
                room.add_player(player)
                rooms[rid] = room
                current_room = room

                await player.send({
                    "type": "room_joined",
                    "room_id": rid,
                    "player_id": player.id,
                    "is_host": True,
                    "game_state": room.game_state(for_player_id=player.id),
                })

            elif msg_type == "join_room":
                rid = data.get("room_id", "")
                if rid not in rooms:
                    await player.send({"type": "error", "message": f"Room {rid} not found."})
                    continue

                room = rooms[rid]
                if room.phase != "lobby":
                    await player.send({"type": "error", "message": "A game is already in progress."})
                    continue

                player.nickname = data.get("nickname", player.nickname)
                room.add_player(player)
                current_room = room

                await player.send({
                    "type": "room_joined",
                    "room_id": rid,
                    "player_id": player.id,
                    "is_host": player.is_host,
                    "game_state": room.game_state(for_player_id=player.id),
                })

                # Notify others
                await room.broadcast({
                    "type": "room_state",
                    "game_state": room.game_state(),
                }, exclude={player.id})

            elif msg_type == "start_game":
                if not current_room or player.id != current_room.host_id:
                    continue
                config = data.get("role_config", current_room.role_config)
                await current_room.start_game(config)

            elif msg_type == "chat":
                if not current_room:
                    continue
                context = data.get("context", "day")
                msg = {
                    "type": "chat_message",
                    "message": {
                        "sender": player.nickname,
                        "sender_id": player.id,
                        "text": data.get("text", ""),
                        "timestamp": int(time.time() * 1000),
                        "context": context,
                    }
                }
                if context == "mafia":
                    # Only send to mafia players
                    await current_room.broadcast_to_role("mafia", msg)
                else:
                    await current_room.broadcast(msg)

            elif msg_type == "mafia_kill":
                if not current_room or player.role != "mafia" or current_room.phase != "night":
                    continue
                current_room.mafia_target = data.get("target_id")
                # Broadcast to all mafias so they see updated target
                await current_room.broadcast_to_role("mafia", {
                    "type": "game_state_update",
                    "game_state": current_room.game_state(for_player_id=player.id),
                })

            elif msg_type == "journalist_investigate":
                if not current_room or player.role != "journalist" or current_room.phase != "night":
                    continue
                current_room.journalist_target = data.get("target_id")

            elif msg_type == "quiz_answer":
                if not current_room or current_room.phase != "night":
                    continue
                await current_room.handle_quiz_answer(player.id, data.get("answer", ""))

            elif msg_type == "vote":
                if not current_room or current_room.phase != "vote":
                    continue
                if not player.alive:
                    continue
                current_room.votes[player.id] = data.get("target_id")
                # Check if all voted
                alive_ids = {p.id for p in current_room.alive_players()}
                if set(current_room.votes.keys()) >= alive_ids:
                    if current_room._timer_task:
                        current_room._timer_task.cancel()
                    await current_room._tally_votes()

            elif msg_type == "final_vote":
                if not current_room or current_room.phase != "final_vote":
                    continue
                if not player.alive:
                    continue
                current_room.final_votes[player.id] = data.get("decision")
                alive_ids = {p.id for p in current_room.alive_players()}
                if set(current_room.final_votes.keys()) >= alive_ids:
                    if current_room._timer_task:
                        current_room._timer_task.cancel()
                    await current_room._tally_final_vote()

            elif msg_type == "extend_timer":
                if current_room:
                    await current_room.extend_day()

            elif msg_type == "skip_day":
                if current_room and player.id == current_room.host_id:
                    await current_room.skip_day()

    except WebSocketDisconnect:
        if current_room:
            current_room.remove_player(player.id)
            if not current_room.players:
                rooms.pop(current_room.room_id, None)
            else:
                await current_room.broadcast({
                    "type": "room_state",
                    "game_state": current_room.game_state(),
                })
    except Exception as e:
        print(f"WebSocket error: {e}")
        if current_room:
            current_room.remove_player(player.id)


# ── Serve frontend (production) ──────────────────────────────────────────────

frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"
if frontend_dist.exists():
    app.mount("/assets", StaticFiles(directory=frontend_dist / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file_path = frontend_dist / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(frontend_dist / "index.html")
