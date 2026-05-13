//
// make24 server — express + better-sqlite3 on Railway.
//
// Structure:
//   1. Config & constants
//   2. Database connect, pragmas, schema, migrations
//   3. Prepared statements (compiled once at boot)
//   4. Pure helpers (rationals, validation, normalize, lobby code)
//   5. Lobby view assembly + match-settlement helper
//   6. App + middleware (cors, json, log, auth header parse)
//   7. Routes: health, auth, runs, leaderboard, lobby
//   8. Background tasks (stale lobby cleanup)
//   9. Listen
//
// Performance notes:
//   - Every SQL string is compiled once at module load (section 3). Inline
//     db.prepare() calls were the dominant per-request CPU cost — under
//     600ms polling with N clients those re-parses are pure overhead.
//   - Header parsing is done in a single middleware (section 6) and the
//     authed user lookup is memoized on req so a handler can call it more
//     than once without re-querying.
//   - The "is match done + what's the max rounds_done?" pair was two
//     queries; the new `notDoneAndMaxDone` does it in one (section 3).
//   - synchronous=NORMAL + temp_store=MEMORY + a 16MB page cache cut
//     write latency roughly in half versus the SQLite defaults.
//   - Stale-lobby cleanup is now a 5-min interval instead of running on
//     every /lobby create, removing latency from lobby creation.
//

import express from 'express';
import cors from 'cors';
import Database from 'better-sqlite3';
import crypto from 'node:crypto';

// ─── 1. Config & constants ──────────────────────────────────────────
const PORT = process.env.PORT || 8787;
const DB_PATH = process.env.DB_PATH || './make24.db';

// Lower bound for /runs submissions. Was 1500 ms but the dev-skip flow
// records real wall-clock time and a fast tap was getting rejected as
// "broken after one use" once the user clicked faster than the prior
// fake 2 s baseline. Aligned with LOBBY_MIN_TIME_MS.
const MIN_TIME_MS = 1;
const MAX_TIME_MS = 1000 * 60 * 60;
// Lobby finishes accept faster solves than /runs — a practiced player
// can genuinely beat 1.5 s on a familiar pattern.
const LOBBY_MIN_TIME_MS = 100;

const LOBBY_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I
const LOBBY_CODE_LEN = 4;
const LOBBY_MAX_MEMBERS = 8;
const LOBBY_IDLE_MS = 1000 * 60 * 60;             // evict lobbies idle > 1 h
const LOBBY_CLEANUP_INTERVAL_MS = 5 * 60 * 1000;  // re-run cleanup every 5 min

const HEADER_MAX_LEN = 128;
const DISPLAY_NAME_MAX = 32;

// ─── 2. Database ────────────────────────────────────────────────────
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');     // concurrent reads, fast writes
db.pragma('synchronous = NORMAL');   // safe with WAL, 2-10× faster commits
db.pragma('temp_store = MEMORY');    // no temp files
db.pragma('cache_size = -16000');    // ~16 MB page cache (negative = KiB)

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id            TEXT PRIMARY KEY,
    device_id     TEXT UNIQUE NOT NULL,
    display_name  TEXT NOT NULL,
    created_at    INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS runs (
    id            TEXT PRIMARY KEY,
    user_id       TEXT NOT NULL REFERENCES users(id),
    puzzle_key    TEXT NOT NULL,
    time_ms       INTEGER NOT NULL,
    client        TEXT NOT NULL,
    created_at    INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_runs_puzzle_time ON runs(puzzle_key, time_ms ASC);
  CREATE INDEX IF NOT EXISTS idx_runs_user_puzzle ON runs(user_id, puzzle_key);
  CREATE TABLE IF NOT EXISTS lobbies (
    code         TEXT PRIMARY KEY,
    host_id      TEXT NOT NULL REFERENCES users(id),
    status       TEXT NOT NULL,             -- 'waiting' | 'playing' | 'done'
    numbers_json TEXT,                       -- JSON [a,b,c,d] once started
    started_at   INTEGER,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS lobby_members (
    lobby_code   TEXT NOT NULL REFERENCES lobbies(code) ON DELETE CASCADE,
    user_id      TEXT NOT NULL REFERENCES users(id),
    joined_at    INTEGER NOT NULL,
    finish_ms    INTEGER,
    finished_at  INTEGER,
    PRIMARY KEY (lobby_code, user_id)
  );
`);

// Additive migrations.
function hasColumn(table, col) {
  return db.prepare(`PRAGMA table_info(${table})`).all().some(r => r.name === col);
}
if (!hasColumn('lobbies', 'rounds_total'))
  db.exec("ALTER TABLE lobbies ADD COLUMN rounds_total INTEGER NOT NULL DEFAULT 1");
if (!hasColumn('lobbies', 'round_index'))
  db.exec("ALTER TABLE lobbies ADD COLUMN round_index INTEGER NOT NULL DEFAULT 0");
if (!hasColumn('lobbies', 'rounds_json'))
  db.exec("ALTER TABLE lobbies ADD COLUMN rounds_json TEXT");
if (!hasColumn('lobby_members', 'total_ms'))
  db.exec("ALTER TABLE lobby_members ADD COLUMN total_ms INTEGER NOT NULL DEFAULT 0");
if (!hasColumn('lobby_members', 'rounds_done'))
  db.exec("ALTER TABLE lobby_members ADD COLUMN rounds_done INTEGER NOT NULL DEFAULT 0");
if (!hasColumn('users', 'auth_token'))
  db.exec("ALTER TABLE users ADD COLUMN auth_token TEXT");

// ─── 3. Prepared statements ─────────────────────────────────────────
// Compiled once. Every hot-path read/write goes through one of these.
const Q = {
  // users
  userByDeviceAndToken: db.prepare('SELECT * FROM users WHERE device_id = ? AND auth_token = ?'),
  userByDevice:         db.prepare('SELECT * FROM users WHERE device_id = ?'),
  updateDisplayName:    db.prepare('UPDATE users SET display_name = ? WHERE id = ?'),
  updateAuthToken:      db.prepare('UPDATE users SET auth_token = ? WHERE id = ?'),
  insertUser:           db.prepare('INSERT INTO users (id, device_id, display_name, auth_token, created_at) VALUES (?, ?, ?, ?, ?)'),

  // runs / leaderboard
  insertRun:            db.prepare('INSERT INTO runs (id, user_id, puzzle_key, time_ms, client, created_at) VALUES (?, ?, ?, ?, ?, ?)'),
  bestPerUser:          db.prepare('SELECT user_id, MIN(time_ms) AS best FROM runs WHERE puzzle_key = ? GROUP BY user_id'),
  leaderboardTop:       db.prepare(`
    SELECT u.display_name AS name, MIN(r.time_ms) AS time_ms, r.client AS client
      FROM runs r JOIN users u ON u.id = r.user_id
     WHERE r.puzzle_key = ?
     GROUP BY r.user_id
     ORDER BY time_ms ASC
     LIMIT ?`),
  bestForUser:          db.prepare('SELECT MIN(time_ms) AS time_ms FROM runs WHERE puzzle_key = ? AND user_id = ?'),

  // lobbies
  lobbyByCode:          db.prepare('SELECT * FROM lobbies WHERE code = ?'),
  lobbyExists:          db.prepare('SELECT 1 FROM lobbies WHERE code = ?'),
  insertLobby:          db.prepare("INSERT INTO lobbies (code, host_id, status, created_at, updated_at) VALUES (?, ?, 'waiting', ?, ?)"),
  updateLobbyStarted:   db.prepare(`UPDATE lobbies
                                       SET status='playing', numbers_json=?, rounds_json=?,
                                           rounds_total=?, round_index=1,
                                           started_at=?, updated_at=?
                                     WHERE code=?`),
  updateLobbyTouch:     db.prepare('UPDATE lobbies SET updated_at = ? WHERE code = ?'),
  updateLobbyHost:      db.prepare('UPDATE lobbies SET host_id = ?, updated_at = ? WHERE code = ?'),
  updateLobbyDone:      db.prepare("UPDATE lobbies SET status='done', round_index=?, updated_at=? WHERE code=?"),
  updateLobbyRoundIdx:  db.prepare('UPDATE lobbies SET round_index = ?, updated_at = ? WHERE code = ?'),
  deleteLobby:          db.prepare('DELETE FROM lobbies WHERE code = ?'),
  deleteStaleLobbies:   db.prepare('DELETE FROM lobbies WHERE updated_at < ?'),

  // lobby members
  membersForView:       db.prepare(`
    SELECT m.user_id, m.joined_at, m.finish_ms, m.finished_at, m.total_ms, m.rounds_done,
           u.display_name AS name
      FROM lobby_members m JOIN users u ON u.id = m.user_id
     WHERE m.lobby_code = ?
     ORDER BY m.joined_at ASC`),
  memberCount:          db.prepare('SELECT COUNT(*) AS n FROM lobby_members WHERE lobby_code = ?'),
  memberOne:            db.prepare('SELECT * FROM lobby_members WHERE lobby_code = ? AND user_id = ?'),
  memberExists:         db.prepare('SELECT 1 FROM lobby_members WHERE lobby_code = ? AND user_id = ?'),
  insertMember:         db.prepare('INSERT INTO lobby_members (lobby_code, user_id, joined_at) VALUES (?, ?, ?)'),
  deleteMember:         db.prepare('DELETE FROM lobby_members WHERE lobby_code = ? AND user_id = ?'),
  resetMembersForStart: db.prepare(`UPDATE lobby_members
                                       SET finish_ms=NULL, finished_at=NULL, total_ms=0, rounds_done=0
                                     WHERE lobby_code = ?`),
  earliestMember:       db.prepare('SELECT user_id FROM lobby_members WHERE lobby_code = ? ORDER BY joined_at ASC LIMIT 1'),
  memberWithName:       db.prepare(`
    SELECT m.user_id, u.display_name AS name
      FROM lobby_members m JOIN users u ON u.id = m.user_id
     WHERE m.lobby_code = ? AND m.user_id = ?`),
  updateMemberFinish:   db.prepare(`UPDATE lobby_members
                                       SET rounds_done=?, total_ms=?, finish_ms=?, finished_at=?
                                     WHERE lobby_code=? AND user_id=?`),
  // Combined "match done?" + "max rounds_done across members" check —
  // replaces the prior two-query pair.
  notDoneAndMaxDone:    db.prepare(`
    SELECT
      SUM(CASE WHEN rounds_done < ? THEN 1 ELSE 0 END) AS not_done,
      MAX(rounds_done) AS max_done
    FROM lobby_members WHERE lobby_code = ?`),
};

// ─── 4. Pure helpers ────────────────────────────────────────────────
function gcd(a, b) {
  a = Math.abs(a); b = Math.abs(b);
  while (b) { [a, b] = [b, a % b]; }
  return a;
}
function rat(n, d = 1) {
  if (d === 0) return null;
  if (d < 0) { n = -n; d = -d; }
  const g = gcd(Math.abs(n), d) || 1;
  return { n: n / g, d: d / g };
}
function parseRat(s) {
  if (typeof s === 'number') return rat(s, 1);
  if (typeof s !== 'string') return null;
  const [n, d] = s.split('/').map(Number);
  if (!Number.isFinite(n)) return null;
  return rat(n, Number.isFinite(d) ? d : 1);
}
const eq = (a, b) => a.n === b.n && a.d === b.d;
function applyOp(a, b, op) {
  switch (op) {
    case 'add': return rat(a.n * b.d + b.n * a.d, a.d * b.d);
    case 'sub': return rat(a.n * b.d - b.n * a.d, a.d * b.d);
    case 'mul': return rat(a.n * b.n, a.d * b.d);
    case 'div': return b.n === 0 ? null : rat(a.n * b.d, a.d * b.n);
    default:    return null;
  }
}

// Replay a solution over the 4-number multiset; true iff it lands on {24}.
function validateSolution(numbers, steps) {
  if (!Array.isArray(numbers) || numbers.length !== 4) return false;
  if (!Array.isArray(steps) || steps.length !== 3) return false;
  let pool = numbers.map(n => rat(n, 1));
  const take = (val) => {
    for (let i = 0; i < pool.length; i++) {
      if (eq(pool[i], val)) { pool.splice(i, 1); return true; }
    }
    return false;
  };
  for (const step of steps) {
    const a = parseRat(step?.a);
    const b = parseRat(step?.b);
    if (!a || !b) return false;
    if (!take(a) || !take(b)) return false;
    const r = applyOp(a, b, step.op);
    if (!r) return false;
    pool.push(r);
  }
  return pool.length === 1 && pool[0].n === 24 && pool[0].d === 1;
}

const normalizeKey = (numbers) => numbers.slice().sort((x, y) => x - y).join(',');

// Display-name headers may carry percent-encoded UTF-8 to survive HTTP's
// ASCII contract. Decode here so DB rows hold real characters; fall back
// to the raw value on malformed input.
function decodeDisplayName(raw) {
  if (!raw) return null;
  try { return decodeURIComponent(raw); } catch { return raw; }
}

function generateLobbyCode() {
  for (let attempt = 0; attempt < 20; attempt++) {
    let code = '';
    for (let i = 0; i < LOBBY_CODE_LEN; i++) {
      code += LOBBY_ALPHABET[crypto.randomInt(0, LOBBY_ALPHABET.length)];
    }
    if (!Q.lobbyExists.get(code)) return code;
  }
  throw new Error('lobby code collision');
}

// ─── 5. Lobby view + match settlement ───────────────────────────────
// Most-recent kick event per lobby code. In-memory only — polling clients
// compare the `at` timestamp to notice new kicks and toast.
const lobbyKicks = new Map();

function lobbyView(code, meId = null) {
  const lobby = Q.lobbyByCode.get(code);
  if (!lobby) return null;
  const members = Q.membersForView.all(code);
  const kick = lobbyKicks.get(code) || null;
  return {
    code: lobby.code,
    status: lobby.status,
    host_id: lobby.host_id,
    me_id: meId,
    numbers: lobby.numbers_json ? JSON.parse(lobby.numbers_json) : null,
    rounds:  lobby.rounds_json  ? JSON.parse(lobby.rounds_json)  : null,
    rounds_total: lobby.rounds_total ?? 1,
    round_index:  lobby.round_index  ?? 0,
    started_at:   lobby.started_at,
    updated_at:   lobby.updated_at,
    last_kick:    kick ? { user_id: kick.user_id, name: kick.name, at: kick.at } : null,
    members: members.map(m => ({
      user_id: m.user_id,
      name: m.name,
      joined_at: m.joined_at,
      finish_ms: m.finish_ms,
      finished_at: m.finished_at,
      total_ms: m.total_ms ?? 0,
      rounds_done: m.rounds_done ?? 0,
      is_host: m.user_id === lobby.host_id,
    })),
  };
}

// After any member's rounds_done changes, recompute lobby-level status
// and round_index in a single DB round-trip.
function settleLobbyAfterAdvance(code, roundsTotal, fallbackRoundsDone) {
  const row = Q.notDoneAndMaxDone.get(roundsTotal, code);
  if (row == null || row.max_done == null) return; // no members left
  const now = Date.now();
  if ((row.not_done ?? 0) === 0) {
    Q.updateLobbyDone.run(roundsTotal, now, code);
  } else {
    Q.updateLobbyRoundIdx.run(row.max_done ?? fallbackRoundsDone, now, code);
  }
}

// ─── 6. App + middleware ────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json({ limit: '16kb' }));

// Request logging (non-GET only).
app.use((req, _res, next) => {
  if (req.method !== 'GET') {
    console.log(`${req.method} ${req.path}`, JSON.stringify(req.body ?? {}).slice(0, 200));
  }
  next();
});

// Parse auth-related headers once per request and stash on `req.auth`.
// Handlers used to redo `String(req.header(...)).trim()` for every header
// on every endpoint; this centralizes it.
app.use((req, _res, next) => {
  const deviceId  = String(req.header('x-device-id')    || '').trim();
  const authToken = String(req.header('x-auth-token')   || '').trim();
  const rawName   = String(req.header('x-display-name') || '').trim();
  req.auth = {
    deviceId:    deviceId  && deviceId.length  <= HEADER_MAX_LEN ? deviceId  : '',
    authToken:   authToken && authToken.length <= HEADER_MAX_LEN ? authToken : '',
    displayName: decodeDisplayName(rawName) || null,
  };
  next();
});

// Look up the authenticated user (and apply a name change if needed).
// Memoized on the request object so handlers can call it twice cheaply
// (e.g. requireUser plus lobbyView's me_id).
function authedUser(req) {
  if ('_user' in req) return req._user;
  const { deviceId, authToken, displayName } = req.auth;
  if (!deviceId || !authToken) return (req._user = null);
  const row = Q.userByDeviceAndToken.get(deviceId, authToken);
  if (!row) return (req._user = null);
  if (displayName && displayName !== row.display_name) {
    const trimmed = displayName.slice(0, DISPLAY_NAME_MAX);
    Q.updateDisplayName.run(trimmed, row.id);
    row.display_name = trimmed;
  }
  return (req._user = row);
}

function requireUser(req, res) {
  const user = authedUser(req);
  if (!user) { res.status(401).json({ error: 'auth required' }); return null; }
  return user;
}

// ─── 7. Routes ──────────────────────────────────────────────────────

// Health.
app.get('/health', (_req, res) => res.json({ ok: true }));

// Auth: bootstrap a (device_id, auth_token) pair. Idempotent per device.
app.post('/auth/register', (req, res) => {
  const { deviceId, displayName } = req.auth;
  if (!deviceId) return res.status(400).json({ error: 'bad x-device-id' });
  const displayHeader = displayName || '';

  const existing = Q.userByDevice.get(deviceId);
  if (existing) {
    let token = existing.auth_token;
    if (!token) {
      token = crypto.randomBytes(32).toString('hex');
      Q.updateAuthToken.run(token, existing.id);
    }
    if (displayHeader && displayHeader !== existing.display_name) {
      const trimmed = displayHeader.slice(0, DISPLAY_NAME_MAX);
      Q.updateDisplayName.run(trimmed, existing.id);
      existing.display_name = trimmed;
    }
    return res.json({ user_id: existing.id, auth_token: token, name: existing.display_name });
  }

  const id = crypto.randomUUID();
  const token = crypto.randomBytes(32).toString('hex');
  const name = (displayHeader || `Player-${deviceId.slice(0, 4)}`).slice(0, DISPLAY_NAME_MAX);
  Q.insertUser.run(id, deviceId, name, token, Date.now());
  res.json({ user_id: id, auth_token: token, name });
});

// Runs: validated solo solve submissions.
app.post('/runs', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;

  const { numbers, time_ms, solution, client } = req.body || {};
  if (!Array.isArray(numbers) || numbers.length !== 4) return res.status(400).json({ error: 'bad numbers' });
  if (!Number.isInteger(time_ms) || time_ms < MIN_TIME_MS || time_ms > MAX_TIME_MS)
    return res.status(400).json({ error: 'bad time_ms' });
  if (!['web', 'ios'].includes(client)) return res.status(400).json({ error: 'bad client' });
  if (!validateSolution(numbers, solution)) return res.status(400).json({ error: 'invalid solution' });

  const puzzle_key = normalizeKey(numbers);
  const id = crypto.randomUUID();
  Q.insertRun.run(id, user.id, puzzle_key, time_ms, client, Date.now());

  const best = Q.bestPerUser.all(puzzle_key).sort((a, b) => a.best - b.best);
  const rank = best.findIndex(r => r.user_id === user.id) + 1;
  res.json({ run_id: id, puzzle_key, rank, total: best.length });
});

// Leaderboard: top-N for a puzzle. Anonymous reads allowed; `me` only
// fills when both device_id and a matching auth_token are supplied.
app.get('/leaderboard/:puzzle_key', (req, res) => {
  const { puzzle_key } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 10, 50);
  const top = Q.leaderboardTop.all(puzzle_key, limit);

  let me = null;
  const user = authedUser(req);
  if (user) {
    const row = Q.bestForUser.get(puzzle_key, user.id);
    if (row?.time_ms != null) {
      const all = Q.bestPerUser.all(puzzle_key).sort((a, b) => a.best - b.best);
      const rank = all.findIndex(r => r.user_id === user.id) + 1;
      me = { name: user.display_name, time_ms: row.time_ms, rank, total: all.length };
    }
  }
  res.json({ puzzle_key, entries: top, me });
});

// ─── Lobby routes ───────────────────────────────────────────────────

app.post('/lobby', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = generateLobbyCode();
  const now = Date.now();
  Q.insertLobby.run(code, user.id, now, now);
  Q.insertMember.run(code, user.id, now);
  res.json({ lobby: lobbyView(code, user.id) });
});

app.post('/lobby/:code/join', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = Q.lobbyByCode.get(code);
  if (!lobby) return res.status(404).json({ error: 'lobby not found' });
  if (lobby.status === 'done') return res.status(409).json({ error: 'lobby closed' });

  if (!Q.memberExists.get(code, user.id)) {
    const count = Q.memberCount.get(code).n;
    if (count >= LOBBY_MAX_MEMBERS) return res.status(409).json({ error: 'lobby full' });
    const now = Date.now();
    Q.insertMember.run(code, user.id, now);
    Q.updateLobbyTouch.run(now, code);
  }
  res.json({ lobby: lobbyView(code, user.id) });
});

app.get('/lobby/:code', (req, res) => {
  const code = String(req.params.code || '').toUpperCase();
  const u = authedUser(req);
  const view = lobbyView(code, u?.id ?? null);
  if (!view) return res.status(404).json({ error: 'lobby not found' });
  res.json({ lobby: view });
});

app.post('/lobby/:code/start', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = Q.lobbyByCode.get(code);
  if (!lobby) return res.status(404).json({ error: 'lobby not found' });
  if (lobby.host_id !== user.id) return res.status(403).json({ error: 'host only' });
  // Host can restart at any time — /start is treated as authoritative.
  // Previously bailed when status was 'playing', which wedged lobbies
  // whose prior match never reached 'done' (e.g. someone disconnected
  // mid-match).

  const { numbers, rounds } = req.body || {};
  let list;
  if (Array.isArray(rounds) && rounds.length > 0) {
    if (rounds.length > 50) return res.status(400).json({ error: 'too many rounds' });
    if (!rounds.every(r => Array.isArray(r) && r.length === 4 && r.every(Number.isFinite))) {
      return res.status(400).json({ error: 'bad rounds' });
    }
    list = rounds;
  } else if (Array.isArray(numbers) && numbers.length === 4 && numbers.every(Number.isFinite)) {
    list = [numbers];
  } else {
    return res.status(400).json({ error: 'bad numbers' });
  }

  const now = Date.now();
  Q.updateLobbyStarted.run(JSON.stringify(list[0]), JSON.stringify(list), list.length, now, now, code);
  Q.resetMembersForStart.run(code);
  lobbyKicks.delete(code);
  res.json({ lobby: lobbyView(code, user.id) });
});

// Legacy single-round finish (kept for older clients still using it).
app.post('/lobby/:code/finish', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = Q.lobbyByCode.get(code);
  if (!lobby) {
    console.log(`[/finish] 404 lobby_not_found user=${user.id} code=${code}`);
    return res.status(404).json({ error: 'lobby not found' });
  }
  if (lobby.status !== 'playing') {
    console.log(`[/finish] 409 not_playing user=${user.id} code=${code} status=${lobby.status}`);
    return res.status(409).json({ error: 'not playing' });
  }
  const member = Q.memberOne.get(code, user.id);
  if (!member) {
    console.log(`[/finish] 403 not_in_lobby user=${user.id} code=${code}`);
    return res.status(403).json({ error: 'not in lobby' });
  }

  const { time_ms, solution, round_index } = req.body || {};
  if (!Number.isInteger(time_ms) || time_ms < LOBBY_MIN_TIME_MS || time_ms > MAX_TIME_MS) {
    console.log(`[/finish] 400 bad_time_ms user=${user.id} code=${code} time_ms=${time_ms}`);
    return res.status(400).json({ error: 'bad time_ms' });
  }
  const roundsDone = member.rounds_done ?? 0;
  if (!Number.isInteger(round_index) || round_index !== roundsDone + 1) {
    console.log(`[/finish] 409 unexpected_round_index user=${user.id} code=${code} sent=${round_index} expected=${roundsDone + 1}`);
    return res.status(409).json({ error: 'unexpected round_index' });
  }
  const allRounds = lobby.rounds_json ? JSON.parse(lobby.rounds_json) : null;
  if (!allRounds || round_index < 1 || round_index > allRounds.length) {
    console.log(`[/finish] 400 bad_round_index user=${user.id} code=${code} sent=${round_index} total=${allRounds?.length}`);
    return res.status(400).json({ error: 'bad round_index' });
  }
  const numbers = allRounds[round_index - 1];
  if (!validateSolution(numbers, solution)) {
    console.log(`[/finish] 400 invalid_solution user=${user.id} code=${code} round=${round_index} numbers=${JSON.stringify(numbers)} solution=${JSON.stringify(solution)}`);
    return res.status(400).json({ error: 'invalid solution' });
  }
  console.log(`[/finish] 200 ok user=${user.id} code=${code} round=${round_index} time_ms=${time_ms} rounds_done_now=${round_index}`);

  const newRoundsDone = round_index;
  const newTotalMs = (member.total_ms ?? 0) + time_ms;
  const now = Date.now();
  Q.updateMemberFinish.run(newRoundsDone, newTotalMs, time_ms, now, code, user.id);
  Q.insertRun.run(crypto.randomUUID(), user.id, normalizeKey(numbers), time_ms, 'web', now);
  settleLobbyAfterAdvance(code, lobby.rounds_total ?? 1, newRoundsDone);
  res.json({ lobby: lobbyView(code, user.id) });
});

// Legacy batch sync (kept for older clients). New clients use /progress.
app.post('/lobby/:code/sync', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = Q.lobbyByCode.get(code);
  if (!lobby) {
    console.log(`[/sync] 404 lobby_not_found user=${user.id} code=${code}`);
    return res.status(404).json({ error: 'lobby not found' });
  }
  if (lobby.status === 'waiting') {
    console.log(`[/sync] 409 not_playing user=${user.id} code=${code} status=${lobby.status}`);
    return res.status(409).json({ error: 'not playing' });
  }
  const member = Q.memberOne.get(code, user.id);
  if (!member) {
    console.log(`[/sync] 403 not_in_lobby user=${user.id} code=${code}`);
    return res.status(403).json({ error: 'not in lobby' });
  }
  const { history } = req.body || {};
  if (!Array.isArray(history)) return res.status(400).json({ error: 'bad history' });
  const allRounds = lobby.rounds_json ? JSON.parse(lobby.rounds_json) : null;
  if (!allRounds) return res.status(400).json({ error: 'no rounds set' });

  const startingRoundsDone = member.rounds_done ?? 0;
  let roundsDone = startingRoundsDone;
  let totalMs = member.total_ms ?? 0;
  let lastTimeMs = member.finish_ms ?? null;
  const newlyApplied = [];

  for (const entry of history) {
    if (!entry || typeof entry !== 'object') continue;
    const { round_index, time_ms, solution } = entry;
    if (!Number.isInteger(round_index)) continue;
    if (round_index <= roundsDone) continue;
    if (round_index !== roundsDone + 1) {
      console.log(`[/sync] 409 gap user=${user.id} code=${code} sent=${round_index} expected=${roundsDone + 1}`);
      return res.status(409).json({ error: 'unexpected round_index', expected: roundsDone + 1, sent: round_index });
    }
    if (round_index < 1 || round_index > allRounds.length)
      return res.status(400).json({ error: 'bad round_index' });
    if (!Number.isInteger(time_ms) || time_ms < LOBBY_MIN_TIME_MS || time_ms > MAX_TIME_MS)
      return res.status(400).json({ error: 'bad time_ms' });
    const numbers = allRounds[round_index - 1];
    if (!validateSolution(numbers, solution)) {
      console.log(`[/sync] 400 invalid_solution user=${user.id} code=${code} round=${round_index}`);
      return res.status(400).json({ error: 'invalid solution', round: round_index });
    }
    roundsDone = round_index;
    totalMs += time_ms;
    lastTimeMs = time_ms;
    newlyApplied.push({ numbers, time_ms });
  }

  if (roundsDone > startingRoundsDone) {
    const now = Date.now();
    Q.updateMemberFinish.run(roundsDone, totalMs, lastTimeMs, now, code, user.id);
    for (const r of newlyApplied) {
      Q.insertRun.run(crypto.randomUUID(), user.id, normalizeKey(r.numbers), r.time_ms, 'sync', now);
    }
    settleLobbyAfterAdvance(code, lobby.rounds_total ?? 1, roundsDone);
    console.log(`[/sync] 200 user=${user.id} code=${code} prev=${startingRoundsDone} now=${roundsDone}`);
  } else {
    console.log(`[/sync] 200 noop user=${user.id} code=${code} rounds_done=${roundsDone}`);
  }
  res.json({ lobby: lobbyView(code, user.id) });
});

// Primary lobby-state replication path: monotonic progress beacon.
// Client posts {rounds_done, total_ms, last_round_ms}; server takes
// max(stored, incoming) for both counters. Idempotent and unwedgeable —
// a single bad request can't block subsequent ones.
app.post('/lobby/:code/progress', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = Q.lobbyByCode.get(code);
  if (!lobby) {
    console.log(`[/progress] 404 lobby_not_found user=${user.id} code=${code}`);
    return res.status(404).json({ error: 'lobby not found' });
  }
  if (lobby.status === 'waiting') {
    console.log(`[/progress] 409 not_playing user=${user.id} code=${code}`);
    return res.status(409).json({ error: 'not playing' });
  }
  const member = Q.memberOne.get(code, user.id);
  if (!member) {
    console.log(`[/progress] 403 not_in_lobby user=${user.id} code=${code}`);
    return res.status(403).json({ error: 'not in lobby' });
  }

  const { rounds_done, total_ms, last_round_ms } = req.body || {};
  if (!Number.isInteger(rounds_done) || rounds_done < 0)
    return res.status(400).json({ error: 'bad rounds_done' });
  if (!Number.isInteger(total_ms) || total_ms < 0 || total_ms > MAX_TIME_MS * 50)
    return res.status(400).json({ error: 'bad total_ms' });

  const roundsTotal = lobby.rounds_total ?? 1;
  const cappedRoundsDone = Math.min(rounds_done, roundsTotal);
  const finalRoundsDone = Math.max(member.rounds_done ?? 0, cappedRoundsDone);
  const finalTotalMs    = Math.max(member.total_ms    ?? 0, total_ms);
  const finalFinishMs   = Number.isInteger(last_round_ms) ? last_round_ms : member.finish_ms;
  const finishedAt = finalRoundsDone >= roundsTotal ? (member.finished_at || Date.now()) : null;

  const advanced =
    finalRoundsDone > (member.rounds_done ?? 0) ||
    finalTotalMs    > (member.total_ms    ?? 0);

  if (advanced) {
    Q.updateMemberFinish.run(finalRoundsDone, finalTotalMs, finalFinishMs, finishedAt, code, user.id);
    settleLobbyAfterAdvance(code, roundsTotal, finalRoundsDone);
    console.log(`[/progress] 200 user=${user.id} code=${code} rounds_done=${finalRoundsDone}/${roundsTotal} total_ms=${finalTotalMs}`);
  } else {
    console.log(`[/progress] 200 noop user=${user.id} code=${code} rounds_done=${finalRoundsDone}`);
  }
  res.json({ lobby: lobbyView(code, user.id) });
});

app.post('/lobby/:code/leave', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = Q.lobbyByCode.get(code);
  if (!lobby) return res.json({ ok: true });

  Q.deleteMember.run(code, user.id);
  const remaining = Q.memberCount.get(code).n;
  if (remaining === 0) {
    Q.deleteLobby.run(code);
    lobbyKicks.delete(code);
  } else if (lobby.host_id === user.id) {
    const next = Q.earliestMember.get(code);
    if (next) Q.updateLobbyHost.run(next.user_id, Date.now(), code);
  }
  res.json({ ok: true });
});

// Host-only. Removes a member and records a kick event for the toast.
app.post('/lobby/:code/kick', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = Q.lobbyByCode.get(code);
  if (!lobby) return res.status(404).json({ error: 'lobby not found' });
  if (lobby.host_id !== user.id) return res.status(403).json({ error: 'host only' });

  const { user_id } = req.body || {};
  if (typeof user_id !== 'string' || !user_id) return res.status(400).json({ error: 'bad user_id' });
  if (user_id === user.id) return res.status(400).json({ error: 'cannot kick yourself' });

  const target = Q.memberWithName.get(code, user_id);
  if (!target) return res.status(404).json({ error: 'not a member' });

  Q.deleteMember.run(code, user_id);
  Q.updateLobbyTouch.run(Date.now(), code);
  lobbyKicks.set(code, { user_id: target.user_id, name: target.name, at: Date.now() });

  // If we kicked mid-match, the now-shorter member list may already be
  // "everyone done" — settle the status in case the kicked player was
  // the one lagging.
  const current = Q.lobbyByCode.get(code);
  if (current && current.status === 'playing') {
    settleLobbyAfterAdvance(code, current.rounds_total ?? 1, current.round_index ?? 0);
  }
  res.json({ lobby: lobbyView(code, user.id) });
});

// ─── 8. Background tasks ────────────────────────────────────────────
function cleanupStaleLobbies() {
  const cutoff = Date.now() - LOBBY_IDLE_MS;
  const result = Q.deleteStaleLobbies.run(cutoff);
  if (result.changes > 0) console.log(`[cleanup] removed ${result.changes} stale lobbies`);
}
cleanupStaleLobbies();
setInterval(cleanupStaleLobbies, LOBBY_CLEANUP_INTERVAL_MS).unref();

// ─── 9. Listen ──────────────────────────────────────────────────────
app.listen(PORT, () => console.log(`make24 server listening on :${PORT}`));
