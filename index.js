import express from 'express';
import cors from 'cors';
import Database from 'better-sqlite3';
import crypto from 'node:crypto';

const PORT = process.env.PORT || 8787;
const DB_PATH = process.env.DB_PATH || './make24.db';
// Lower-bound on submitted solve times. Was 1500ms to keep junk scores off
// the leaderboard, but the dev-skip flow now records real wall-clock time
// and a fast tap (< 1.5s) was getting silently rejected as a 400 — making
// the skip button appear "broken after one use" once the user clicked faster
// than the prior fake 2s baseline. Bringing this in line with LOBBY_MIN_TIME_MS
// so both code paths share the same forgiving floor.
const MIN_TIME_MS = 1;
const MAX_TIME_MS = 1000 * 60 * 60;

// ─────────────────────────────────────────────
// DB
// ─────────────────────────────────────────────
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
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
  CREATE INDEX IF NOT EXISTS idx_runs_puzzle_time
    ON runs(puzzle_key, time_ms ASC);
  CREATE INDEX IF NOT EXISTS idx_runs_user_puzzle
    ON runs(user_id, puzzle_key);
  CREATE TABLE IF NOT EXISTS lobbies (
    code         TEXT PRIMARY KEY,
    host_id      TEXT NOT NULL REFERENCES users(id),
    status       TEXT NOT NULL,              -- 'waiting' | 'playing' | 'done'
    numbers_json TEXT,                        -- JSON [a,b,c,d] once started
    started_at   INTEGER,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS lobby_members (
    lobby_code   TEXT NOT NULL REFERENCES lobbies(code) ON DELETE CASCADE,
    user_id      TEXT NOT NULL REFERENCES users(id),
    joined_at    INTEGER NOT NULL,
    finish_ms    INTEGER,                     -- solve time in ms, null if not finished
    finished_at  INTEGER,                     -- wall clock when submission arrived
    PRIMARY KEY (lobby_code, user_id)
  );
`);

// Additive migrations for the multi-round lobby feature.
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
// Per-user secret minted at registration. Required on every authenticated
// request via x-auth-token. Existing rows are NULL until the client calls
// /auth/register, which upgrades them in place.
if (!hasColumn('users', 'auth_token'))
  db.exec("ALTER TABLE users ADD COLUMN auth_token TEXT");

// ─────────────────────────────────────────────
// Rational replay (server-side solution validator)
// ─────────────────────────────────────────────
function gcd(a, b) { a = Math.abs(a); b = Math.abs(b); while (b) { [a, b] = [b, a % b]; } return a; }
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
function eq(a, b) { return a.n === b.n && a.d === b.d; }
function applyOp(a, b, op) {
  if (op === 'add') return rat(a.n * b.d + b.n * a.d, a.d * b.d);
  if (op === 'sub') return rat(a.n * b.d - b.n * a.d, a.d * b.d);
  if (op === 'mul') return rat(a.n * b.n, a.d * b.d);
  if (op === 'div') return b.n === 0 ? null : rat(a.n * b.d, a.d * b.n);
  return null;
}

// Replay a solution over the starting number multiset.
// steps: [{ a: "5", op: "mul", b: "1" }, ...]
// Returns true iff the final multiset is exactly {24}.
function validateSolution(numbers, steps) {
  if (!Array.isArray(numbers) || numbers.length !== 4) return false;
  if (!Array.isArray(steps) || steps.length !== 3) return false;

  // Multiset of current values, stored as array of rationals.
  let pool = numbers.map(n => rat(n, 1));

  const take = (val) => {
    for (let i = 0; i < pool.length; i++) {
      if (eq(pool[i], val)) {
        pool.splice(i, 1);
        return true;
      }
    }
    return false;
  };

  for (const step of steps) {
    const a = parseRat(step?.a);
    const b = parseRat(step?.b);
    if (!a || !b) return false;
    if (!take(a)) return false;
    if (!take(b)) return false;
    const r = applyOp(a, b, step.op);
    if (!r) return false;
    pool.push(r);
  }
  return pool.length === 1 && pool[0].n === 24 && pool[0].d === 1;
}

function normalizeKey(numbers) {
  return numbers.slice().sort((x, y) => x - y).join(',');
}

// ─────────────────────────────────────────────
// Server
// ─────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json({ limit: '16kb' }));
app.use((req, _res, next) => {
  if (req.method !== 'GET') console.log(`${req.method} ${req.path}`, JSON.stringify(req.body ?? {}).slice(0, 200));
  next();
});

// Display-name headers may carry percent-encoded UTF-8 to survive HTTP's
// ASCII-only contract (clients encode emoji / unicode before sending). Decode
// here so DB rows hold the real characters; falls back to the raw value when
// it's already plain ASCII or malformed.
function decodeDisplayName(raw) {
  if (!raw) return null;
  try { return decodeURIComponent(raw); }
  catch { return raw; }
}

// Look up an authenticated user by (device_id, auth_token) pair. Returns null
// if either header is missing or the pair doesn't match a known user. Client
// bootstraps via /auth/register, which is the only endpoint that creates
// users — every other endpoint trusts the (device_id, auth_token) pair.
function authedUser(deviceId, authToken, displayName) {
  if (!deviceId || typeof deviceId !== 'string' || deviceId.length > 128) return null;
  if (!authToken || typeof authToken !== 'string' || authToken.length > 128) return null;
  const row = db.prepare('SELECT * FROM users WHERE device_id = ? AND auth_token = ?')
    .get(deviceId, authToken);
  if (!row) return null;
  if (displayName && displayName !== row.display_name) {
    const trimmed = displayName.slice(0, 32);
    db.prepare('UPDATE users SET display_name = ? WHERE id = ?').run(trimmed, row.id);
    row.display_name = trimmed;
  }
  return row;
}

// Look up a user by device_id alone — used only for unauthenticated reads
// (e.g. leaderboard "me" lookup). Never creates rows.
function lookupUserByDevice(deviceId) {
  if (!deviceId || typeof deviceId !== 'string' || deviceId.length > 128) return null;
  return db.prepare('SELECT * FROM users WHERE device_id = ?').get(deviceId) || null;
}

app.get('/health', (_req, res) => res.json({ ok: true }));

// Bootstrap endpoint: client sends x-device-id (and optionally x-display-name);
// server returns the user record + a freshly-minted auth_token. For an existing
// device this is idempotent — same token is returned on every call (legacy
// users with no token get one minted on first call).
app.post('/auth/register', (req, res) => {
  const deviceId = String(req.header('x-device-id') || '').trim();
  if (!deviceId || deviceId.length > 128)
    return res.status(400).json({ error: 'bad x-device-id' });
  const displayHeader = decodeDisplayName(String(req.header('x-display-name') || '').trim()) || '';

  const existing = db.prepare('SELECT * FROM users WHERE device_id = ?').get(deviceId);
  if (existing) {
    let token = existing.auth_token;
    if (!token) {
      token = crypto.randomBytes(32).toString('hex');
      db.prepare('UPDATE users SET auth_token = ? WHERE id = ?').run(token, existing.id);
    }
    if (displayHeader && displayHeader !== existing.display_name) {
      const trimmed = displayHeader.slice(0, 32);
      db.prepare('UPDATE users SET display_name = ? WHERE id = ?').run(trimmed, existing.id);
      existing.display_name = trimmed;
    }
    return res.json({ user_id: existing.id, auth_token: token, name: existing.display_name });
  }

  const id = crypto.randomUUID();
  const token = crypto.randomBytes(32).toString('hex');
  const name = (displayHeader || `Player-${deviceId.slice(0, 4)}`).slice(0, 32);
  db.prepare(
    'INSERT INTO users (id, device_id, display_name, auth_token, created_at) VALUES (?, ?, ?, ?, ?)'
  ).run(id, deviceId, name, token, Date.now());
  return res.json({ user_id: id, auth_token: token, name });
});

app.post('/runs', (req, res) => {
  const deviceId = String(req.header('x-device-id') || '').trim();
  const authToken = String(req.header('x-auth-token') || '').trim();
  const displayName = decodeDisplayName(String(req.header('x-display-name') || '').trim()) || null;
  const user = authedUser(deviceId, authToken, displayName);
  if (!user) return res.status(401).json({ error: 'auth required' });

  const { numbers, time_ms, solution, client } = req.body || {};
  if (!Array.isArray(numbers) || numbers.length !== 4) return res.status(400).json({ error: 'bad numbers' });
  if (!Number.isInteger(time_ms) || time_ms < MIN_TIME_MS || time_ms > MAX_TIME_MS)
    return res.status(400).json({ error: 'bad time_ms' });
  if (!['web', 'ios'].includes(client)) return res.status(400).json({ error: 'bad client' });
  if (!validateSolution(numbers, solution)) return res.status(400).json({ error: 'invalid solution' });

  const puzzle_key = normalizeKey(numbers);
  const id = crypto.randomUUID();
  db.prepare(`INSERT INTO runs (id, user_id, puzzle_key, time_ms, client, created_at)
              VALUES (?, ?, ?, ?, ?, ?)`)
    .run(id, user.id, puzzle_key, time_ms, client, Date.now());

  // Compute rank (best time per user).
  const best = db.prepare(`
    SELECT user_id, MIN(time_ms) AS best FROM runs WHERE puzzle_key = ? GROUP BY user_id
  `).all(puzzle_key).sort((a, b) => a.best - b.best);
  const rank = best.findIndex(r => r.user_id === user.id) + 1;

  res.json({ run_id: id, puzzle_key, rank, total: best.length });
});

app.get('/leaderboard/:puzzle_key', (req, res) => {
  const deviceId = String(req.header('x-device-id') || '').trim();
  const authToken = String(req.header('x-auth-token') || '').trim();
  const { puzzle_key } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 10, 50);

  const top = db.prepare(`
    SELECT u.display_name AS name, MIN(r.time_ms) AS time_ms, r.client AS client
      FROM runs r JOIN users u ON u.id = r.user_id
     WHERE r.puzzle_key = ?
     GROUP BY r.user_id
     ORDER BY time_ms ASC
     LIMIT ?
  `).all(puzzle_key, limit);

  // Anonymous reads still allowed; "me" only resolves when both device_id
  // and a matching auth_token are supplied.
  let me = null;
  if (deviceId && authToken) {
    const user = authedUser(deviceId, authToken, null);
    if (user) {
      const row = db.prepare(`
        SELECT MIN(time_ms) AS time_ms FROM runs WHERE puzzle_key = ? AND user_id = ?
      `).get(puzzle_key, user.id);
      if (row?.time_ms != null) {
        const all = db.prepare(`
          SELECT user_id, MIN(time_ms) AS best FROM runs WHERE puzzle_key = ? GROUP BY user_id
        `).all(puzzle_key).sort((a, b) => a.best - b.best);
        const rank = all.findIndex(r => r.user_id === user.id) + 1;
        me = { name: user.display_name, time_ms: row.time_ms, rank, total: all.length };
      }
    }
  }

  res.json({ puzzle_key, entries: top, me });
});

// ─────────────────────────────────────────────
// Lobby
// ─────────────────────────────────────────────
const LOBBY_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I
const LOBBY_CODE_LEN = 4;
const LOBBY_MAX_MEMBERS = 8;
const LOBBY_IDLE_MS = 1000 * 60 * 60; // 1h

// Most-recent kick event per lobby code. Kept in memory only — polling clients
// compare the `at` timestamp to notice new kicks and show a toast.
// { user_id, name, at }
const lobbyKicks = new Map();

function generateLobbyCode() {
  for (let attempt = 0; attempt < 20; attempt++) {
    let code = '';
    for (let i = 0; i < LOBBY_CODE_LEN; i++) {
      code += LOBBY_ALPHABET[crypto.randomInt(0, LOBBY_ALPHABET.length)];
    }
    const exists = db.prepare('SELECT 1 FROM lobbies WHERE code = ?').get(code);
    if (!exists) return code;
  }
  throw new Error('lobby code collision');
}

function lobbyView(code, meId = null) {
  const lobby = db.prepare('SELECT * FROM lobbies WHERE code = ?').get(code);
  if (!lobby) return null;
  const members = db.prepare(`
    SELECT m.user_id, m.joined_at, m.finish_ms, m.finished_at, m.total_ms, m.rounds_done, u.display_name AS name
      FROM lobby_members m JOIN users u ON u.id = m.user_id
     WHERE m.lobby_code = ?
     ORDER BY m.joined_at ASC
  `).all(code);
  const kick = lobbyKicks.get(code) || null;
  return {
    code: lobby.code,
    status: lobby.status,
    host_id: lobby.host_id,
    me_id: meId,
    numbers: lobby.numbers_json ? JSON.parse(lobby.numbers_json) : null,
    rounds: lobby.rounds_json ? JSON.parse(lobby.rounds_json) : null,
    rounds_total: lobby.rounds_total ?? 1,
    round_index: lobby.round_index ?? 0,
    started_at: lobby.started_at,
    updated_at: lobby.updated_at,
    last_kick: kick ? { user_id: kick.user_id, name: kick.name, at: kick.at } : null,
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

function requireUser(req, res) {
  const deviceId = String(req.header('x-device-id') || '').trim();
  const authToken = String(req.header('x-auth-token') || '').trim();
  const displayName = decodeDisplayName(String(req.header('x-display-name') || '').trim()) || null;
  const user = authedUser(deviceId, authToken, displayName);
  if (!user) {
    res.status(401).json({ error: 'auth required' });
    return null;
  }
  return user;
}

function cleanupStaleLobbies() {
  const cutoff = Date.now() - LOBBY_IDLE_MS;
  db.prepare('DELETE FROM lobbies WHERE updated_at < ?').run(cutoff);
}

app.post('/lobby', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  cleanupStaleLobbies();

  const code = generateLobbyCode();
  const now = Date.now();
  db.prepare(`INSERT INTO lobbies (code, host_id, status, created_at, updated_at)
              VALUES (?, ?, 'waiting', ?, ?)`).run(code, user.id, now, now);
  db.prepare(`INSERT INTO lobby_members (lobby_code, user_id, joined_at)
              VALUES (?, ?, ?)`).run(code, user.id, now);
  res.json({ lobby: lobbyView(code, user.id) });
});

app.post('/lobby/:code/join', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = db.prepare('SELECT * FROM lobbies WHERE code = ?').get(code);
  if (!lobby) return res.status(404).json({ error: 'lobby not found' });
  if (lobby.status === 'done') return res.status(409).json({ error: 'lobby closed' });

  const count = db.prepare('SELECT COUNT(*) AS n FROM lobby_members WHERE lobby_code = ?').get(code).n;
  const already = db.prepare('SELECT 1 FROM lobby_members WHERE lobby_code = ? AND user_id = ?').get(code, user.id);
  if (!already) {
    if (count >= LOBBY_MAX_MEMBERS) return res.status(409).json({ error: 'lobby full' });
    db.prepare(`INSERT INTO lobby_members (lobby_code, user_id, joined_at)
                VALUES (?, ?, ?)`).run(code, user.id, Date.now());
    db.prepare('UPDATE lobbies SET updated_at = ? WHERE code = ?').run(Date.now(), code);
  }
  res.json({ lobby: lobbyView(code, user.id) });
});

app.get('/lobby/:code', (req, res) => {
  const code = String(req.params.code || '').toUpperCase();
  const deviceId = String(req.header('x-device-id') || '').trim();
  const authToken = String(req.header('x-auth-token') || '').trim();
  // me_id resolution requires a valid (device_id, auth_token) pair so a
  // bystander who happens to know a lobby code can't surface another user's
  // identity in the response. Anonymous polling still works (members visible)
  // but me_id stays null.
  let meId = null;
  if (deviceId && authToken) {
    const u = authedUser(deviceId, authToken, null);
    if (u) meId = u.id;
  }
  const view = lobbyView(code, meId);
  if (!view) return res.status(404).json({ error: 'lobby not found' });
  res.json({ lobby: view });
});

app.post('/lobby/:code/start', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = db.prepare('SELECT * FROM lobbies WHERE code = ?').get(code);
  if (!lobby) return res.status(404).json({ error: 'lobby not found' });
  if (lobby.host_id !== user.id) return res.status(403).json({ error: 'host only' });
  // Host can restart at any time. Previously we bailed when status was
  // 'playing', which wedged the lobby whenever a prior match got stuck
  // (e.g. a player disconnected at 9/10 so it never reached 'done') — the
  // Start button would silently no-op. Treat /start as an authoritative reset.

  const { numbers, rounds } = req.body || {};
  // Accept either legacy { numbers: [a,b,c,d] } for a single round,
  // or { rounds: [[a,b,c,d], ...] } for multi-round play.
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

  const first = list[0];
  const total = list.length;
  const now = Date.now();
  db.prepare(`UPDATE lobbies
                 SET status='playing', numbers_json=?, rounds_json=?,
                     rounds_total=?, round_index=1,
                     started_at=?, updated_at=?
               WHERE code=?`)
    .run(JSON.stringify(first), JSON.stringify(list), total, now, now, code);
  // Reset prior finishes, cumulative totals, and per-player round progress.
  db.prepare(`UPDATE lobby_members
                 SET finish_ms=NULL, finished_at=NULL, total_ms=0, rounds_done=0
               WHERE lobby_code=?`).run(code);
  // Drop any stale kick event so it doesn't re-fire a toast on the fresh match.
  lobbyKicks.delete(code);
  res.json({ lobby: lobbyView(code, user.id) });
});

app.post('/lobby/:code/finish', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = db.prepare('SELECT * FROM lobbies WHERE code = ?').get(code);
  if (!lobby) {
    console.log(`[/finish] 404 lobby_not_found user=${user.id} code=${code}`);
    return res.status(404).json({ error: 'lobby not found' });
  }
  if (lobby.status !== 'playing') {
    console.log(`[/finish] 409 not_playing user=${user.id} code=${code} status=${lobby.status}`);
    return res.status(409).json({ error: 'not playing' });
  }

  const member = db.prepare('SELECT * FROM lobby_members WHERE lobby_code=? AND user_id=?').get(code, user.id);
  if (!member) {
    console.log(`[/finish] 403 not_in_lobby user=${user.id} code=${code}`);
    return res.status(403).json({ error: 'not in lobby' });
  }

  const { time_ms, solution, round_index } = req.body || {};
  // Lobby finishes use a relaxed time floor (100 ms) — a practiced player can
  // genuinely solve a familiar pattern in well under the global MIN_TIME_MS.
  // Rejecting fast lobby solves with 400 used to cascade into a poisoned-match
  // freeze on the iOS client. The MAX bound stays the same to catch obvious
  // tab-left-open submissions.
  const LOBBY_MIN_TIME_MS = 100;
  if (!Number.isInteger(time_ms) || time_ms < LOBBY_MIN_TIME_MS || time_ms > MAX_TIME_MS) {
    console.log(`[/finish] 400 bad_time_ms user=${user.id} code=${code} time_ms=${time_ms}`);
    return res.status(400).json({ error: 'bad time_ms' });
  }

  // round_index is 1-based and must be this player's next expected round.
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

  // Advance this player's progress independently — no waiting for others.
  const newRoundsDone = round_index;
  const newTotalMs = (member.total_ms ?? 0) + time_ms;
  db.prepare(`UPDATE lobby_members
                 SET rounds_done=?, total_ms=?, finish_ms=?, finished_at=?
               WHERE lobby_code=? AND user_id=?`)
    .run(newRoundsDone, newTotalMs, time_ms, Date.now(), code, user.id);

  // Record a global leaderboard run.
  const runId = crypto.randomUUID();
  db.prepare(`INSERT INTO runs (id, user_id, puzzle_key, time_ms, client, created_at)
              VALUES (?, ?, ?, ?, ?, ?)`)
    .run(runId, user.id, normalizeKey(numbers), time_ms, 'web', Date.now());

  // Check whether ALL members have now completed all rounds.
  const roundsTotal = lobby.rounds_total ?? 1;
  const notDoneCount = db.prepare(
    'SELECT COUNT(*) AS n FROM lobby_members WHERE lobby_code=? AND rounds_done < ?'
  ).get(code, roundsTotal).n;

  if (notDoneCount === 0) {
    // Everyone finished every round — match is over.
    db.prepare(`UPDATE lobbies SET status='done', round_index=?, updated_at=? WHERE code=?`)
      .run(roundsTotal, Date.now(), code);
  } else {
    // Keep round_index at the max completed across all members (for display purposes).
    const maxRow = db.prepare(
      'SELECT MAX(rounds_done) AS m FROM lobby_members WHERE lobby_code=?'
    ).get(code);
    db.prepare(`UPDATE lobbies SET round_index=?, updated_at=? WHERE code=?`)
      .run(maxRow.m ?? newRoundsDone, Date.now(), code);
  }

  res.json({ lobby: lobbyView(code, user.id) });
});

// Idempotent batch upsert for a player's round history. Client sends its full
// local list of solved rounds; server applies any rounds beyond its own
// rounds_done, validating each in order. This makes per-round delivery robust
// to flaky networks: if every individual /finish retry burned out (which is
// what stranded the iOS player at 9/10 from peers' perspectives), the next
// successful /sync will catch the server fully up.
app.post('/lobby/:code/sync', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = db.prepare('SELECT * FROM lobbies WHERE code = ?').get(code);
  if (!lobby) {
    console.log(`[/sync] 404 lobby_not_found user=${user.id} code=${code}`);
    return res.status(404).json({ error: 'lobby not found' });
  }
  // We accept /sync against a 'done' lobby too — the last submission may
  // arrive slightly after the lobby flipped done because some other player's
  // submission was the one that triggered the transition.
  if (lobby.status === 'waiting') {
    console.log(`[/sync] 409 not_playing user=${user.id} code=${code} status=${lobby.status}`);
    return res.status(409).json({ error: 'not playing' });
  }
  const member = db.prepare('SELECT * FROM lobby_members WHERE lobby_code=? AND user_id=?').get(code, user.id);
  if (!member) {
    console.log(`[/sync] 403 not_in_lobby user=${user.id} code=${code}`);
    return res.status(403).json({ error: 'not in lobby' });
  }

  const { history } = req.body || {};
  if (!Array.isArray(history)) {
    return res.status(400).json({ error: 'bad history' });
  }
  const allRounds = lobby.rounds_json ? JSON.parse(lobby.rounds_json) : null;
  if (!allRounds) {
    return res.status(400).json({ error: 'no rounds set' });
  }

  const startingRoundsDone = member.rounds_done ?? 0;
  let roundsDone = startingRoundsDone;
  let totalMs = member.total_ms ?? 0;
  let lastTimeMs = member.finish_ms ?? null;
  const LOBBY_MIN_TIME_MS = 100;
  const newlyAppliedNumbers = []; // for /runs inserts at the end

  for (const entry of history) {
    if (!entry || typeof entry !== 'object') continue;
    const { round_index, time_ms, solution } = entry;
    if (!Number.isInteger(round_index)) continue;
    // Idempotent: skip rounds the server has already recorded.
    if (round_index <= roundsDone) continue;
    if (round_index !== roundsDone + 1) {
      console.log(`[/sync] 409 gap user=${user.id} code=${code} sent=${round_index} expected=${roundsDone + 1}`);
      return res.status(409).json({ error: 'unexpected round_index', expected: roundsDone + 1, sent: round_index });
    }
    if (round_index < 1 || round_index > allRounds.length) {
      return res.status(400).json({ error: 'bad round_index' });
    }
    if (!Number.isInteger(time_ms) || time_ms < LOBBY_MIN_TIME_MS || time_ms > MAX_TIME_MS) {
      return res.status(400).json({ error: 'bad time_ms' });
    }
    const numbers = allRounds[round_index - 1];
    if (!validateSolution(numbers, solution)) {
      console.log(`[/sync] 400 invalid_solution user=${user.id} code=${code} round=${round_index}`);
      return res.status(400).json({ error: 'invalid solution', round: round_index });
    }
    roundsDone = round_index;
    totalMs += time_ms;
    lastTimeMs = time_ms;
    newlyAppliedNumbers.push({ round_index, numbers, time_ms });
  }

  if (roundsDone > startingRoundsDone) {
    db.prepare(`UPDATE lobby_members
                   SET rounds_done=?, total_ms=?, finish_ms=?, finished_at=?
                 WHERE lobby_code=? AND user_id=?`)
      .run(roundsDone, totalMs, lastTimeMs, Date.now(), code, user.id);

    for (const r of newlyAppliedNumbers) {
      const runId = crypto.randomUUID();
      db.prepare(`INSERT INTO runs (id, user_id, puzzle_key, time_ms, client, created_at)
                  VALUES (?, ?, ?, ?, ?, ?)`)
        .run(runId, user.id, normalizeKey(r.numbers), r.time_ms, 'sync', Date.now());
    }

    // Match-completion check is identical to /finish's.
    const roundsTotal = lobby.rounds_total ?? 1;
    const notDoneCount = db.prepare(
      'SELECT COUNT(*) AS n FROM lobby_members WHERE lobby_code=? AND rounds_done < ?'
    ).get(code, roundsTotal).n;

    if (notDoneCount === 0) {
      db.prepare(`UPDATE lobbies SET status='done', round_index=?, updated_at=? WHERE code=?`)
        .run(roundsTotal, Date.now(), code);
    } else {
      const maxRow = db.prepare(
        'SELECT MAX(rounds_done) AS m FROM lobby_members WHERE lobby_code=?'
      ).get(code);
      db.prepare(`UPDATE lobbies SET round_index=?, updated_at=? WHERE code=?`)
        .run(maxRow.m ?? roundsDone, Date.now(), code);
    }
    console.log(`[/sync] 200 user=${user.id} code=${code} prev=${startingRoundsDone} now=${roundsDone}`);
  } else {
    console.log(`[/sync] 200 noop user=${user.id} code=${code} rounds_done=${roundsDone}`);
  }

  res.json({ lobby: lobbyView(code, user.id) });
});

// Monotonic progress beacon. Client posts the integers it cares about and
// the server takes max(stored, incoming). No solution validation, no
// per-round history, no ordering constraints — just a pair of counters
// that can only ever increase. This replaces /sync as the primary lobby
// state-replication path because /sync's validation pipeline was getting
// wedged when a single round's submission triggered a 400, blocking
// every subsequent round behind it. Progress can't be wedged that way:
// every successful call brings the server closer to the truth.
//
// Tradeoff: lobby rounds don't grant solo-leaderboard credit anymore.
// That responsibility now belongs to /runs (which still validates).
app.post('/lobby/:code/progress', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = db.prepare('SELECT * FROM lobbies WHERE code = ?').get(code);
  if (!lobby) {
    console.log(`[/progress] 404 lobby_not_found user=${user.id} code=${code}`);
    return res.status(404).json({ error: 'lobby not found' });
  }
  if (lobby.status === 'waiting') {
    console.log(`[/progress] 409 not_playing user=${user.id} code=${code}`);
    return res.status(409).json({ error: 'not playing' });
  }
  const member = db.prepare('SELECT * FROM lobby_members WHERE lobby_code=? AND user_id=?').get(code, user.id);
  if (!member) {
    console.log(`[/progress] 403 not_in_lobby user=${user.id} code=${code}`);
    return res.status(403).json({ error: 'not in lobby' });
  }

  const { rounds_done, total_ms, last_round_ms } = req.body || {};
  if (!Number.isInteger(rounds_done) || rounds_done < 0) {
    return res.status(400).json({ error: 'bad rounds_done' });
  }
  if (!Number.isInteger(total_ms) || total_ms < 0 || total_ms > MAX_TIME_MS * 50) {
    return res.status(400).json({ error: 'bad total_ms' });
  }

  const roundsTotal = lobby.rounds_total ?? 1;
  const cappedRoundsDone = Math.min(rounds_done, roundsTotal);
  const finalRoundsDone = Math.max(member.rounds_done ?? 0, cappedRoundsDone);
  const finalTotalMs = Math.max(member.total_ms ?? 0, total_ms);
  const finalFinishMs = Number.isInteger(last_round_ms) ? last_round_ms : member.finish_ms;
  const finishedAt = finalRoundsDone >= roundsTotal
    ? (member.finished_at || Date.now())
    : null;

  const advanced =
    finalRoundsDone > (member.rounds_done ?? 0) ||
    finalTotalMs > (member.total_ms ?? 0);

  if (advanced) {
    db.prepare(`UPDATE lobby_members
                   SET rounds_done=?, total_ms=?, finish_ms=?, finished_at=?
                 WHERE lobby_code=? AND user_id=?`)
      .run(finalRoundsDone, finalTotalMs, finalFinishMs, finishedAt, code, user.id);

    const notDoneCount = db.prepare(
      'SELECT COUNT(*) AS n FROM lobby_members WHERE lobby_code=? AND rounds_done < ?'
    ).get(code, roundsTotal).n;

    if (notDoneCount === 0) {
      db.prepare(`UPDATE lobbies SET status='done', round_index=?, updated_at=? WHERE code=?`)
        .run(roundsTotal, Date.now(), code);
    } else {
      const maxRow = db.prepare(
        'SELECT MAX(rounds_done) AS m FROM lobby_members WHERE lobby_code=?'
      ).get(code);
      db.prepare(`UPDATE lobbies SET round_index=?, updated_at=? WHERE code=?`)
        .run(maxRow.m ?? finalRoundsDone, Date.now(), code);
    }
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
  const lobby = db.prepare('SELECT * FROM lobbies WHERE code = ?').get(code);
  if (!lobby) return res.json({ ok: true });

  db.prepare('DELETE FROM lobby_members WHERE lobby_code=? AND user_id=?').run(code, user.id);

  const remaining = db.prepare('SELECT COUNT(*) AS n FROM lobby_members WHERE lobby_code=?').get(code).n;
  if (remaining === 0) {
    db.prepare('DELETE FROM lobbies WHERE code=?').run(code);
    lobbyKicks.delete(code);
  } else if (lobby.host_id === user.id) {
    // Transfer host to earliest remaining member
    const next = db.prepare(
      'SELECT user_id FROM lobby_members WHERE lobby_code=? ORDER BY joined_at ASC LIMIT 1'
    ).get(code);
    if (next) db.prepare('UPDATE lobbies SET host_id=?, updated_at=? WHERE code=?')
      .run(next.user_id, Date.now(), code);
  }
  res.json({ ok: true });
});

// Host-only. Removes a member from the lobby and records a kick event so
// other clients can show a toast.
app.post('/lobby/:code/kick', (req, res) => {
  const user = requireUser(req, res);
  if (!user) return;
  const code = String(req.params.code || '').toUpperCase();
  const lobby = db.prepare('SELECT * FROM lobbies WHERE code = ?').get(code);
  if (!lobby) return res.status(404).json({ error: 'lobby not found' });
  if (lobby.host_id !== user.id) return res.status(403).json({ error: 'host only' });

  const { user_id } = req.body || {};
  if (typeof user_id !== 'string' || !user_id)
    return res.status(400).json({ error: 'bad user_id' });
  if (user_id === user.id)
    return res.status(400).json({ error: 'cannot kick yourself' });

  const target = db.prepare(`
    SELECT m.user_id, u.display_name AS name
      FROM lobby_members m JOIN users u ON u.id = m.user_id
     WHERE m.lobby_code = ? AND m.user_id = ?
  `).get(code, user_id);
  if (!target) return res.status(404).json({ error: 'not a member' });

  db.prepare('DELETE FROM lobby_members WHERE lobby_code=? AND user_id=?').run(code, user_id);
  db.prepare('UPDATE lobbies SET updated_at=? WHERE code=?').run(Date.now(), code);

  lobbyKicks.set(code, { user_id: target.user_id, name: target.name, at: Date.now() });

  // If kicking happened mid-match, re-evaluate whether the match is now "done"
  // (everyone remaining has finished every round). Otherwise leave status alone.
  const currentLobby = db.prepare('SELECT * FROM lobbies WHERE code=?').get(code);
  if (currentLobby && currentLobby.status === 'playing') {
    const roundsTotal = currentLobby.rounds_total ?? 1;
    const counts = db.prepare(
      'SELECT COUNT(*) AS n, COALESCE(SUM(CASE WHEN rounds_done >= ? THEN 1 ELSE 0 END), 0) AS done FROM lobby_members WHERE lobby_code=?'
    ).get(roundsTotal, code);
    if (counts.n > 0 && counts.n === counts.done) {
      db.prepare(`UPDATE lobbies SET status='done', updated_at=? WHERE code=?`)
        .run(Date.now(), code);
    }
  }

  res.json({ lobby: lobbyView(code, user.id) });
});

app.listen(PORT, () => console.log(`make24 server listening on :${PORT}`));
