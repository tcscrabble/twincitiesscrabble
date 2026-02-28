export const onRequestPost = async (context: any) => {
  const { request, env } = context;

  // ---- Auth ----
  const expectedToken = env.IMPORT_TOKEN;
  const authHeader = request.headers.get("Authorization");

  if (!expectedToken || authHeader !== `Bearer ${expectedToken}`) {
    return new Response("Unauthorized", { status: 401 });
  }

  // ---- Parse body ----
  let body: any;
  try {
    body = await request.json();
  } catch {
    return new Response("Invalid JSON", { status: 400 });
  }

  const rawGames: any[] = Array.isArray(body?.games) ? body.games : [];
  if (!rawGames.length) {
    return new Response(JSON.stringify({ ok: true, received: 0, inserted: 0 }), {
      headers: { "content-type": "application/json" },
    });
  }

  // ---- Helpers ----
  const normName = (s: any) =>
    String(s ?? "")
      .trim()
      .replace(/\s+/g, " ");

  const normDate = (s: any) => {
    // Expecting yyyy-mm-dd or something Date() can parse
    const str = String(s ?? "").trim();
    // If already yyyy-mm-dd, keep it
    if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;
    const d = new Date(str);
    if (Number.isNaN(d.getTime())) return "";
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  };

  const toInt = (v: any) => {
    const n = Number(v);
    return Number.isFinite(n) ? Math.trunc(n) : NaN;
  };

  type CanonGame = {
    date: string;
    location: string;
    p1: string; // lexicographically smaller
    p2: string; // lexicographically larger
    s1: number; // score for p1
    s2: number; // score for p2
    game_no?: number; // optional ordering hint
  };

  // ---- Normalize + filter placeholders ----
  const candidates: CanonGame[] = [];
  for (const g of rawGames) {
    const date = normDate(g.date);
    const location = normName(g.location) || "Unknown";

    const player = normName(g.player);
    const opponent = normName(g.opponent);

    const myScore = toInt(g.my_score);
    const oppScore = toInt(g.opp_score);

    // Skip placeholders / incomplete rows
    if (!date || !player || !opponent) continue;
    if (!Number.isFinite(myScore) || !Number.isFinite(oppScore)) continue;

    const gameNo = Number.isFinite(toInt(g.game_no)) ? toInt(g.game_no) : undefined;

    // Canonicalize so the same game (listed twice) becomes identical:
    // p1/p2 are sorted by name; s1/s2 follow that ordering.
    let p1 = player;
    let p2 = opponent;
    let s1 = myScore;
    let s2 = oppScore;

    if (p2.localeCompare(p1) < 0) {
      // swap
      [p1, p2] = [p2, p1];
      [s1, s2] = [s2, s1];
    }

    candidates.push({ date, location, p1, p2, s1, s2, game_no: gameNo });
  }

  // ---- Deduplicate ----
  // Key includes date/location + ordered players + ordered scores (by ordered players).
  const seen = new Set<string>();
  const games: CanonGame[] = [];

  for (const g of candidates) {
    const key = `${g.date}|${g.location}|${g.p1}|${g.p2}|${g.s1}|${g.s2}`;
    if (seen.has(key)) continue;
    seen.add(key);
    games.push(g);
  }

  // Stable ordering: by date, location, then game_no (if present), then names/scores
  games.sort((a, b) => {
    if (a.date !== b.date) return a.date.localeCompare(b.date);
    if (a.location !== b.location) return a.location.localeCompare(b.location);

    const an = a.game_no ?? 1e9;
    const bn = b.game_no ?? 1e9;
    if (an !== bn) return an - bn;

    const p = a.p1.localeCompare(b.p1);
    if (p !== 0) return p;
    const q = a.p2.localeCompare(b.p2);
    if (q !== 0) return q;
    if (a.s1 !== b.s1) return a.s1 - b.s1;
    return a.s2 - b.s2;
  });

  // ---- Build unique players + sessions ----
  const playerNames = Array.from(
    new Set(games.flatMap((g) => [g.p1, g.p2]).filter(Boolean))
  ).sort((a, b) => a.localeCompare(b));

  const sessionKeys = Array.from(
    new Set(games.map((g) => `${g.date}|${g.location}`))
  ).sort((a, b) => a.localeCompare(b));

  // ---- DB ops (wipe + reinsert) ----
  // NOTE: Order matters due to FK constraints.
  const db = env.DB;

  // Best-effort transactional import
  try {
    await db.prepare("BEGIN").run();

    // Wipe existing data (reverse dependency order)
    await db.prepare("DELETE FROM games").run();
    await db.prepare("DELETE FROM rounds").run();
    await db.prepare("DELETE FROM sessions").run();
    await db.prepare("DELETE FROM players").run();

    // Optional: reset AUTOINCREMENT counters (safe to ignore if it errors)
    try {
      await db.prepare("DELETE FROM sqlite_sequence WHERE name IN ('players','sessions','rounds','games')").run();
    } catch {}

    // Insert players
    const playerIdByName = new Map<string, number>();

    for (const name of playerNames) {
      // SQLite supports RETURNING; D1 generally does too.
      let id: number | undefined;
      try {
        const res = await db
          .prepare("INSERT INTO players (name) VALUES (?) RETURNING id")
          .bind(name)
          .first();
        id = res?.id;
      } catch {
        // Fallback if RETURNING is not supported
        await db.prepare("INSERT INTO players (name) VALUES (?)").bind(name).run();
        const row = await db.prepare("SELECT id FROM players WHERE name = ?").bind(name).first();
        id = row?.id;
      }
      if (!id) throw new Error(`Failed to insert/find player id for: ${name}`);
      playerIdByName.set(name, id);
    }

    // Insert sessions
    const sessionIdByKey = new Map<string, number>();
    for (const key of sessionKeys) {
      const [date, location] = key.split("|");
      let id: number | undefined;
      try {
        const res = await db
          .prepare("INSERT INTO sessions (session_date, location) VALUES (?, ?) RETURNING id")
          .bind(date, location)
          .first();
        id = res?.id;
      } catch {
        await db
          .prepare("INSERT INTO sessions (session_date, location) VALUES (?, ?)")
          .bind(date, location)
          .run();
        const row = await db
          .prepare("SELECT id FROM sessions WHERE session_date = ? AND location = ?")
          .bind(date, location)
          .first();
        id = row?.id;
      }
      if (!id) throw new Error(`Failed to insert/find session id for: ${key}`);
      sessionIdByKey.set(key, id);
    }

    // Insert rounds + games
    // Current model: one game per round; round_number increments per session in sorted order.
    let roundCount = 0;
    let gameCount = 0;

    const roundNumberBySessionKey = new Map<string, number>();

    for (const g of games) {
      const sKey = `${g.date}|${g.location}`;
      const sessionId = sessionIdByKey.get(sKey);
      if (!sessionId) throw new Error(`Missing session for ${sKey}`);

      const nextRound = (roundNumberBySessionKey.get(sKey) ?? 0) + 1;
      roundNumberBySessionKey.set(sKey, nextRound);

      // Create round
      let roundId: number | undefined;
      try {
        const res = await db
          .prepare("INSERT INTO rounds (session_id, round_number) VALUES (?, ?) RETURNING id")
          .bind(sessionId, nextRound)
          .first();
        roundId = res?.id;
      } catch {
        await db
          .prepare("INSERT INTO rounds (session_id, round_number) VALUES (?, ?)")
          .bind(sessionId, nextRound)
          .run();
        const row = await db
          .prepare("SELECT id FROM rounds WHERE session_id = ? AND round_number = ?")
          .bind(sessionId, nextRound)
          .first();
        roundId = row?.id;
      }
      if (!roundId) throw new Error(`Failed to insert/find round id for session ${sKey} round ${nextRound}`);
      roundCount++;

      // Create game
      const p1Id = playerIdByName.get(g.p1);
      const p2Id = playerIdByName.get(g.p2);
      if (!p1Id || !p2Id) throw new Error(`Missing player id(s) for game: ${g.p1} vs ${g.p2}`);

      await db
        .prepare(
          `INSERT INTO games (round_id, player1_id, player2_id, player1_score, player2_score)
           VALUES (?, ?, ?, ?, ?)`
        )
        .bind(roundId, p1Id, p2Id, g.s1, g.s2)
        .run();
      gameCount++;
    }

    await db.prepare("COMMIT").run();

    return new Response(
      JSON.stringify({
        ok: true,
        received: rawGames.length,
        normalized: candidates.length,
        deduped: games.length,
        inserted: {
          players: playerNames.length,
          sessions: sessionKeys.length,
          rounds: roundCount,
          games: gameCount,
        },
      }),
      { headers: { "content-type": "application/json" } }
    );
  } catch (err: any) {
    try {
      await env.DB.prepare("ROLLBACK").run();
    } catch {}

    return new Response(
      JSON.stringify({
        ok: false,
        error: String(err?.message ?? err),
      }),
      { status: 500, headers: { "content-type": "application/json" } }
    );
  }
};
