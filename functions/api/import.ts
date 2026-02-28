// functions/api/import.ts

type Env = {
  DB: D1Database;
  IMPORT_TOKEN: string;
};

type RawGame = {
  // Accept a few possible field names so the importer is resilient
  date?: string;            // "2026-02-12" or "2/12/2026" etc.
  session_date?: string;    // alternate
  location?: string;

  round_number?: number;    // optional; we'll generate if missing

  player?: string;          // player name (my name)
  opponent?: string;        // opponent name

  player_name?: string;     // alternate
  opponent_name?: string;   // alternate

  my_score?: number;        // alternate
  opp_score?: number;       // alternate
  player_score?: number;
  opponent_score?: number;

  // if you already have ids in the payload, we ignore and recompute server-side
};

type NormalizedGame = {
  session_date: string; // ISO yyyy-mm-dd
  location: string | null;
  round_number: number;

  player_name: string;
  opponent_name: string;

  player_score: number;
  opponent_score: number;
};

function jsonResponse(obj: any, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function badRequest(message: string, extra?: any) {
  return jsonResponse({ ok: false, error: message, ...extra }, 400);
}

function unauthorized() {
  return new Response("Unauthorized", { status: 401 });
}

function normalizeName(s: unknown): string {
  const v = String(s ?? "").trim();
  // Collapse internal whitespace
  return v.replace(/\s+/g, " ");
}

function parseScore(x: unknown): number | null {
  if (x === null || x === undefined || x === "") return null;
  const n = typeof x === "number" ? x : Number(String(x).trim());
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function toISODate(s: unknown): string | null {
  if (s === null || s === undefined) return null;
  const v = String(s).trim();
  if (!v) return null;

  // Already ISO yyyy-mm-dd?
  const iso = v.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (iso) return v;

  // Try mm/dd/yyyy (or m/d/yyyy)
  const mdy = v.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (mdy) {
    const mm = mdy[1].padStart(2, "0");
    const dd = mdy[2].padStart(2, "0");
    const yyyy = mdy[3];
    return `${yyyy}-${mm}-${dd}`;
  }

  // Last resort: Date.parse (can be timezone-y; still ok for dates)
  const t = Date.parse(v);
  if (!Number.isFinite(t)) return null;
  const d = new Date(t);
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function normalizeGames(rawGames: any[]): { games: NormalizedGame[]; warnings: string[] } {
  const warnings: string[] = [];
  const out: NormalizedGame[] = [];

  // If round_number missing, we assign sequential per (session_date, location) group
  const roundCounters = new Map<string, number>();

  for (let i = 0; i < rawGames.length; i++) {
    const g: RawGame = rawGames[i] ?? {};

    const session_date =
      toISODate(g.date) ?? toISODate(g.session_date);

    const location = g.location ? String(g.location).trim() : null;

    const player_name =
      normalizeName(g.player ?? g.player_name);

    const opponent_name =
      normalizeName(g.opponent ?? g.opponent_name);

    const player_score =
      parseScore(g.my_score ?? g.player_score);

    const opponent_score =
      parseScore(g.opp_score ?? g.opponent_score);

    // skip placeholder rows (you mentioned those exist)
    // If either score is missing, treat as placeholder and skip.
    if (player_score === null || opponent_score === null) {
      warnings.push(`Row ${i}: skipped (missing score)`);
      continue;
    }

    if (!session_date) {
      warnings.push(`Row ${i}: skipped (missing/invalid date)`);
      continue;
    }
    if (!player_name || !opponent_name) {
      warnings.push(`Row ${i}: skipped (missing player/opponent name)`);
      continue;
    }

    let round_number = Number.isFinite(g.round_number as any) ? Number(g.round_number) : NaN;
    if (!Number.isFinite(round_number) || round_number <= 0) {
      const key = `${session_date}||${location ?? ""}`;
      const next = (roundCounters.get(key) ?? 0) + 1;
      roundCounters.set(key, next);
      round_number = next;
    } else {
      round_number = Math.trunc(round_number);
    }

    out.push({
      session_date,
      location,
      round_number,
      player_name,
      opponent_name,
      player_score,
      opponent_score,
    });
  }

  return { games: out, warnings };
}

export const onRequestPost: PagesFunction<Env> = async (context) => {
  const { request, env } = context;

  // ---- Auth ----
  const expectedToken = env.IMPORT_TOKEN;
  const authHeader = request.headers.get("Authorization") || "";

  if (!expectedToken || authHeader !== `Bearer ${expectedToken}`) {
    return unauthorized();
  }

  // ---- Parse JSON ----
  let body: any;
  try {
    body = await request.json();
  } catch {
    return badRequest("Invalid JSON");
  }

  const rawGames = body?.games;
  if (!Array.isArray(rawGames)) {
    return badRequest("Body must be { games: [...] }");
  }

  const { games, warnings } = normalizeGames(rawGames);

  // Guardrail: refuse to wipe if nothing to import (optional but useful)
  if (games.length === 0) {
    return jsonResponse({
      ok: true,
      received: rawGames.length,
      normalized: 0,
      wiped: false,
      message: "No valid games after normalization; did not modify database.",
      warnings,
    });
  }

  const db = env.DB;

  // ---- Wipe + reinsert (NO SQL BEGIN/COMMIT) ----
  try {
    // Wipe in dependency order
    await db.prepare("DELETE FROM games").run();
    await db.prepare("DELETE FROM rounds").run();
    await db.prepare("DELETE FROM sessions").run();
    await db.prepare("DELETE FROM players").run();

    // Reset autoincrement counters (best-effort)
    try {
      await db
        .prepare("DELETE FROM sqlite_sequence WHERE name IN ('players','sessions','rounds','games')")
        .run();
    } catch {
      // ignore if sqlite_sequence doesn't exist / permissions etc.
    }

    // ---- Insert players (unique) ----
    const playerNameToId = new Map<string, number>();
    const allNames = new Set<string>();
    for (const g of games) {
      allNames.add(g.player_name);
      allNames.add(g.opponent_name);
    }

    // Stable insert order helps repeatability
    const sortedNames = Array.from(allNames).sort((a, b) => a.localeCompare(b));

    for (const name of sortedNames) {
      const res = await db
        .prepare("INSERT INTO players (name) VALUES (?)")
        .bind(name)
        .run();

      // D1 returns meta.last_row_id
      const id = (res as any).meta?.last_row_id as number | undefined;
      if (!id) throw new Error(`Failed inserting player: ${name}`);
      playerNameToId.set(name, id);
    }

    // ---- Insert sessions (unique by date+location) ----
    const sessionKeyToId = new Map<string, number>();
    const sessionKeys = new Set<string>();

    for (const g of games) {
      const key = `${g.session_date}||${g.location ?? ""}`;
      sessionKeys.add(key);
    }

    const sortedSessionKeys = Array.from(sessionKeys).sort((a, b) => a.localeCompare(b));

    for (const key of sortedSessionKeys) {
      const [session_date, locationRaw] = key.split("||");
      const location = locationRaw ? locationRaw : null;

      const res = await db
        .prepare("INSERT INTO sessions (session_date, location) VALUES (?, ?)")
        .bind(session_date, location)
        .run();

      const id = (res as any).meta?.last_row_id as number | undefined;
      if (!id) throw new Error(`Failed inserting session: ${key}`);
      sessionKeyToId.set(key, id);
    }

    // ---- Insert rounds (unique by session_id + round_number) ----
    const roundKeyToId = new Map<string, number>();
    const roundKeys = new Set<string>();

    for (const g of games) {
      const sessionKey = `${g.session_date}||${g.location ?? ""}`;
      const session_id = sessionKeyToId.get(sessionKey);
      if (!session_id) throw new Error(`Missing session_id for ${sessionKey}`);
      const rkey = `${session_id}||${g.round_number}`;
      roundKeys.add(rkey);
    }

    const sortedRoundKeys = Array.from(roundKeys).sort((a, b) => a.localeCompare(b));

    for (const rkey of sortedRoundKeys) {
      const [sessionIdStr, roundNumStr] = rkey.split("||");
      const session_id = Number(sessionIdStr);
      const round_number = Number(roundNumStr);

      const res = await db
        .prepare("INSERT INTO rounds (session_id, round_number) VALUES (?, ?)")
        .bind(session_id, round_number)
        .run();

      const id = (res as any).meta?.last_row_id as number | undefined;
      if (!id) throw new Error(`Failed inserting round: ${rkey}`);
      roundKeyToId.set(rkey, id);
    }

    // ---- Insert games ----
    let insertedGames = 0;

    for (const g of games) {
      const sessionKey = `${g.session_date}||${g.location ?? ""}`;
      const session_id = sessionKeyToId.get(sessionKey)!;
      const round_id = roundKeyToId.get(`${session_id}||${g.round_number}`)!;

      const player1_id = playerNameToId.get(g.player_name)!;
      const player2_id = playerNameToId.get(g.opponent_name)!;

      // Guard against self-play (bad input)
      if (player1_id === player2_id) {
        warnings.push(`Skipped game: ${g.player_name} vs itself on ${g.session_date}`);
        continue;
      }

      await db
        .prepare(
          `INSERT INTO games (round_id, player1_id, player2_id, player1_score, player2_score)
           VALUES (?, ?, ?, ?, ?)`
        )
        .bind(round_id, player1_id, player2_id, g.player_score, g.opponent_score)
        .run();

      insertedGames++;
    }

    return jsonResponse({
      ok: true,
      received: rawGames.length,
      normalized: games.length,
      wiped: true,
      inserted: {
        players: sortedNames.length,
        sessions: sortedSessionKeys.length,
        rounds: sortedRoundKeys.length,
        games: insertedGames,
      },
      warnings,
    });
  } catch (err: any) {
    return jsonResponse(
      { ok: false, error: String(err?.message ?? err) },
      500
    );
  }
};
