export const onRequestGet: PagesFunction<Env> = async ({ request, env }) => {
  const url = new URL(request.url);
  const rawYear = url.searchParams.get("year");
  const year = rawYear ? Number(rawYear) : new Date().getFullYear();
  const club = url.searchParams.get("club") ?? null; // e.g. "NM" or "DAY"

  if (!Number.isInteger(year) || year < 1900 || year > 3000) {
    return new Response(JSON.stringify({ error: "Invalid year" }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }

  // Club filter is optional — if omitted, returns all clubs combined
  const clubFilter = club ? `AND c.club_key = ?` : "";

  const sql = `
    SELECT
      p.player_id AS id,
      p.display_name AS name,
      COUNT(g.game_id) AS games,
      COALESCE(SUM(CASE WHEN g.result = 'W' THEN 1 ELSE 0 END), 0) AS wins,
      COALESCE(SUM(CASE WHEN g.result = 'L' THEN 1 ELSE 0 END), 0) AS losses,
      COALESCE(SUM(g.player_score), 0) AS total_points
    FROM players p
    LEFT JOIN games g
      ON g.player_id = p.player_id
      AND substr(g.session_date, 1, 4) = ?
    LEFT JOIN clubs c
      ON g.club_id = c.club_id
    ${clubFilter}
    GROUP BY p.player_id, p.display_name
    HAVING games > 0
    ORDER BY wins DESC, total_points DESC;
  `;

  try {
    const stmt = club
      ? env.DB.prepare(sql).bind(String(year), club)
      : env.DB.prepare(sql).bind(String(year));

    const { results } = await stmt.all();

    const rows = (results as any[]).map((r) => {
      const games = Number(r.games) || 0;
      const wins = Number(r.wins) || 0;
      const win_pct = games ? Math.round((wins / games) * 1000) / 10 : 0;
      return { ...r, win_pct };
    });

    return new Response(
      JSON.stringify({ year, club: club ?? "all", results: rows }),
      { headers: { "content-type": "application/json" } }
    );

  } catch (err: any) {
    return new Response(
      JSON.stringify({ error: "Failed to load leaderboard", message: err.message }),
      { status: 500, headers: { "content-type": "application/json" } }
    );
  }
};

interface Env {
  DB: D1Database;
}
