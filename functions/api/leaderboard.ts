export const onRequestGet: PagesFunction<Env> = async ({ request, env }) => {
  const url = new URL(request.url);
  const rawYear = url.searchParams.get("year");
  const year = rawYear ? Number(rawYear) : new Date().getFullYear();
  const rawClub = url.searchParams.get("club");
  const club = rawClub && rawClub !== "ALL" ? rawClub.toUpperCase() : null;

  if (!Number.isInteger(year) || year < 1900 || year > 3000) {
    return new Response(JSON.stringify({ error: "Invalid year" }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }

  const clubFilter = club ? `AND c.club_key = ?` : "";

  const sql = `
    WITH player_games AS (
      SELECT
        g.game_id,
        g.session_date,
        c.club_key,
        g.player_id AS player_id,
        g.player_score AS points_for,
        g.opponent_score AS points_against
      FROM games g
      JOIN clubs c ON c.club_id = g.club_id
      WHERE substr(g.session_date, 1, 4) = ?
        ${clubFilter}

      UNION ALL

      SELECT
        g.game_id,
        g.session_date,
        c.club_key,
        g.opponent_id AS player_id,
        g.opponent_score AS points_for,
        g.player_score AS points_against
      FROM games g
      JOIN clubs c ON c.club_id = g.club_id
      WHERE substr(g.session_date, 1, 4) = ?
        ${clubFilter}
    )
    SELECT
      p.player_id AS id,
      p.display_name AS name,
      COUNT(pg.game_id) AS games,
      COALESCE(SUM(CASE WHEN pg.points_for > pg.points_against THEN 1 ELSE 0 END), 0) AS wins,
      COALESCE(SUM(CASE WHEN pg.points_for < pg.points_against THEN 1 ELSE 0 END), 0) AS losses,
      COALESCE(SUM(CASE WHEN pg.points_for = pg.points_against THEN 1 ELSE 0 END), 0) AS ties,
      COALESCE(SUM(pg.points_for), 0) AS total_points,
      COALESCE(SUM(pg.points_for - pg.points_against), 0) AS spread
    FROM players p
    JOIN player_games pg ON pg.player_id = p.player_id
    WHERE p.is_placeholder_visitor = 0
    GROUP BY p.player_id, p.display_name
    HAVING games > 0
    ORDER BY
      CAST(wins AS REAL) / NULLIF(games, 0) DESC,
      games DESC,
      spread DESC,
      total_points DESC,
      name ASC;
  `;

  try {
    const stmt = club
      ? env.DB.prepare(sql).bind(String(year), club, String(year), club)
      : env.DB.prepare(sql).bind(String(year), String(year));

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
