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

  const sql = `
    SELECT
      p.player_id AS id,
      p.display_name AS name,

      COUNT(g.game_id) AS games,

      COALESCE(SUM(
        CASE
          WHEN g.player_id = p.player_id AND g.player_score > g.opponent_score THEN 1
          WHEN g.opponent_id = p.player_id AND g.opponent_score > g.player_score THEN 1
          ELSE 0
        END
      ), 0) AS wins,

      COALESCE(SUM(
        CASE
          WHEN g.player_id = p.player_id AND g.player_score < g.opponent_score THEN 1
          WHEN g.opponent_id = p.player_id AND g.opponent_score < g.player_score THEN 1
          ELSE 0
        END
      ), 0) AS losses,

      COALESCE(SUM(
        CASE
          WHEN g.player_score = g.opponent_score THEN 1
          ELSE 0
        END
      ), 0) AS ties,

      COALESCE(SUM(
        CASE
          WHEN g.player_id = p.player_id THEN g.player_score
          ELSE g.opponent_score
        END
      ), 0) AS total_points

    FROM players p
    JOIN games g
      ON g.player_id = p.player_id
      OR g.opponent_id = p.player_id
    JOIN clubs c
      ON c.club_id = g.club_id

    WHERE
      substr(g.session_date, 1, 4) = ?
      AND (? IS NULL OR c.club_key = ?)

    GROUP BY p.player_id, p.display_name
    HAVING games > 0

    ORDER BY
      CAST(wins AS REAL) / games DESC,
      games DESC,
      wins DESC,
      total_points DESC,
      name ASC;
  `;

  try {
    const { results } = await env.DB.prepare(sql)
      .bind(String(year), club, club)
      .all();

    const rows = (results as any[]).map((r) => {
      const games = Number(r.games) || 0;
      const wins = Number(r.wins) || 0;
      const win_pct = games ? Math.round((wins / games) * 1000) / 10 : 0;

      return {
        ...r,
        games,
        wins,
        losses: Number(r.losses) || 0,
        ties: Number(r.ties) || 0,
        total_points: Number(r.total_points) || 0,
        win_pct,
      };
    });

    return new Response(
      JSON.stringify({ year, club: club ?? "all", results: rows }),
      { headers: { "content-type": "application/json" } }
    );
  } catch (err: any) {
    return new Response(
      JSON.stringify({
        error: "Failed to load leaderboard",
        message: err.message,
      }),
      { status: 500, headers: { "content-type": "application/json" } }
    );
  }
};

interface Env {
  DB: D1Database;
}
