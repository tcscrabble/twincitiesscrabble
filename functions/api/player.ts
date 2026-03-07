export const onRequestGet: PagesFunction = async ({ request, env }) => {
  const url = new URL(request.url);
  const rawId = url.searchParams.get("id");
  const playerId = Number(rawId);

  if (!rawId || !Number.isInteger(playerId) || playerId <= 0) {
    return new Response(JSON.stringify({ error: "Invalid player id" }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }

  try {
    const player = await env.DB.prepare(
      `
      SELECT player_id AS id, display_name AS name
      FROM players
      WHERE player_id = ?
      `
    )
      .bind(playerId)
      .first();

    if (!player) {
      return new Response(JSON.stringify({ error: "Player not found" }), {
        status: 404,
        headers: { "content-type": "application/json" },
      });
    }

    const stats = await env.DB.prepare(
      `
      SELECT
        COUNT(*) AS games,
        SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses,
        SUM(CASE WHEN result = 'T' THEN 1 ELSE 0 END) AS ties,
        ROUND(
          100.0 * SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) / COUNT(*),
          1
        ) AS win_pct,
        SUM(spread) AS total_spread,
        ROUND(AVG(spread), 1) AS avg_spread,
        SUM(player_score) AS points_for,
        SUM(opponent_score) AS points_against,
        ROUND(AVG(player_score), 1) AS avg_score,
        ROUND(AVG(opponent_score), 1) AS avg_opp_score
      FROM games
      WHERE player_id = ?
      `
    )
      .bind(playerId)
      .first();

    const gamesResult = await env.DB.prepare(
      `
      SELECT
        g.game_id,
        g.round_number,
        g.session_date as session,
        g.player_score AS my_score,
        g.opponent_score AS opp_score,
        g.spread,
        o.player_id AS opponent_id,
        o.display_name AS opponent_name
      FROM games g
      JOIN players o
        ON g.opponent_id = o.player_id
      WHERE g.player_id = ?
      ORDER BY g.session_date DESC, g.round_number DESC
      `
    )
      .bind(playerId)
      .all();

    return new Response(
      JSON.stringify({
        player,
        stats,
        games: gamesResult.results ?? [],
      }),
      {
        headers: { "content-type": "application/json" },
      }
    );
  } catch (err: any) {
    return new Response(
      JSON.stringify({
        error: "Failed to load player",
        message: err.message,
      }),
      {
        status: 500,
        headers: { "content-type": "application/json" },
      }
    );
  }
};
