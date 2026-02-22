// functions/api/player.ts

export const onRequestGet = async (context: any) => {
  const { request, env } = context;

  const url = new URL(request.url);
  const idStr = url.searchParams.get("id");

  if (!idStr) {
    return new Response(JSON.stringify({ error: "Missing id parameter" }, null, 2), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }

  const playerId = Number(idStr);
  if (!Number.isFinite(playerId)) {
    return new Response(JSON.stringify({ error: "Invalid id parameter" }, null, 2), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }

  // Fetch the player
  const player = await env.DB.prepare("SELECT id, name FROM players WHERE id = ?")
    .bind(playerId)
    .first();

  if (!player) {
    return new Response(JSON.stringify({ error: "Player not found" }, null, 2), {
      status: 404,
      headers: { "content-type": "application/json" },
    });
  }

  // Fetch the games for that player (plus derived fields your UI likely wants)
  // NOTE: Adjust the aliases (my_score/opp_score/opponent_id/etc.) if your index.html expects different names.
  const gamesResult = await env.DB.prepare(`
    SELECT
      g.id,
      g.round_id,
      CASE
        WHEN g.player1_id = ? THEN g.player2_id
        ELSE g.player1_id
      END AS opponent_id,
      p.name AS opponent_name,
      CASE
        WHEN g.player1_id = ? THEN g.player1_score
        ELSE g.player2_score
      END AS my_score,
      CASE
        WHEN g.player1_id = ? THEN g.player2_score
        ELSE g.player1_score
      END AS opp_score,
      g.created_at
    FROM games g
    JOIN players p
      ON p.id = CASE
        WHEN g.player1_id = ? THEN g.player2_id
        ELSE g.player1_id
      END
    WHERE g.player1_id = ? OR g.player2_id = ?
    ORDER BY g.created_at DESC
  `)
    // 6 placeholders above
    .bind(playerId, playerId, playerId, playerId, playerId, playerId)
    .all();

  return new Response(
    JSON.stringify(
      {
        player,
        games: gamesResult.results,
      },
      null,
      2
    ),
    {
      headers: { "content-type": "application/json" },
    }
  );
};
