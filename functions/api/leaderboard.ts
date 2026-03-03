export const onRequestGet: PagesFunction<Env> = async ({ env }) => {
  const sql = `
    SELECT
      p.player_id,
      p.name,
      COUNT(g.id) AS games,

      COALESCE(SUM(
        CASE
          WHEN g.player1_id = p.id AND g.player1_score > g.player2_score THEN 1
          WHEN g.player2_id = p.id AND g.player2_score > g.player1_score THEN 1
          ELSE 0
        END
      ), 0) AS wins,

      COALESCE(SUM(
        CASE
          WHEN g.player1_id = p.id AND g.player1_score < g.player2_score THEN 1
          WHEN g.player2_id = p.id AND g.player2_score < g.player1_score THEN 1
          ELSE 0
        END
      ), 0) AS losses,

      COALESCE(SUM(
        CASE
          WHEN g.player1_id = p.id THEN g.player1_score
          WHEN g.player2_id = p.id THEN g.player2_score
          ELSE 0
        END
      ), 0) AS total_points

    FROM players p
    LEFT JOIN games g
      ON p.id = g.player1_id
      OR p.id = g.player2_id

    GROUP BY p.player_id, p.name
    ORDER BY wins DESC, total_points DESC;
  `;

  const { results } = await env.DB.prepare(sql).all();

  const rows = results.map((r: any) => {
  const games = Number(r.games) || 0;
  const wins = Number(r.wins) || 0;
  const win_pct = games ? Math.round((wins / games) * 1000) / 10 : 0; // 1 decimal
  return { ...r, win_pct };
});

return new Response(JSON.stringify({ results: rows }, null, 2), {
  headers: { "content-type": "application/json" },
});

};

interface Env {
  DB: D1Database;
}
