export const onRequestGet: PagesFunction<Env> = async ({ env }) => {
  const sql = `
    SELECT
  p.id,
  p.name,
  COUNT(g.id) AS games_played,
  SUM(
    CASE
      WHEN g.player1_id = p.id AND g.player1_score > g.player2_score THEN 1
      WHEN g.player2_id = p.id AND g.player2_score > g.player1_score THEN 1
      ELSE 0
    END
  ) AS wins,
  SUM(
    CASE
      WHEN g.player1_id = p.id AND g.player1_score < g.player2_score THEN 1
      WHEN g.player2_id = p.id AND g.player2_score < g.player1_score THEN 1
      ELSE 0
    END
  ) AS losses,
  SUM(
    CASE
      WHEN g.player1_id = p.id THEN g.player1_score
      WHEN g.player2_id = p.id THEN g.player2_score
      ELSE 0
    END
  ) AS total_points
FROM players p
LEFT JOIN games g
  ON p.id = g.player1_id OR p.id = g.player2_id
GROUP BY p.id
ORDER BY wins DESC, total_points DESC;

  const { results } = await env.DB.prepare(sql).all();

  return new Response(JSON.stringify(results, null, 2), {
    headers: { "content-type": "application/json" },
  });
};

interface Env {
  DB: D1Database;
}
