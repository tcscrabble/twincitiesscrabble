export const onRequestGet = async (context: any) => {
  const { request, env } = context;

  const url = new URL(request.url);
  const id = url.searchParams.get("id");

  if (!id) {
    return new Response(
      JSON.stringify({ error: "Missing id parameter" }),
      { status: 400, headers: { "content-type": "application/json" } }
    );
  }

  const player = await env.DB
    .prepare("SELECT id, name FROM players WHERE id = ?")
    .bind(id)
    .first();

  if (!player) {
    return new Response(
      JSON.stringify({ error: "Player not found" }),
      { status: 404, headers: { "content-type": "application/json" } }
    );
  }

  const games = await env.DB
    .prepare(`
      SELECT *
      FROM games
      WHERE player1_id = ? OR player2_id = ?
      ORDER BY id DESC
    `)
    .bind(id, id)
    .all();

  return new Response(
    JSON.stringify({ player, games: games.results }, null, 2),
    { headers: { "content-type": "application/json" } }
  );
};
