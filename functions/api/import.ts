export const onRequestPost = async (context: any) => {
  const { request, env } = context;

  const expectedToken = env.IMPORT_TOKEN;
  const authHeader = request.headers.get("Authorization");

  if (!expectedToken || authHeader !== `Bearer ${expectedToken}`) {
    return new Response("Unauthorized", { status: 401 });
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return new Response("Invalid JSON", { status: 400 });
  }

  const { games } = body;

  if (!Array.isArray(games)) {
    return new Response("Missing games array", { status: 400 });
  }

  for (const game of games) {
    const {
      player1_name,
      player2_name,
      player1_score,
      player2_score,
      played_at
    } = game;

    if (
      !player1_name ||
      !player2_name ||
      player1_score == null ||
      player2_score == null
    ) {
      continue;
    }

    // Upsert players
    const p1 = await upsertPlayer(env, player1_name);
    const p2 = await upsertPlayer(env, player2_name);

    // Insert game
    await env.DB.prepare(`
      INSERT INTO games (
        player1_id,
        player2_id,
        player1_score,
        player2_score,
        created_at
      )
      VALUES (?, ?, ?, ?, ?)
    `)
      .bind(p1, p2, player1_score, player2_score, played_at || new Date().toISOString())
      .run();
  }

  return new Response(JSON.stringify({ success: true }), {
    headers: { "content-type": "application/json" }
  });
};

async function upsertPlayer(env: any, name: string) {
  const existing = await env.DB.prepare(
    "SELECT id FROM players WHERE name = ?"
  )
    .bind(name)
    .first();

  if (existing) return existing.id;

  const result = await env.DB.prepare(
    "INSERT INTO players (name) VALUES (?)"
  )
    .bind(name)
    .run();

  return result.meta.last_row_id;
}
