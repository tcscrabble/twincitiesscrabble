export const onRequestGet = async () => {
  return new Response("Use POST /api/import", {
    status: 405,
    headers: { "content-type": "text/plain; charset=utf-8" },
  });
};

export const onRequestPost = async (context: any) => {
  const { request, env } = context;

  const expectedToken = env.IMPORT_TOKEN;
  const authHeader = request.headers.get("Authorization");

  if (!expectedToken || authHeader !== `Bearer ${expectedToken}`) {
    return new Response("Unauthorized", { status: 401 });
  }

  let body: any;
  try {
    body = await request.json();
  } catch {
    return new Response("Invalid JSON", { status: 400 });
  }

  const { games } = body;
  // TODO: insert into D1 here

  return new Response(JSON.stringify({ ok: true, received: Array.isArray(games) ? games.length : 0 }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
};
