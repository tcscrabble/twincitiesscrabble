export const onRequestGet: PagesFunction = async (context) => {
  const { env } = context;

  try {
    const check = await env.DB.prepare("SELECT 1 AS ok").first();

    return new Response(
      JSON.stringify({
        ok: true,
        source: "pages-functions",
        timestamp: new Date().toISOString()
      }),
      { headers: { "content-type": "application/json" } }
    );

  } catch (err: any) {
    return new Response(
      JSON.stringify({
        status: "error",
        database: "failed",
        message: err.message
      }),
      {
        status: 500,
        headers: { "content-type": "application/json" }
      }
    );
  }
};
