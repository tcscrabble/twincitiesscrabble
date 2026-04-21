export const onRequestGet: PagesFunction = async ({ request, env }) => {
  const url = new URL(request.url);
  const rawId = url.searchParams.get("id");
  const playerId = Number(rawId);
  const rawClub = url.searchParams.get("club");
  const clubKey = rawClub && rawClub !== "ALL" ? rawClub.toUpperCase() : null;

  const rawYear = url.searchParams.get("year");
  const year = rawYear ? Number(rawYear) : new Date().getFullYear();

  if (!rawId || !Number.isInteger(playerId) || playerId <= 0) {
    return new Response(JSON.stringify({ error: "Invalid player id" }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }

  if (!Number.isInteger(year) || year < 1900 || year > 3000) {
    return new Response(JSON.stringify({ error: "Invalid year" }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }

  let player;
  let stats;
  let gamesResult;

  try {
    player = await env.DB.prepare(
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

    stats = await env.DB.prepare(
      `
      SELECT
        COUNT(*) AS games,
        SUM(
          CASE
            WHEN (g.player_id = ? AND g.player_score > g.opponent_score)
              OR (g.opponent_id = ? AND g.opponent_score > g.player_score)
            THEN 1 ELSE 0
          END
        ) AS wins,
        SUM(
          CASE
            WHEN (g.player_id = ? AND g.player_score < g.opponent_score)
              OR (g.opponent_id = ? AND g.opponent_score < g.player_score)
            THEN 1 ELSE 0
          END
        ) AS losses,
        SUM(
          CASE
            WHEN g.player_score = g.opponent_score
            THEN 1 ELSE 0
          END
        ) AS ties,
        SUM(
          CASE
            WHEN g.player_id = ?
            THEN g.player_score - g.opponent_score
            ELSE g.opponent_score - g.player_score
          END
        ) AS total_spread,
        AVG(
          CASE
            WHEN g.player_id = ?
            THEN g.player_score - g.opponent_score
            ELSE g.opponent_score - g.player_score
          END
        ) AS avg_spread,
        SUM(
          CASE
            WHEN g.player_id = ?
            THEN g.player_score
            ELSE g.opponent_score
          END
        ) AS points_for,
        SUM(
          CASE
            WHEN g.player_id = ?
            THEN g.opponent_score
            ELSE g.player_score
          END
        ) AS points_against
      FROM games g
      JOIN clubs c
        ON c.club_id = g.club_id
      WHERE
        (g.player_id = ? OR g.opponent_id = ?)
        AND substr(g.session_date, 1, 4) = ?
        AND (? IS NULL OR c.club_key = ?)
      `
    )
      .bind(
        playerId, // wins
        playerId,
        playerId, // losses
        playerId,
        playerId, // total_spread
        playerId, // avg_spread
        playerId, // points_for
        playerId, // points_against
        playerId, // where
        playerId,
        String(year),
        clubKey,
        clubKey
      )
      .first();

      gamesResult = await env.DB.prepare(
        `
        SELECT
          g.game_id,
          g.session_date,
          c.club_key,
          c.name AS club_name,
          g.round_number,
      
          CASE
            WHEN g.player_id = ? THEN opp.player_id
            ELSE p.player_id
          END AS opponent_id,
      
          CASE
            WHEN g.player_id = ? THEN opp.display_name
            ELSE p.display_name
          END AS opponent_name,
      
          CASE
            WHEN g.player_id = ? THEN g.player_score
            ELSE g.opponent_score
          END AS my_score,
      
          CASE
            WHEN g.player_id = ? THEN g.opponent_score
            ELSE g.player_score
          END AS opp_score
      
        FROM games g
        JOIN clubs c
          ON c.club_id = g.club_id
        JOIN players p
          ON p.player_id = g.player_id
        JOIN players opp
          ON opp.player_id = g.opponent_id
      
        WHERE
          (g.player_id = ? OR g.opponent_id = ?)
          AND substr(g.session_date, 1, 4) = ?
          AND (? IS NULL OR c.club_key = ?)
      
        ORDER BY g.session_date DESC, g.round_number DESC, g.game_id DESC
        `
      )
        .bind(
          playerId,
          playerId,
          playerId,
          playerId,
          playerId,
          playerId,
          String(year),
          clubKey,
          clubKey
        )
        .all();

    return new Response(
      JSON.stringify({
        player,
        year,
        stats,
        games: gamesResult?.results ?? [],
      }),
      {
        headers: { "content-type": "application/json" },
      }
    );
  } catch (err: any) {
    console.error("Player API error:", err);
    return new Response(
      JSON.stringify({
        error: "Player API error",
        detail: String(err),
        source: "NEW_PLAYER_TS",
      }),
      {
        status: 500,
        headers: { "content-type": "application/json" },
      }
    );
  }
};
