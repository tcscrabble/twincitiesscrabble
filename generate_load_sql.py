import argparse, json, hashlib, re, sys
from pathlib import Path

def norm_name(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"\s+", " ", name)
    return name

def player_key(display_name: str) -> str:
    # You can swap this later to prefer a 4-letter code when you add it.
    # For now, normalize to uppercase and collapse spaces.
    n = norm_name(display_name).upper()
    return n

def sql_quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"

PLAYER_RATING_FIELDS = [
    "naspa_name",
    "naspa_rating",
    "wgpo_name",
    "wgpo_url",
    "wgpo_nwl_rating",
    "wgpo_wow_rating",
    "cross_tables_url",
    "rating_notes",
]

INTEGER_PLAYER_RATING_FIELDS = {
    "naspa_rating",
    "wgpo_nwl_rating",
    "wgpo_wow_rating",
}

def sql_nullable_text(value) -> str:
    if value is None:
        return "NULL"
    value = norm_name(str(value))
    if value == "":
        return "NULL"
    return sql_quote(value)

def sql_nullable_int(value) -> str:
    if value is None:
        return "NULL"
    value = str(value).strip().replace(",", "")
    if value == "":
        return "NULL"
    try:
        return str(int(float(value)))
    except ValueError:
        return "NULL"

def sql_player_rating_value(field: str, value) -> str:
    if field in INTEGER_PLAYER_RATING_FIELDS:
        return sql_nullable_int(value)
    return sql_nullable_text(value)

RATING_PAYLOAD_INTEGER_FIELDS = {
    "naspa_rating",
    "wgpo_rating",
    "wgpo_wow_rating",
    "cross_tables_rating",
}

RATING_PAYLOAD_TEXT_FIELDS = {
    "naspa_url",
    "wgpo_url",
    "cross_tables_url",
    "rating_source_notes",
    "ratings_updated_at",
}

def rating_payload_value(field: str, value) -> str:
    if field in RATING_PAYLOAD_INTEGER_FIELDS:
        return sql_nullable_int(value)
    if field in RATING_PAYLOAD_TEXT_FIELDS:
        return sql_nullable_text(value)
    return "NULL"

GAME_VERIFICATION_STATUSES = {
    "VERIFIED",
    "ACCEPTED_MISMATCH",
    "UNRESOLVED_MISMATCH",
    "UNMATCHED",
}

def player_rating_upsert_sql(rating: dict) -> str | None:
    display_name = norm_name(str(rating.get("display_name") or ""))
    if not display_name:
        return None
    key = player_key(display_name)

    fields = [
        "naspa_rating",
        "wgpo_rating",
        "wgpo_wow_rating",
        "cross_tables_rating",
        "naspa_url",
        "wgpo_url",
        "cross_tables_url",
        "rating_source_notes",
        "ratings_updated_at",
    ]
    values = ",\n  ".join(rating_payload_value(field, rating.get(field)) for field in fields)
    assignments = ",\n  ".join(f"{field} = excluded.{field}" for field in fields)

    return f"""INSERT INTO player_ratings (
  player_id,
  naspa_rating,
  wgpo_rating,
  wgpo_wow_rating,
  cross_tables_rating,
  naspa_url,
  wgpo_url,
  cross_tables_url,
  rating_source_notes,
  ratings_updated_at
)
SELECT
  p.player_id,
  {values}
FROM players p
WHERE p.player_key = {sql_quote(key)}
ON CONFLICT(player_id) DO UPDATE SET
  {assignments};"""

def normalize_player_metadata(player: dict) -> dict | None:
    display_name = norm_name(str(player.get("display_name") or ""))
    if not display_name:
        return None

    normalized = {
        "player_key": norm_name(str(player.get("player_key") or player_key(display_name))).upper(),
        "display_name": display_name,
        "is_placeholder_visitor": int(player.get("is_placeholder_visitor", 0) or 0),
    }

    for field in PLAYER_RATING_FIELDS:
        normalized[field] = player.get(field)

    return normalized

def player_upsert_sql(display_name: str, is_placeholder: int = 0, metadata: dict | None = None) -> str:
    display_name = norm_name(display_name)
    key = player_key(display_name)

    if metadata is not None:
        display_name = norm_name(str(metadata.get("display_name") or display_name))
        key = norm_name(str(metadata.get("player_key") or player_key(display_name))).upper()
        is_placeholder = max(int(is_placeholder or 0), int(metadata.get("is_placeholder_visitor", 0) or 0))

    columns = ["player_key", "display_name", "is_placeholder_visitor"]
    values = [sql_quote(key), sql_quote(display_name), str(int(is_placeholder or 0))]
    assignments = [
        "display_name=excluded.display_name",
        "is_placeholder_visitor=MAX(players.is_placeholder_visitor, excluded.is_placeholder_visitor)",
    ]

    if metadata is not None:
        for field in PLAYER_RATING_FIELDS:
            columns.append(field)
            values.append(sql_player_rating_value(field, metadata.get(field)))
            assignments.append(f"{field}=excluded.{field}")

    return (
        f"INSERT INTO players ({', '.join(columns)}) "
        f"VALUES ({', '.join(values)}) "
        f"ON CONFLICT(player_key) DO UPDATE SET {', '.join(assignments)};"
    )

def record_hash(rec: dict) -> str:
    blob = json.dumps(rec, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()

def main():
    parser = argparse.ArgumentParser(description="Generate D1 load SQL for Twin Cities Scrabble.")
    parser.add_argument("input_json", type=Path)
    parser.add_argument("output_sql", type=Path)
    parser.add_argument("--ratings", type=Path, help="Optional ratings_payload.json from ratings_refresh.py")
    args = parser.parse_args()

    in_path = args.input_json
    out_path = args.output_sql

    data = json.loads(in_path.read_text(encoding="utf-8"))
    games = data.get("games", [])
    player_payload = data.get("players", [])
    wipe = bool(data.get("wipe", False))
    ratings_payload = {"ratings": []}
    if args.ratings and args.ratings.exists():
        ratings_payload = json.loads(args.ratings.read_text(encoding="utf-8"))

    lines = []
    lines.append("PRAGMA foreign_keys = ON;")
    # lines.append("BEGIN;")

    if wipe:
        lines.append("DELETE FROM games;")
        lines.append("DELETE FROM player_ratings;")
        lines.append("DELETE FROM players;")
        lines.append("DELETE FROM clubs;")

    player_metadata_by_key = {}
    for player in player_payload:
        metadata = normalize_player_metadata(player)
        if not metadata:
            continue
        player_metadata_by_key[metadata["player_key"]] = metadata
        lines.append(
            player_upsert_sql(
                metadata["display_name"],
                metadata.get("is_placeholder_visitor", 0),
                metadata,
            )
        )

    for rating in ratings_payload.get("ratings", []):
        sql = player_rating_upsert_sql(rating)
        if sql:
            lines.append(sql)

    # Ensure clubs/players exist, then insert games
    for g in games:
        # Basic validation (fail fast but safely)
        required = ["session_date", "location", "player_name", "opponent_name", "player_score", "opponent_score"]
        missing = [k for k in required if k not in g or g[k] in (None, "")]
        if missing:
            # Skip malformed row; you can change to raise if you prefer strictness
            continue

        session_date = str(g["session_date"])
        location = norm_name(str(g["location"]))
        rnd = g.get("round_number")
        p_name = norm_name(str(g["player_name"]))
        o_name = norm_name(str(g["opponent_name"]))
        try:
            p_score = int(g["player_score"])
            o_score = int(g["opponent_score"])
        except (TypeError, ValueError):
            continue

        p_is_placeholder = int(g.get("player_is_placeholder_visitor", 0) or 0)
        o_is_placeholder = int(g.get("opponent_is_placeholder_visitor", 0) or 0)
        visitor_note = g.get("visitor_note")
        verification_status = norm_name(str(g.get("verification_status") or "VERIFIED")).upper()
        if verification_status not in GAME_VERIFICATION_STATUSES:
            verification_status = "VERIFIED"
        mismatch_key = g.get("mismatch_key")
        mismatch_type = g.get("mismatch_type")

        spread = p_score - o_score
        result = "W" if spread > 0 else ("L" if spread < 0 else "T")

        club_key = location.upper()
        club_name = location

        p_key = player_key(p_name)
        o_key = player_key(o_name)
        p_metadata = player_metadata_by_key.get(p_key)
        o_metadata = player_metadata_by_key.get(o_key)

        # Canonical game identity: same real-world game gets same hash
        player_a, player_b = sorted([p_key, o_key])
        score_low, score_high = sorted([p_score, o_score])

        raw_hash = record_hash({
            "session_date": session_date,
            "location": club_key,
            "player_a": player_a,
            "player_b": player_b,
            "score_low": score_low,
            "score_high": score_high,
        })

        # Upsert club
        lines.append(
            f"INSERT INTO clubs (club_key, name) VALUES ({sql_quote(club_key)}, {sql_quote(club_name)}) "
            f"ON CONFLICT(club_key) DO UPDATE SET name=excluded.name;"
        )

        # Upsert players
        lines.append(player_upsert_sql(p_name, p_is_placeholder, p_metadata))
        lines.append(player_upsert_sql(o_name, o_is_placeholder, o_metadata))
        # Insert game (idempotent by raw_hash)
        # Note: we resolve ids via subqueries (fine for D1/SQLite; slower but simple).
        try:
            round_sql = "NULL" if rnd in (None, "") else str(int(rnd))
        except (TypeError, ValueError):
            round_sql = "NULL"
        visitor_note_sql = "NULL" if visitor_note in (None, "") else sql_quote(str(visitor_note))
        mismatch_key_sql = sql_nullable_text(mismatch_key)
        mismatch_type_sql = sql_nullable_text(mismatch_type)

        lines.append(
            "INSERT INTO games (session_date, club_id, round_number, player_id, opponent_id, "
            "player_score, opponent_score, spread, result, raw_hash, visitor_note, "
            "verification_status, mismatch_key, mismatch_type) VALUES ("
            f"{sql_quote(session_date)}, "
            f"(SELECT club_id FROM clubs WHERE club_key={sql_quote(club_key)}), "
            f"{round_sql}, "
            f"(SELECT player_id FROM players WHERE player_key={sql_quote(p_key)}), "
            f"(SELECT player_id FROM players WHERE player_key={sql_quote(o_key)}), "
            f"{p_score}, {o_score}, {spread}, {sql_quote(result)}, {sql_quote(raw_hash)}, {visitor_note_sql}, "
            f"{sql_quote(verification_status)}, {mismatch_key_sql}, {mismatch_type_sql}"
            ") "
            "ON CONFLICT(raw_hash) DO NOTHING;"
        )

    # lines.append("COMMIT;")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {out_path} with {len(games)} source game rows.")

if __name__ == "__main__":
    main()
