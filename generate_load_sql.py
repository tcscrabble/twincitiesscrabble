import json, hashlib, re, sys
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

def record_hash(rec: dict) -> str:
    blob = json.dumps(rec, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()

def main():
    if len(sys.argv) < 3:
        print("Usage: python generate_load_sql.py input.json output.sql")
        sys.exit(2)

    in_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    data = json.loads(in_path.read_text(encoding="utf-8"))
    games = data.get("games", [])
    wipe = bool(data.get("wipe", False))

    lines = []
    lines.append("PRAGMA foreign_keys = ON;")
    # lines.append("BEGIN;")

    if wipe:
        lines.append("DELETE FROM games;")
        lines.append("DELETE FROM players;")
        lines.append("DELETE FROM clubs;")

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
        p_score = int(g["player_score"])
        o_score = int(g["opponent_score"])

        spread = p_score - o_score
        result = "W" if spread > 0 else ("L" if spread < 0 else "T")

        club_key = location.upper()
        club_name = location

        p_key = player_key(p_name)
        o_key = player_key(o_name)

        raw_hash = record_hash({
            "session_date": session_date,
            "location": club_key,
            "round_number": rnd,
            "player_id": p_key,
            "opponent_id": o_key,
            "player_score": p_score,
            "opponent_score": o_score,
        })

        # Upsert club
        lines.append(
            f"INSERT INTO clubs (club_key, name) VALUES ({sql_quote(club_key)}, {sql_quote(club_name)}) "
            f"ON CONFLICT(club_key) DO UPDATE SET name=excluded.name;"
        )

        # Upsert players
        lines.append(
            f"INSERT INTO players (player_key, display_name) VALUES ({sql_quote(p_key)}, {sql_quote(p_name)}) "
            f"ON CONFLICT(player_key) DO UPDATE SET display_name=excluded.display_name;"
        )
        lines.append(
            f"INSERT INTO players (player_key, display_name) VALUES ({sql_quote(o_key)}, {sql_quote(o_name)}) "
            f"ON CONFLICT(player_key) DO UPDATE SET display_name=excluded.display_name;"
        )
        # Insert game (idempotent by raw_hash)
        # Note: we resolve ids via subqueries (fine for D1/SQLite; slower but simple).
        round_sql = "NULL" if rnd in (None, "") else str(int(rnd))
        lines.append(
            "INSERT INTO games (session_date, club_id, round_number, player_id, opponent_id, "
            "player_score, opponent_score, spread, result, raw_hash) VALUES ("
            f"{sql_quote(session_date)}, "
            f"(SELECT club_id FROM clubs WHERE club_key={sql_quote(club_key)}), "
            f"{round_sql}, "
            f"(SELECT player_id FROM players WHERE player_key={sql_quote(p_key)}), "
            f"(SELECT player_id FROM players WHERE player_key={sql_quote(o_key)}), "
            f"{p_score}, {o_score}, {spread}, {sql_quote(result)}, {sql_quote(raw_hash)}"
            ") "
            "ON CONFLICT(raw_hash) DO NOTHING;"
        )

    # lines.append("COMMIT;")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {out_path} with {len(games)} source game rows.")

if __name__ == "__main__":
    main()
