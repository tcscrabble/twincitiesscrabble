import sqlite3
import subprocess
import sys
import tempfile
import unittest
import json
from pathlib import Path

from generate_load_sql import canonical_game_identity, raw_game_hash
from make_import_payload import canonical_player_name, make_mismatch_key, validate_and_filter_games


ROOT = Path(__file__).resolve().parent


def game(player, opponent, player_score, opponent_score, club="NM", session_date="2026-07-09", row=1):
    return {
        "session_date": session_date,
        "location": club,
        "round_number": row,
        "player_name": player,
        "opponent_name": opponent,
        "player_score": player_score,
        "opponent_score": opponent_score,
        "player_is_placeholder_visitor": 0,
        "opponent_is_placeholder_visitor": 0,
        "opponent_is_known_cross_club_visitor": 0,
        "opponent_is_marked_visitor": 0,
        "opponent_is_session_visitor": 0,
        "source_row": row,
    }


def issue_types(issues):
    return [issue["type"] for issue in issues]


class ImportValidationTests(unittest.TestCase):
    def test_lisa_odom_variants_resolve_to_single_canonical_name(self):
        variants = ["LISA ODOM", "Lisa Odom", "Lisa O", "Lisa o"]

        self.assertEqual({"Lisa Odom"}, {canonical_player_name(name) for name in variants})

    def test_one_ordinary_game_reported_by_both_players_has_no_mismatch(self):
        rows = [
            game("Bill Bigler", "Dave Vonderhaar", 401, 395, row=1),
            game("Dave Vonderhaar", "Bill Bigler", 395, 401, row=2),
        ]

        loadable, issues = validate_and_filter_games(rows)

        self.assertEqual([], issues)
        self.assertEqual(1, len(loadable))
        self.assertEqual("VERIFIED", loadable[0]["verification_status"])

    def test_two_games_same_players_same_club_date_reported_by_both_players_has_no_mismatch(self):
        rows = [
            game("Bill Bigler", "Dave Vonderhaar", 401, 395, row=1),
            game("Bill Bigler", "Dave Vonderhaar", 372, 418, row=2),
            game("Dave Vonderhaar", "Bill Bigler", 395, 401, row=3),
            game("Dave Vonderhaar", "Bill Bigler", 418, 372, row=4),
        ]

        loadable, issues = validate_and_filter_games(rows)

        self.assertEqual([], issues)
        self.assertEqual(2, len(loadable))

    def test_partial_match_reports_exactly_one_extra_one_sided_report(self):
        rows = [
            game("Bill Bigler", "Dave Vonderhaar", 401, 395, row=1),
            game("Bill Bigler", "Dave Vonderhaar", 372, 418, row=2),
            game("Bill Bigler", "Dave Vonderhaar", 350, 340, row=3),
            game("Dave Vonderhaar", "Bill Bigler", 395, 401, row=4),
            game("Dave Vonderhaar", "Bill Bigler", 418, 372, row=5),
        ]

        loadable, issues = validate_and_filter_games(rows)

        self.assertEqual(["EXTRA_ONE_SIDED_REPORT"], issue_types(issues))
        self.assertEqual(3, len(loadable))
        self.assertEqual(350, issues[0]["game"]["player_score"])

    def test_same_players_same_date_different_clubs_are_evaluated_separately(self):
        rows = [
            game("Bill Bigler", "Dave Vonderhaar", 401, 395, club="NM", row=1),
            game("Dave Vonderhaar", "Bill Bigler", 395, 401, club="NM", row=2),
            game("Bill Bigler", "Dave Vonderhaar", 372, 418, club="DAY", row=3),
            game("Dave Vonderhaar", "Bill Bigler", 418, 372, club="DAY", row=4),
        ]

        loadable, issues = validate_and_filter_games(rows)

        self.assertEqual([], issues)
        self.assertEqual(2, len(loadable))

    def test_duplicate_identical_score_lines_reported_twice_by_both_players_have_no_mismatch(self):
        rows = [
            game("Bill Bigler", "Dave Vonderhaar", 401, 395, row=1),
            game("Bill Bigler", "Dave Vonderhaar", 401, 395, row=2),
            game("Dave Vonderhaar", "Bill Bigler", 395, 401, row=3),
            game("Dave Vonderhaar", "Bill Bigler", 395, 401, row=4),
        ]

        loadable, issues = validate_and_filter_games(rows)

        self.assertEqual([], issues)
        self.assertEqual(2, len(loadable))

    def test_duplicate_identical_score_lines_with_one_missing_opponent_report_has_one_unmatched_occurrence(self):
        rows = [
            game("Bill Bigler", "Dave Vonderhaar", 401, 395, row=1),
            game("Bill Bigler", "Dave Vonderhaar", 401, 395, row=2),
            game("Dave Vonderhaar", "Bill Bigler", 395, 401, row=3),
        ]

        loadable, issues = validate_and_filter_games(rows)

        self.assertEqual(["EXTRA_ONE_SIDED_REPORT"], issue_types(issues))
        self.assertEqual(2, len(loadable))

    def test_accepted_mismatch_suppression_still_marks_matching_issue_accepted(self):
        rows = [
            game("Bill Bigler", "Dave Vonderhaar", 401, 395, row=1),
            game("Bill Bigler", "Dave Vonderhaar", 401, 395, row=2),
            game("Dave Vonderhaar", "Bill Bigler", 395, 401, row=3),
        ]
        accepted_key = make_mismatch_key("EXTRA_ONE_SIDED_REPORT", rows[1])

        loadable, issues = validate_and_filter_games(rows, {accepted_key})

        self.assertEqual(["EXTRA_ONE_SIDED_REPORT"], issue_types(issues))
        self.assertEqual(accepted_key, issues[0]["mismatch_key"])
        self.assertEqual("ACCEPTED_MISMATCH", issues[0]["game"]["verification_status"])
        self.assertIn(issues[0]["game"], loadable)


class RawHashTests(unittest.TestCase):
    def test_hash_preserves_score_orientation_by_canonical_player(self):
        bill_win = canonical_game_identity(
            "2026-07-09",
            "NM",
            "BILL BIGLER",
            "DAVE VONDERHAAR",
            401,
            395,
        )
        dave_win = canonical_game_identity(
            "2026-07-09",
            "NM",
            "BILL BIGLER",
            "DAVE VONDERHAAR",
            395,
            401,
        )

        self.assertNotEqual(raw_game_hash(bill_win), raw_game_hash(dave_win))

    def test_generated_sql_keeps_repeated_identical_games_and_remains_idempotent(self):
        payload = {
            "wipe": False,
            "players": [],
            "games": [
                game("Bill Bigler", "Dave Vonderhaar", 401, 395, row=1),
                game("Bill Bigler", "Dave Vonderhaar", 401, 395, row=2),
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            payload_path = tmp_path / "payload.json"
            sql_path = tmp_path / "load.sql"
            db_path = tmp_path / "test.db"
            payload_path.write_text(json.dumps(payload), encoding="utf-8")

            subprocess.run(
                [sys.executable, str(ROOT / "generate_load_sql.py"), str(payload_path), str(sql_path)],
                check=True,
                cwd=ROOT,
            )

            con = sqlite3.connect(db_path)
            try:
                con.executescript((ROOT / "schema.sql").read_text(encoding="utf-8"))
                load_sql = sql_path.read_text(encoding="utf-8")
                con.executescript(load_sql)
                con.executescript(load_sql)
                game_count = con.execute("SELECT COUNT(*) FROM games").fetchone()[0]
                hash_count = con.execute("SELECT COUNT(DISTINCT raw_hash) FROM games").fetchone()[0]
            finally:
                con.close()

        self.assertEqual(2, game_count)
        self.assertEqual(2, hash_count)


if __name__ == "__main__":
    unittest.main()
