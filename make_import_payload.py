import argparse
import csv
import json
import re
import time
from datetime import datetime, date, timedelta
from typing import Optional, Tuple, List, Dict, Any, Set
from pathlib import Path
import shutil

def excel_col_to_idx(col: str) -> int:
    """Convert Excel-like column letters (A, Z, AA, AE, etc.) to 0-based index."""
    col = col.strip().upper()
    n = 0
    for ch in col:
        if not ("A" <= ch <= "Z"):
            raise ValueError(f"Bad column: {col}")
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1

def cell(row, idx):
    if idx is None:
        return ""
    if idx < 0 or idx >= len(row):
        return ""
    return (row[idx] or "").strip()

CODE_RE = re.compile(r"^[A-Z]{4}$")
_DATE_YYYY_MM_DD = re.compile(r"^\d{4}-\d{1,2}-\d{1,2}$")
_DATE_M_D_YYYY   = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
_DATE_DD_MON_YYYY = re.compile(r"^\d{1,2}-[A-Za-z]{3}-\d{4}$")  # e.g. 12-Feb-2026

EXPECTED_WEEKDAY_BY_CLUB = {
    "NM": 3,   # Thursday; Monday=0
    "DAY": 0,  # Monday
}

def expected_weekday_warning(club: str, d: date, player_name: str) -> Optional[str]:
    club = (club or "").upper()
    expected = EXPECTED_WEEKDAY_BY_CLUB.get(club)
    if expected is None:
        return None

    if d.weekday() != expected:
        expected_name = "Thursday" if expected == 3 else "Monday"
        actual_name = d.strftime("%A")
        return (
            f"{club}: {player_name} has date {d.strftime('%Y-%m-%d')} "
            f"({actual_name}); expected {expected_name}. "
            f"If this was a rescheduled session, confirm opponent entries use the same date."
        )

    return None

def parse_date(s: str):
    if len(s) > 32 or s.lower() in {"date", "session_date", "session date"}:
        print("parse_date suspicious:", repr(s)[:120])
    """
    Return datetime.date or None.
    Must NEVER hang.
    """
    if not s:
        return None

    # normalize & guard against giant garbage strings
    s = str(s).strip()
    if not s:
        return None

    # If the CSV got corrupted, "date" cells can become huge blobs.
    # Bail fast.
    if len(s) > 64:
        return None

    lower = s.lower()
    if lower in {"date", "session_date", "session date"}:
        return None

    # Try known formats (3 only; fast; avoids regex hangs entirely)
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d-%b-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
        except Exception:
            return None

    return None

def parse_int(s: str):
    s = (s or "").strip()
    if s == "":
        return None
    # Remove commas
    s = s.replace(",", "")
    try:
        return int(float(s))
    except ValueError:
        return None

def make_short_name(full_name: str) -> Optional[str]:
    # "Peter Haugan" -> "Peter H"
    parts = [p for p in full_name.strip().split() if p]
    if len(parts) < 2:
        return None
    first = parts[0]
    last = parts[-1]
    if not last:
        return None
    return f"{first} {last[0]}"

def looks_like_full_name(s: str) -> bool:
    # Loose heuristic: contains a space and at least 2 alpha chars
    s = s.strip()
    if " " not in s:
        return False
    return sum(ch.isalpha() for ch in s) >= 2

def _norm(x) -> str:
    if x is None:
        return ""

    s = str(x)

    # HARD GUARD: if a cell is huge, do NOT try to split/regex it.
    # This prevents hangs when the CSV is malformed and a whole file chunk lands in one cell.
    MAX_LEN = 5000
    if len(s) > MAX_LEN:
        # Keep a debug breadcrumb so we can find the offending row later
        print(f"[WARN] huge cell len={len(s)}; truncating to {MAX_LEN}. head={s[:80]!r}")
        return s[:MAX_LEN].strip()

    # Normal case: cheap whitespace normalization
    return " ".join(s.strip().split())

def is_block_code(s: str) -> bool:
    s = (s or "").strip().upper()
    return bool(CODE_RE.match(s))

def looks_like_name_piece(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return False
    if any(ch.isdigit() for ch in s):
        return False
    if "/" in s or "time" in s.lower() or "games" in s.lower() or "confirmed" in s.lower():
        return False
    letters = sum(ch.isalpha() for ch in s)
    return letters >= 2

def assemble_name(a: str, b: str) -> str:
    a = (a or "").strip()
    b = (b or "").strip()
    if a and b:
        return f"{a} {b}".strip()
    return (a or b).strip()

def looks_like_full_name(s: str) -> bool:
    s = _norm(s)
    parts = [p for p in s.split(" ") if p]
    # require at least 2 tokens, and at least one letter in each
    return len(parts) >= 2 and all(any(ch.isalpha() for ch in p) for p in parts)

def full_to_short(full_name: str) -> Optional[str]:
    """
    'Zachary Kent' -> 'Zachary K'
    """
    full_name = _norm(full_name)
    parts = [p for p in full_name.split(" ") if p]
    if len(parts) < 2:
        return None
    first = parts[0]
    last = parts[-1]
    if not any(ch.isalpha() for ch in first):
        return None
    # last initial
    last_initial = next((ch for ch in last if ch.isalpha()), None)
    if not last_initial:
        return None
    return f"{first.capitalize()} {last_initial.upper()}"

def opponent_short_norm(s: str) -> str:
    """
    Normalize opponent short name like:
    'zach k' / 'Zach K.' / 'Zach   k' -> 'Zach K'
    """
    s = _norm(s).replace(".", "")
    parts = [p for p in s.split(" ") if p]
    if len(parts) == 2 and len(parts[1]) == 1:
        return f"{parts[0].capitalize()} {parts[1].upper()}"
    return s

def detect_player_block_name(rows, i, player_idx, cell):
    """
    Returns (full_name_or_None, rows_to_advance).

    Recognizes these patterns in the player block column:

    1. CODE
       Full Name

    2. CODE
       First
       Last

    3. Full Name
       CODE

    4. First
       Last
       CODE

    5. First
       CODE
       Last
    """
    w0 = _norm(cell(rows[i], player_idx)) if i < len(rows) else ""
    w1 = _norm(cell(rows[i + 1], player_idx)) if i + 1 < len(rows) else ""
    w2 = _norm(cell(rows[i + 2], player_idx)) if i + 2 < len(rows) else ""

    def valid_name(s: str) -> bool:
        return bool(s) and (looks_like_full_name(s) or looks_like_name_piece(s))

    # Pattern 1: CODE / Full Name
    if is_block_code(w0) and looks_like_full_name(w1):
        return w1, 2

    # Pattern 2: CODE / First / Last
    if is_block_code(w0) and looks_like_name_piece(w1) and looks_like_name_piece(w2):
        full = assemble_name(w1, w2)
        if looks_like_full_name(full):
            return full, 3

    # Pattern 3: Full Name / CODE
    if looks_like_full_name(w0) and is_block_code(w1):
        return w0, 2

    # Pattern 4: First / Last / CODE
    if looks_like_name_piece(w0) and looks_like_name_piece(w1) and is_block_code(w2):
        full = assemble_name(w0, w1)
        if looks_like_full_name(full):
            return full, 3

    # Pattern 5: First / CODE / Last
    if looks_like_name_piece(w0) and is_block_code(w1) and looks_like_name_piece(w2):
        full = assemble_name(w0, w2)
        if looks_like_full_name(full):
            return full, 3

    return None, 1

VISITOR_RE = re.compile(r"\s*[\(\[]\s*visitor\s*[\)\]]\s*$", re.IGNORECASE)

def split_visitor_marker(name: str) -> tuple[str, bool]:
    name = _norm(name)
    marked = bool(VISITOR_RE.search(name))
    clean = VISITOR_RE.sub("", name).strip()
    return clean, marked

def make_placeholder_visitor_name(name: str) -> str:
    name = _norm(name)
    if name.lower().endswith("(visitor)"):
        return name
    return f"{name} (visitor)"

# Manual aliases that should always resolve to a canonical player name.
# Add to this list when a spreadsheet uses an abbreviation that cannot be
# generated automatically from the full player name.
CANONICAL_NAME_MAP = {
    "Vince V": "Vincent VanDover",
    "Manon StA": "Manon St. Amant",
    "Manon S": "Manon St. Amant",
    "Manon St Amant": "Manon St. Amant",
    "Bil B": "Bill Bigler",
    "Bill B": "Bill Bigler",
    "Jason V": "Jason Vaysberg",
    "Lisa O": "Lisa Odom",
}

# Known players who should be treated as session visitors even when the
# spreadsheet opponent cell does not include an explicit "(visitor)" marker.
SESSION_VISITOR_OVERRIDES = {
    ("DAY", "2026-01-05", "BILL BIGLER"),
    ("DAY", "2026-01-05", "MANON ST. AMANT"),
    ("NM", "2026-04-16", "MEGAN O'CONNELL"),
}

def is_session_visitor_override(location: str, session_date: str, player_name: str) -> bool:
    key = (
        _norm(location).upper(),
        _norm(session_date),
        _norm(player_name).upper(),
    )
    return key in SESSION_VISITOR_OVERRIDES

PLAYER_RATING_FIELDS = [
    "naspa_name",
    "naspa_rating",
    "wgpo_name",
    "wgpo_nwl_rating",
    "wgpo_wow_rating",
    "rating_notes",
]

INTEGER_PLAYER_RATING_FIELDS = {
    "naspa_rating",
    "wgpo_nwl_rating",
    "wgpo_wow_rating",
}

def player_key_for_payload(display_name: str) -> str:
    return _norm(display_name).upper()

def parse_optional_rating_int(value):
    value = _norm(value).replace(",", "")
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None

def parse_optional_rating_text(value):
    value = _norm(value)
    return value or None

def canonical_player_name(name: str) -> str:
    name = _norm(name)
    return _norm(CANONICAL_NAME_MAP.get(name, name))

def read_player_ratings(
    ratings_path: str,
    canonical_player_names: List[str],
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    path = Path(ratings_path)
    warnings: List[str] = []
    ratings_by_display_name: Dict[str, Dict[str, Any]] = {}

    if not path.exists():
        warnings.append(f"player ratings file not found: {ratings_path}")
        return ratings_by_display_name, warnings

    canonical_by_key = {
        player_key_for_payload(canonical_player_name(name)): canonical_player_name(name)
        for name in canonical_player_names
        if canonical_player_name(name)
    }

    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "display_name" not in reader.fieldnames:
            warnings.append(f"{ratings_path}: missing required display_name header")
            return ratings_by_display_name, warnings

        for row_number, row in enumerate(reader, start=2):
            display_name = canonical_player_name(row.get("display_name") or "")
            if not display_name:
                continue

            canonical_name = canonical_by_key.get(player_key_for_payload(display_name))
            if not canonical_name:
                warnings.append(
                    f"{ratings_path}: row {row_number} skipped; "
                    f"display_name {display_name!r} is not in the imported player roster"
                )
                continue

            if canonical_name in ratings_by_display_name:
                warnings.append(
                    f"{ratings_path}: row {row_number} duplicates {canonical_name!r}; "
                    "using the later row"
                )

            ratings_by_display_name[canonical_name] = {
                "naspa_name": parse_optional_rating_text(row.get("naspa_name")),
                "naspa_rating": parse_optional_rating_int(row.get("naspa_rating")),
                "wgpo_name": parse_optional_rating_text(row.get("wgpo_name")),
                "wgpo_nwl_rating": parse_optional_rating_int(row.get("wgpo_nwl_rating")),
                "wgpo_wow_rating": parse_optional_rating_int(row.get("wgpo_wow_rating")),
                "rating_notes": parse_optional_rating_text(
                    row.get("rating_notes") if row.get("rating_notes") is not None else row.get("notes")
                ),
            }

    return ratings_by_display_name, warnings

def build_player_payload(
    canonical_player_names: List[str],
    games: List[Dict[str, Any]],
    ratings_by_display_name: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    players_by_key: Dict[str, Dict[str, Any]] = {}

    def add_player(display_name: str, is_placeholder_visitor: int = 0):
        display_name = canonical_player_name(display_name)
        if not display_name:
            return

        key = player_key_for_payload(display_name)
        player = players_by_key.setdefault(
            key,
            {
                "player_key": key,
                "display_name": display_name,
                "is_placeholder_visitor": 0,
            },
        )
        player["is_placeholder_visitor"] = max(
            int(player.get("is_placeholder_visitor", 0) or 0),
            int(is_placeholder_visitor or 0),
        )

    for name in canonical_player_names:
        add_player(name)

    for g in games:
        add_player(g.get("player_name"), g.get("player_is_placeholder_visitor", 0))
        add_player(g.get("opponent_name"), g.get("opponent_is_placeholder_visitor", 0))

    players = sorted(players_by_key.values(), key=lambda p: p["display_name"].upper())

    for player in players:
        ratings = ratings_by_display_name.get(player["display_name"], {})
        for field in PLAYER_RATING_FIELDS:
            player[field] = ratings.get(field)

    return players

def read_csv_rows(csv_path: str) -> List[List[str]]:
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        return list(csv.reader(f))

def resolve_column_indices(
    rows: List[List[str]],
    mode: str,
    date_col: str,
    player_block_col: str,
    opp_col: str,
    player_score_col: str,
    opp_score_col: str,
    round_col: Optional[str] = None,
    skip_rows_before: int = 0,
) -> Tuple[int, int, int, int, int, Optional[int], int]:
    if mode == "letters":
        date_idx = excel_col_to_idx(date_col)
        player_idx = excel_col_to_idx(player_block_col)
        opp_idx = excel_col_to_idx(opp_col)
        ps_idx = excel_col_to_idx(player_score_col)
        os_idx = excel_col_to_idx(opp_score_col)
        round_idx = excel_col_to_idx(round_col) if round_col else None
        start_row = skip_rows_before
    elif mode == "header":
        if not rows:
            raise ValueError("Cannot use header mode with an empty CSV")
        header = rows[0]

        def find_idx(name: str) -> int:
            try:
                return header.index(name)
            except ValueError:
                raise ValueError(f"Header not found: {name}")

        date_idx = find_idx(date_col)
        player_idx = find_idx(player_block_col)
        opp_idx = find_idx(opp_col)
        ps_idx = find_idx(player_score_col)
        os_idx = find_idx(opp_score_col)
        round_idx = find_idx(round_col) if round_col else None
        start_row = 1 + skip_rows_before
    else:
        raise ValueError("--mode must be 'header' or 'letters'")

    return date_idx, player_idx, opp_idx, ps_idx, os_idx, round_idx, start_row

def collect_full_names_from_rows(rows: List[List[str]], player_idx: int, start_row: int) -> List[str]:
    full_names: List[str] = []
    seen_full = set()

    i = start_row
    while i < len(rows):
        full_name, advance = detect_player_block_name(rows, i, player_idx, cell)
        if full_name:
            full_name = _norm(full_name)
            if full_name and full_name not in seen_full:
                seen_full.add(full_name)
                full_names.append(full_name)
        i += max(advance, 1)

    return full_names

def build_short_to_full(full_names: List[str]) -> Tuple[Dict[str, str], Set[str]]:
    from collections import defaultdict

    short_candidates = defaultdict(list)

    canonical_full_names = list(full_names)
    for full in CANONICAL_NAME_MAP.values():
        full = _norm(full)
        if full and full not in canonical_full_names:
            canonical_full_names.append(full)

    for fn in canonical_full_names:
        short = full_to_short(fn)
        if short:
            short_candidates[short].append(fn)

    short_to_full: Dict[str, str] = {}
    collisions: Set[str] = set()

    for short, names in short_candidates.items():
        uniq = sorted(set(names))
        if len(uniq) == 1:
            short_to_full[short] = uniq[0]
        else:
            collisions.add(short)

    for alias, full in CANONICAL_NAME_MAP.items():
        alias_key = opponent_short_norm(alias)
        full = _norm(full)
        if alias_key:
            short_to_full[alias_key] = full

    return short_to_full, collisions

def parse_club_args(club_args: Optional[List[str]]) -> List[Dict[str, str]]:
    clubs = []
    for arg in club_args or []:
        if "=" not in arg:
            raise ValueError(f"Bad --club value {arg!r}; use KEY=filename.csv")
        key, path = arg.split("=", 1)
        key = _norm(key).upper()
        path = path.strip().strip('"')
        if not key or not path:
            raise ValueError(f"Bad --club value {arg!r}; use KEY=filename.csv")
        clubs.append({"club": key, "path": path})
    return clubs

def build_games_from_csv(
    csv_path: str,
    mode: str,
    date_col: str,
    player_block_col: str,
    opp_col: str,
    player_score_col: str,
    opp_score_col: str,
    round_col: Optional[str] = None,
    location: Optional[str] = None,
    skip_rows_before: int = 0,
    global_full_names: Optional[List[str]] = None,
    global_short_to_full: Optional[Dict[str, str]] = None,
    global_collisions: Optional[Set[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[str], List[str], Set[str]]:
    """
    Reads your Scrabble export where:
      - Column W contains either:
          * a 4-letter block code (first 2 of first + first 2 of last), then
          * next row contains the full player name
          * subsequent rows contain games and notes
      - Column AE contains the date (Excel/Sheets date or string date)
      - Column AH contains opponent short name like 'Zach K'
      - Column AF contains player score
      - Column AI contains opponent score
    """

    rows = read_csv_rows(csv_path)

    if not rows:
        return [], ["Empty CSV"], [], set()

    date_idx, player_idx, opp_idx, ps_idx, os_idx, round_idx, start_row = resolve_column_indices(
        rows=rows,
        mode=mode,
        date_col=date_col,
        player_block_col=player_block_col,
        opp_col=opp_col,
        player_score_col=player_score_col,
        opp_score_col=opp_score_col,
        round_col=round_col,
        skip_rows_before=skip_rows_before,
    )

    warnings: List[str] = []

    # -------------------------------------------------------------------------
    # PASS 1: Use the shared all-club roster if provided; otherwise build a roster
    # from this single file for backwards compatibility.
    # -------------------------------------------------------------------------
    local_full_names = collect_full_names_from_rows(rows, player_idx, start_row)
    full_names = list(global_full_names) if global_full_names is not None else local_full_names

    # Normalized roster sets used to distinguish true unmatched games from
    # cross-club known visitors. Example: Jason Vaysberg may appear as a regular
    # in the North Metro file and as a visitor/opponent in the Daytime file.
    # In that case, the Daytime row may legitimately have no matching Jason
    # player block, but we still want to load the game as a known-player game.
    local_full_name_set = {_norm(CANONICAL_NAME_MAP.get(n, n)) for n in local_full_names}
    global_full_name_set = {_norm(CANONICAL_NAME_MAP.get(n, n)) for n in full_names}

    if global_short_to_full is not None:
        short_to_full = dict(global_short_to_full)
        collisions = set(global_collisions or set())
    else:
        short_to_full, collisions = build_short_to_full(full_names)

    # -------------------------------------------------------------------------
    # PASS 2: Parse games using current player block (not per-row player cell)
    # -------------------------------------------------------------------------

    games: List[Dict[str, Any]] = []

    current_player: Optional[str] = None
    current_session_date = None
    current_date = None
    round_by_date: Dict[Tuple[str, str], int] = {}  # (date, location) -> next round

    i = start_row
    while i < len(rows):
        row = rows[i]

        full_name, advance = detect_player_block_name(rows, i, player_idx, cell)
        if full_name:
            current_player = _norm(full_name)
            player_name_final = CANONICAL_NAME_MAP.get(current_player, current_player)
            current_session_date = None

            # If the current row also contains game data, keep processing this same row
            # as a game row below.  Otherwise, skip past the block header rows.
            date_raw0 = _norm(cell(row, date_idx)) 
            opp_raw0 = _norm(cell(row, opp_idx)) 
            ps_raw0 = _norm(cell(row, ps_idx)) 
            os_raw0 = _norm(cell(row, os_idx))

            same_row_has_game = bool(opp_raw0 and ps_raw0 and os_raw0)

            if not same_row_has_game:
                i += advance
                continue


        # otherwise: not a block boundary

        if not current_player:
            i += 1
            continue

        date_raw = _norm(cell(row, date_idx))

        # Ignore header-like "dates"
        if date_raw.lower() in {"date", "session_date", "session date"}:
            date_raw = ""
    
        opp_raw = _norm(cell(row, opp_idx))
        ps_raw = _norm(cell(row, ps_idx))
        os_raw = _norm(cell(row, os_idx))

        # Must have a date OR have already seen one in this player block
        if not date_raw and not current_session_date:
            i += 1
            continue
        # silently ignore rows that have no game info at all
        if not (opp_raw or ps_raw or os_raw):
            i += 1
            continue

        # warn only if it *looks like someone started entering a game* but it's incomplete
        if not (opp_raw and ps_raw and os_raw):
            warnings.append(f"Row {i+1}: skipped (missing opponent/score).")
            i += 1
            continue

        # Parse date using your existing function
        try:
            parsed_date = parse_date(date_raw) if date_raw else None
            if parsed_date:
                current_session_date = parsed_date
            elif current_session_date:
                parsed_date = current_session_date
            else:
                warnings.append(f"Row {i}: skipped (no date found yet).")
                i += 1
                continue
        except Exception as e:
            warnings.append(f"Row {i+1}: skipped (bad date '{date_raw}': {e}).")
            i += 1
            continue

        # normalize to string
        if hasattr(parsed_date, "strftime"):
            session_date_str = parsed_date.strftime("%Y-%m-%d")
        else:
            session_date_str = str(parsed_date)

        if hasattr(parsed_date, "weekday"):
            date_warning = expected_weekday_warning(location, parsed_date, player_name_final)
            if date_warning:
                warnings.append(date_warning)

        # Round number
        if round_idx is not None:
            r_raw = _norm(cell(row, round_idx))
            try:
                round_number = int(r_raw) if r_raw else None
            except:
                round_number = None
        else:
            key = (session_date_str, location or "")
            nxt = round_by_date.get(key, 1)
            round_number = nxt
            round_by_date[key] = nxt + 1

        # Parse scores
        try:
            player_score = int(float(ps_raw))
            opponent_score = int(float(os_raw))
        except Exception:
            warnings.append(f"Row {i+1}: skipped (non-numeric score).")
            i += 1
            continue

        # Expand opponent short name if unique
        opp_clean, opponent_marked_visitor = split_visitor_marker(opp_raw)

        opp_key = opponent_short_norm(opp_clean)
        opponent_full = short_to_full.get(opp_key, opp_clean)
        opponent_name_final = CANONICAL_NAME_MAP.get(opponent_full, opponent_full)

        opponent_name_final = _norm(opponent_name_final)

        opponent_resolved = (
            opponent_full != opp_clean
            or opponent_name_final != opponent_full
            or opponent_name_final in global_full_name_set
        )

        opponent_known_in_global_roster = opponent_name_final in global_full_name_set
        opponent_seen_in_this_file = opponent_name_final in local_full_name_set
        opponent_is_session_visitor_override = is_session_visitor_override(
            location,
            session_date_str,
            opponent_name_final,
        )
        opponent_is_known_cross_club_visitor = (
            opponent_known_in_global_roster
            and (
                not opponent_seen_in_this_file
                or opponent_is_session_visitor_override
            )
        )
        opponent_is_session_visitor = (
            opponent_marked_visitor
            or opponent_is_session_visitor_override
        )

        opponent_is_placeholder_visitor = 0
        visitor_note = None

        if opponent_is_session_visitor:
            if opponent_resolved:
                visitor_note = f"Known player visiting {location}"
            else:
                opponent_name_final = make_placeholder_visitor_name(opp_clean)
                opponent_is_placeholder_visitor = 1
                visitor_note = "Unresolved visitor"
                opponent_is_known_cross_club_visitor = False
        elif opponent_is_known_cross_club_visitor:
            visitor_note = f"Known cross-club player visiting {location}"
        
        games.append({
            "source_csv": str(csv_path),
            "source_row": i + 1,
            "session_date": session_date_str,
            "location": location,
            "round_number": round_number,
            "player_name": player_name_final,
            "opponent_name": opponent_name_final,
            "player_score": player_score,
            "opponent_score": opponent_score,
            "opponent_is_placeholder_visitor": opponent_is_placeholder_visitor,
            "opponent_is_known_cross_club_visitor": 1 if opponent_is_known_cross_club_visitor else 0,
            "opponent_is_marked_visitor": 1 if opponent_marked_visitor else 0,
            "opponent_is_session_visitor": 1 if opponent_is_session_visitor else 0,
            "visitor_note": visitor_note,
        })

        i += 1

    # Print collision summary (optional)
    if collisions:
        print(f"opponent short-name collisions: {len(collisions)} (will not expand those):")
        for s in sorted(collisions):
            print(" -", s)

    return games, warnings, full_names, collisions


def validate_and_filter_games(games: List[Dict[str, Any]]):
    from collections import defaultdict

    def norm_name_for_match(name: str) -> str:
        return _norm(name).upper()

    def reporter_for_match(g):
        return norm_name_for_match(g["player_name"])

    def canonical_score_sig(g, a_norm):
        # Put scores into canonical player order. This makes A 300-250 vs B
        # match the reciprocal B 250-300 regardless of round number or row order.
        if reporter_for_match(g) == a_norm:
            return (g["player_score"], g["opponent_score"])
        return (g["opponent_score"], g["player_score"])

    def date_for_match(g):
        try:
            return datetime.strptime(str(g.get("session_date") or ""), "%Y-%m-%d").date()
        except ValueError:
            return None

    def winner_for_reported_game(g):
        if g["player_score"] > g["opponent_score"]:
            return norm_name_for_match(g["player_name"])
        if g["player_score"] < g["opponent_score"]:
            return norm_name_for_match(g["opponent_name"])
        return "TIE"

    def is_intentional_one_sided_visitor(g):
        # Only accept one-sided games when parser metadata gives positive
        # visitor evidence. Do not infer this from "known player" alone.
        return bool(
            g.get("opponent_is_known_cross_club_visitor")
            or g.get("opponent_is_marked_visitor")
            or g.get("opponent_is_session_visitor")
        )

    def possible_date_mismatch_candidates(g, no_obvious_issues):
        g_date = date_for_match(g)
        if not g_date:
            return []

        g_player = norm_name_for_match(g["player_name"])
        g_opponent = norm_name_for_match(g["opponent_name"])
        g_location = g.get("location") or ""

        candidates = []
        for item in no_obvious_issues:
            candidate = item["game"]
            if candidate is g:
                continue
            if (candidate.get("location") or "") != g_location:
                continue

            candidate_date = date_for_match(candidate)
            if not candidate_date:
                continue

            days_apart = abs((candidate_date - g_date).days)
            if days_apart == 0 or days_apart > 45:
                continue

            if norm_name_for_match(candidate["player_name"]) != g_opponent:
                continue
            if norm_name_for_match(candidate["opponent_name"]) != g_player:
                continue
            if candidate["player_score"] != g["opponent_score"]:
                continue
            if candidate["opponent_score"] != g["player_score"]:
                continue

            candidates.append(candidate)

        return sorted(
            candidates,
            key=lambda c: (
                c.get("session_date") or "",
                c.get("player_name") or "",
                c.get("opponent_name") or "",
                c.get("player_score") or 0,
                c.get("opponent_score") or 0,
            ),
        )

    pair_groups = defaultdict(list)

    for g in games:
        session_date = g["session_date"]

        # Keep old historical records without mismatch validation.
        if str(session_date)[:4] < "2026":
            pair_groups[("__OLD__", id(g))].append(g)
            continue

        p_norm = norm_name_for_match(g["player_name"])
        o_norm = norm_name_for_match(g["opponent_name"])
        a_norm, b_norm = sorted([p_norm, o_norm])

        key = (
            session_date,
            g.get("location") or "",
            a_norm,
            b_norm,
        )

        pair_groups[key].append(g)

    clean_games = []
    issues = []

    for key, rows in pair_groups.items():
        # Pre-2026 rows: keep without validating.
        if key[0] == "__OLD__":
            clean_games.extend(rows)
            continue

        session_date, location, a_norm, b_norm = key

        rows_by_sig = defaultdict(lambda: defaultdict(list))

        for g in rows:
            rows_by_sig[canonical_score_sig(g, a_norm)][reporter_for_match(g)].append(g)

        used_ids = set()

        for sig in sorted(rows_by_sig):
            a_rows = rows_by_sig[sig].get(a_norm, [])
            b_rows = rows_by_sig[sig].get(b_norm, [])

            matched_count = min(len(a_rows), len(b_rows))

            for i in range(matched_count):
                clean_games.append(a_rows[i])
                used_ids.add(id(a_rows[i]))
                used_ids.add(id(b_rows[i]))

        leftover = [g for g in rows if id(g) not in used_ids]

        if not leftover:
            continue

        reporters = {reporter_for_match(g) for g in rows}

        for g in leftover:
            # Keep intentional one-sided visitor games. This covers explicitly
            # marked visitors, session overrides, and known cross-club visitors.
            # Ordinary missing counterpart reports are still skipped and reported.
            if is_intentional_one_sided_visitor(g):
                clean_games.append(g)
                continue

            reporter = reporter_for_match(g)
            opposite_reporter = b_norm if reporter == a_norm else a_norm
            candidates = [
                x for x in leftover
                if x is not g and reporter_for_match(x) == opposite_reporter
            ]

            if candidates or opposite_reporter in reporters:
                issue_type = "SCORE_MISMATCH"
                for c in candidates:
                    if winner_for_reported_game(g) != winner_for_reported_game(c):
                        issue_type = "WINNER_DISAGREEMENT"
                        break

                issues.append({
                    "type": issue_type,
                    "game": g,
                    "candidates": candidates,
                })
            else:
                issues.append({
                    "type": "NO_OBVIOUS_MATCH",
                    "game": g,
                })

    no_obvious_issues = [
        item for item in issues
        if item["type"] == "NO_OBVIOUS_MATCH"
    ]

    for item in no_obvious_issues:
        candidates = possible_date_mismatch_candidates(item["game"], no_obvious_issues)
        if candidates:
            item["type"] = "POSSIBLE_DATE_MISMATCH"
            item["candidates"] = candidates

    return clean_games, issues

def archive_existing_output(out_file: str) -> None:
    out_path = Path(out_file)

    if not out_path.exists():
        return

    archive_dir = Path.cwd() / "Archive"
    archive_dir.mkdir(exist_ok=True)

    datestamp = datetime.now().strftime("%Y_%b_%d")
    archived_path = archive_dir / f"{out_path.stem}_{datestamp}{out_path.suffix}"

    counter = 2
    while archived_path.exists():
        archived_path = archive_dir / f"{out_path.stem}_{datestamp}_{counter}{out_path.suffix}"
        counter += 1

    try:
        shutil.move(str(out_path), str(archived_path))
        print(f"Archived existing output file: {out_path} -> {archived_path}")
    except Exception as e:
        print(f"[WARN] Could not archive existing output file: {out_path}")
        print(f"[WARN] Reason: {e}")
        print("[WARN] Continuing anyway and attempting to overwrite output file.")

def main():
    ap = argparse.ArgumentParser(description="Convert Scrabble CSV(s) to import payload JSON")

    # Backward-compatible single-club form:
    #   python make_import_payload.py file.csv --location NM --out nm_payload.json
    ap.add_argument("csv", nargs="?", help="Path to one CSV exported from Excel/Google Sheets")

    # New all-club form:
    #   python make_import_payload.py --club NM=nm.csv --club DAY=day.csv --out combined_payload.json
    ap.add_argument(
        "--club",
        action="append",
        default=None,
        help="Club input in KEY=filename.csv format. May be repeated, e.g. --club NM=nm.csv --club DAY=day.csv",
    )

    ap.add_argument("--mode", choices=["letters", "header"], default="letters",
                    help="Column mapping mode")

    # Your sheet mapping (can override on CLI)
    ap.add_argument("--date", default="AE", help="Date column letter or header")
    ap.add_argument("--player_block", default="W", help="Column/header that contains player block header/name/notes")
    ap.add_argument("--player_score", default="AF", help="Player score column letter or header")
    ap.add_argument("--opponent", default="AH", help="Opponent abbreviation column letter or header")
    ap.add_argument("--opponent_score", default="AI", help="Opponent score column letter or header")
    ap.add_argument("--round", default=None, help="Optional round column letter or header")

    ap.add_argument("--location", default=None, help="Single-club mode only: location applied to every row, e.g. NM or DAY")
    ap.add_argument("--skip_rows_before", type=int, default=0, help="Skip first N rows (notes/preamble)")

    ap.add_argument("--ratings", default="player_ratings.csv", help="Curated player ratings CSV")
    ap.add_argument("--wipe", action="store_true", help="Set wipe:true in output payload")
    ap.add_argument("--out", default="import_payload.json", help="Output JSON file name")

    args = ap.parse_args()

    try:
        clubs = parse_club_args(args.club)
    except ValueError as e:
        ap.error(str(e))

    if clubs and args.csv:
        ap.error("Use either positional csv + --location OR repeated --club KEY=file.csv, not both.")

    if not clubs:
        if not args.csv:
            ap.error("Provide either a positional csv or at least one --club KEY=file.csv.")
        if not args.location:
            ap.error("Single-club mode requires --location. Multi-club mode uses --club KEY=file.csv.")
        clubs = [{"club": _norm(args.location).upper(), "path": args.csv}]
        multi_club_mode = False
    else:
        multi_club_mode = True

    all_warnings: List[str] = []
    all_full_names: List[str] = []
    seen_full = set()

    # Global roster pass across all club files.
    for club in clubs:
        rows = read_csv_rows(club["path"])
        if not rows:
            all_warnings.append(f"{club['club']}: empty CSV")
            continue

        _, player_idx, _, _, _, _, start_row = resolve_column_indices(
            rows=rows,
            mode=args.mode,
            date_col=args.date,
            player_block_col=args.player_block,
            opp_col=args.opponent,
            player_score_col=args.player_score,
            opp_score_col=args.opponent_score,
            round_col=args.round,
            skip_rows_before=args.skip_rows_before,
        )

        for full_name in collect_full_names_from_rows(rows, player_idx, start_row):
            canonical = CANONICAL_NAME_MAP.get(full_name, full_name)
            canonical = _norm(canonical)
            if canonical and canonical not in seen_full:
                seen_full.add(canonical)
                all_full_names.append(canonical)

    short_to_full, collisions = build_short_to_full(all_full_names)

    all_games: List[Dict[str, Any]] = []

    for club in clubs:
        games, warnings, _, _ = build_games_from_csv(
            csv_path=club["path"],
            mode=args.mode,
            date_col=args.date,
            player_score_col=args.player_score,
            opp_col=args.opponent,
            opp_score_col=args.opponent_score,
            player_block_col=args.player_block,
            round_col=args.round,
            location=club["club"],
            skip_rows_before=args.skip_rows_before,
            global_full_names=all_full_names,
            global_short_to_full=short_to_full,
            global_collisions=collisions,
        )
        all_games.extend(games)
        all_warnings.extend([f"{club['club']}: {w}" for w in warnings])

    clean_games, unmatched_games = validate_and_filter_games(all_games)

    player_names_for_ratings: List[str] = []
    seen_player_names_for_ratings = set()

    def add_player_name_for_ratings(name: str) -> None:
        canonical = canonical_player_name(name)
        if canonical and canonical not in seen_player_names_for_ratings:
            seen_player_names_for_ratings.add(canonical)
            player_names_for_ratings.append(canonical)

    for name in all_full_names:
        add_player_name_for_ratings(name)

    for g in clean_games:
        add_player_name_for_ratings(g.get("player_name"))
        add_player_name_for_ratings(g.get("opponent_name"))

    player_ratings, rating_warnings = read_player_ratings(
        args.ratings,
        player_names_for_ratings,
    )
    all_warnings.extend([f"ratings: {w}" for w in rating_warnings])

    player_payload = build_player_payload(
        player_names_for_ratings,
        clean_games,
        player_ratings,
    )

    payload = {"wipe": bool(args.wipe), "players": player_payload, "games": clean_games}

    out_path = Path(args.out).resolve()
    print(f"Writing output to: {out_path}")

    archive_existing_output(str(out_path))

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"clubs processed: {', '.join(c['club'] for c in clubs)}")
    print(f"global roster players found: {len(all_full_names)}")
    print(f"raw games parsed: {len(all_games)}")
    print(f"clean games written: {len(clean_games)}")
    print(f"players written: {len(player_payload)}")
    print(f"player rating rows matched: {len(player_ratings)}")
    print(f"unmatched games skipped: {len(unmatched_games)}")

    if unmatched_games:
        if multi_club_mode:
            report_path = "unmatched_games_report_ALL.txt"
        else:
            report_path = f"unmatched_games_report_{clubs[0]['club']}.txt"

        def write_unmatched_report(path: str, items: List[Dict[str, Any]]) -> None:
            with open(path, "w", encoding="utf-8") as f:
                for item in items:
                    g = item["game"]

                    f.write(
                        f"{g['session_date']} | {g['location']} | "
                        f"{g['player_name']} vs {g['opponent_name']} "
                        f"{g['player_score']}-{g['opponent_score']} | "
                        f"{item['type']} | source_row={g.get('source_row', 'unknown')}\n"
                    )

                    if item["type"] in {"SCORE_MISMATCH", "WINNER_DISAGREEMENT"} and item.get("candidates"):
                        f.write("  Candidate opponent entries:\n")
                        for og in item["candidates"]:
                            f.write(
                                f"    {og['player_name']} vs {og['opponent_name']} "
                                f"{og['player_score']}-{og['opponent_score']} "
                                f"(source_row={og.get('source_row', 'unknown')})\n"
                            )
                    elif item["type"] == "POSSIBLE_DATE_MISMATCH":
                        f.write("  Possible reciprocal date-mismatch entries:\n")
                        for og in item["candidates"]:
                            f.write(
                                f"    {og['session_date']} | {og['location']} | "
                                f"{og['player_name']} vs {og['opponent_name']} "
                                f"{og['player_score']}-{og['opponent_score']} "
                                f"(source_row={og.get('source_row', 'unknown')})\n"
                            )

                    f.write("\n")

        write_unmatched_report(report_path, unmatched_games)
        print(f"⚠️  Wrote unmatched report: {report_path}")

        filtered_report_path = str(
            Path(report_path).with_name(
                f"{Path(report_path).stem}_no_score_mismatches{Path(report_path).suffix}"
            )
        )
        non_score_mismatch_games = [
            item for item in unmatched_games
            if item["type"] != "SCORE_MISMATCH"
        ]

        write_unmatched_report(filtered_report_path, non_score_mismatch_games)
        print(f"⚠️  Wrote non-score-mismatch report: {filtered_report_path}")

    print(f"Wrote {out_path}")
    print(f"games written: {len(clean_games)}")

    if collisions:
        print(f"opponent short-name collisions: {len(collisions)} (will not auto-expand those):")
        for c in sorted(list(collisions))[:20]:
            print("  -", c)
        if len(collisions) > 20:
            print("  ...")

    if all_warnings:
        print(f"warnings: {len(all_warnings)} (showing up to 25)")
        for w in all_warnings[:25]:
            print(" -", w)

if __name__ == "__main__":
    main()
