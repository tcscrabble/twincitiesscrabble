import argparse
import csv
import json
import re
import time
from datetime import datetime, date, timedelta
from typing import Optional, Tuple, List, Dict, Any, Set

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

    # --- column mapping helpers (keep your existing logic if you already have it) ---
    def col_to_index_letters(col: str) -> int:
        # Excel-like letters -> 0-based index: A->0, B->1, ... Z->25, AA->26, etc.
        col = col.strip().upper()
        n = 0
        for ch in col:
            if not ("A" <= ch <= "Z"):
                raise ValueError(f"Bad column letter: {col}")
            n = n * 26 + (ord(ch) - ord("A") + 1)
        return n - 1

    # If mode == "header", these values are header strings; if mode == "letters", these are letters.
    # We'll support both, but your use-case is letters.
    import csv

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f))

    if not rows:
        return [], ["Empty CSV"]

    # Decide indices
    if mode == "letters":
        date_idx = col_to_index_letters(date_col)
        player_idx = col_to_index_letters(player_block_col)
        opp_idx = col_to_index_letters(opp_col)
        ps_idx = col_to_index_letters(player_score_col)
        os_idx = col_to_index_letters(opp_score_col)
        round_idx = col_to_index_letters(round_col) if round_col else None
        start_row = skip_rows_before
    elif mode == "header":
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

    warnings: List[str] = []

    def cell(row: List[str], idx: int) -> str:
        return row[idx] if idx is not None and idx < len(row) else ""

    # -------------------------------------------------------------------------
    # PASS 1: Collect real player full names from blocks and build opponent map
    # -------------------------------------------------------------------------
    full_names: List[str] = []
    seen_full = set()

    i = start_row
    while i < len(rows):
        full_name, advance = detect_player_block_name(rows, i, player_idx, cell)

        if full_name:
            full_name = _norm(full_name)
            if full_name not in seen_full:
                seen_full.add(full_name)
                full_names.append(full_name)

        i += advance

    from collections import defaultdict

    short_candidates = defaultdict(list)

    for fn in full_names:
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

    # -------------------------------------------------------------------------
    # PASS 2: Parse games using current player block (not per-row player cell)
    # -------------------------------------------------------------------------

    CANONICAL_NAME_MAP = {"Vince V": "Vincent VanDover",
                          "Manon StA": "Manon St. Amant",
                          "Manon S": "Manon St. Amant",
                          }

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
        opp_key = opponent_short_norm(opp_raw)
        opponent_full = short_to_full.get(opp_key, opp_raw)
        opponent_name_final = CANONICAL_NAME_MAP.get(opponent_full, opponent_full)

        games.append({
            "session_date": session_date_str,
            "location": location,
            "round_number": round_number,
            "player_name": player_name_final,
            "opponent_name": opponent_name_final,
            "player_score": player_score,
            "opponent_score": opponent_score,
        })

        i += 1

    # Print collision summary (optional)
    if collisions:
        print(f"opponent short-name collisions: {len(collisions)} (will not expand those):")
        for s in sorted(collisions):
            print(" -", s)

    return games, warnings, full_names, collisions

def main():
    ap = argparse.ArgumentParser(description="Convert Scrabble CSV to /api/import payload JSON")

    ap.add_argument("csv", help="Path to CSV exported from Excel/Google Sheets")

    ap.add_argument("--mode", choices=["letters"], default="letters",
                    help="Column mapping mode (letters only in this version)")

    # Your sheet mapping (can override on CLI)
    ap.add_argument("--date", default="AE", help="Date column letter")
    ap.add_argument("--player_block", default="W", help="Column letter that contains player block header/name/notes")
    ap.add_argument("--player_score", default="AF", help="Player score column letter")
    ap.add_argument("--opponent", default="AH", help="Opponent (abbrev) column letter")
    ap.add_argument("--opponent_score", default="AI", help="Opponent score column letter")

    ap.add_argument("--location", default=None, help="Constant location applied to every row (e.g., 'TCAS')")
    ap.add_argument("--skip_rows_before", type=int, default=0, help="Skip first N rows (notes/preamble)")

    ap.add_argument("--wipe", action="store_true", help="Set wipe:true in output payload")
    ap.add_argument("--out", default="import_payload.json", help="Output JSON file name")

    args = ap.parse_args()

    games, warnings, full_names, collisions = build_games_from_csv(
        csv_path=args.csv,
        mode=args.mode,
        date_col=args.date,
        player_score_col=args.player_score,
        opp_col=args.opponent,
        opp_score_col=args.opponent_score,
        player_block_col=args.player_block,
        location=args.location,
        skip_rows_before=args.skip_rows_before,
    )

    payload = {"wipe": bool(args.wipe), "games": games}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {args.out}")
    print(f"games: {len(games)}")
    if collisions:
        print(f"opponent short-name collisions: {len(collisions)} (will not expand those):")
        for c in sorted(list(collisions))[:20]:
            print("  -", c)
        if len(collisions) > 20:
            print("  ...")

    if warnings:
        print(f"warnings: {len(warnings)} (showing up to 25)")
        for w in warnings[:25]:
            print(" -", w)

if __name__ == "__main__":
    main()
