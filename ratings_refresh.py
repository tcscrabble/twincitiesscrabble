import argparse
import csv
import html
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import URLError, HTTPError
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

NASPA_CURRENT_RATINGS_URL = "https://www.scrabbleplayers.org/ratings/data/full/current.txt"
NASPA_PLAYER_URL = "https://www.scrabbleplayers.org/cgi-bin/player.pl?naspa={naspa_id}"
WGPO_PLAYERS_URL = "https://wordgameplayers.org/stats/players/"
WGPO_PLAYER_URL = "https://www.wordgameplayers.org/stats/player/{wgpo_id}/"
CROSS_TABLES_PLAYER_URL = "https://www.cross-tables.com/results.php?p={cross_tables_id}"

HTTP_TIMEOUT_SECONDS = 20


@dataclass
class ExternalId:
    display_name: str
    naspa_name: str | None = None
    naspa_id: str | None = None
    wgpo_id: str | None = None
    cross_tables_id: str | None = None
    cross_tables_url: str | None = None


def norm_space(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def norm_lookup_name(value: Any) -> str:
    value = norm_space(value).lower()
    value = re.sub(r"[^a-z0-9 ]+", "", value)
    return re.sub(r"\s+", " ", value).strip()


def player_key(display_name: Any) -> str:
    return norm_space(display_name).upper()


def nullable_int(value: Any) -> int | None:
    if value is None:
        return None
    value = str(value).strip().replace(",", "")
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def nullable_text(value: Any) -> str | None:
    value = norm_space(value)
    return value or None


def fetch_text(url: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": "TwinCitiesScrabbleRatingsRefresh/1.0 (+https://twincitiesscrabble.com)"
        },
    )
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        raw = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="replace")


def read_external_ids(path: Path) -> dict[str, ExternalId]:
    if not path.exists():
        return {}

    mappings: dict[str, ExternalId] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            display_name = norm_space(row.get("display_name"))
            if not display_name:
                continue
            mappings[player_key(display_name)] = ExternalId(
                display_name=display_name,
                naspa_name=nullable_text(row.get("naspa_name")),
                naspa_id=nullable_text(row.get("naspa_id")),
                wgpo_id=nullable_text(row.get("wgpo_id")),
                cross_tables_id=nullable_text(row.get("cross_tables_id")),
                cross_tables_url=nullable_text(row.get("cross_tables_url")),
            )
    return mappings


def read_players(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    players_by_key: dict[str, dict[str, Any]] = {}

    for player in data.get("players", []):
        display_name = norm_space(player.get("display_name"))
        if display_name:
            players_by_key[player_key(display_name)] = player

    for game in data.get("games", []):
        for name_key, placeholder_key in (
            ("player_name", "player_is_placeholder_visitor"),
            ("opponent_name", "opponent_is_placeholder_visitor"),
        ):
            display_name = norm_space(game.get(name_key))
            if not display_name or player_key(display_name) in players_by_key:
                continue
            players_by_key[player_key(display_name)] = {
                "display_name": display_name,
                "is_placeholder_visitor": int(game.get(placeholder_key, 0) or 0),
            }

    return sorted(players_by_key.values(), key=lambda p: norm_lookup_name(p.get("display_name")))


def base_rating_row(display_name: str, updated_at: str) -> dict[str, Any]:
    return {
        "display_name": display_name,
        "naspa_rating": None,
        "wgpo_rating": None,
        "wgpo_wow_rating": None,
        "cross_tables_rating": None,
        "naspa_url": None,
        "wgpo_url": None,
        "cross_tables_url": None,
        "rating_source_notes": None,
        "ratings_updated_at": updated_at,
    }


def build_name_index(records: list[dict[str, Any]], name_key: str) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        key = norm_lookup_name(record.get(name_key))
        if key:
            index.setdefault(key, []).append(record)
    return index


def parse_naspa_full_list(text: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for raw_line in text.splitlines():
        line = html.unescape(raw_line).strip()
        if not line or "rating" in line.lower() and "name" in line.lower():
            continue

        tab_parts = [norm_space(part) for part in line.split("\t")]
        if len(tab_parts) >= 2 and nullable_int(tab_parts[1]) is not None:
            name = tab_parts[0]
            if len(name) >= 3:
                records.append(
                    {
                        "name": name,
                        "rating": nullable_int(tab_parts[1]),
                        "naspa_id": None,
                    }
                )
            continue

        naspa_id_match = re.search(r"\b[A-Z]{2}\d{6}\b", line)
        rating_matches = re.findall(r"(?<!\d)([1-2]\d{3}|[5-9]\d{2})(?!\d)", line)
        if not rating_matches:
            continue

        rating = nullable_int(rating_matches[0])
        if rating is None:
            continue

        name = line
        if naspa_id_match:
            name = name.replace(naspa_id_match.group(0), " ")
        name = re.sub(r"(?<!\d)([1-2]\d{3}|[5-9]\d{2})(?!\d)", " ", name)
        name = re.sub(r"\b\d+\b", " ", name)
        name = norm_space(name.strip(" ,-|\t"))
        if len(name) < 3:
            continue

        records.append(
            {
                "name": name,
                "rating": rating,
                "naspa_id": naspa_id_match.group(0) if naspa_id_match else None,
            }
        )
    return records


def parse_naspa_player_page(text: str) -> int | None:
    match = re.search(r"NASPA\s+TWL\s+Rating:\s*([0-9,]+)", text, re.I)
    return nullable_int(match.group(1)) if match else None


def parse_wgpo_players(text: str) -> list[dict[str, Any]]:
    cleaned = html.unescape(re.sub(r"<[^>]+>", "\n", text))
    section = None
    records: dict[str, dict[str, Any]] = {}

    for raw_line in cleaned.splitlines():
        line = norm_space(raw_line)
        if not line:
            continue
        if line.upper() == "WOW":
            section = "wow"
            continue
        if line.lower() == "collins":
            section = "collins"
            continue
        if section not in {"wow", "collins"}:
            continue

        match = re.match(r"^\d+\s+(.+?)\s+([0-9]{3,4})$", line)
        if not match:
            continue
        name = norm_space(match.group(1))
        rating = nullable_int(match.group(2))
        if not name or rating is None:
            continue

        key = norm_lookup_name(name)
        record = records.setdefault(key, {"name": name, "wgpo_rating": None, "wgpo_wow_rating": None})
        if section == "wow":
            record["wgpo_wow_rating"] = rating
        else:
            record["wgpo_rating"] = rating

    return list(records.values())


def parse_wgpo_player_page(text: str) -> dict[str, int | None]:
    cleaned = html.unescape(re.sub(r"<[^>]+>", "\n", text))
    ratings: dict[str, int | None] = {"wgpo_rating": None, "wgpo_wow_rating": None}

    wow_match = re.search(r"(?<!\d)([0-9]{3,4})\s+WOW\b", cleaned, re.I)
    collins_match = re.search(r"(?<!\d)([0-9]{3,4})\s+Collins\b", cleaned, re.I)
    ratings["wgpo_wow_rating"] = nullable_int(wow_match.group(1)) if wow_match else None
    ratings["wgpo_rating"] = nullable_int(collins_match.group(1)) if collins_match else None
    return ratings


def parse_cross_tables_player_page(text: str) -> int | None:
    cleaned = html.unescape(re.sub(r"<[^>]+>", " ", text))
    patterns = [
        r"\bCurrent\s+Rating\s*:?\s*([0-9]{3,4})\b",
        r"\bRating\s*:?\s*([0-9]{3,4})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, cleaned, re.I)
        if match:
            return nullable_int(match.group(1))
    return None


def unique_name_match(
    source: str,
    display_name: str,
    lookup_name: str,
    index: dict[str, list[dict[str, Any]]],
    warnings: list[str],
) -> dict[str, Any] | None:
    matches = index.get(norm_lookup_name(lookup_name))
    if not matches:
        return None
    if len(matches) > 1:
        warnings.append(f'{source}: Could not find unique match for "{display_name}"')
        return None
    return matches[0]


def preload_sources(warnings: list[str]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    naspa_index: dict[str, list[dict[str, Any]]] = {}
    wgpo_index: dict[str, list[dict[str, Any]]] = {}

    try:
        naspa_records = parse_naspa_full_list(fetch_text(NASPA_CURRENT_RATINGS_URL))
        naspa_index = build_name_index(naspa_records, "name")
    except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
        warnings.append(f"NASPA: Site unavailable; skipped NASPA refresh ({exc})")

    try:
        wgpo_records = parse_wgpo_players(fetch_text(WGPO_PLAYERS_URL))
        wgpo_index = build_name_index(wgpo_records, "name")
    except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
        warnings.append(f"WGPO: Site unavailable; skipped WGPO refresh ({exc})")

    return naspa_index, wgpo_index


def apply_payload_fallback(row: dict[str, Any], player: dict[str, Any], notes: list[str]) -> None:
    fallback_fields = {
        "naspa_rating": "naspa_rating",
        "wgpo_rating": "wgpo_nwl_rating",
        "wgpo_wow_rating": "wgpo_wow_rating",
        "cross_tables_url": "cross_tables_url",
        "wgpo_url": "wgpo_url",
        "rating_source_notes": "rating_notes",
    }
    used = False
    for target, source in fallback_fields.items():
        if row.get(target) not in (None, ""):
            continue
        value = player.get(source)
        if value in (None, ""):
            continue
        row[target] = nullable_int(value) if target.endswith("_rating") else nullable_text(value)
        used = True
    if used:
        notes.append("Used existing payload rating metadata where live refresh did not provide a value")


def refresh_player(
    player: dict[str, Any],
    external_id: ExternalId | None,
    naspa_index: dict[str, list[dict[str, Any]]],
    wgpo_index: dict[str, list[dict[str, Any]]],
    updated_at: str,
    warnings: list[str],
) -> dict[str, Any]:
    display_name = norm_space(player.get("display_name"))
    row = base_rating_row(display_name, updated_at)
    notes: list[str] = []

    if external_id and external_id.naspa_id:
        row["naspa_url"] = NASPA_PLAYER_URL.format(naspa_id=quote_plus(external_id.naspa_id))
        try:
            row["naspa_rating"] = parse_naspa_player_page(fetch_text(row["naspa_url"]))
            if row["naspa_rating"] is None:
                warnings.append(f'NASPA: No rating found for "{display_name}" using ID {external_id.naspa_id}')
        except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
            warnings.append(f'NASPA: Could not refresh "{display_name}" using ID {external_id.naspa_id} ({exc})')
    else:
        lookup_name = external_id.naspa_name if external_id and external_id.naspa_name else player.get("naspa_name") or display_name
        match = unique_name_match("NASPA", display_name, lookup_name, naspa_index, warnings)
        if match:
            row["naspa_rating"] = match.get("rating")
            if match.get("naspa_id"):
                row["naspa_url"] = NASPA_PLAYER_URL.format(naspa_id=quote_plus(str(match["naspa_id"])))

    if external_id and external_id.wgpo_id:
        row["wgpo_url"] = WGPO_PLAYER_URL.format(wgpo_id=quote_plus(external_id.wgpo_id))
        try:
            ratings = parse_wgpo_player_page(fetch_text(row["wgpo_url"]))
            row["wgpo_rating"] = ratings.get("wgpo_rating")
            row["wgpo_wow_rating"] = ratings.get("wgpo_wow_rating")
            if row["wgpo_rating"] is None and row["wgpo_wow_rating"] is None:
                warnings.append(f'WGPO: No rating found for "{display_name}" using ID {external_id.wgpo_id}')
        except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
            warnings.append(f'WGPO: Could not refresh "{display_name}" using ID {external_id.wgpo_id} ({exc})')
    else:
        lookup_name = player.get("wgpo_name") or display_name
        match = unique_name_match("WGPO", display_name, lookup_name, wgpo_index, warnings)
        if match:
            row["wgpo_rating"] = match.get("wgpo_rating")
            row["wgpo_wow_rating"] = match.get("wgpo_wow_rating")

    cross_tables_url = None
    if external_id and external_id.cross_tables_url:
        cross_tables_url = external_id.cross_tables_url
    elif external_id and external_id.cross_tables_id:
        cross_tables_url = CROSS_TABLES_PLAYER_URL.format(cross_tables_id=quote_plus(external_id.cross_tables_id))
    elif player.get("cross_tables_url"):
        cross_tables_url = player.get("cross_tables_url")

    if cross_tables_url:
        row["cross_tables_url"] = cross_tables_url
        try:
            row["cross_tables_rating"] = parse_cross_tables_player_page(fetch_text(cross_tables_url))
            if row["cross_tables_rating"] is None:
                notes.append("Cross-tables rating unavailable from player page")
        except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
            warnings.append(f'Cross-tables: Could not refresh "{display_name}" ({exc})')

    apply_payload_fallback(row, player, notes)

    if (
        row["naspa_rating"] is None
        and row["wgpo_rating"] is None
        and row["wgpo_wow_rating"] is None
        and row["cross_tables_rating"] is None
    ):
        notes.append("No unambiguous rating match found")

    row["rating_source_notes"] = "; ".join(dict.fromkeys(notes)) if notes else None
    return row


def write_empty_payload(out_path: Path, warnings: list[str]) -> None:
    updated_at = datetime.now().replace(microsecond=0).isoformat()
    payload = {"ratings": [], "warnings": warnings}
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    out_path.with_name("ratings_warnings.txt").write_text("\n".join(warnings) + ("\n" if warnings else ""), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh cached external Scrabble ratings.")
    parser.add_argument("--external-ids", required=True, type=Path)
    parser.add_argument("--players-json", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    warnings: list[str] = []
    updated_at = datetime.now().replace(microsecond=0).isoformat()

    try:
        external_ids = read_external_ids(args.external_ids)
        players = read_players(args.players_json)
        naspa_index, wgpo_index = preload_sources(warnings)

        ratings = []
        for player in players:
            display_name = norm_space(player.get("display_name"))
            external_id = external_ids.get(player_key(display_name))
            if not external_id:
                warnings.append(f'Mapping: No external ID row for "{display_name}"; used names as fallback')
            ratings.append(refresh_player(player, external_id, naspa_index, wgpo_index, updated_at, warnings))

        payload = {"ratings": ratings, "warnings": warnings}
        args.out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        args.out.with_name("ratings_warnings.txt").write_text(
            "\n".join(warnings) + ("\n" if warnings else ""),
            encoding="utf-8",
        )
        print(f"Wrote {args.out} with {len(ratings)} rating rows and {len(warnings)} warnings.")
        return 0
    except Exception as exc:
        warnings.append(f"ratings_refresh.py failed: {exc}")
        write_empty_payload(args.out, warnings)
        print(f"WARNING: ratings refresh failed; wrote empty {args.out}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
