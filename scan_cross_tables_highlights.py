import argparse
import csv
import html
import json
import math
import os
import re
import smtplib
import sys
from dataclasses import dataclass, field
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

CROSS_TABLES_HOME_URL = "https://www.cross-tables.com/"
CROSS_TABLES_TOURNEY_URL = "https://www.cross-tables.com/tourney.php?tourneyid={tourney_id}"
HTTP_TIMEOUT_SECONDS = 20


@dataclass
class LocalPlayer:
    display_name: str
    name_keys: set[str] = field(default_factory=set)
    cross_tables_ids: set[str] = field(default_factory=set)
    cross_tables_url: str | None = None


@dataclass
class ResultRow:
    tourney_id: str
    tournament_name: str
    tournament_date: str | None
    url: str
    division: str
    place: int
    player_name: str
    player_id: str | None
    wins: float
    losses: float | None
    ties: float | None
    spread: int | None
    old_rating: int | None
    new_rating: int | None

    @property
    def rating_gain(self) -> int | None:
        if self.old_rating is None or self.new_rating is None:
            return None
        if self.old_rating <= 0:
            return None
        return self.new_rating - self.old_rating


def norm_space(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def norm_lookup_name(value: Any) -> str:
    value = html.unescape(norm_space(value)).lower()
    value = re.sub(r"[^a-z0-9 ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def nullable_int(value: Any) -> int | None:
    value = norm_space(value).replace(",", "")
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def nullable_float(value: Any) -> float | None:
    value = norm_space(value)
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def strip_tags(value: str) -> str:
    value = re.sub(r"<br\s*/?>", " ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    return norm_space(html.unescape(value).replace("\xa0", " "))


def fetch_text(url: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 TwinCitiesScrabbleHighlightScanner/1.0",
        },
    )
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        raw = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="replace")


def extract_cross_tables_id(value: Any) -> str | None:
    text = norm_space(value)
    if not text:
        return None
    match = re.search(r"[?&](?:p|playerid)=(\d+)", text)
    if match:
        return match.group(1)
    if re.fullmatch(r"\d+", text):
        return text
    return None


def display_name_keys(display_name: str) -> set[str]:
    keys = {norm_lookup_name(display_name)}
    parts = [p for p in norm_lookup_name(display_name).split(" ") if p]
    if len(parts) >= 2:
        keys.add(" ".join([parts[-1], *parts[:-1]]))
    return {key for key in keys if key}


def add_local_player(players_by_name: dict[str, LocalPlayer], player: LocalPlayer) -> None:
    for key in player.name_keys:
        existing = players_by_name.get(key)
        if existing and existing.display_name != player.display_name:
            existing.name_keys.discard(key)
            players_by_name[key] = LocalPlayer(display_name="", name_keys={key})
        elif not existing:
            players_by_name[key] = player


def read_external_ids(path: Path | None) -> dict[str, dict[str, str]]:
    if not path or not path.exists():
        return {}

    rows: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            display_name = norm_space(row.get("display_name"))
            if not display_name:
                continue
            rows[norm_lookup_name(display_name)] = {k: norm_space(v) for k, v in row.items()}
    return rows


def read_local_players(players_json: Path, external_ids_path: Path | None, warnings: list[str]) -> tuple[dict[str, LocalPlayer], dict[str, LocalPlayer]]:
    data = json.loads(players_json.read_text(encoding="utf-8"))
    external_ids = read_external_ids(external_ids_path)
    players_by_name: dict[str, LocalPlayer] = {}
    players_by_cross_tables_id: dict[str, LocalPlayer] = {}
    local_players: list[LocalPlayer] = []

    for raw_player in data.get("players", []):
        display_name = norm_space(raw_player.get("display_name"))
        if not display_name:
            continue
        player = LocalPlayer(display_name=display_name)
        player.name_keys.update(display_name_keys(display_name))

        naspa_name = norm_space(raw_player.get("naspa_name"))
        if naspa_name:
            player.name_keys.add(norm_lookup_name(naspa_name))

        cross_tables_url = norm_space(raw_player.get("cross_tables_url"))
        if cross_tables_url:
            player.cross_tables_url = cross_tables_url
            cross_tables_id = extract_cross_tables_id(cross_tables_url)
            if cross_tables_id:
                player.cross_tables_ids.add(cross_tables_id)

        external_row = external_ids.get(norm_lookup_name(display_name))
        if external_row:
            cross_tables_id = extract_cross_tables_id(external_row.get("cross_tables_id"))
            if cross_tables_id:
                player.cross_tables_ids.add(cross_tables_id)
            cross_tables_id = extract_cross_tables_id(external_row.get("cross_tables_url"))
            if cross_tables_id:
                player.cross_tables_ids.add(cross_tables_id)
            if external_row.get("cross_tables_url") and not player.cross_tables_url:
                player.cross_tables_url = external_row["cross_tables_url"]

        local_players.append(player)

    for player in local_players:
        add_local_player(players_by_name, player)
        for cross_tables_id in player.cross_tables_ids:
            existing = players_by_cross_tables_id.get(cross_tables_id)
            if existing and existing.display_name != player.display_name:
                warnings.append(
                    f'Cross-tables ID {cross_tables_id} is mapped to both "{existing.display_name}" and "{player.display_name}"; ID match skipped'
                )
                players_by_cross_tables_id.pop(cross_tables_id, None)
            else:
                players_by_cross_tables_id[cross_tables_id] = player

    return players_by_name, players_by_cross_tables_id


def parse_recent_tournaments(home_html: str, max_tournaments: int) -> list[dict[str, str]]:
    block_match = re.search(r"<div id='rtblock'[^>]*>(?P<block>.*?)</table>", home_html, re.S | re.I)
    block = block_match.group("block") if block_match else home_html
    tournaments: list[dict[str, str]] = []

    row_pattern = re.compile(
        r"<tr[^>]*id='rowrecent\d+'[^>]*>.*?<td[^>]*>(?P<date>.*?)</td>.*?"
        r"<a href='tourney\.php\?tourneyid=(?P<id>\d+)'>(?P<title>.*?)</a>",
        re.S | re.I,
    )
    for match in row_pattern.finditer(block):
        tournaments.append(
            {
                "id": match.group("id"),
                "date": strip_tags(match.group("date")),
                "title": strip_tags(match.group("title")),
                "url": CROSS_TABLES_TOURNEY_URL.format(tourney_id=match.group("id")),
            }
        )
        if len(tournaments) >= max_tournaments:
            break

    return tournaments


def parse_tournament_label(tourney_html: str, fallback: dict[str, str]) -> tuple[str, str | None]:
    title_match = re.search(r"<title>(?P<title>.*?)\s+-\s+Tournament Results", tourney_html, re.S | re.I)
    title = strip_tags(title_match.group("title")) if title_match else fallback.get("title", "Cross-tables tournament")

    date_match = re.search(r"class='datalabel'.*?<br>(?P<date>[^<]+)<br>", tourney_html, re.S | re.I)
    date = strip_tags(date_match.group("date")) if date_match else fallback.get("date")
    return title, date


def parse_tournament_rows(tourney_id: str, tourney_html: str, fallback: dict[str, str]) -> list[ResultRow]:
    tournament_name, tournament_date = parse_tournament_label(tourney_html, fallback)
    url = CROSS_TABLES_TOURNEY_URL.format(tourney_id=tourney_id)
    rows: list[ResultRow] = []

    table_pattern = re.compile(
        r"<table id='division(?P<div>\d+)'[^>]*>.*?<tbody>(?P<body>.*?)</tbody>.*?</table>",
        re.S | re.I,
    )
    for table_match in table_pattern.finditer(tourney_html):
        division = table_match.group("div")
        body = table_match.group("body")
        for row_match in re.finditer(r"<tr class='row\d+\s*'[^>]*>(?P<row>.*?)</tr>", body, re.S | re.I):
            row_html = row_match.group("row")
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.S | re.I)
            if len(cells) < 12:
                continue
            player_id_match = re.search(r"results\.php\?p=(\d+)", row_html)
            rows.append(
                ResultRow(
                    tourney_id=tourney_id,
                    tournament_name=tournament_name,
                    tournament_date=tournament_date,
                    url=url,
                    division=division,
                    place=nullable_int(strip_tags(cells[1])) or 0,
                    player_name=strip_tags(cells[3]),
                    player_id=player_id_match.group(1) if player_id_match else None,
                    wins=nullable_float(strip_tags(cells[4])) or 0,
                    losses=nullable_float(strip_tags(cells[5])),
                    ties=nullable_float(strip_tags(cells[6])),
                    spread=nullable_int(strip_tags(cells[8])),
                    old_rating=nullable_int(strip_tags(cells[9])),
                    new_rating=nullable_int(strip_tags(cells[11])),
                )
            )

    return [row for row in rows if row.place > 0 and row.player_name]


def find_local_player(
    row: ResultRow,
    players_by_name: dict[str, LocalPlayer],
    players_by_cross_tables_id: dict[str, LocalPlayer],
    warnings: list[str],
) -> LocalPlayer | None:
    if row.player_id and row.player_id in players_by_cross_tables_id:
        return players_by_cross_tables_id[row.player_id]

    key = norm_lookup_name(row.player_name)
    match = players_by_name.get(key)
    if match and match.display_name:
        return match
    if match and not match.display_name:
        warnings.append(f'Cross-tables: Ambiguous local name match for "{row.player_name}"; skipped')
    return None


def ordinal(value: int) -> str:
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def evaluate_highlights(
    rows: list[ResultRow],
    players_by_name: dict[str, LocalPlayer],
    players_by_cross_tables_id: dict[str, LocalPlayer],
    warnings: list[str],
) -> list[dict[str, Any]]:
    highlights: list[dict[str, Any]] = []
    rows_by_division: dict[str, list[ResultRow]] = {}
    for row in rows:
        rows_by_division.setdefault(row.division, []).append(row)

    for division_rows in rows_by_division.values():
        division_size = len(division_rows)
        if not division_size:
            continue
        top_25_cutoff = math.ceil(division_size * 0.25) if division_size >= 8 else 0
        max_wins = max(row.wins for row in division_rows)
        max_wins_count = sum(1 for row in division_rows if row.wins == max_wins)

        for row in division_rows:
            local_player = find_local_player(row, players_by_name, players_by_cross_tables_id, warnings)
            if not local_player:
                continue

            reasons: list[str] = []
            if row.place <= 3:
                reasons.append(f"finished {ordinal(row.place)} in Division {row.division}")
            if top_25_cutoff and row.place <= top_25_cutoff:
                reasons.append(f"top 25% of Division {row.division} ({row.place} of {division_size})")
            if row.rating_gain is not None and row.rating_gain >= 50:
                reasons.append(f"gained {row.rating_gain:+d} rating points")
            if row.wins == max_wins and max_wins_count == 1:
                reasons.append(f"won Division {row.division} outright")

            if not reasons:
                continue

            highlights.append(
                {
                    "display_name": local_player.display_name,
                    "cross_tables_name": row.player_name,
                    "cross_tables_id": row.player_id,
                    "tourney_id": row.tourney_id,
                    "tournament_name": row.tournament_name,
                    "tournament_date": row.tournament_date,
                    "division": row.division,
                    "division_size": division_size,
                    "place": row.place,
                    "wins": row.wins,
                    "losses": row.losses,
                    "ties": row.ties,
                    "spread": row.spread,
                    "old_rating": row.old_rating,
                    "new_rating": row.new_rating,
                    "rating_gain": row.rating_gain,
                    "url": row.url,
                    "reasons": list(dict.fromkeys(reasons)),
                }
            )

    return highlights


def format_record(record: dict[str, Any]) -> str:
    record_bits = [format_count(record["wins"], "win")]
    if record.get("losses") is not None:
        record_bits.append(format_count(record["losses"], "loss", "losses"))
    if record.get("ties"):
        record_bits.append(format_count(record["ties"], "tie"))

    rating_text = ""
    if record.get("old_rating") is not None and record.get("new_rating") is not None:
        gain = record.get("rating_gain")
        gain_text = f" ({gain:+d})" if gain is not None else ""
        rating_text = f", rating {record['old_rating']} -> {record['new_rating']}{gain_text}"

    date_text = f" ({record['tournament_date']})" if record.get("tournament_date") else ""
    return (
        f"- {record['display_name']} - {record['tournament_name']}{date_text}, "
        f"Division {record['division']}: {ordinal(record['place'])} of {record['division_size']}, "
        f"{', '.join(record_bits)}, spread {record.get('spread') if record.get('spread') is not None else 'n/a'}"
        f"{rating_text}. Reasons: {', '.join(record['reasons'])}. {record['url']}"
    )


def format_count(value: float, singular: str, plural: str | None = None) -> str:
    plural = plural or f"{singular}s"
    word = singular if value == 1 else plural
    return f"{value:g} {word}"


def build_report(payload: dict[str, Any]) -> str:
    lines = [
        "Cross-tables local highlights",
        f"Scanned at: {payload['scanned_at']}",
        f"Tournaments scanned: {len(payload['tournaments_scanned'])}",
        "",
    ]

    highlights = payload.get("highlights", [])
    if highlights:
        lines.append("Highlights:")
        for record in highlights:
            lines.append(format_record(record))
    else:
        lines.append("No local highlights found in the scanned recent Cross-tables tournaments.")

    warnings = payload.get("warnings", [])
    if warnings:
        lines.extend(["", "Warnings:"])
        lines.extend(f"- {warning}" for warning in warnings)

    return "\n".join(lines) + "\n"


def date_sort_value(value: Any) -> int:
    text = norm_space(value)
    for fmt in ("%m/%d/%y", "%B %d, %Y"):
        try:
            return int(datetime.strptime(text, fmt).strftime("%Y%m%d"))
        except ValueError:
            pass
    return 0


def division_sort_value(value: Any) -> int:
    parsed = nullable_int(value)
    return parsed if parsed is not None else 999


def email_subject(highlight_count: int) -> str:
    if highlight_count == 1:
        return "Twin Cities Scrabble Cross-tables highlight: 1 local result"
    return f"Twin Cities Scrabble Cross-tables highlights: {highlight_count} local results"


def write_email_draft(path: Path, recipients: list[str], subject: str, body: str, from_addr: str) -> None:
    message = EmailMessage()
    message["From"] = from_addr
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject
    message.set_content(body)
    path.write_text(message.as_string(), encoding="utf-8")


def send_email(recipients: list[str], subject: str, body: str, report_path: Path, warnings: list[str]) -> None:
    if not recipients:
        return

    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    from_addr = os.environ.get("SMTP_FROM") or smtp_user or "twincitiesscrabble@example.com"
    draft_path = report_path.with_name("cross_tables_highlights_email.eml")

    if not smtp_host:
        warnings.append(
            "Email: SMTP_HOST is not set; wrote cross_tables_highlights_email.eml instead of sending"
        )
        write_email_draft(draft_path, recipients, subject, body, from_addr)
        return

    message = EmailMessage()
    message["From"] = from_addr
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject
    message.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=HTTP_TIMEOUT_SECONDS) as smtp:
            if os.environ.get("SMTP_STARTTLS", "1").lower() not in {"0", "false", "no"}:
                smtp.starttls()
            if smtp_user and smtp_password:
                smtp.login(smtp_user, smtp_password)
            smtp.send_message(message)
    except Exception as exc:
        warnings.append(f"Email: Could not send highlight email ({exc}); wrote cross_tables_highlights_email.eml")
        write_email_draft(draft_path, recipients, subject, body, from_addr)


def scan(args: argparse.Namespace) -> dict[str, Any]:
    warnings: list[str] = []
    scanned_at = datetime.now().replace(microsecond=0).isoformat()
    players_by_name, players_by_cross_tables_id = read_local_players(
        args.players_json,
        args.external_ids,
        warnings,
    )
    tournaments: list[dict[str, str]] = []
    highlights: list[dict[str, Any]] = []

    try:
        home_html = fetch_text(args.home_url)
        tournaments = parse_recent_tournaments(home_html, args.max_tournaments)
        if not tournaments:
            warnings.append("Cross-tables: Could not find recent tournaments on the home page")
    except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
        warnings.append(f"Cross-tables: Site unavailable; skipped highlight scan ({exc})")

    for tournament in tournaments:
        try:
            tourney_html = fetch_text(tournament["url"])
            rows = parse_tournament_rows(tournament["id"], tourney_html, tournament)
            if not rows:
                warnings.append(f"Cross-tables: No parseable result rows for {tournament['title']} ({tournament['id']})")
                continue
            highlights.extend(evaluate_highlights(rows, players_by_name, players_by_cross_tables_id, warnings))
        except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
            warnings.append(f"Cross-tables: Could not scan {tournament['title']} ({tournament['id']}): {exc}")

    highlights.sort(
        key=lambda h: (
            -date_sort_value(h.get("tournament_date")),
            h.get("tournament_name") or "",
            division_sort_value(h.get("division")),
            h.get("place") or 999,
            h.get("display_name") or "",
        )
    )
    return {
        "scanned_at": scanned_at,
        "tournaments_scanned": tournaments,
        "highlights": highlights,
        "warnings": warnings,
    }


def write_outputs(payload: dict[str, Any], out_path: Path, report_path: Path, warnings_path: Path) -> None:
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    report_path.write_text(build_report(payload), encoding="utf-8")
    warnings = payload.get("warnings", [])
    warnings_path.write_text("\n".join(warnings) + ("\n" if warnings else ""), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan recent Cross-tables tournaments for local player highlights.")
    parser.add_argument("--players-json", required=True, type=Path)
    parser.add_argument("--external-ids", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--warnings", required=True, type=Path)
    parser.add_argument("--email-to", action="append", default=[])
    parser.add_argument("--home-url", default=CROSS_TABLES_HOME_URL)
    parser.add_argument("--max-tournaments", type=int, default=10)
    args = parser.parse_args()

    try:
        payload = scan(args)
        report = build_report(payload)
        send_email(
            args.email_to,
            email_subject(len(payload.get("highlights", []))),
            report,
            args.report,
            payload["warnings"],
        )
        write_outputs(payload, args.out, args.report, args.warnings)
        print(
            f"Wrote {args.out} with {len(payload.get('highlights', []))} highlights "
            f"and {len(payload.get('warnings', []))} warnings."
        )
    except Exception as exc:
        warnings = [f"scan_cross_tables_highlights.py failed: {exc}"]
        payload = {
            "scanned_at": datetime.now().replace(microsecond=0).isoformat(),
            "tournaments_scanned": [],
            "highlights": [],
            "warnings": warnings,
        }
        write_outputs(payload, args.out, args.report, args.warnings)
        print(f"WARNING: Cross-tables highlight scan failed but wrote empty outputs: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
