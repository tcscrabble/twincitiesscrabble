# \# AGENTS.md

# 

# \# Twin Cities Scrabble Repository Guide

# 

# This repository powers the Twin Cities Scrabble website and statistics system.

# 

# Primary goals:

# \- publish club standings and player statistics

# \- import and validate club spreadsheet data

# \- preserve historical data integrity

# \- minimize manual maintenance burden

# \- avoid risky architectural rewrites

# 

# This repository intentionally uses a lightweight architecture:

# \- static frontend

# \- Cloudflare Pages Functions backend

# \- Cloudflare D1 database

# \- Python-based import pipeline

# 

# Agents should prefer conservative, surgical edits over broad refactors.

# 

# \---

# 

# \# High-Level Architecture

# 

# \## Frontend

# 

# Primary frontend file:

# \- `index.html`

# 

# The frontend is intentionally implemented as:

# \- one large HTML file

# \- embedded CSS

# \- embedded JavaScript

# 

# Do NOT:

# \- migrate to React/Vue/etc.

# \- split into many frontend files

# \- introduce build tooling unless explicitly requested

# \- replace styling architecture

# 

# The frontend:

# \- calls backend API endpoints

# \- renders leaderboard and player pages

# \- controls filtering and navigation

# \- contains most layout and responsive behavior

# 

# Important:

# \- mobile layout behavior is intentional

# \- Scrabble-tile title layout is manually controlled

# \- CSS should usually be added near the bottom of the existing `<style>` block

# 

# \---

# 

# \## Backend

# 

# Backend uses:

# \- Cloudflare Pages Functions

# \- TypeScript

# \- D1 (SQLite-like database)

# 

# Key API files:

# \- `leaderboard.ts`

# \- `player.ts`

# 

# These endpoints are tightly coupled to frontend expectations.

# 

# Do NOT casually:

# \- rename API fields

# \- change JSON response shapes

# \- remove fields

# \- change sorting semantics

# 

# Always inspect frontend usage before changing backend output.

# 

# \---

# 

# \## Import Pipeline

# 

# Primary import logic:

# \- `make\_import\_payload.py`

# 

# Generated artifacts:

# \- `combined\_payload.json`

# \- `combined\_load.sql`

# 

# Database schema:

# \- `schema.sql`

# 

# Helper scripts:

# \- `generate\_load\_sql.py`

# \- `reset\_and\_reload.ps1`

# 

# The import pipeline contains significant business logic and validation rules.

# Changes here can silently corrupt historical statistics.

# 

# Proceed carefully.

# 

# \---

# 

# \# Critical Repository Rules

# 

# \## Generated Files

# 

# These files are generated artifacts and should NOT usually be edited manually:

# 

# \- `combined\_payload.json`

# \- `combined\_load.sql`

# \- mismatch reports

# \- generated import payloads

# 

# Preferred workflow:

# 1\. modify source logic

# 2\. regenerate artifacts

# 3\. inspect outputs

# 

# Do NOT hand-edit generated SQL unless explicitly requested.

# 

# \---

# 

# \## Preserve Historical Data Integrity

# 

# Historical statistics matter.

# 

# Avoid:

# \- changing canonical player identifiers

# \- changing `raw\_hash` logic casually

# \- changing visitor logic without reviewing leaderboard impact

# \- changing matching heuristics without understanding edge cases

# 

# Small import changes can affect:

# \- historical standings

# \- duplicate detection

# \- mismatch reporting

# \- player records

# \- visitor filtering

# 

# \---

# 

# \## Preserve Existing UI Design Patterns

# 

# This site intentionally has:

# \- a classic/static-web feel

# \- embedded styling

# \- manually controlled responsive behavior

# \- lightweight dependencies

# 

# Avoid:

# \- introducing frameworks

# \- introducing Tailwind

# \- introducing component systems

# \- large-scale CSS rewrites

# 

# New UI should visually match existing sections.

# 

# \---

# 

# \## Cloudflare Constraints

# 

# This repository uses:

# \- Cloudflare Pages

# \- Cloudflare Pages Functions

# \- D1

# 

# Important:

# \- D1 transaction support is limited

# \- avoid SQL transaction assumptions

# \- avoid introducing Durable Objects unless explicitly requested

# 

# Historically, Worker route conflicts caused production problems.

# 

# Do NOT:

# \- introduce standalone Workers

# \- introduce workers-autoconfig branches

# \- duplicate routing systems

# 

# unless explicitly requested.

# 

# \---

# 

# \# Repository Layout

# 

# \## Frontend

# 

# \### `index.html`

# Contains:

# \- homepage

# \- leaderboard rendering

# \- player rendering

# \- filters

# \- navigation

# \- embedded CSS

# \- embedded JS

# 

# This is the primary frontend source.

# 

# \---

# 

# \## Backend API

# 

# \### `leaderboard.ts`

# Responsible for:

# \- leaderboard aggregation

# \- attendance filtering

# \- visitor exclusion

# \- year filtering

# \- club filtering

# \- win percentage calculation

# 

# Frontend expects fields such as:

# \- `id`

# \- `name`

# \- `games`

# \- `wins`

# \- `losses`

# \- `ties`

# \- `avg\_score`

# \- `opp\_avg`

# \- `spread`

# \- `win\_pct`

# 

# Changing these fields requires frontend updates.

# 

# \---

# 

# \### `player.ts`

# Responsible for:

# \- player detail endpoint

# \- player statistics

# \- game history

# \- opponent linking

# \- year filtering

# 

# Historically fragile area:

# \- opponent links

# \- player IDs

# \- filtering synchronization

# 

# Always test thoroughly after edits.

# 

# \---

# 

# \## Import Pipeline

# 

# \### `make\_import\_payload.py`

# Primary source-of-truth import logic.

# 

# Responsibilities include:

# \- spreadsheet parsing

# \- player block detection

# \- canonical name mapping

# \- short-name expansion

# \- collision handling

# \- mismatch detection

# \- validation

# \- visitor handling

# \- payload generation

# 

# Most import business rules live here.

# 

# \---

# 

# \### `generate\_load\_sql.py`

# Generates SQL load scripts from payloads.

# 

# Should remain aligned with:

# \- schema

# \- payload structure

# \- import expectations

# 

# \---

# 

# \### `schema.sql`

# Database schema source-of-truth.

# 

# Schema changes require coordinated updates to:

# \- import pipeline

# \- backend APIs

# \- frontend rendering

# \- queries

# \- filtering logic

# 

# \---

# 

# \### `reset\_and\_reload.ps1`

# Convenience script for:

# \- downloading spreadsheets

# \- generating payloads

# \- regenerating SQL

# \- resetting/reloading D1

# 

# Useful for full validation cycles.

# 

# \---

# 

# \# Import Pipeline Expectations

# 

# \## Spreadsheet Structure

# 

# Source spreadsheets contain:

# \- player blocks

# \- varying name formats

# \- optional short codes

# \- repeated sections

# \- future empty rows

# 

# Player blocks may appear in multiple formats.

# 

# The parser intentionally supports:

# \- CODE + Full Name

# \- Full Name + CODE

# \- split first/last names

# \- mixed formatting

# 

# Do NOT simplify parsing assumptions without reviewing all supported layouts.

# 

# \---

# 

# \## Canonical Name Logic

# 

# Canonicalization is extremely important.

# 

# The system uses:

# \- full names

# \- generated short names

# \- manual alias maps

# 

# Examples:

# \- `Bil B -> Bill Bigler`

# \- `Jason V -> Jason Vaysberg`

# 

# Collision handling is intentional.

# 

# Historically:

# \- some short names collide

# \- some players use inconsistent naming

# \- spreadsheets are not perfectly standardized

# 

# Do NOT remove collision detection.

# 

# \---

# 

# \## Visitor Logic

# 

# Visitors should:

# \- appear on player pages

# \- participate in game history

# \- count toward opponent statistics

# 

# Visitors should NOT:

# \- appear in leaderboard standings

# \- qualify as regular players

# 

# Regular-player threshold:

# \- 25% of possible games

# 

# This logic is intentional and important.

# 

# \---

# 

# \## Match Validation Logic

# 

# The import pipeline attempts to match:

# \- both sides of each reported game

# 

# Possible outcomes include:

# \- matched games

# \- mismatches

# \- no obvious match

# 

# The system intentionally supports:

# \- multiple same-day games between players

# \- asymmetric spreadsheet errors

# \- missing reciprocal entries

# 

# Do NOT assume:

# \- one game per opponent/date

# \- perfect spreadsheet consistency

# 

# \---

# 

# \## Date Validation

# 

# Expected weekdays:

# \- DAY club -> Monday

# \- NM club -> Thursday

# 

# Warnings are intentionally non-fatal because:

# \- holidays

# \- rescheduled sessions

# \- special events

# 

# exist.

# 

# \---

# 

# \# Frontend / Backend Coupling

# 

# This repository has strong coupling between:

# \- API responses

# \- frontend rendering

# \- filtering behavior

# \- sorting behavior

# 

# When changing backend fields:

# 1\. inspect frontend rendering

# 2\. inspect sorting

# 3\. inspect filtering

# 4\. inspect player pages

# 

# When changing frontend rendering:

# 1\. confirm backend fields exist

# 2\. confirm field names match exactly

# 3\. confirm null handling

# 

# Historically common failures:

# \- missing `id`

# \- broken opponent links

# \- renamed fields

# \- changed aggregation semantics

# 

# \---

# 

# \# Safe Editing Practices

# 

# \## Before Editing

# 

# Agents should:

# 1\. inspect related files first

# 2\. understand coupling

# 3\. avoid speculative rewrites

# 4\. preserve patterns already in use

# 

# \---

# 

# \## Prefer Surgical Edits

# 

# Preferred:

# \- minimal diffs

# \- focused fixes

# \- preserving architecture

# 

# Avoid:

# \- broad refactors

# \- formatting-only rewrites

# \- moving code unnecessarily

# \- replacing working systems

# 

# \---

# 

# \## Frontend Changes

# 

# After frontend changes, verify:

# \- homepage renders

# \- leaderboard renders

# \- player pages render

# \- filters still work

# \- mobile layout still works

# \- tile title layout still works

# 

# \---

# 

# \## Backend Changes

# 

# After backend changes, verify:

# \- API returns valid JSON

# \- filtering still works

# \- year handling still works

# \- club handling still works

# \- frontend still renders correctly

# 

# \---

# 

# \## Import Changes

# 

# After import changes, verify:

# \- payload generation succeeds

# \- SQL generation succeeds

# \- reload succeeds

# \- mismatch reporting still works

# \- visitor logic still works

# \- duplicate handling still works

# 

# Inspect outputs before deployment.

# 

# \---

# 

# \# Known Historical Failure Modes

# 

# These issues have occurred previously.

# 

# Agents should avoid reintroducing them.

# 

# \## Frontend/API Mismatches

# Examples:

# \- frontend expecting fields not returned by API

# \- renamed JSON properties

# \- missing `id`

# 

# \---

# 

# \## Opponent Link Failures

# Player-page opponent links historically broke due to:

# \- incorrect player IDs

# \- inconsistent query behavior

# \- frontend/backend mismatch

# 

# \---

# 

# \## Visitor Leakage

# Visitors accidentally appeared in leaderboards when:

# \- attendance filtering changed

# \- visitor filtering changed

# \- cross-club logic changed

# 

# \---

# 

# \## Tie Handling Bugs

# Historically:

# \- ties were omitted

# \- ties displayed incorrectly

# \- all games accidentally displayed as ties

# 

# \---

# 

# \## Duplicate Import Problems

# Changing matching or hashing logic can create:

# \- duplicate rows

# \- raw\_hash conflicts

# \- inconsistent history

# 

# \---

# 

# \## Cloudflare Routing Conflicts

# Historically:

# \- Worker routes conflicted with Pages Functions

# \- API behavior diverged unexpectedly

# 

# Avoid introducing parallel routing systems.

# 

# \---

# 

# \# Standard Workflows

# 

# \## Full Reload Workflow

# 

# Typical workflow:

# 1\. download latest spreadsheets

# 2\. generate payload

# 3\. generate SQL

# 4\. apply schema if needed

# 5\. reload D1

# 6\. verify leaderboard

# 7\. verify player pages

# 

# \---

# 

# \## Schema Change Workflow

# 

# When changing schema:

# 1\. update `schema.sql`

# 2\. update import generation

# 3\. update backend queries

# 4\. update frontend rendering

# 5\. perform full reload

# 6\. validate outputs carefully

# 

# \---

# 

# \## Leaderboard Change Workflow

# 

# When changing leaderboard behavior:

# 1\. inspect `leaderboard.ts`

# 2\. inspect frontend rendering

# 3\. inspect sorting logic

# 4\. verify mobile rendering

# 5\. verify player page consistency

# 

# \---

# 

# \# Preferred Agent Behavior

# 

# Agents should:

# \- preserve architecture

# \- preserve visual style

# \- preserve API compatibility

# \- preserve import semantics

# \- explain risky changes before implementing

# 

# Agents should NOT:

# \- rewrite working systems

# \- introduce frameworks

# \- aggressively reorganize files

# \- simplify validation logic without understanding edge cases

# 

# When uncertain:

# \- inspect more files

# \- ask clarifying questions

# \- prefer conservative behavior

# 

# This repository prioritizes:

# \- correctness

# \- historical consistency

# \- maintainability

# \- operational stability

# 

##### over architectural purity.

