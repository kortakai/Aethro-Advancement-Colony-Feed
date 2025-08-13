# Aethro Advancement & Colony Feed

Real-time MySQL feed for Minecraft server activity — capturing player advancements and MineColonies building progress. Syncs events from Minecraft log tailing and NBT file scanning into a deduplicated feed for use on websites, dashboards, or notifications. Got questions? Visit our discord at https://discord.gg/aethro

## Features

- **Advancement tracking** – Watches server logs (`latest.log` / `debug.log`) for advancements and writes them to MySQL without duplicates.
- **MineColonies event detection** – Reads colony `.dat` NBT files directly to detect newly built structures and level-ups, no log spam required.
- **Duplicate prevention** – Uses database checks and a persisted `colony_state.json` to avoid logging the same event multiple times.
- **Restart-safe** – Handles log rotation, reconnects to MySQL, and uses UTC-safe timestamps.

## Requirements

- Python 3
- [pymysql](https://pypi.org/project/PyMySQL/)
- [nbtlib](https://pypi.org/project/nbtlib/)
- MySQL (or compatible) database

## Installation

```bash
git clone <repo-url> aethro-adv-feed
cd aethro-adv-feed
pip install pymysql nbtlib
Edit adv_feed.py to set your DB_HOST, DB_USER, DB_PASS, MC_ROOT, and LOG_PATHS.

