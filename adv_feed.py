#!/usr/bin/env python3
import os, time, re, glob, json, traceback
import pymysql
import nbtlib
from nbtlib.tag import Compound, List, String

# =========================
# Config
# =========================
DB_HOST = "localhost"
DB_NAME = "playersync"
DB_USER = "playersync"
DB_PASS = "test1"

LOG_OUT = "/opt/aethro-advfeed/adv_feed.log"
STATE_FILE = "/opt/aethro-advfeed/colony_state.json"

START_AT_END = True
MC_ROOT = "/home/amp/.ampdata/instances/ShadowsofAethro02/Minecraft"
WORLD_DIR = os.path.join(MC_ROOT, "shadow")

LOG_PATHS = [
    os.path.join(MC_ROOT, "logs", "latest.log"),
    os.path.join(MC_ROOT, "logs", "debug.log"),
]

COLONY_DIR = os.path.join(WORLD_DIR, "minecolonies", "minecraft", "overworld")
USERCACHE = os.path.join(WORLD_DIR, "usercache.json")

COLONY_MAP_REFRESH = 60          # seconds
COLONY_SCAN_INTERVAL = 30        # seconds

# =========================
# Regex
# =========================
ADV_PAT = re.compile(
    r'\]:\s*(?:\[[^\]]+\]\s*)*(?P<player>[A-Za-z0-9_]{1,16})\s+has\s+'
    r'(?:made|completed|reached|earned)\s+(?:the\s+)?(?:advancement|challenge|goal)\s+\[(?P<title>[^\]]+)\]',
    re.IGNORECASE
)

MC_DEBUG_PAT = re.compile(
    r'colony\s+(?P<cid>\d+)\s*-\s*'
    r'(?:(?P<action>new|finished|constructed|upgraded|built|created)\s+)?'
    r'building(?:\s+(?P<bclass>[.\w]+))?'
    r'(?:\s+for\s+Block\{(?P<block>[\w:]+)\})?'
    r'(?:\s+at\s+BlockPos\{x=(?P<x>-?\d+),\s*y=(?P<y>-?\d+),\s*z=(?P<z>-?\d+)\})?',
    re.IGNORECASE
)

# =========================
# Utils
# =========================
def log(msg: str):
    try:
        with open(LOG_OUT, "a", encoding="utf-8") as f:
            f.write(msg.rstrip() + "\n")
    except Exception:
        pass
    print(msg, flush=True)

def db():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )

def ensure_tables(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS mc_advancements(
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      player VARCHAR(32) NOT NULL,
      title  VARCHAR(128) NOT NULL
    ) ENGINE=InnoDB;""")
    cur.execute("""CREATE TABLE IF NOT EXISTS mc_colony_events(
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      colony_id INT NULL,
      colony_name VARCHAR(128) NULL,
      owner VARCHAR(32) NULL,
      action VARCHAR(32) NULL,
      building_id VARCHAR(128) NULL,
      building_name VARCHAR(128) NULL,
      block_id VARCHAR(128) NULL,
      x INT NULL, y INT NULL, z INT NULL,
      raw_text TEXT NOT NULL
    ) ENGINE=InnoDB;""")

def open_tail(path):
    f = open(path, "r", encoding="utf-8", errors="ignore")
    f.seek(0, os.SEEK_END if START_AT_END else os.SEEK_SET)
    return f, os.fstat(f.fileno()).st_ino, f.tell()

def humanize(s: str) -> str:
    s = str(s).strip().replace("_", " ")
    return " ".join(w.capitalize() for w in re.split(r"[\s\-]+", s) if w)

# =========================
# Colony names and owners
# =========================
def _uuid_to_name_map():
    m = {}
    try:
        with open(USERCACHE, "r", encoding="utf-8") as f:
            for e in json.load(f):
                if "uuid" in e and "name" in e:
                    m[e["uuid"].lower()] = e["name"]
    except Exception:
        pass
    return m

def load_colony_map():
    out = {}
    if not os.path.isdir(COLONY_DIR):
        return out
    uuid_to_name = _uuid_to_name_map()
    id_pat = re.compile(r'colony[-_]*?(\d+)', re.IGNORECASE)

    for p in glob.glob(os.path.join(COLONY_DIR, "colony*")):
        if not os.path.isfile(p):
            continue
        m = id_pat.search(os.path.basename(p))
        if not m:
            continue
        cid = int(m.group(1))
        try:
            n = nbtlib.load(p)
            root = n.root if hasattr(n, "root") else n
        except Exception:
            continue

        name = ""
        for k in ("Name","name","colonyName","colonyname"):
            if k in root:
                name = str(root[k]); break

        owner = ""
        cand = None
        for k in ("Owner","owner","Mayor","mayor","colonyOwner","colonyowner"):
            if k in root:
                cand = root[k]; break
        if isinstance(cand, String):
            owner = str(cand)
        elif isinstance(cand, Compound):
            for sub in ("name","Name","ownerName","playerName","UUID","uuid","Id","id"):
                if sub in cand:
                    owner = str(cand[sub]); break
        elif isinstance(cand, str):
            owner = cand
        if owner and len(owner) >= 32 and owner.count("-") >= 4:
            owner = uuid_to_name.get(owner.lower(), owner)

        out[cid] = {"name": name.strip(), "owner": owner.strip()}

    return out

# =========================
# MineColonies NBT building scan
# =========================
def pretty_building_from_hint(s: str) -> str:
    s = str(s)
    s_low = s.lower()
    def hum(x): return " ".join(w.capitalize() for w in x.replace("_"," ").split())
    m = re.search(r"minecolonies:blockhut([a-z0-9_]+)", s_low)
    if m:
        tail = m.group(1)
        return hum(tail) + ("" if tail.endswith("hut") else " Hut")
    m = re.search(r"minecolonies:([a-z0-9_]+)", s_low)
    if m:
        return hum(m.group(1))
    m = re.search(r"com\.minecolonies\.building\.([a-zA-Z0-9_]+)", s)
    if m:
        return hum(m.group(1))
    for k in ("citizen hut","builder","warehouse","cook","sawmill","lumberjack","residence",
              "town hall","barracks","guard","library","university","plantation","miner","fisher"):
        if k in s_low:
            return hum(k)
    return ""

def _deep_strings(tag, limit=64):
    out = []
    def walk(t):
        nonlocal out
        if len(out) >= limit: return
        if isinstance(t, String):
            out.append(str(t))
        elif isinstance(t, Compound):
            for k in t.keys():
                walk(t[k])
        elif isinstance(t, List):
            for v in t:
                walk(v)
        elif isinstance(t, (str, bytes)):
            out.append(t.decode() if isinstance(t, bytes) else str(t))
    walk(tag)
    return out

def extract_buildings_from_root(root) -> list:
    def pos_of(comp: Compound):
        x = comp.get("x"); y = comp.get("y"); z = comp.get("z")
        if x is not None and y is not None and z is not None:
            try: return int(x), int(y), int(z)
            except Exception: pass
        loc = comp.get("location")
        if isinstance(loc, dict):
            try: return int(loc.get("x",0)), int(loc.get("y",0)), int(loc.get("z",0))
            except Exception: pass
        return None

    def lvl_of(comp: Compound):
        for k in ("buildingLevel","level","Level"):
            if k in comp:
                try: return int(comp[k])
                except Exception: pass
        return 0

    def pick_name(comp: Compound) -> str:
        for k in ("buildingName","name","hut","style","schematicName","schematic","blueprint","type","building","class","blockId","block"):
            if k in comp:
                n = pretty_building_from_hint(comp[k])
                if n: return n
        for s in _deep_strings(comp, limit=64):
            n = pretty_building_from_hint(s)
            if n: return n
        return "Building"

    candidates = []

    for key in ("buildings","Buildings","buildingList","BuildingList"):
        if key in root and isinstance(root[key], List):
            for it in root[key]:
                if isinstance(it, Compound):
                    if pos_of(it): candidates.append(it)

    def walk(tag):
        if isinstance(tag, Compound):
            keys = set(tag.keys())
            if ({"x","y","z"}.issubset(keys) or "location" in keys):
                candidates.append(tag)
            for k in keys: walk(tag[k])
        elif isinstance(tag, List):
            for v in tag: walk(v)
    walk(root)

    best = {}
    for comp in candidates:
        pos = pos_of(comp)
        if not pos: continue
        x,y,z = pos
        level = lvl_of(comp)
        name = pick_name(comp)
        bclass = str(comp.get("buildingId") or comp.get("building") or comp.get("class") or "")
        block  = str(comp.get("blockId") or comp.get("block") or "")
        key = (x,y,z)
        item = {"x":x,"y":y,"z":z,"level":level,"building_id":bclass,"block_id":block,"building_name":name}
        if key not in best or level > best[key]["level"]:
            best[key] = item

    return list(best.values())

def load_colony_build_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_colony_build_state(state: dict):
    tmp = STATE_FILE + ".tmp"
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)

def _insert_colony(cur, cid, colony_map, action, building_id, building_name, block_id, x, y, z, raw):
    info = colony_map.get(cid, {}) if isinstance(colony_map, dict) else {}
    colony_name = info.get("name")
    owner = info.get("owner")

    cur.execute("""SELECT 1 FROM mc_colony_events
                   WHERE colony_id=%s AND action=%s AND building_name=%s AND x=%s AND y=%s AND z=%s
                   ORDER BY ts DESC LIMIT 1""",
                (cid, action, building_name, x, y, z))
    if cur.fetchone():
        return

    try:
        cur.execute(
            """INSERT INTO mc_colony_events
               (colony_id,colony_name,owner,action,building_id,building_name,block_id,x,y,z,raw_text)
               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (cid, colony_name, owner, action, building_id, building_name, block_id, x, y, z, raw)
        )
        log(f"Inserted COLONY (NBT/log): #{cid} {action} {building_name} @{x},{y},{z} owner={owner or '-'}")
    except Exception:
        log("DB error colony insert:\n" + traceback.format_exc())

def scan_colony_completions(cur, colony_map):
    state = load_colony_build_state()
    id_pat = re.compile(r'colony[-_]*?(\d+)', re.IGNORECASE)
    changed = 0

    for p in glob.glob(os.path.join(COLONY_DIR, "colony*")):
        if not os.path.isfile(p): continue
        m = id_pat.search(os.path.basename(p))
        if not m: continue
        cid = m.group(1)

        try:
            n = nbtlib.load(p)
            root = n.root if hasattr(n, "root") else n
        except Exception:
            continue

        buildings = extract_buildings_from_root(root)
        prev = state.get(cid, {})
        curr = {}

        for b in buildings:
            key = f"{b['x']},{b['y']},{b['z']}"
            curr[key] = {"level": int(b["level"]),
                         "name": b["building_name"],
                         "bid": b["building_id"],
                         "block": b["block_id"]}

            prev_entry = prev.get(key)
            if prev_entry is None and b["level"] >= 1:
                _insert_colony(cur, cid=int(cid), colony_map=colony_map, action="constructed",
                               building_id=b["building_id"], building_name=b["building_name"], block_id=b["block_id"],
                               x=b["x"], y=b["y"], z=b["z"],
                               raw=f"NBT constructed {b['building_name']} L{b['level']} @ {key}")
                changed += 1
            elif prev_entry is not None and int(b["level"]) > int(prev_entry.get("level", 0)):
                _insert_colony(cur, cid=int(cid), colony_map=colony_map, action=f"level_{int(b['level'])}",
                               building_id=b["building_id"], building_name=b["building_name"], block_id=b["block_id"],
                               x=b["x"], y=b["y"], z=b["z"],
                               raw=f"NBT level-up {b['building_name']} -> L{b['level']} @ {key}")
                changed += 1

        state[cid] = curr

    if changed:
        save_colony_build_state(state)
        log(f"NBT colony scan: {changed} new events")

# =========================
# Advancements via logs
# =========================
def already_logged_adv(cur, player, title):
    cur.execute("SELECT 1 FROM mc_advancements WHERE player=%s AND title=%s ORDER BY ts DESC LIMIT 1",
                (player, title))
    return cur.fetchone() is not None

# =========================
# Main
# =========================
def main():
    log("adv_feed starting")
    conn = db()
    cur = conn.cursor()
    ensure_tables(cur)
    log("DB ready")

    tails = {}
    for path in LOG_PATHS:
        if os.path.isfile(path):
            f, inode, pos = open_tail(path)
            tails[path] = {"f": f, "inode": inode, "pos": pos}
            log(f"Watching {path}")
    if not tails:
        log("No logs found. Advancements will not import from logs.")

    colony_map = {}
    next_map = 0
    next_scan = 0

    while True:
        now = time.time()

        if now >= next_map:
            colony_map = load_colony_map()
            next_map = now + COLONY_MAP_REFRESH

        if now >= next_scan:
            scan_colony_completions(cur, colony_map)
            next_scan = now + COLONY_SCAN_INTERVAL

        progressed = False

        for path, st in list(tails.items()):
            f = st["f"]
            line = f.readline()
            if not line:
                try:
                    s = os.stat(path)
                    if s.st_ino != st["inode"] or s.st_size < st["pos"]:
                        try:
                            f.close()
                        except Exception:
                            pass
                        f, inode, pos = open_tail(path)
                        tails[path] = {"f": f, "inode": inode, "pos": pos}
                        log(f"Reopened {path}")
                except FileNotFoundError:
                    pass
                continue

            progressed = True
            st["pos"] = f.tell()
            s = line.strip()

            m = ADV_PAT.search(s)
            if m:
                player = m.group("player").strip()
                title = m.group("title").strip()
                if not already_logged_adv(cur, player, title):
                    try:
                        cur.execute("INSERT INTO mc_advancements(player,title) VALUES(%s,%s)", (player, title))
                        log(f"Inserted ADV: {player} | {title}")
                    except Exception:
                        log("DB error adv:\n" + traceback.format_exc())
                        try:
                            conn.close()
                        except Exception:
                            pass
                        time.sleep(1)
                        conn = db()
                        cur = conn.cursor()
                        ensure_tables(cur)
                continue

            mc = MC_DEBUG_PAT.search(s)
            if mc:
                cid = int(mc.group("cid"))
                action = (mc.group("action") or "event").lower()
                building_id = mc.group("bclass") or ""
                block_id = mc.group("block") or ""
                x = int(mc.group("x")) if mc.group("x") else 0
                y = int(mc.group("y")) if mc.group("y") else 0
                z = int(mc.group("z")) if mc.group("z") else 0

                bname = pretty_building_from_hint(block_id or building_id) or "Building"
                _insert_colony(cur, cid, colony_map, action,
                               building_id, bname, block_id, x, y, z, s)
                continue

        if not progressed:
            time.sleep(0.1)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        log("Fatal:\n" + traceback.format_exc())
