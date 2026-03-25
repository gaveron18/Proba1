"""
USPP METAR backend — aiohttp + SQLite
Fetches METAR every 60s, saves to DB, serves history API.
Port: 8094
"""
import asyncio
import sqlite3
import json
from datetime import datetime
from aiohttp import web, ClientSession

DB       = '/home/new/Proba1/data.db'
METAR_URL = 'https://aviationweather.gov/api/data/metar?ids=USPP&format=json'
RUNWAYS  = [('03', 30), ('21', 210)]
PREFERRED = '21'
PREFERRED_HEADING = 210


def init_db():
    conn = sqlite3.connect(DB)
    conn.execute('''CREATE TABLE IF NOT EXISTS runway_log (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        ts        TEXT NOT NULL,
        runway    TEXT,
        heading   INTEGER,
        wind_dir  INTEGER,
        wind_spd  INTEGER,
        wind_gst  INTEGER,
        is_calm   INTEGER DEFAULT 0,
        is_vrb    INTEGER DEFAULT 0,
        raw_metar TEXT
    )''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_ts ON runway_log(ts)')
    conn.commit()
    conn.close()


def angle_diff(a, b):
    d = abs(a - b) % 360
    return 360 - d if d > 180 else d


def get_active_runway(wind_dir, wind_spd, is_vrb):
    if is_vrb or wind_spd == 0 or wind_dir is None:
        return PREFERRED, PREFERRED_HEADING, True, bool(is_vrb)
    best_name, best_heading, best_diff = None, None, 999
    for name, heading in RUNWAYS:
        diff = angle_diff(wind_dir, heading)
        if diff < best_diff:
            best_diff = diff
            best_name, best_heading = name, heading
    return best_name, best_heading, False, False


async def fetch_and_save():
    try:
        async with ClientSession() as session:
            async with session.get(METAR_URL, timeout=10) as resp:
                data = await resp.json(content_type=None)
        if not data:
            return
        m = data[0]
        raw      = m.get('rawOb', '')
        is_vrb   = 'VRB' in raw
        wind_dir = None if is_vrb else m.get('wdir')
        wind_spd = m.get('wspd', 0) or 0
        wind_gst = m.get('wgst')
        runway, heading, is_calm, is_vrb2 = get_active_runway(wind_dir, wind_spd, is_vrb)
        ts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        conn = sqlite3.connect(DB)
        conn.execute(
            '''INSERT INTO runway_log
               (ts, runway, heading, wind_dir, wind_spd, wind_gst, is_calm, is_vrb, raw_metar)
               VALUES (?,?,?,?,?,?,?,?,?)''',
            (ts, runway, heading, wind_dir, wind_spd, wind_gst,
             int(is_calm), int(is_vrb), raw)
        )
        conn.commit()
        conn.close()
        print(f'[{ts}] Saved: ВПП {runway}, wind {wind_dir}°/{wind_spd}kt')
    except Exception as e:
        print(f'[ERROR] fetch_and_save: {e}')


async def bg_task(app):
    # First fetch immediately on start
    await fetch_and_save()
    while True:
        await asyncio.sleep(60)
        await fetch_and_save()


# ── API handlers ────────────────────────────────────────────────────────────

CORS = {'Access-Control-Allow-Origin': '*'}


async def history_handler(request):
    from_ts = request.rel_url.query.get('from', '')
    to_ts   = request.rel_url.query.get('to', '')
    limit   = min(int(request.rel_url.query.get('limit', 500)), 2000)
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    if from_ts and to_ts:
        rows = conn.execute(
            '''SELECT * FROM runway_log
               WHERE ts >= ? AND ts <= ?
               ORDER BY ts DESC LIMIT ?''',
            (from_ts, to_ts, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            'SELECT * FROM runway_log ORDER BY ts DESC LIMIT ?', (limit,)
        ).fetchall()
    conn.close()
    return web.Response(
        text=json.dumps([dict(r) for r in rows], ensure_ascii=False),
        content_type='application/json', headers=CORS
    )


async def stats_handler(request):
    from_ts = request.rel_url.query.get('from', '')
    to_ts   = request.rel_url.query.get('to', '')
    conn = sqlite3.connect(DB)
    if from_ts and to_ts:
        rows = conn.execute(
            '''SELECT runway, COUNT(*) as cnt FROM runway_log
               WHERE ts >= ? AND ts <= ? GROUP BY runway''',
            (from_ts, to_ts)
        ).fetchall()
        total = conn.execute(
            'SELECT COUNT(*) FROM runway_log WHERE ts >= ? AND ts <= ?',
            (from_ts, to_ts)
        ).fetchone()[0]
    else:
        rows = conn.execute(
            'SELECT runway, COUNT(*) as cnt FROM runway_log GROUP BY runway'
        ).fetchall()
        total = conn.execute('SELECT COUNT(*) FROM runway_log').fetchone()[0]
    conn.close()
    result = {'total': total, 'runways': {r[0]: r[1] for r in rows}}
    return web.Response(
        text=json.dumps(result, ensure_ascii=False),
        content_type='application/json', headers=CORS
    )


async def on_startup(app):
    app['bg'] = asyncio.create_task(bg_task(app))


async def on_cleanup(app):
    app['bg'].cancel()
    try:
        await app['bg']
    except asyncio.CancelledError:
        pass


def main():
    init_db()
    app = web.Application()
    app.router.add_get('/api/history', history_handler)
    app.router.add_get('/api/stats',   stats_handler)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    print('Starting USPP METAR backend on 127.0.0.1:8094')
    web.run_app(app, host='127.0.0.1', port=8094, access_log=None)


if __name__ == '__main__':
    main()
