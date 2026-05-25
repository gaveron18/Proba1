"""
USPP METAR backend — aiohttp + SQLite
Fetches METAR every 60s, saves to DB, serves history API.
Port: 8094
"""
import asyncio
import sqlite3
import json
from datetime import datetime
from io import BytesIO
from aiohttp import web, ClientSession
import openpyxl
from openpyxl.styles import Font

import os
DB       = os.path.join(os.path.dirname(__file__), 'data.db')
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
    conn.execute('PRAGMA journal_mode=WAL')
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
        await asyncio.sleep(900)
        await fetch_and_save()


# ── API handlers ────────────────────────────────────────────────────────────

CORS = {'Access-Control-Allow-Origin': '*'}


async def history_handler(request):
    from_ts = request.rel_url.query.get('from', '')
    to_ts   = request.rel_url.query.get('to', '')
    limit   = min(int(request.rel_url.query.get('limit', 1500)), 10000)
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



async def metar_handler(request):
    try:
        async with ClientSession() as session:
            async with session.get(METAR_URL, timeout=10) as resp:
                data = await resp.json(content_type=None)
        return web.Response(
            text=json.dumps(data, ensure_ascii=False),
            content_type='application/json', headers=CORS
        )
    except Exception as e:
        return web.Response(
            text=json.dumps({'error': str(e)}),
            content_type='application/json', headers=CORS,
            status=502
        )


async def export_handler(request):
    from_ts = request.rel_url.query.get('from', '')
    to_ts   = request.rel_url.query.get('to', '')

    if not from_ts or not to_ts:
        return web.Response(text=json.dumps({'error': 'from and to are required'}),
                            content_type='application/json', status=400, headers=CORS)
    try:
        from_dt = datetime.fromisoformat(from_ts.replace('Z', '+00:00'))
        to_dt   = datetime.fromisoformat(to_ts.replace('Z', '+00:00'))
    except ValueError:
        return web.Response(text=json.dumps({'error': 'invalid date format, use ISO 8601'}),
                            content_type='application/json', status=400, headers=CORS)
    if from_dt >= to_dt:
        return web.Response(text=json.dumps({'error': 'from must be before to'}),
                            content_type='application/json', status=400, headers=CORS)
    if (to_dt - from_dt).days > 90:
        return web.Response(text=json.dumps({'error': 'range must not exceed 90 days'}),
                            content_type='application/json', status=400, headers=CORS)

    try:
        conn = sqlite3.connect(DB, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            'SELECT * FROM runway_log WHERE ts >= ? AND ts <= ? ORDER BY ts ASC',
            (from_ts, to_ts)
        ).fetchall()
        stats_rows = conn.execute(
            'SELECT runway, COUNT(*) as cnt FROM runway_log WHERE ts >= ? AND ts <= ? GROUP BY runway',
            (from_ts, to_ts)
        ).fetchall()
        total = conn.execute(
            'SELECT COUNT(*) FROM runway_log WHERE ts >= ? AND ts <= ?',
            (from_ts, to_ts)
        ).fetchone()[0]
        conn.close()
    except Exception as e:
        print(f'[ERROR] export: {e}')
        return web.Response(text=json.dumps({'error': 'database error'}),
                            content_type='application/json', status=500, headers=CORS)

    bold = Font(bold=True)
    wb   = openpyxl.Workbook()

    ws1 = wb.active
    ws1.title = 'Данные'
    ws1.append(['№', 'Время (UTC)', 'ВПП', 'Курс (°)', 'Откуда (°)',
                'Скорость (уз)', 'Порыв (уз)', 'Штиль', 'Переменный', 'METAR'])
    for cell in ws1[1]:
        cell.font = bold
    for i, r in enumerate(rows, 1):
        ws1.append([
            i,
            r['ts'],
            r['runway'],
            r['heading'],
            r['wind_dir'],
            r['wind_spd'],
            r['wind_gst'] if r['wind_gst'] else None,
            'Да' if r['is_calm'] else 'Нет',
            'Да' if r['is_vrb']  else 'Нет',
            r['raw_metar'] or '',
        ])

    ws2 = wb.create_sheet('Статистика')
    ws2.append(['Параметр', 'Значение'])
    for cell in ws2[1]:
        cell.font = bold
    runway_counts = {r['runway']: r['cnt'] for r in stats_rows}
    pct = lambda n: f"{n} ({round(n / total * 100) if total else 0}%)"
    ws2.append(['Период с', from_ts])
    ws2.append(['Период по', to_ts])
    ws2.append(['Всего записей', total])
    ws2.append(['ВПП 21', pct(runway_counts.get('21', 0))])
    ws2.append(['ВПП 03', pct(runway_counts.get('03', 0))])

    buf = BytesIO()
    wb.save(buf)

    fn_from  = from_ts[:10].replace('-', '')
    fn_to    = to_ts[:10].replace('-', '')
    filename = f'USPP_{fn_from}_{fn_to}.xlsx'

    return web.Response(
        body=buf.getvalue(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{filename}"', **CORS}
    )


async def index_handler(request):
    import os as _os
    return __import__('aiohttp').web.FileResponse(
        _os.path.join(_os.path.dirname(__file__), 'index.html')
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
    app.router.add_get('/', index_handler)
    app.router.add_get('/api/metar',   metar_handler)
    app.router.add_get('/api/history', history_handler)
    app.router.add_get('/api/stats',   stats_handler)
    app.router.add_get('/api/export',  export_handler)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    print('Starting USPP METAR backend on 0.0.0.0:8094')
    web.run_app(app, host='0.0.0.0', port=8094, access_log=None)


if __name__ == '__main__':
    main()
