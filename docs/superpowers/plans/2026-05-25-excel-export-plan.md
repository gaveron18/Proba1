# План реализации: Excel-экспорт (Proba1)
Date: 2026-05-25
Spec: `docs/superpowers/specs/2026-05-25-excel-export-design.md`

---

## Phase 0: Allowed APIs (уже проверено)

**openpyxl 3.1.5** — установлен, проверен живым запуском.

```python
import openpyxl
from openpyxl.styles import Font
from io import BytesIO

wb = openpyxl.Workbook()
ws1 = wb.active;  ws1.title = "Данные"
ws2 = wb.create_sheet("Статистика")
ws1.append(["Заголовок1", "Заголовок2"])          # добавить строку
for cell in ws1[1]: cell.font = Font(bold=True)   # жирный заголовок
ws1.append([1, None, "текст"])                    # None → пустая ячейка
buf = BytesIO(); wb.save(buf)
xlsx_bytes = buf.getvalue()                       # → bytes для HTTP
```

Живой пример в проекте: `/home/new/ConvertNtm/ntm_export.py:48–183`

**aiohttp binary response:**
```python
return web.Response(
    body=xlsx_bytes,
    content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    headers={
        'Content-Disposition': 'attachment; filename="USPP_20260523_20260525.xlsx"',
        **CORS
    }
)
```

**Anti-patterns:**
- НЕ использовать `get_sheet_by_name()` — deprecated с 3.1
- НЕ передавать `''` для NULL-значений числовых колонок — использовать `None`
- НЕ использовать `buf.seek(0)` + `buf.read()` — `buf.getvalue()` проще

---

## Phase 1: server.py — эндпоинт /api/export

**Файл:** `/home/new/Proba1/server.py`
**Ref:** `/home/new/ConvertNtm/ntm_export.py:48–183` (паттерн для копирования)

### Задачи

**1a. Включить WAL-режим в init_db()**

Добавить после `conn.execute('CREATE INDEX ...')`:
```python
conn.execute('PRAGMA journal_mode=WAL')
```

**1b. Добавить импорты** (после существующих импортов):
```python
import openpyxl
from openpyxl.styles import Font
from io import BytesIO
```

**1c. Написать export_handler** — вставить перед `index_handler`:

```python
async def export_handler(request):
    from_ts = request.rel_url.query.get('from', '')
    to_ts   = request.rel_url.query.get('to', '')

    # Валидация
    if not from_ts or not to_ts:
        return web.Response(
            text=json.dumps({'error': 'from and to are required'}),
            content_type='application/json', status=400, headers=CORS)
    try:
        from_dt = datetime.fromisoformat(from_ts.replace('Z', '+00:00'))
        to_dt   = datetime.fromisoformat(to_ts.replace('Z', '+00:00'))
    except ValueError:
        return web.Response(
            text=json.dumps({'error': 'invalid date format, use ISO 8601'}),
            content_type='application/json', status=400, headers=CORS)
    if from_dt >= to_dt:
        return web.Response(
            text=json.dumps({'error': 'from must be before to'}),
            content_type='application/json', status=400, headers=CORS)
    if (to_dt - from_dt).days > 90:
        return web.Response(
            text=json.dumps({'error': 'range must not exceed 90 days'}),
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
        return web.Response(
            text=json.dumps({'error': 'database error'}),
            content_type='application/json', status=500, headers=CORS)

    # Формируем xlsx
    bold = Font(bold=True)
    wb = openpyxl.Workbook()

    # Лист 1 — Данные
    ws1 = wb.active
    ws1.title = 'Данные'
    headers = ['№', 'Время (UTC)', 'ВПП', 'Курс (°)', 'Откуда (°)',
               'Скорость (уз)', 'Порыв (уз)', 'Штиль', 'Переменный', 'METAR']
    ws1.append(headers)
    for cell in ws1[1]:
        cell.font = bold
    for i, r in enumerate(rows, 1):
        ws1.append([
            i,
            r['ts'],
            r['runway'],
            r['heading'],                            # int или None
            r['wind_dir'],                           # int или None
            r['wind_spd'],                           # int
            r['wind_gst'] if r['wind_gst'] else None,
            'Да' if r['is_calm'] else 'Нет',
            'Да' if r['is_vrb']  else 'Нет',
            r['raw_metar'] or '',
        ])

    # Лист 2 — Статистика
    ws2 = wb.create_sheet('Статистика')
    stat_headers = ['Параметр', 'Значение']
    ws2.append(stat_headers)
    for cell in ws2[1]:
        cell.font = bold
    runway_counts = {r['runway']: r['cnt'] for r in stats_rows}
    pct = lambda n: f"{n} ({round(n/total*100) if total else 0}%)"
    ws2.append(['Период с', from_ts])
    ws2.append(['Период по', to_ts])
    ws2.append(['Всего записей', total])
    ws2.append(['ВПП 21', pct(runway_counts.get('21', 0))])
    ws2.append(['ВПП 03', pct(runway_counts.get('03', 0))])

    buf = BytesIO()
    wb.save(buf)
    xlsx_bytes = buf.getvalue()

    # Имя файла: USPP_YYYYMMDD_YYYYMMDD.xlsx
    fn_from = from_ts[:10].replace('-', '')
    fn_to   = to_ts[:10].replace('-', '')
    filename = f'USPP_{fn_from}_{fn_to}.xlsx'

    return web.Response(
        body=xlsx_bytes,
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={
            'Content-Disposition': f'attachment; filename="{filename}"',
            **CORS
        }
    )
```

**1d. Зарегистрировать роут** в `main()`:
```python
app.router.add_get('/api/export', export_handler)
```

### Проверка Phase 1
```bash
# Перезапустить сервер (через ssh new@localhost)
# Тест 400 — без параметров:
curl -s 'http://localhost:8094/api/export' | python3 -m json.tool
# → {"error": "from and to are required"}

# Тест 400 — диапазон > 90 дней:
curl -s 'http://localhost:8094/api/export?from=2026-01-01T00:00:00Z&to=2026-12-31T00:00:00Z' | python3 -m json.tool
# → {"error": "range must not exceed 90 days"}

# Тест 200 — скачать файл:
curl -s 'http://localhost:8094/api/export?from=2026-05-23T00:00:00Z&to=2026-05-25T23:59:59Z' -o /tmp/test.xlsx
python3 -c "import openpyxl; wb=openpyxl.load_workbook('/tmp/test.xlsx'); print(wb.sheetnames); print(wb['Данные'].max_row, 'rows')"
# → ['Данные', 'Статистика'], N rows
```

---

## Phase 2: index.html — кнопка "Экспорт"

**Файл:** `/home/new/Proba1/index.html`

### Задачи

**2a. Найти кнопку "Загрузить"** (строка ~589) и добавить рядом кнопку "Экспорт":

Найти в HTML:
```html
<button onclick="loadDB()">Загрузить</button>
```

Заменить на:
```html
<button onclick="loadDB()">Загрузить</button>
<button onclick="exportXLSX()">Экспорт</button>
```

**2b. Добавить функцию `exportXLSX()`** — вставить рядом с функцией `loadDB()`:

```javascript
function exportXLSX() {
    const from = toUTC(document.getElementById('fromDt').value);
    const to   = toUTC(document.getElementById('toDt').value);
    if (!from || !to) { alert('Выберите период'); return; }
    window.location.href =
        `/api/export?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`;
}
```

### Проверка Phase 2
- Открыть `http://173.249.2.184:8094/` в браузере
- Установить диапазон дат
- Нажать "Экспорт" → браузер должен сразу скачать `.xlsx`
- Открыть файл: проверить что есть 2 листа, числа числами, пустые ячейки пустыми

---

## Phase 3: Финальная проверка

```bash
# 1. Структура файла
python3 -c "
import openpyxl
wb = openpyxl.load_workbook('/tmp/test.xlsx')
print('Листы:', wb.sheetnames)
ws = wb['Данные']
print('Строк данных:', ws.max_row - 1)
print('Заголовки:', [c.value for c in ws[1]])
print('Тип wind_spd (row 2):', ws['F2'].data_type)  # должно быть 'n'
ws2 = wb['Статистика']
print('Статистика:', [[c.value for c in r] for r in ws2])
"

# 2. Anti-pattern check — нет устаревших API
grep -n 'get_sheet_by_name\|create_named_range' /home/new/Proba1/server.py
# → пусто

# 3. WAL-режим активен
python3 -c "import sqlite3; c=sqlite3.connect('/home/new/Proba1/data.db'); print(c.execute('PRAGMA journal_mode').fetchone())"
# → ('wal',)

# 4. Имя файла без спецсимволов
curl -sI 'http://localhost:8094/api/export?from=2026-05-23T00:00:00Z&to=2026-05-25T23:59:59Z' | grep -i disposition
# → Content-Disposition: attachment; filename="USPP_20260523_20260525.xlsx"
```
