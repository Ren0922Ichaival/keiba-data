"""
地方競馬 データ取得サーバー
keiba.go.jp の出馬表から馬番・人気・単勝オッズを取得して返す Flask API
"""
from flask import Flask, jsonify, request, send_from_directory
import requests
from bs4 import BeautifulSoup
import re
import json
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent / 'scripts'))
from fetch_races import parse_refund_soup as parse_refund_page

app = Flask(__name__)

@app.after_request
def add_cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    return response

BASE = 'https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo'

# keiba.go.jp の k_babaCode（TodayRaceInfoTopリンクから実測確認済み）
VENUE_CODES = {
    '帯広': 3,
    '水沢': 11,
    '浦和': 18,
    '大井': 20,   # 旧設定44は誤り→20が正しい
    '金沢': 22,
    '笠松': 23,   # 旧設定47は誤り→23が正しい
    '名古屋': 24,
    '園田': 27,
    '門別': 30,
    '高知': 31,
    '佐賀': 32,
    '盛岡': 35,
    '船橋': 43,
    '川崎': 45,
    '姫路': 51,
}
CODE_TO_VENUE = {v: k for k, v in VENUE_CODES.items()}

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
    'Referer': 'https://www.keiba.go.jp/',
}


def get_soup(url, params=None):
    res = requests.get(url, params=params, headers=HEADERS, timeout=15)
    res.raise_for_status()
    return BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')


def fmt_date(d: str) -> str:
    """YYYY-MM-DD or YYYY/MM/DD → YYYY/MM/DD"""
    if not d:
        return datetime.now().strftime('%Y/%m/%d')
    return d.replace('-', '/')


import unicodedata as _ud


def _normalize(text: str) -> str:
    """全角英数字・スペース・半角カタカナ（濁点含む）を正規化。
    NFKC で半角カナ→全角カナ・全角英数→半角英数・全角空白→半角空白等を一括変換する。
    """
    return _ud.normalize('NFKC', text)


def _parse_text_for_conditions(text: str, venue: str = '') -> dict:
    """
    任意のテキストからレース条件を抽出する共通ロジック。
    全角正規化 → 複数パターンで馬場/天候/距離/コースを検出。
    """
    t = _normalize(text)
    info: dict = {'track': None, 'weather': None, 'distance': None, 'surface': None}

    # ── 馬場状態 ──
    m = re.search(r'馬場(?:状態)?[：:\s]{0,3}\S{0,4}?(不良|稍重|重|良|軽)', t)
    if m:
        raw = m.group(1)
        info['track'] = '良' if raw == '軽' else raw

    # ── 天候 ──
    m = re.search(r'天候[：:\s]*(晴|曇り?|雨|小雨|雪)', t)
    if m:
        info['weather'] = '曇' if '曇' in m.group(1) else m.group(1)

    # ── 距離・コース（パターン1: コース種別 + 距離 + m） ──
    # 例: ダ1200m / ダート1200m / 芝1600m / 障1000m / 直1000m
    m = re.search(r'(芝|ダ(?:ート)?|障(?:害)?|直(?:線)?)\s*(\d{3,4})\s*m', t, re.IGNORECASE)
    if m:
        surf_raw, dist_raw = m.group(1), m.group(2)
        info['distance'] = int(dist_raw)
        info['surface'] = ('芝'   if '芝' in surf_raw else
                           '障害' if '障' in surf_raw else
                           '直線' if '直' in surf_raw else 'ダート')

    # ── 距離・コース（パターン2: 距離 + m + コース種別） ──
    # 例: 1200mダート / 1400m芝
    if info['distance'] is None:
        m = re.search(r'(\d{3,4})\s*m\s*(芝|ダート?|障害?|直線?)', t)
        if m:
            info['distance'] = int(m.group(1))
            surf_raw = m.group(2)
            info['surface'] = ('芝'   if '芝' in surf_raw else
                               '障害' if '障' in surf_raw else
                               '直線' if '直' in surf_raw else 'ダート')

    # ── 距離のみ（コース不明）: "距離1200" / "1200m" 単独 ──
    if info['distance'] is None:
        m = re.search(r'距離[：:\s]*(\d{3,4})', t)
        if not m:
            # 数字+m が単独で存在し、前後が非数字の場合
            m = re.search(r'(?<!\d)(\d{3,4})\s*m(?!\d)', t)
        if m:
            info['distance'] = int(m.group(1))

    # ── surface フォールバック: 距離は取れたが surface 未取得 → テキスト全体から探す ──
    if info['distance'] is not None and info['surface'] is None:
        if re.search(r'ダート|ダ(?![ンィッ])', t):
            info['surface'] = 'ダート'
        elif re.search(r'(?<![一二三四五六七八九十])芝', t):
            info['surface'] = '芝'
        elif '障害' in t or re.search(r'障(?![害])', t):
            info['surface'] = '障害'
        elif '直線' in t:
            info['surface'] = '直線'

    # ── ばんえい（帯広）専用 ──
    if venue == '帯広':
        if info['distance'] is None:
            m2 = re.search(r'(\d{3,4})\s*m', t)
            info['distance'] = int(m2.group(1)) if m2 else 200
        # 距離が取れている/いないに関わらず surface は直線
        info['surface'] = '直線'
        # 馬場が標準カテゴリで取れなかった場合、馬場水分(%)から推定
        # 例: "馬場：1.6" → 稍重
        if info['track'] is None:
            mm = re.search(r'馬場[：:\s]*(\d+(?:\.\d+)?)(?!\d)', t)
            if mm:
                try:
                    moisture = float(mm.group(1))
                    if moisture <= 1.5:
                        info['track'] = '良'
                    elif moisture <= 2.5:
                        info['track'] = '稍重'
                    elif moisture <= 3.5:
                        info['track'] = '重'
                    else:
                        info['track'] = '不良'
                except ValueError:
                    pass

    return info


def parse_race_info(soup: BeautifulSoup, venue: str = '') -> dict:
    """
    出馬表HTML からレース条件（馬場状態・天候・距離・コース種別）を抽出する。
    取得できない項目は None を返す。venue='帯広' でばんえい専用解析。
    """
    return _parse_text_for_conditions(soup.get_text(' ', strip=True), venue)


def parse_conditions_from_refund_page(date_str: str, code: int, venue: str = '') -> dict:
    """
    RefundMoneyList（払戻金一覧）ページから全レースの条件を一括抽出する。
    このページは過去日付でも取得可能。
    戻り値: {raceNo(int): {track, weather, distance, surface}}
    """
    try:
        soup = get_soup(
            f'{BASE}/RefundMoneyList',
            params={'k_raceDate': date_str, 'k_babaCode': code},
        )
    except Exception:
        return {}

    # ページ全体テキストを正規化
    full = _normalize(soup.get_text(' ', strip=True))

    conditions: dict = {}
    # レース番号マーカーの出現位置をすべて列挙
    markers = list(re.finditer(r'(?<![0-9])(\d{1,2})R(?![0-9])', full))
    for idx, m in enumerate(markers):
        rno = int(m.group(1))
        if not (1 <= rno <= 12) or rno in conditions:
            continue
        # このレース番号から次のレース番号マーカーまでのテキスト（最大1000文字）
        start = m.start()
        end   = markers[idx + 1].start() if idx + 1 < len(markers) else start + 1000
        end   = min(end, start + 1000)
        segment = full[start:end]
        info = _parse_text_for_conditions(segment, venue)
        # いずれかのフィールドが取れたら保存（weatherのみでも保持）
        if any(v is not None for v in info.values()):
            conditions[rno] = info

    return conditions


def _extract_trainer(text: str) -> str | None:
    """調教師セルから「鈴木太一（大井）」→ 「鈴木太一」のように所属を除いた調教師名を取り出す"""
    if not text:
        return None
    # 全角/半角括弧の前で分割
    name = re.split(r'[（(]', text, maxsplit=1)[0].strip()
    return name or None


def _direct_tds(tr):
    """tr の DIRECT 子要素の td のみを返す（ネストされた result table の td を除外）"""
    if tr is None:
        return []
    return [c for c in tr.children if getattr(c, 'name', None) == 'td']


def parse_horses(soup: BeautifulSoup) -> list:
    """
    出馬表HTML から馬番・人気・単勝オッズ・馬体重・騎手・馬名・父・母父・調教師を抽出する。
    keiba.go.jp の実HTMLは1頭につき5つの sibling tr 構造:
      行0 (horseNum有): 枠/馬番/馬名/騎手/オッズ + (右側に着別成績テーブルがネスト)
      行1 (noBorder)  : 性齢/毛色/生年/負担重量
      行2             : [0]父名 [1]調教師（所属） [2]馬体重(増減)
      行3             : [0]母名 [1]生産者
      行4             : [0]（母父名）  [1]生産者
    親 table 内の sibling 関係で辿る（ネストされた arrival テーブルを混入させない）。
    """
    horses = []
    seen = set()

    # horseNum セルを起点に処理
    for hn_cell in soup.find_all('td', class_='horseNum'):
        try:
            hn = int(hn_cell.get_text(strip=True))
            if not (1 <= hn <= 16):
                continue
        except (ValueError, TypeError):
            continue
        if hn in seen:
            continue

        row0 = hn_cell.find_parent('tr')
        if row0 is None:
            continue

        # 行0 のセル
        odds_val = None
        pop_val  = None
        odds_cell = row0.find('td', class_='odds_weight')
        if odds_cell:
            t = odds_cell.get_text(' ', strip=True)
            m = re.search(r'([\d]+\.[\d]+)\s*[（(](\d+)人気[）)]', t)
            if m:
                odds_val = float(m.group(1))
                pop_val  = int(m.group(2))
        if odds_val is None:
            m = re.search(r'([\d]+\.[\d]+)\s*[（(](\d+)人気[）)]',
                          row0.get_text(' ', strip=True))
            if m:
                odds_val = float(m.group(1))
                pop_val  = int(m.group(2))

        # 騎手: class があれば優先、なければ row0 の direct td 群から
        jockey = None
        jk_cell = row0.find('td', class_='jocky') or row0.find('td', class_='jockey')
        if jk_cell:
            jockey = re.split(r'[（(]', jk_cell.get_text(strip=True), maxsplit=1)[0].strip() or None
        else:
            tds = _direct_tds(row0)
            # [0]courseNum [1]horseNum [2]horseName [3]騎手(所属) [4]odds_weight
            if len(tds) >= 4:
                jockey = re.split(r'[（(]', tds[3].get_text(' ', strip=True),
                                  maxsplit=1)[0].strip() or None

        # 馬名
        horse_name = None
        name_cell = row0.find('td', class_='horseName')
        if name_cell:
            horse_name = name_cell.get_text(strip=True) or None

        # 行1〜4 を sibling で辿る（次の horseNum を持つ行が来たら停止）
        sub_rows = []
        cur = row0.find_next_sibling('tr')
        while cur is not None and len(sub_rows) < 4:
            if cur.find('td', class_='horseNum'):
                break
            sub_rows.append(cur)
            cur = cur.find_next_sibling('tr')

        weight = None
        weight_diff = None
        sire = None
        trainer = None
        broodmare_sire = None

        # 行2 (sub_rows[1]): 父名・調教師・体重
        if len(sub_rows) >= 2:
            r2 = sub_rows[1]
            wc = r2.find('td', class_='odds_weight')
            if wc:
                wt = wc.get_text(' ', strip=True)
                wm = re.search(r'(\d{3,4})\s*(?:\(([+-]\d+)\))?', wt)
                if wm:
                    weight = int(wm.group(1))
                    if wm.group(2):
                        weight_diff = int(wm.group(2))
            tds = _direct_tds(r2)
            if len(tds) >= 1:
                txt = tds[0].get_text(strip=True)
                if txt and len(txt) >= 2 and not txt.startswith(('（', '(')):
                    sire = txt
            if len(tds) >= 2:
                trainer = _extract_trainer(tds[1].get_text(strip=True))

        # 行4 (sub_rows[3]): 母父
        if len(sub_rows) >= 4:
            r4 = sub_rows[3]
            tds = _direct_tds(r4)
            if len(tds) >= 1:
                txt = tds[0].get_text(strip=True)
                m = re.match(r'^[（(]\s*(.+?)\s*[）)]$', txt)
                if m:
                    broodmare_sire = m.group(1)

        seen.add(hn)
        entry = {
            'hn': hn,
            'pop': pop_val,
            'odds': odds_val,
            'weight': weight,
            'weightDiff': weight_diff,
        }
        if jockey:         entry['jockey']        = jockey
        if horse_name:     entry['horseName']     = horse_name
        if sire:           entry['sire']          = sire
        if broodmare_sire: entry['broodmareSire'] = broodmare_sire
        if trainer:        entry['trainer']       = trainer
        horses.append(entry)

    horses.sort(key=lambda h: h['hn'])
    return horses


# ──────────────────────────────────────────────
#  API エンドポイント
# ──────────────────────────────────────────────

@app.route('/api/ping')
def ping():
    """サーバー起動確認用"""
    return jsonify({'status': 'ok', 'version': '1.0'})


@app.route('/api/schedule')
def schedule():
    """本日の開催競馬場一覧（未知コードも返す）"""
    date = fmt_date(request.args.get('date', ''))
    try:
        res = requests.get(
            f'{BASE}/TodayRaceInfoTop',
            headers=HEADERS, timeout=10
        )
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')

        # 既存の discovered_codes を読み込む
        data_dir = Path(__file__).parent / 'data'
        discovered_path = data_dir / 'discovered_codes.json'
        discovered: dict = {}
        if discovered_path.exists():
            try:
                discovered = json.loads(discovered_path.read_text(encoding='utf-8'))
            except Exception:
                pass

        venues = []
        seen_codes = set()
        new_discovered = False

        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'RaceList' not in href:
                continue
            m = re.search(r'k_babaCode=(\d+)', href)
            if not m:
                continue
            code = int(m.group(1))
            if code in seen_codes:
                continue
            seen_codes.add(code)
            name = CODE_TO_VENUE.get(code)
            if name:
                venues.append({'code': code, 'name': name})
            else:
                # 未知コード: リンクテキストから競馬場名を推定
                link_text = a.get_text(strip=True)
                code_str = str(code)
                if not link_text:
                    link_text = f'不明(code={code})'
                if code_str not in discovered:
                    discovered[code_str] = link_text
                    new_discovered = True
                venues.append({'code': code, 'name': discovered[code_str], 'unknown': True})

        # 新しい未知コードがあれば保存
        if new_discovered:
            data_dir.mkdir(exist_ok=True)
            discovered_path.write_text(
                json.dumps(discovered, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )

        return jsonify({'date': date, 'venues': venues})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/races')
def races():
    """指定競馬場のレース一覧"""
    date = fmt_date(request.args.get('date', ''))
    venue = request.args.get('venue', '')
    code = VENUE_CODES.get(venue)

    if not code:
        return jsonify({'error': f'競馬場「{venue}」は未対応です'}), 400

    try:
        res = requests.get(
            f'{BASE}/RaceList',
            params={'k_raceDate': date, 'k_babaCode': code},
            headers=HEADERS, timeout=10
        )
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')

        race_list = []
        seen_nos = set()
        for a in soup.find_all('a', href=True):
            if 'DebaTable' not in a['href']:
                continue
            m = re.search(r'k_raceNo=(\d+)', a['href'])
            if not m:
                continue
            rno = int(m.group(1))
            if rno not in seen_nos:
                seen_nos.add(rno)
                label = a.get_text(strip=True)
                race_list.append({'no': rno, 'name': label or f'{rno}R'})

        race_list.sort(key=lambda r: r['no'])
        return jsonify({'venue': venue, 'date': date, 'races': race_list})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/entries')
def entries():
    """指定レースの出走馬データ（馬番・人気・単勝オッズ）"""
    date = fmt_date(request.args.get('date', ''))
    venue = request.args.get('venue', '')
    race_no_raw = request.args.get('race', '1')
    code = VENUE_CODES.get(venue)

    if not code:
        return jsonify({'error': f'競馬場「{venue}」は未対応です'}), 400
    try:
        race_no = int(race_no_raw)
        if not 1 <= race_no <= 12:  # 地方競馬の1日最大レース数は12R
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({'error': 'race は 1 から 12 の整数で指定してください'}), 400

    try:
        res = requests.get(
            f'{BASE}/DebaTable',
            params={'k_raceDate': date, 'k_babaCode': code, 'k_raceNo': str(race_no)},
            headers=HEADERS, timeout=10
        )
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')

        horses    = parse_horses(soup)
        race_info = parse_race_info(soup, venue=venue)

        if not horses:
            return jsonify({
                'error': 'データが取得できませんでした。発走前または出馬表未公開の可能性があります。',
                'venue': venue, 'date': date, 'race': race_no
            }), 404

        return jsonify({
            'venue': venue,
            'date': date,
            'race': race_no,
            'horses': horses,
            'total': len(horses),
            'raceInfo': race_info,   # 馬場状態・天候・距離・コース
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _write_conditions_to_json(date_key: str, venue_name: str, race_no: int, info: dict) -> None:
    """
    data/YYYY-MM-DD.json の対象レースに条件を書き戻す。
    ファイルが存在しない場合や対象レースが見つからない場合は無視する。
    """
    try:
        # date_key は 'YYYY-MM-DD' または 'YYYY/MM/DD'
        normalized_key = date_key.replace('/', '-')
        json_path = Path(__file__).parent / 'data' / f'{normalized_key}.json'
        if not json_path.exists():
            return
        data = json.loads(json_path.read_text(encoding='utf-8'))
        changed = False
        for v in data.get('venues', []):
            if v.get('name') != venue_name:
                continue
            for r in v.get('races', []):
                if int(r.get('raceNo', -1)) != int(race_no):
                    continue
                for k in ('track', 'weather', 'distance', 'surface'):
                    if info.get(k) is not None and not r.get(k):
                        r[k] = info[k]
                        changed = True
                break
            break
        if changed:
            tmp_fd, tmp_path = tempfile.mkstemp(dir=json_path.parent, suffix='.tmp')
            try:
                with os.fdopen(tmp_fd, 'w', encoding='utf-8') as f:
                    f.write(json.dumps(data, ensure_ascii=False, indent=2))
                os.replace(tmp_path, json_path)
            except Exception:
                os.unlink(tmp_path)
                raise
    except Exception as e:
        print(f'  _write_conditions_to_json 失敗: {date_key} {venue_name} {race_no}R: {e}')


@app.route('/api/batch_conditions', methods=['POST'])
def batch_conditions():
    """
    複数レースのレース条件を一括取得する（条件補完用）。
    戦略:
      1. 同一venue+dateの RefundMoneyList を1回だけ取得し、全レースの条件を一括抽出
      2. それでも取れない場合のみ DebaTable を個別に試みる
      3. 取得成功時は data/YYYY-MM-DD.json にも書き戻す
    リクエスト: [{venue, date, raceNo}, ...]
    レスポンス: [{venue, date, raceNo, ok, track?, weather?, distance?, surface?}, ...]
    """
    races = request.get_json(silent=True) or []
    if not races:
        return jsonify([])
    if not isinstance(races, list):
        return jsonify({'error': 'リクエストは配列で指定してください'}), 400
    if len(races) > 200:
        return jsonify({'error': '一度に処理できるレースは200件までです'}), 400

    # ── Step 1: venue+date ごとに RefundMoneyList を1回取得してキャッシュ ──
    # キー: (venue, date_key)  値: {raceNo: {track,weather,distance,surface}}
    refund_cache: dict = {}
    unique_vd = {(r.get('venue', ''), r.get('date', '')) for r in races}
    for venue_name, date_key in unique_vd:
        code = VENUE_CODES.get(venue_name)
        if not code:
            continue
        cache_key = (venue_name, date_key)
        if cache_key in refund_cache:
            continue
        try:
            cond_map = parse_conditions_from_refund_page(
                fmt_date(date_key), code, venue=venue_name
            )
            refund_cache[cache_key] = cond_map
            time.sleep(0.4)
        except Exception:
            refund_cache[cache_key] = {}

    # ── Step 2: 各レースに条件を割り当て、RefundMoneyList で取れなければ DebaTable を試みる ──
    results_out = []
    for r in races:
        if not isinstance(r, dict):
            results_out.append({'ok': False, 'error': '各要素はオブジェクトで指定してください'})
            continue
        venue   = r.get('venue', '')
        date_k  = r.get('date', '')
        date    = fmt_date(date_k)
        race_no_raw = r.get('raceNo')
        code    = VENUE_CODES.get(venue)

        try:
            race_no = int(race_no_raw)
            if not 1 <= race_no <= 12:  # 地方競馬の1日最大レース数は12R
                raise ValueError
        except (TypeError, ValueError):
            results_out.append({'venue': venue, 'date': date_k, 'raceNo': race_no_raw,
                                 'ok': False, 'error': 'raceNo は 1 から 12 の整数で指定してください'})
            continue

        if not code:
            results_out.append({'venue': venue, 'date': date_k, 'raceNo': race_no,
                                 'ok': False, 'error': f'未対応競馬場: {venue}'})
            continue

        # RefundMoneyList キャッシュから取得（distance/surface は取れるが
        # 帯広(ばんえい)など一部競馬場は track/weather が欠けるため、
        # 不足フィールドがあれば必ず DebaTable で補完する）
        info = dict(refund_cache.get((venue, date_k), {}).get(race_no, {}))

        # 4フィールドのいずれかが欠けていたら DebaTable で補完を試みる
        needs_deba = not info.get('track') or not info.get('weather') \
                     or not info.get('distance') or not info.get('surface')
        if needs_deba:
            try:
                res = requests.get(
                    f'{BASE}/DebaTable',
                    params={'k_raceDate': date, 'k_babaCode': code, 'k_raceNo': str(race_no)},
                    headers=HEADERS, timeout=12,
                )
                res.raise_for_status()
                soup = BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')
                deba_info = parse_race_info(soup, venue=venue)
                # DebaTable の値で不足分を補完（既存値は上書きしない）
                for k in ('track', 'weather', 'distance', 'surface'):
                    if not info.get(k) and deba_info.get(k):
                        info[k] = deba_info[k]
                time.sleep(0.3)
            except Exception as e:
                print(f'  DebaTable 補完失敗 {venue} {date_k} {race_no}R: {e}')

        entry = {'venue': venue, 'date': date_k, 'raceNo': race_no, 'ok': True}
        for k in ('track', 'weather', 'distance', 'surface'):
            if info.get(k) is not None:
                entry[k] = info[k]

        # 条件が取れたら JSON ファイルにも書き戻す
        if info.get('distance') or info.get('track'):
            _write_conditions_to_json(date_k, venue, race_no, info)

        results_out.append(entry)

    return jsonify(results_out)


@app.route('/api/results')
def results():
    """指定競馬場の全レース着順を返す（RefundMoneyListから取得）"""
    date  = fmt_date(request.args.get('date', ''))
    venue = request.args.get('venue', '')
    code  = VENUE_CODES.get(venue)

    if not code:
        return jsonify({'error': f'競馬場「{venue}」は未対応です'}), 400

    try:
        res = requests.get(
            f'{BASE}/RefundMoneyList',
            params={'k_raceDate': date, 'k_babaCode': code},
            headers=HEADERS, timeout=10
        )
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')
        all_results, _ = parse_refund_page(soup)
        return jsonify({'venue': venue, 'date': date, 'results': all_results})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/payouts')
def payouts():
    """指定競馬場の全レース払戻金を返す（RefundMoneyListから単勝・複勝・馬連等）"""
    date  = fmt_date(request.args.get('date', ''))
    venue = request.args.get('venue', '')
    code  = VENUE_CODES.get(venue)

    if not code:
        return jsonify({'error': f'競馬場「{venue}」は未対応です'}), 400

    try:
        res = requests.get(
            f'{BASE}/RefundMoneyList',
            params={'k_raceDate': date, 'k_babaCode': code},
            headers=HEADERS, timeout=10
        )
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')
        _, all_payouts = parse_refund_page(soup)
        return jsonify({'venue': venue, 'date': date, 'payouts': all_payouts})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/localdata/<date_key>')
def local_data(date_key):
    """data/YYYY-MM-DD.json をクライアントに返す（条件補完用）"""
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', date_key):
        return jsonify({'error': 'invalid date'}), 400
    data_dir = Path(__file__).parent / 'data'
    filepath = data_dir / f'{date_key}.json'
    if not filepath.exists():
        return jsonify({'error': 'not found'}), 404
    return send_from_directory(str(data_dir), f'{date_key}.json',
                               mimetype='application/json')


@app.route('/api/available_dates')
def available_dates():
    """data/ フォルダ内の YYYY-MM-DD.json ファイルの日付一覧を返す"""
    data_dir = Path(__file__).parent / 'data'
    dates = sorted(
        p.stem for p in data_dir.glob('????-??-??.json')
    )
    return jsonify({'dates': dates})


if __name__ == '__main__':
    print('=' * 50)
    print(' 地方競馬 分析サーバー')
    print(' http://127.0.0.1:5000')
    print(' 停止: Ctrl+C')
    print('=' * 50)
    app.run(host='127.0.0.1', port=5000, debug=False)
