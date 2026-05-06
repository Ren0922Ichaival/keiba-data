"""
地方競馬 クラウドボット
GitHub Actions で定期実行し、仮想売買シミュレーションを data/bot_state.json に蓄積する
"""
import json
import math
import os
import re
import tempfile
import time
from datetime import datetime, timezone, timedelta
from itertools import combinations
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from fetch_races import _parse_text_for_conditions, parse_refund_soup

JST = timezone(timedelta(hours=9))
BASE = 'https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo'

VENUE_CODES = {
    '帯広': 3,  '水沢': 11, '浦和': 18, '大井': 20, '金沢': 22,
    '笠松': 23, '名古屋': 24, '園田': 27, '門別': 30, '高知': 31,
    '佐賀': 32, '盛岡': 35, '船橋': 43, '川崎': 45, '姫路': 51,
}
ALT_CODES = {19: '船橋', 36: '門別'}
CODE_TO_VENUE = {v: k for k, v in VENUE_CODES.items()}
CODE_TO_VENUE.update(ALT_CODES)

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
    'Referer': 'https://www.keiba.go.jp/',
}

# 地方競馬の人気別デフォルト勝率（業界平均値） — index 0 unused
DEFAULT_POP_WIN = [0, 0.355, 0.205, 0.130, 0.090, 0.063, 0.045, 0.033,
                   0.025, 0.018, 0.014, 0.011, 0.009, 0.007, 0.006, 0.005, 0.004]

DATA_DIR   = Path('data')
STATE_PATH = DATA_DIR / 'bot_state.json'
SETTINGS_PATH = DATA_DIR / 'bot_settings.json'

DEFAULT_SETTINGS = {
    'initialBalance': 100000,
    'betAmount': 1000,
    'strategy': 'safe_place',
    'minOdds': 1.0,
    'maxOdds': 8.0,
    'venueFilter': '',
}

DEFAULT_STATE = {
    'balance': None,
    'pending': [],
    'history': [],
    'log': [],
}


# ── ユーティリティ ────────────────────────────────────────────────

def get_soup(url, params=None):
    res = requests.get(url, params=params, headers=HEADERS, timeout=15)
    res.raise_for_status()
    return BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')


def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except Exception as e:
            print(f'[WARN] {path} の読み込み失敗 ({e}) — デフォルト値で初期化します')
    return default.copy()


def save_json(path: Path, obj):
    DATA_DIR.mkdir(exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(tmp_fd, 'w', encoding='utf-8') as f:
            f.write(json.dumps(obj, ensure_ascii=False, indent=2))
        os.replace(tmp_path, path)
    except Exception:
        os.unlink(tmp_path)
        raise


def add_log(state: dict, msg: str, level: str = 'inf'):
    now = datetime.now(JST).strftime('%H:%M')
    state['log'].insert(0, {'time': now, 'msg': msg, 'type': level})
    if len(state['log']) > 200:
        state['log'] = state['log'][:200]
    print(f'[{now}] {msg}')


# ── 過去データ読み込み ─────────────────────────────────────────────

def load_all_past_entries() -> list:
    """data/*.json から全エントリーを読み込み
    [{hn,pop,odds,pos,jockey,horseName,weightDiff,
      sire,broodmareSire,trainer,_track,_distance,_venue,_date}] 形式で返す"""
    entries = []
    for p in sorted(DATA_DIR.glob('????-??-??.json')):
        try:
            obj  = json.loads(p.read_text(encoding='utf-8'))
            date = obj.get('date', p.stem)  # YYYY-MM-DD
            for venue in obj.get('venues', []):
                vname = venue.get('name', '')
                for race in venue.get('races', []):
                    r_track    = race.get('track')
                    r_distance = race.get('distance')
                    has_result = any(e.get('pos') is not None for e in race.get('entries', []))
                    for e in race.get('entries', []):
                        entry = {
                            'hn':    e.get('hn'),
                            'pop':   e.get('pop'),
                            'odds':  e.get('odds'),
                            'pos':   e.get('pos'),
                            '_venue':    vname,
                            '_date':     date,
                            '_track':    r_track,
                            '_distance': r_distance,
                            '_hasResult': has_result,
                        }
                        if e.get('jockey'):                  entry['jockey']        = e['jockey']
                        if e.get('horseName'):               entry['horseName']      = e['horseName']
                        if e.get('weightDiff') is not None:  entry['weightDiff']     = e['weightDiff']
                        if e.get('sire'):                    entry['sire']           = e['sire']
                        if e.get('broodmareSire'):           entry['broodmareSire']  = e['broodmareSire']
                        if e.get('trainer'):                 entry['trainer']        = e['trainer']
                        entries.append(entry)
        except Exception:
            pass
    return entries


# ── keiba.go.jp スクレイピング ────────────────────────────────────

def get_schedule() -> list:
    """本日の開催場リスト [{name, code}]"""
    try:
        soup = get_soup(f'{BASE}/TodayRaceInfoTop')
        venues, seen = [], set()
        for a in soup.find_all('a', href=True):
            if 'RaceList' not in a['href']:
                continue
            m = re.search(r'k_babaCode=(\d+)', a['href'])
            if not m:
                continue
            code = int(m.group(1))
            if code in seen:
                continue
            seen.add(code)
            name = CODE_TO_VENUE.get(code)
            if name:
                venues.append({'name': name, 'code': code})
        return venues
    except Exception as e:
        print(f'スケジュール取得失敗: {e}')
        return []


def get_race_list(date_str: str, code: int) -> list:
    """指定場のレース番号リスト"""
    try:
        soup = get_soup(f'{BASE}/RaceList', params={'k_raceDate': date_str, 'k_babaCode': code})
        races, seen = [], set()
        for a in soup.find_all('a', href=True):
            if 'DebaTable' not in a['href']:
                continue
            m = re.search(r'k_raceNo=(\d+)', a['href'])
            if not m:
                continue
            rno = int(m.group(1))
            if rno not in seen:
                seen.add(rno)
                races.append(rno)
        return sorted(races)
    except Exception as e:
        print(f'  レース一覧取得失敗: {e}')
        return []


def parse_race_info_soup(soup, venue: str = '') -> dict:
    """出馬表HTMLからレース条件を抽出。venue='帯広' でばんえい専用解析。"""
    return _parse_text_for_conditions(soup.get_text(' ', strip=True), venue)


def _direct_tds(tr):
    """tr の直接の子の td（ネスト table の td 除外）"""
    if tr is None:
        return []
    return [c for c in tr.children if getattr(c, 'name', None) == 'td']


def _extract_trainer(text):
    if not text:
        return None
    name = re.split(r'[（(]', text, maxsplit=1)[0].strip()
    return name or None


def get_entries(date_str: str, code: int, race_no: int, venue: str = '') -> tuple:
    """出走馬データ ([{hn, pop, odds, jockey, sire, broodmareSire, trainer, ...}], race_info)
    DebaTable の 1頭=5行構造をパースする。
    """
    try:
        soup = get_soup(f'{BASE}/DebaTable',
                        params={'k_raceDate': date_str, 'k_babaCode': code, 'k_raceNo': race_no})
        horses, seen = [], set()
        for hn_cell in soup.find_all('td', class_='horseNum'):
            try:
                hn = int(hn_cell.get_text(strip=True))
                if not (1 <= hn <= 16) or hn in seen:
                    continue
            except (ValueError, TypeError):
                continue

            row0 = hn_cell.find_parent('tr')
            if row0 is None:
                continue

            odds_val = pop_val = None
            odds_cell = row0.find('td', class_='odds_weight')
            if odds_cell:
                t = odds_cell.get_text(' ', strip=True)
                m = re.search(r'([\d]+\.[\d]+)\s*[（(](\d+)人気[）)]', t)
                if m:
                    odds_val = float(m.group(1))
                    pop_val  = int(m.group(2))

            jockey = None
            jk_cell = row0.find('td', class_='jocky') or row0.find('td', class_='jockey')
            if jk_cell:
                jockey = re.split(r'[（(]', jk_cell.get_text(strip=True), maxsplit=1)[0].strip() or None
            else:
                tds = _direct_tds(row0)
                if len(tds) >= 4:
                    jockey = re.split(r'[（(]', tds[3].get_text(' ', strip=True),
                                      maxsplit=1)[0].strip() or None

            horse_name = None
            name_cell = row0.find('td', class_='horseName')
            if name_cell:
                horse_name = name_cell.get_text(strip=True) or None

            sub_rows = []
            cur = row0.find_next_sibling('tr')
            while cur is not None and len(sub_rows) < 4:
                if cur.find('td', class_='horseNum'):
                    break
                sub_rows.append(cur)
                cur = cur.find_next_sibling('tr')

            weight = weight_diff = None
            sire = trainer = broodmare_sire = None

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

            if len(sub_rows) >= 4:
                r4 = sub_rows[3]
                tds = _direct_tds(r4)
                if len(tds) >= 1:
                    txt = tds[0].get_text(strip=True)
                    m = re.match(r'^[（(]\s*(.+?)\s*[）)]$', txt)
                    if m:
                        broodmare_sire = m.group(1)

            seen.add(hn)
            entry = {'hn': hn, 'pop': pop_val, 'odds': odds_val}
            if jockey:                  entry['jockey']        = jockey
            if horse_name:              entry['horseName']     = horse_name
            if sire:                    entry['sire']          = sire
            if broodmare_sire:          entry['broodmareSire'] = broodmare_sire
            if trainer:                 entry['trainer']       = trainer
            if weight is not None:      entry['weight']        = weight
            if weight_diff is not None: entry['weightDiff']    = weight_diff
            horses.append(entry)
        horses.sort(key=lambda h: h['hn'])
        race_info = parse_race_info_soup(soup, venue=venue)
        return horses, race_info
    except Exception as e:
        print(f'  エントリー取得失敗: {e}')
        return [], {}


def get_results(date_str: str, code: int) -> tuple:
    """着順結果と払戻金 ({raceNo(int): {hn(int): pos(int)}}, {raceNo: payouts})"""
    try:
        soup = get_soup(f'{BASE}/RefundMoneyList',
                        params={'k_raceDate': date_str, 'k_babaCode': code})
        return parse_refund_soup(soup)
    except Exception as e:
        print(f'  着順取得失敗: {e}')
        return {}, {}


# ── 予想ロジック（JS と完全一致） ────────────────────────────────

def build_hist_stats(all_entries: list, venue: str, ref_date: str = None) -> dict:
    """統計データ構築。ref_date 指定時は直近データに重みをかける（指数減衰 90日）"""
    completed = [e for e in all_entries if e.get('_hasResult') is not False]
    if ref_date:
        # _date なしのエントリはスキーマ追加前の旧データ → 統計から除外しない（後方互換）
        completed = [e for e in completed if not e.get('_date') or e.get('_date') < ref_date]
    v_entries = [e for e in completed if e.get('_venue') == venue] if venue else []
    use_venue = len(v_entries) >= 30
    base = v_entries if use_venue else completed

    def get_w(e):
        if not ref_date or not e.get('_date'):
            return 1.0
        try:
            d0 = datetime.fromisoformat(ref_date)
            d1 = datetime.fromisoformat(e['_date'])
            days = max(0, (d0 - d1).days)
        except Exception:
            return 1.0
        return math.exp(-days / 90)

    def w_sum(arr, pred):
        return sum(get_w(e) for e in arr if pred(e))

    def w_total(arr):
        return sum(get_w(e) for e in arr) or 1

    pop_stats, hn_stats, jockey_stats = {}, {}, {}
    for p in range(1, 17):
        es = [e for e in base if e.get('pop') == p]
        if len(es) >= 4:
            tot  = w_total(es)
            wr   = w_sum(es, lambda e: e.get('pos') == 1) / tot
            pr   = w_sum(es, lambda e: e.get('pos') is not None and e.get('pos') <= 3) / tot
            w    = min(1.0, len(es) / 40)
            dflt = DEFAULT_POP_WIN[p] if p < len(DEFAULT_POP_WIN) else 0.01
            pop_stats[p] = {'wr': w * wr + (1 - w) * dflt, 'pr': w * pr + (1 - w) * dflt * 3,
                            'raw': wr, 'n': len(es)}
    for h in range(1, 17):
        es = [e for e in base if e.get('hn') == h]
        if len(es) >= 4:
            tot = w_total(es)
            wr  = w_sum(es, lambda e: e.get('pos') == 1) / tot
            pr  = w_sum(es, lambda e: e.get('pos') is not None and e.get('pos') <= 3) / tot
            w   = min(1.0, len(es) / 30)
            hn_stats[h] = {'wr': w * wr + (1 - w) * (1 / 8), 'pr': w * pr + (1 - w) * (3 / 8),
                           'raw': wr, 'n': len(es)}
    # 騎手別統計（最低4件以上の記録が必要）
    jockeys = {e.get('jockey') for e in base if e.get('jockey')}
    for jk in jockeys:
        es = [e for e in base if e.get('jockey') == jk]
        if len(es) >= 4:
            tot = w_total(es)
            wr  = w_sum(es, lambda e: e.get('pos') == 1) / tot
            pr  = w_sum(es, lambda e: e.get('pos') is not None and e.get('pos') <= 3) / tot
            w   = min(1.0, len(es) / 20)
            jockey_stats[jk] = {'wr': w * wr + (1 - w) * (1 / 8),
                                 'pr': w * pr + (1 - w) * (3 / 8),
                                 'raw': wr, 'n': len(es)}
    # 馬別累積実績統計（最低5走以上）
    horse_stats = {}
    horse_names = {e.get('horseName') for e in base if e.get('horseName')}
    for nm in horse_names:
        es = [e for e in base if e.get('horseName') == nm]
        if len(es) >= 5:
            tot = w_total(es)
            wr  = w_sum(es, lambda e: e.get('pos') == 1) / tot
            pr  = w_sum(es, lambda e: e.get('pos') is not None and e.get('pos') <= 3) / tot
            w   = min(1.0, len(es) / 15)
            horse_stats[nm] = {'wr': w * wr + (1 - w) * (1 / 8),
                                'pr': w * pr + (1 - w) * (3 / 8),
                                'raw': wr, 'n': len(es)}

    # 調教師別統計（最低6走以上）
    trainer_stats = {}
    trainers = {e.get('trainer') for e in base if e.get('trainer')}
    for tr in trainers:
        es = [e for e in base if e.get('trainer') == tr]
        if len(es) >= 6:
            tot = w_total(es)
            wr  = w_sum(es, lambda e: e.get('pos') == 1) / tot
            pr  = w_sum(es, lambda e: e.get('pos') is not None and e.get('pos') <= 3) / tot
            w   = min(1.0, len(es) / 25)
            trainer_stats[tr] = {'wr': w * wr + (1 - w) * (1 / 8),
                                  'pr': w * pr + (1 - w) * (3 / 8),
                                  'raw': wr, 'n': len(es)}

    # 父系（種牡馬）統計（最低6走以上、馬場/距離別も同時集計）
    sire_stats = {}
    sires = {e.get('sire') for e in base if e.get('sire')}
    for s in sires:
        es = [e for e in base if e.get('sire') == s]
        if len(es) >= 6:
            tot = w_total(es)
            wr  = w_sum(es, lambda e: e.get('pos') == 1) / tot
            pr  = w_sum(es, lambda e: e.get('pos') is not None and e.get('pos') <= 3) / tot
            w   = min(1.0, len(es) / 30)
            stat = {'wr': w * wr + (1 - w) * (1 / 8),
                    'pr': w * pr + (1 - w) * (3 / 8),
                    'raw': wr, 'n': len(es)}
            # Lv3用: 重/不良馬場別・短/長距離別勝率
            heavy_es = [e for e in es if e.get('_track') in ('重', '不良')]
            if len(heavy_es) >= 4:
                ht = w_total(heavy_es)
                stat['heavyWr'] = w_sum(heavy_es, lambda e: e.get('pos') == 1) / ht
                stat['heavyN']  = len(heavy_es)
            short_es = [e for e in es if e.get('_distance') and e.get('_distance') <= 1300]
            if len(short_es) >= 4:
                st2 = w_total(short_es)
                stat['shortWr'] = w_sum(short_es, lambda e: e.get('pos') == 1) / st2
                stat['shortN']  = len(short_es)
            long_es = [e for e in es if e.get('_distance') and e.get('_distance') >= 1700]
            if len(long_es) >= 4:
                lt = w_total(long_es)
                stat['longWr'] = w_sum(long_es, lambda e: e.get('pos') == 1) / lt
                stat['longN']  = len(long_es)
            sire_stats[s] = stat

    # 母父（broodmare sire）統計（最低6走以上）
    bms_stats = {}
    bms_list = {e.get('broodmareSire') for e in base if e.get('broodmareSire')}
    for b in bms_list:
        es = [e for e in base if e.get('broodmareSire') == b]
        if len(es) >= 6:
            tot = w_total(es)
            wr  = w_sum(es, lambda e: e.get('pos') == 1) / tot
            pr  = w_sum(es, lambda e: e.get('pos') is not None and e.get('pos') <= 3) / tot
            w   = min(1.0, len(es) / 30)
            bms_stats[b] = {'wr': w * wr + (1 - w) * (1 / 8),
                             'pr': w * pr + (1 - w) * (3 / 8),
                             'raw': wr, 'n': len(es)}

    return {
        'popStats':     pop_stats,
        'hnStats':      hn_stats,
        'jockeyStats':  jockey_stats,
        'horseStats':   horse_stats,
        'trainerStats': trainer_stats,
        'sireStats':    sire_stats,
        'bmsStats':     bms_stats,
        'useVenue':     use_venue,
        'baseName':     venue if use_venue else '全競馬場',
        'baseN':        len(base),
    }


# --- Harville式（三連複/三連単の正確な確率計算）---
def harville_p(prob_map: dict, a, b, c) -> float:
    pa, pb, pc = prob_map.get(a, 0), prob_map.get(b, 0), prob_map.get(c, 0)
    pb2 = pb / (1 - pa) if (1 - pa) > 1e-9 else 0
    pc3 = pc / (1 - pa - pb) if (1 - pa - pb) > 1e-9 else 0
    return pa * pb2 * pc3


def trio_harville_p(prob_map: dict, hn1, hn2, hn3) -> float:
    perms = [
        (hn1, hn2, hn3), (hn1, hn3, hn2),
        (hn2, hn1, hn3), (hn2, hn3, hn1),
        (hn3, hn1, hn2), (hn3, hn2, hn1),
    ]
    return max(1e-9, sum(harville_p(prob_map, a, b, c) for a, b, c in perms))


def quinella_harville_p(prob_map: dict, hn1, hn2) -> float:
    p1, p2 = prob_map.get(hn1, 0), prob_map.get(hn2, 0)
    p12 = p1 * (p2 / (1 - p1)) if (1 - p1) > 1e-9 else 0
    p21 = p2 * (p1 / (1 - p2)) if (1 - p2) > 1e-9 else 0
    return max(1e-9, p12 + p21)


def field_size_corrected_probs(prob_map: dict, field_size: int) -> dict:
    alpha = 0.85 if field_size >= 12 else 0.92 if field_size >= 9 else 1.0
    if alpha == 1.0:
        return prob_map
    raw = {k: max(v, 1e-9) ** alpha for k, v in prob_map.items()}
    total = sum(raw.values()) or 1
    return {k: v / total for k, v in raw.items()}


STRAIGHT_VENUES = {'帯広'}  # ばんえい競馬（直線走路）は馬番補正なし


def get_race_cond_mult(h: dict, race_info: dict, venue: str,
                      sire_stats: dict = None) -> float:
    """レース条件（馬場・天候・距離・頭数・レース番号 + 父×馬場/距離適性）による補正倍率"""
    mult       = 1.0
    track      = race_info.get('track')
    weather    = race_info.get('weather')
    distance   = race_info.get('distance')
    field_size = race_info.get('fieldSize', 8)
    race_no    = race_info.get('raceNo', 99)
    hn         = h.get('hn', 8)
    pop        = h.get('pop', 8)
    is_straight = venue in STRAIGHT_VENUES

    is_heavy  = track in ('重', '不良')
    is_slight = track == '稍重'

    # ── Lv3 父×馬場/距離 適性補正 ──
    if sire_stats and h.get('sire') and sire_stats.get(h['sire']):
        ss = sire_stats[h['sire']]
        base_wr = ss.get('wr') or 0.1
        if is_heavy and ss.get('heavyWr') is not None and ss.get('heavyN', 0) >= 4:
            ratio = ss['heavyWr'] / max(base_wr, 0.01)
            mult *= max(0.95, min(1.05, 0.9 + ratio * 0.075))
        if distance and distance <= 1300 and ss.get('shortWr') is not None and ss.get('shortN', 0) >= 4:
            ratio = ss['shortWr'] / max(base_wr, 0.01)
            mult *= max(0.95, min(1.05, 0.9 + ratio * 0.075))
        if distance and distance >= 1700 and ss.get('longWr') is not None and ss.get('longN', 0) >= 4:
            ratio = ss['longWr'] / max(base_wr, 0.01)
            mult *= max(0.95, min(1.05, 0.9 + ratio * 0.075))

    # 馬場状態（内枠有利、荒れやすい）
    if not is_straight:
        if is_heavy:
            hn_m = (1.07 if hn <= 2 else 1.04 if hn <= 4 else 1.01 if hn <= 7
                    else 0.99 if hn <= 10 else 0.96 if hn <= 13 else 0.93)
            mult *= hn_m
        elif is_slight:
            hn_m = (1.03 if hn <= 2 else 1.02 if hn <= 4 else 1.00 if hn <= 7
                    else 0.99 if hn <= 10 else 0.97)
            mult *= hn_m

    if is_heavy:
        if   pop == 1: mult *= 0.94
        elif pop == 2: mult *= 0.97
        elif pop >= 6: mult *= 1.05
        elif pop >= 4: mult *= 1.02
    elif is_slight:
        if   pop == 1: mult *= 0.98
        elif pop >= 6: mult *= 1.01

    # 天候（馬場「良」でも雨なら軽微に補正）
    is_rainy = weather in ('雨', '小雨', '雪')
    if is_rainy and not is_heavy and not is_slight and not is_straight:
        hn_m = 1.02 if hn <= 3 else 0.98 if hn >= 12 else 1.0
        mult *= hn_m
        if pop == 1: mult *= 0.99

    # 距離
    if distance and not is_straight:
        if distance <= 900:
            hn_m = (1.07 if hn <= 2 else 1.04 if hn <= 4 else 1.01 if hn <= 6
                    else 0.99 if hn <= 9 else 0.96 if hn <= 12 else 0.93)
            mult *= hn_m
        elif distance <= 1200:
            hn_m = (1.05 if hn <= 2 else 1.03 if hn <= 4 else 1.01 if hn <= 7
                    else 0.99 if hn <= 10 else 0.97 if hn <= 13 else 0.95)
            mult *= hn_m
        elif distance <= 1600:
            hn_m = 1.02 if hn <= 3 else 1.01 if hn <= 6 else 1.00 if hn <= 11 else 0.98
            mult *= hn_m
        elif distance <= 2000:
            hn_m = 1.01 if hn <= 3 else 0.99 if hn >= 13 else 1.0
            mult *= hn_m

    # 頭数（少頭数≤7: 1人気有利、大頭数≥11: 多人気有利）
    if field_size <= 7:
        if   pop == 1: mult *= 1.05
        elif pop <= 3: mult *= 1.02
        elif pop >= 7: mult *= 0.92
    elif field_size >= 13:
        if   pop == 1: mult *= 0.95
        elif pop <= 3: mult *= 0.98
        elif pop >= 7: mult *= 1.04
    elif field_size >= 11:
        if   pop == 1: mult *= 0.97
        elif pop >= 7: mult *= 1.02

    # 早期レース（1-3R）は荒れやすい
    if race_no <= 3:
        if   pop == 1: mult *= 0.97
        elif pop >= 5: mult *= 1.02

    return mult


def score_horses(horses: list, hist_stats: dict, race_info: dict = None, venue: str = '') -> list:
    pop_stats     = hist_stats['popStats']
    hn_stats      = hist_stats['hnStats']
    jockey_stats  = hist_stats.get('jockeyStats', {})
    horse_stats   = hist_stats.get('horseStats', {})
    trainer_stats = hist_stats.get('trainerStats', {})
    sire_stats    = hist_stats.get('sireStats', {})
    bms_stats     = hist_stats.get('bmsStats', {})
    field_size    = len(horses)
    has_odds       = any(h.get('odds') for h in horses)
    has_jockeys    = any(h.get('jockey') and jockey_stats.get(h['jockey']) for h in horses)
    has_horse_hist = any(h.get('horseName') and horse_stats.get(h['horseName']) for h in horses)
    has_trainers   = any(h.get('trainer') and trainer_stats.get(h['trainer']) for h in horses)
    has_sires      = any(h.get('sire') and sire_stats.get(h['sire']) for h in horses)
    has_bms        = any(h.get('broodmareSire') and bms_stats.get(h['broodmareSire']) for h in horses)
    race_info      = race_info or {}

    # オーバーラウンド除去（地方競馬の控除率は約25%）
    overround = (sum(1 / h['odds'] for h in horses if h.get('odds')) or 1) if has_odds else 1

    # 血統・調教師シグナル分の合計重み（最大 15%）
    ped_sire_w = 0.06 if has_sires    else 0.0
    ped_bms_w  = 0.04 if has_bms      else 0.0
    tr_w       = 0.05 if has_trainers else 0.0
    ped_total  = ped_sire_w + ped_bms_w + tr_w
    main_w     = 1.0 - ped_total

    raw = []
    for h in horses:
        pop    = h.get('pop')
        odds   = h.get('odds')
        hn     = h.get('hn')
        jockey = h.get('jockey')

        mkt_prob = ((1 / odds) / overround) if odds else (
            DEFAULT_POP_WIN[pop] if pop and 1 <= pop < len(DEFAULT_POP_WIN) else 1 / field_size
        )

        if pop and pop in pop_stats:
            pop_score = pop_stats[pop]['wr']
            pop_place = min(pop_stats[pop].get('pr', pop_score * 3), 1.0)
        elif pop and 1 <= pop < len(DEFAULT_POP_WIN):
            pop_score = DEFAULT_POP_WIN[pop]
            pop_place = min(pop_score * 3, 1.0)
        else:
            pop_score = 1 / field_size
            pop_place = min(3 / field_size, 1.0)

        if hn and hn in hn_stats:
            hn_score = hn_stats[hn]['wr']
            hn_place = hn_stats[hn].get('pr', hn_score * 3)
        else:
            hn_score = 1 / field_size
            hn_place = 3 / field_size

        # 騎手スコア
        if jockey and jockey in jockey_stats:
            jk_score = jockey_stats[jockey]['wr']
            jk_place = jockey_stats[jockey].get('pr', jk_score * 3)
        else:
            jk_score = 1 / field_size
            jk_place = 3 / field_size

        # 馬別累積実績スコア
        horse_name = h.get('horseName')
        if horse_name and horse_name in horse_stats:
            horse_score = horse_stats[horse_name]['wr']
            horse_pl    = horse_stats[horse_name].get('pr', horse_score * 3)
        else:
            horse_score = 1 / field_size
            horse_pl    = 3 / field_size

        # 調教師スコア (Lv4)
        trainer = h.get('trainer')
        if trainer and trainer in trainer_stats:
            tr_score = trainer_stats[trainer]['wr']
            tr_place = trainer_stats[trainer].get('pr', tr_score * 3)
        else:
            tr_score = 1 / field_size
            tr_place = 3 / field_size

        # 父系（種牡馬）スコア (Lv2)
        sire = h.get('sire')
        if sire and sire in sire_stats:
            sire_score = sire_stats[sire]['wr']
            sire_place = sire_stats[sire].get('pr', sire_score * 3)
        else:
            sire_score = 1 / field_size
            sire_place = 3 / field_size

        # 母父スコア (Lv5)
        bms = h.get('broodmareSire')
        if bms and bms in bms_stats:
            bms_score = bms_stats[bms]['wr']
            bms_place = bms_stats[bms].get('pr', bms_score * 3)
        else:
            bms_score = 1 / field_size
            bms_place = 3 / field_size

        # 既存5シグナルのベーススコア（main_w で正規化）
        if has_odds and has_jockeys and has_horse_hist:
            base_comp  = 0.40*mkt_prob + 0.20*pop_score + 0.12*hn_score + 0.13*jk_score + 0.15*horse_score
            base_pl    = (0.36*min(mkt_prob*3, 1) + 0.20*pop_place
                          + 0.14*hn_place + 0.13*jk_place + 0.17*horse_pl)
        elif has_odds and has_horse_hist:
            base_comp  = 0.44*mkt_prob + 0.23*pop_score + 0.14*hn_score + 0.19*horse_score
            base_pl    = 0.40*min(mkt_prob*3, 1) + 0.23*pop_place + 0.17*hn_place + 0.20*horse_pl
        elif has_jockeys and has_horse_hist:
            base_comp  = 0.44*pop_score + 0.19*hn_score + 0.19*jk_score + 0.18*horse_score
            base_pl    = 0.40*pop_place  + 0.22*hn_place + 0.20*jk_place + 0.18*horse_pl
        elif has_horse_hist:
            base_comp  = 0.50*pop_score + 0.30*hn_score + 0.20*horse_score
            base_pl    = 0.45*pop_place  + 0.35*hn_place + 0.20*horse_pl
        elif has_odds and has_jockeys:
            base_comp  = 0.45*mkt_prob + 0.25*pop_score + 0.15*hn_score + 0.15*jk_score
            base_pl    = (0.42*min(mkt_prob*3, 1) + 0.25*pop_place
                          + 0.18*hn_place + 0.15*jk_place)
        elif has_odds:
            base_comp  = 0.50*mkt_prob + 0.30*pop_score + 0.20*hn_score
            base_pl    = 0.45*min(mkt_prob*3, 1) + 0.30*pop_place + 0.25*hn_place
        elif has_jockeys:
            base_comp  = 0.55*pop_score + 0.25*hn_score + 0.20*jk_score
            base_pl    = 0.50*pop_place  + 0.30*hn_place + 0.20*jk_place
        else:
            base_comp  = 0.60*pop_score + 0.40*hn_score
            base_pl    = 0.55*pop_place  + 0.45*hn_place

        # 血統・調教師シグナルを合算
        composite       = main_w * base_comp  + ped_sire_w * sire_score + ped_bms_w * bms_score + tr_w * tr_score
        composite_place = main_w * base_pl    + ped_sire_w * sire_place + ped_bms_w * bms_place + tr_w * tr_place

        # 馬体重変動補正
        weight_mult = 1.0
        wd = h.get('weightDiff')
        if wd is not None:
            if   wd <= -10: weight_mult = 0.95
            elif wd <=  -6: weight_mult = 0.98
            elif wd >=  10: weight_mult = 0.96
            elif wd >=   6: weight_mult = 0.98

        # レース条件補正（Lv3 父×馬場/距離 適性補正を含む）
        cond_mult = get_race_cond_mult(
            h, {**race_info, 'fieldSize': field_size}, venue, sire_stats
        )
        raw.append({
            **h,
            'mktProb': mkt_prob, 'popScore': pop_score, 'hnScore': hn_score,
            'jkScore': jk_score, 'horseScore': horse_score,
            'trScore': tr_score, 'sireScore': sire_score, 'bmsScore': bms_score,
            'composite':      composite       * cond_mult * weight_mult,
            'compositePlace': composite_place * cond_mult * weight_mult,
            'condMult': cond_mult, 'weightMult': weight_mult,
        })

    total     = sum(r['composite']      for r in raw) or 1
    tot_pl    = sum(r['compositePlace'] for r in raw) or 1
    mkt_tot   = sum(r['mktProb']        for r in raw) or 1
    pop_tot   = sum(r['popScore']       for r in raw) or 1
    hn_tot    = sum(r['hnScore']        for r in raw) or 1
    horse_tot = sum(r['horseScore']     for r in raw) or 1
    sire_tot  = sum(r['sireScore']      for r in raw) or 1
    bms_tot   = sum(r['bmsScore']       for r in raw) or 1
    tr_tot    = sum(r['trScore']        for r in raw) or 1

    ranked = [{**r,
               'prob':     r['composite']      / total,
               'probPl':   r['compositePlace'] / tot_pl,
               'mktPct':   r['mktProb']    / mkt_tot,
               'popPct':   r['popScore']   / pop_tot,
               'hnPct':    r['hnScore']    / hn_tot,
               'horsePct': r['horseScore'] / horse_tot,
               'sirePct':  r['sireScore']  / sire_tot,
               'bmsPct':   r['bmsScore']   / bms_tot,
               'trPct':    r['trScore']    / tr_tot,
               } for r in raw]
    ranked.sort(key=lambda x: x['prob'], reverse=True)
    return ranked


def bot_decide(ranked: list, settings: dict, balance: float):
    strategy      = settings.get('strategy', DEFAULT_SETTINGS['strategy'])
    bet_amount    = settings.get('betAmount', 1000)
    min_odds      = settings.get('minOdds', DEFAULT_SETTINGS['minOdds'])
    max_odds      = settings.get('maxOdds', DEFAULT_SETTINGS['maxOdds'])
    use_kelly     = settings.get('useKelly', False)
    kelly_frac    = settings.get('kellyFraction', 0.25)
    amount        = min(bet_amount, balance)
    if amount <= 0:
        return None

    def _place_safety_score(h: dict, rank_index: int) -> float:
        field_size = max(len(ranked), 1)
        pop = h.get('pop')
        pop_strength = ((field_size + 1 - pop) / field_size) if pop else 0.5
        rank_bonus = max(0, 1 - rank_index / max(field_size - 1, 1))
        return (
            0.45 * h.get('probPl', 0) +
            0.30 * h.get('mktPct', h.get('prob', 0)) +
            0.15 * pop_strength +
            0.10 * rank_bonus
        )

    def _safe_place_target():
        field_size = max(len(ranked), 1)
        by_place = sorted(enumerate(ranked), key=lambda item: item[1].get('probPl', 0), reverse=True)
        candidates = by_place[:min(3, len(by_place))]
        if not candidates:
            return None
        idx, target = max(candidates, key=lambda item: _place_safety_score(item[1], item[0]))

        baseline = 1 / field_size
        pop = target.get('pop')
        odds = target.get('odds')
        top_gap = ranked[0].get('prob', 0) - (ranked[1].get('prob', 0) if len(ranked) > 1 else 0)

        # 的中率優先: 人気薄・高オッズ・極端な混戦は見送る。
        if pop and pop > 4:
            return None
        if odds and (odds < min_odds or odds > min(max_odds, 8.0)):
            return None
        if target.get('probPl', 0) < baseline * 1.08:
            return None
        if target.get('mktPct') and target.get('mktPct', 0) < baseline * 0.92:
            return None
        if idx > 1 and top_gap < baseline * 0.18:
            return None
        target = dict(target)
        target['confidenceScore'] = round(_place_safety_score(target, idx), 4)
        return target

    def _prob_map():
        return field_size_corrected_probs({h['hn']: h['prob'] for h in ranked}, len(ranked))

    # 三連複・三連単・馬連BOX（Harville式オッズ推定）
    if strategy in (
        'trio_box', 'trifecta', 'trio_box4', 'trio_box5',
        'quinella_box3', 'trifecta_box3', 'trifecta_box4',
    ):
        if len(ranked) < 3:
            return None
        prob_map = _prob_map()
        by_place = sorted(ranked, key=lambda h: h.get('probPl', 0), reverse=True)

        if strategy == 'quinella_box3':
            top = by_place[:3]
            p = sum(
                quinella_harville_p(prob_map, top[i]['hn'], top[j]['hn'])
                for i, j in [(0, 1), (0, 2), (1, 2)]
            )
            combo_count = 3
            # JS と同様に 1点あたり bet_amount を掛ける（合計 = per_amount × combo_count）
            per_amount = max(100, min(bet_amount, int(balance // combo_count)))
            total_amount = per_amount * combo_count
            if total_amount > balance:
                return None
            estimated_odds = max(2.0, round((0.75 / p) / combo_count * 10) / 10)
            return {
                'hn': top[0]['hn'], 'horses': [h['hn'] for h in top],
                'pop': top[0].get('pop'), 'betType': 'quinella_box3',
                'winOdds': None, 'estimatedOdds': estimated_odds,
                'amount': total_amount, '_comboCount': combo_count, '_perAmount': per_amount,
            }

        if strategy in ('trio_box4', 'trio_box5'):
            need = 4 if strategy == 'trio_box4' else 5
            if len(ranked) < need:
                return None
            top = by_place[:need]
            combo_count = 4 if strategy == 'trio_box4' else 10
            p = sum(
                trio_harville_p(prob_map, top[i]['hn'], top[j]['hn'], top[k]['hn'])
                for i, j, k in combinations(range(need), 3)
            )
            per_amount = max(100, min(bet_amount, int(balance // combo_count)))
            total_amount = per_amount * combo_count
            if total_amount > balance:
                return None
            estimated_odds = max(3.0, round((0.75 / p) / combo_count * 10) / 10)
            return {
                'hn': top[0]['hn'], 'horses': [h['hn'] for h in top],
                'pop': top[0].get('pop'), 'betType': strategy,
                'winOdds': None, 'estimatedOdds': estimated_odds,
                'amount': total_amount, '_comboCount': combo_count, '_perAmount': per_amount,
            }

        if strategy in ('trifecta_box3', 'trifecta_box4'):
            need = 3 if strategy == 'trifecta_box3' else 4
            if len(ranked) < need:
                return None
            top = sorted(ranked, key=lambda h: prob_map.get(h['hn'], 0), reverse=True)[:need]
            combo_count = 6 if strategy == 'trifecta_box3' else 24
            p = harville_p(prob_map, top[0]['hn'], top[1]['hn'], top[2]['hn'])
            per_amount = max(100, min(bet_amount, int(balance // combo_count)))
            total_amount = per_amount * combo_count
            if total_amount > balance:
                return None
            estimated_odds = max(6.0, round((0.75 / p) / combo_count * 10) / 10)
            return {
                'hn': top[0]['hn'], 'horses': [h['hn'] for h in top],
                'pop': top[0].get('pop'), 'betType': strategy,
                'winOdds': None, 'estimatedOdds': estimated_odds,
                'amount': total_amount, '_comboCount': combo_count, '_perAmount': per_amount,
            }

        top3 = by_place[:3]
        if strategy == 'trifecta':
            top3 = sorted(top3, key=lambda h: prob_map.get(h['hn'], 0), reverse=True)

        if strategy == 'trio_box':
            p = trio_harville_p(prob_map, top3[0]['hn'], top3[1]['hn'], top3[2]['hn'])
            estimated_odds = max(3.0, round(0.75 / p * 10) / 10)
        else:
            p = harville_p(prob_map, top3[0]['hn'], top3[1]['hn'], top3[2]['hn'])
            estimated_odds = max(6.0, round(0.75 / p * 10) / 10)

        return {
            'hn':            top3[0]['hn'],
            'horses':        [h['hn'] for h in top3],
            'pop':           top3[0].get('pop'),
            'betType':       'trio' if strategy == 'trio_box' else 'trifecta',
            'winOdds':       None,
            'estimatedOdds': estimated_odds,
            'amount':        amount,
        }

    # 単勝・複勝系
    target   = None
    bet_type = 'win'

    if strategy == 'ai_win':
        target, bet_type = ranked[0], 'win'
    elif strategy == 'ai_place':
        target, bet_type = max(ranked, key=lambda h: h.get('probPl', 0)), 'place'
    elif strategy == 'safe_place':
        target, bet_type = _safe_place_target(), 'place'
    elif strategy == 'pop1_win':
        target = next((h for h in ranked if h.get('pop') == 1), ranked[0])
        bet_type = 'win'
    elif strategy == 'value_win':
        target = next((h for h in ranked if h.get('odds') and h.get('mktPct')
                       and h['prob'] > h['mktPct'] * 1.05), None)
        bet_type = 'win'
    elif strategy == 'dark_horse':
        t3 = [h for h in ranked[:3] if h.get('odds')]
        target = max(t3, key=lambda h: h['odds']) if t3 else None
        bet_type = 'win'
    elif strategy == 'kelly_win':
        target, bet_type = ranked[0], 'win'
        if target and target.get('odds') and target.get('prob', 0) > 0:
            # 控除率補正: sum(1/odds) = overround、prob * overround = 控除分を除いたフェア確率
            overround = sum(1 / h['odds'] for h in ranked if h.get('odds')) or 1.0
            fair_p = min(0.99, target['prob'] * overround)
            b = target['odds'] - 1
            f = max(0, (fair_p * (b + 1) - 1) / b)
            if f <= 0:
                return None
            amount = max(100, min(int(balance * f * kelly_frac), int(balance * 0.15)))
            if amount > balance:
                return None

    if not target:
        return None

    # オッズフィルター（win / ai_place は win オッズで判定）
    if target.get('odds') and (bet_type == 'win' or strategy == 'ai_place'):
        if target['odds'] < min_odds or target['odds'] > max_odds:
            return None

    # useKelly フラグ有効時（kelly_win 以外）
    # Kelly はベット額サイジングツール: f>0 のときだけ Kelly 額を適用し、
    # f<=0（エッジなし）のときはデフォルト額でベットを続行する（ベット見送りにしない）
    if use_kelly and strategy != 'kelly_win':
        p = b = None
        if bet_type == 'win' and target.get('odds') and target.get('prob', 0) > 0:
            p = target['prob']
            b = target['odds'] - 1
        elif bet_type == 'place' and target.get('odds') and target.get('probPl', 0) > 0:
            # 複勝オッズ推定: 単勝超過収益率の約30%（単勝3倍→複勝1.6倍相当）
            est_place_odds = max(1.1, 1 + (target['odds'] - 1) * 0.3)
            # probPl は合計1.0の正規化スコア → 実複勝確率に換算
            field_size = max(len(ranked), 1)
            est_place_prob = min(0.98, target['probPl'] * min(3, field_size))
            p = est_place_prob
            b = est_place_odds - 1
        if p is not None and b is not None:
            f = (p * (b + 1) - 1) / b
            if f > 0:
                kelly_amount = max(100, min(int(balance * f * kelly_frac), int(balance * 0.15)))
                if kelly_amount <= balance:
                    amount = kelly_amount
            # f <= 0: エッジなし → デフォルト amount のままベット（return None しない）

    return {
        'hn':            target['hn'],
        'horses':        None,
        'pop':           target.get('pop'),
        'betType':       bet_type,
        'winOdds':       target.get('odds'),
        'estimatedOdds': None,
        'amount':        amount,
        'confidenceScore': target.get('confidenceScore'),
    }


def _top_finishers(result_map: dict, within: int) -> list:
    """result_map から within 着以内の馬番 int リストを返す。型変換失敗は無視する。"""
    top = []
    for h, p in result_map.items():
        try:
            if p is not None and int(p) <= within:
                top.append(int(h))
        except (TypeError, ValueError):
            continue
    return top


def settle_trade(state: dict, trade: dict, result_map: dict, race_payouts: dict = None):
    """pending bet を決済して history に移動する"""
    def get_pos(hn):
        raw = result_map.get(hn) or result_map.get(str(hn))
        try:
            return int(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    decision = trade['decision']
    bet_type = decision['betType']
    won = False

    if bet_type == 'win':
        won = get_pos(decision['hn']) == 1
    elif bet_type == 'place':
        p = get_pos(decision['hn'])
        won = p is not None and p <= 3
    elif bet_type == 'trio':
        top3 = _top_finishers(result_map, 3)
        won = (len(top3) >= 3 and
               all(hn in top3 for hn in decision['horses']))
    elif bet_type == 'trifecta':
        won = all(get_pos(hn) == i + 1 for i, hn in enumerate(decision['horses']))
    elif bet_type in ('trio_box4', 'trio_box5'):
        top3 = _top_finishers(result_map, 3)
        won = len(top3) >= 3 and all(hn in decision['horses'] for hn in top3)
    elif bet_type == 'trifecta_box3':
        # trio と同じ条件: 選んだ3頭が全て3着以内に入っていれば的中（順不同）
        top3 = _top_finishers(result_map, 3)
        won = (len(top3) >= 3 and all(hn in top3 for hn in decision['horses']))
    elif bet_type == 'trifecta_box4':
        top3 = _top_finishers(result_map, 3)
        won = len(top3) >= 3 and all(hn in decision['horses'] for hn in top3)
    elif bet_type == 'quinella_box3':
        top2 = _top_finishers(result_map, 2)
        won = len(top2) >= 2 and all(hn in decision['horses'] for hn in top2)

    def _lookup_payout(bet_key: str, horses_set=None, hn: int = None) -> float | None:
        """race_payouts から実払戻倍率を取得。見つからなければ None を返す。"""
        if not race_payouts or not won:
            return None
        for entry in race_payouts.get(bet_key, []):
            if horses_set is None:
                # 単勝・複勝: ベットした馬番のエントリを検索
                if hn is not None:
                    h_nums = set(re.findall(r'\d+', str(entry.get('horses', ''))))
                    if str(hn) not in h_nums:
                        continue
                return entry['payout'] / 100
            # 三連複・三連単・馬連: horses フィールドに全馬番が含まれるか確認
            h_txt = str(entry.get('horses', ''))
            nums = set(re.findall(r'\d+', h_txt))
            if horses_set == nums:
                return entry['payout'] / 100
        return None

    if bet_type == 'place':
        real = _lookup_payout('複勝', hn=decision.get('hn'))
        # 実払戻が取得できない場合は 1.0（元本返還相当）で保守的に計上
        eff = real if real is not None else 1.0
    elif bet_type == 'win':
        real = _lookup_payout('単勝', hn=decision.get('hn'))
        eff = real if real is not None else (decision.get('winOdds') or 1.0)
    elif bet_type == 'trio':
        hs = {str(h) for h in (decision.get('horses') or [])}
        real = _lookup_payout('三連複', hs)
        eff = real if real is not None else (decision.get('estimatedOdds') or 1.0)
    elif bet_type == 'trifecta':
        hs = {str(h) for h in (decision.get('horses') or [])}
        real = _lookup_payout('三連単', hs)
        eff = real if real is not None else (decision.get('estimatedOdds') or 1.0)
    elif bet_type in ('trio_box4', 'trio_box5'):
        # 的中時は勝ち3頭を特定して三連複払戻を検索する
        if won:
            top3_hns = _top_finishers(result_map, 3)
            real = _lookup_payout('三連複', {str(h) for h in top3_hns}) if len(top3_hns) == 3 else None
            if real is not None:
                # real は1点(100円)あたりの倍率。BOXは comboCount 点買いなので
                # amount=perAmount×comboCount に対する実効倍率 = real / comboCount
                n = decision.get('_comboCount', 4 if bet_type == 'trio_box4' else 10)
                eff = real / n
            else:
                eff = decision.get('estimatedOdds') or 1.0
        else:
            eff = decision.get('estimatedOdds') or 1.0
    elif bet_type in ('trifecta_box3', 'trifecta_box4'):
        # 的中時は実際の1〜3着馬セットで三連単払戻を検索する
        if won:
            top3_hns = _top_finishers(result_map, 3)
            real = _lookup_payout('三連単', {str(h) for h in top3_hns}) if len(top3_hns) == 3 else None
            if real is not None:
                n = decision.get('_comboCount', 6 if bet_type == 'trifecta_box3' else 24)
                eff = real / n
            else:
                eff = decision.get('estimatedOdds') or 1.0
        else:
            eff = decision.get('estimatedOdds') or 1.0
    elif bet_type == 'quinella_box3':
        # 的中時は1〜2着馬を特定して馬連払戻を検索する
        if won:
            top2_hns = _top_finishers(result_map, 2)
            real = _lookup_payout('馬連', {str(h) for h in top2_hns}) if len(top2_hns) == 2 else None
            if real is not None:
                n = decision.get('_comboCount', 3)
                eff = real / n
            else:
                eff = decision.get('estimatedOdds') or 1.0
        else:
            eff = decision.get('estimatedOdds') or 1.0
    else:
        eff = decision.get('winOdds') or decision.get('estimatedOdds') or 1.0

    payout = int(decision['amount'] * eff) if won else 0
    profit = payout - decision['amount']
    state['balance'] = round(state['balance'] + payout)

    settled = {**trade, 'status': 'won' if won else 'lost',
               'effectiveOdds': eff, 'payout': payout, 'profit': profit,
               'balanceAfter': state['balance'],
               'settledAt': datetime.now(JST).isoformat()}
    state['pending'] = [t for t in state['pending'] if t['tradeKey'] != trade['tradeKey']]
    state['history'].insert(0, settled)
    if len(state['history']) > 500:
        state['history'] = state['history'][:500]

    type_label = {'win': '単勝', 'place': '複勝', 'trio': '三連複', 'trifecta': '三連単'}.get(bet_type, bet_type)
    res_label  = f'的中! +¥{profit:,}' if won else f'外れ -¥{decision["amount"]:,}'
    add_log(state, f'{trade["venue"]}{trade["raceNo"]}R {type_label} → {res_label} (残高 ¥{state["balance"]:,})',
            'ok' if won else 'err')


def settle_pending_trades(state: dict):
    """日付をまたいだ pending も含めて決済できるものを決済する。"""
    pending = list(state.get('pending', []))
    groups = {}
    for trade in pending:
        venue = trade.get('venue')
        date_key = trade.get('date')
        code = VENUE_CODES.get(venue)
        if not venue or not date_key or not code:
            continue
        groups.setdefault((venue, date_key, code), []).append(trade)

    for (venue, date_key, code), trades in groups.items():
        date_str = date_key.replace('-', '/')
        time.sleep(0.5)
        result_map, payout_map = get_results(date_str, code)
        if not result_map:
            continue
        for trade in list(trades):
            rno = trade.get('raceNo')
            rr = result_map.get(rno) or result_map.get(str(rno))
            if rr:
                rp = payout_map.get(rno) or payout_map.get(str(rno)) or {}
                settle_trade(state, trade, rr, rp)


# ── メイン ────────────────────────────────────────────────────────

def main():
    now      = datetime.now(JST)
    date_str = now.strftime('%Y/%m/%d')
    date_key = now.strftime('%Y-%m-%d')

    print(f'=== クラウドBot実行: {date_str} ===')

    DATA_DIR.mkdir(exist_ok=True)

    # 設定・状態の読み込み
    settings = load_json(SETTINGS_PATH, DEFAULT_SETTINGS)
    state    = load_json(STATE_PATH, DEFAULT_STATE)
    if state.get('balance') is None:
        state['balance'] = settings.get('initialBalance', 100000)
        print(f'  残高初期化: ¥{state["balance"]:,}')

    add_log(state, f'実行開始 {date_str} 残高¥{state["balance"]:,}', 'inf')

    # 過去データで統計構築
    all_entries = load_all_past_entries()
    print(f'  過去エントリー数: {len(all_entries)}')

    # 先に全 pending を決済する。前日以前の未決済が残ると残高が戻らないため。
    settle_pending_trades(state)

    # 今日の開催場
    venues = get_schedule()
    if not venues:
        add_log(state, '本日の開催場なし', 'inf')
        save_json(STATE_PATH, state)
        return

    venue_filter = settings.get('venueFilter', '')
    if venue_filter:
        venues = [v for v in venues if v['name'] == venue_filter]
    add_log(state, f'開催場: {" ".join(v["name"] for v in venues)}', 'inf')

    # 処理済みキーセット
    processed_keys = {t['tradeKey'] for t in state.get('pending', []) + state.get('history', [])}

    for venue in venues:
        name = venue['name']
        code = venue['code']
        print(f'\n--- {name} ---')

        # 新規ベット
        time.sleep(0.5)
        race_nos = get_race_list(date_str, code)
        if not race_nos:
            continue
        print(f'  レース数: {len(race_nos)}')

        hist_stats = build_hist_stats(all_entries, name, ref_date=date_key)

        for rno in race_nos:
            key = f'{date_key}_{name}_{rno}'
            if key in processed_keys:
                continue

            time.sleep(0.5)
            horses, race_info = get_entries(date_str, code, rno, venue=name)
            if not horses:
                continue

            race_info['raceNo'] = rno
            ranked   = score_horses(horses, hist_stats, race_info, name)
            decision = bot_decide(ranked, settings, state['balance'])
            if not decision:
                continue

            state['balance'] -= decision['amount']
            state['balance'] = round(state['balance'])

            trade = {
                'tradeKey': key,
                'date':     date_key,
                'venue':    name,
                'raceNo':   rno,
                'decision': decision,
                'status':   'pending',
                'placedAt': now.isoformat(),
            }
            state.setdefault('pending', []).append(trade)
            processed_keys.add(key)

            type_label = {'win': '単勝', 'place': '複勝', 'trio': '三連複', 'trifecta': '三連単'}.get(decision['betType'], decision['betType'])
            target_label = ('-'.join(str(h) for h in decision['horses']) + '番'
                            if decision['horses'] else f'{decision["hn"]}番')
            add_log(state,
                    f'{name}{rno}R → {type_label} {target_label} ¥{decision["amount"]:,} ベット', 'ok')

    # サマリー
    n_pending = len(state.get('pending', []))
    n_history = len(state.get('history', []))
    wins = sum(1 for t in state.get('history', []) if t.get('status') == 'won')
    add_log(state, f'完了: 未決済{n_pending}件 / 履歴{n_history}件 / 的中{wins}件 残高¥{state["balance"]:,}', 'inf')

    save_json(STATE_PATH, state)
    print(f'\n保存完了: {STATE_PATH}')
    print(f'残高: ¥{state["balance"]:,}  未決済: {n_pending}件  履歴: {n_history}件')


if __name__ == '__main__':
    main()
