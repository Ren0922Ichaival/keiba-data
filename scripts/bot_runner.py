"""
地方競馬 クラウドボット
GitHub Actions で定期実行し、仮想売買シミュレーションを data/bot_state.json に蓄積する
"""
import json
import math
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))
BASE = 'https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo'

VENUE_CODES = {
    '帯広': 3,  '水沢': 11, '浦和': 18, '金沢': 22, '名古屋': 24,
    '園田': 27, '門別': 30, '高知': 31, '佐賀': 32,
    '盛岡': 35, '船橋': 43, '大井': 44, '川崎': 45, '笠松': 47, '姫路': 51,
}
CODE_TO_VENUE = {v: k for k, v in VENUE_CODES.items()}

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
    'strategy': 'ai_win',
    'minOdds': 1.5,
    'maxOdds': 20.0,
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
    return BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')


def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            pass
    return default.copy()


def save_json(path: Path, obj):
    DATA_DIR.mkdir(exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding='utf-8')


def add_log(state: dict, msg: str, level: str = 'inf'):
    now = datetime.now(JST).strftime('%H:%M')
    state['log'].insert(0, {'time': now, 'msg': msg, 'type': level})
    if len(state['log']) > 200:
        state['log'] = state['log'][:200]
    print(f'[{now}] {msg}')


# ── 過去データ読み込み ─────────────────────────────────────────────

def load_all_past_entries() -> list:
    """data/*.json から全エントリーを読み込み [{hn,pop,odds,pos,_venue,_date}] 形式で返す"""
    entries = []
    for p in sorted(DATA_DIR.glob('????-??-??.json')):
        try:
            obj  = json.loads(p.read_text(encoding='utf-8'))
            date = obj.get('date', p.stem)  # YYYY-MM-DD
            for venue in obj.get('venues', []):
                vname = venue.get('name', '')
                for race in venue.get('races', []):
                    for e in race.get('entries', []):
                        entries.append({
                            'hn':    e.get('hn'),
                            'pop':   e.get('pop'),
                            'odds':  e.get('odds'),
                            'pos':   e.get('pos'),
                            '_venue': vname,
                            '_date':  date,
                        })
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


def get_entries(date_str: str, code: int, race_no: int) -> list:
    """出走馬データ [{hn, pop, odds}]"""
    try:
        soup = get_soup(f'{BASE}/DebaTable',
                        params={'k_raceDate': date_str, 'k_babaCode': code, 'k_raceNo': race_no})
        horses, seen = [], set()
        for row in soup.find_all('tr'):
            hn_cell = row.find('td', class_='horseNum')
            if not hn_cell:
                continue
            try:
                hn = int(hn_cell.get_text(strip=True))
                if not (1 <= hn <= 16) or hn in seen:
                    continue
            except (ValueError, TypeError):
                continue
            odds_val = pop_val = None
            odds_cell = row.find('td', class_='odds_weight')
            if odds_cell:
                t = odds_cell.get_text(' ', strip=True)
                m = re.search(r'([\d]+\.[\d]+)\s*[（(](\d+)人気[）)]', t)
                if m:
                    odds_val = float(m.group(1))
                    pop_val  = int(m.group(2))
            seen.add(hn)
            horses.append({'hn': hn, 'pop': pop_val, 'odds': odds_val})
        horses.sort(key=lambda h: h['hn'])
        return horses
    except Exception as e:
        print(f'  エントリー取得失敗: {e}')
        return []


def get_results(date_str: str, code: int) -> dict:
    """着順結果 {raceNo(int): {hn(int): pos(int)}}"""
    try:
        soup = get_soup(f'{BASE}/RefundMoneyList',
                        params={'k_raceDate': date_str, 'k_babaCode': code})
        results = {}
        current_race = None
        for elem in soup.find_all(['p', 'table']):
            if elem.name == 'p':
                m = re.match(r'^(\d+)R$', elem.get_text(strip=True))
                if m:
                    current_race = int(m.group(1))
            elif elem.name == 'table' and current_race and current_race not in results:
                pos_map = {}
                for row in elem.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 3:
                        continue
                    try:
                        pos = int(cells[0].get_text(strip=True))
                        hn  = int(cells[2].get_text(strip=True))
                        if 1 <= pos <= 20 and 1 <= hn <= 16:
                            pos_map[hn] = pos
                    except (ValueError, TypeError):
                        continue
                if pos_map:
                    results[current_race] = pos_map
                current_race = None
        return results
    except Exception as e:
        print(f'  着順取得失敗: {e}')
        return {}


# ── 予想ロジック（JS と完全一致） ────────────────────────────────

def build_hist_stats(all_entries: list, venue: str, ref_date: str = None) -> dict:
    """統計データ構築。ref_date 指定時は直近データに重みをかける（指数減衰 90日）"""
    v_entries = [e for e in all_entries if e.get('_venue') == venue] if venue else []
    use_venue = len(v_entries) >= 30
    base = v_entries if use_venue else all_entries

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

    pop_stats, hn_stats = {}, {}
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

    return {
        'popStats': pop_stats,
        'hnStats':  hn_stats,
        'useVenue': use_venue,
        'baseName': venue if use_venue else '全競馬場',
        'baseN': len(base),
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


def score_horses(horses: list, hist_stats: dict) -> list:
    pop_stats  = hist_stats['popStats']
    hn_stats   = hist_stats['hnStats']
    field_size = len(horses)
    has_odds   = any(h.get('odds') for h in horses)

    # オーバーラウンド除去（地方競馬の控除率は約25%）
    overround = (sum(1 / h['odds'] for h in horses if h.get('odds')) or 1) if has_odds else 1

    raw = []
    for h in horses:
        pop  = h.get('pop')
        odds = h.get('odds')
        hn   = h.get('hn')

        mkt_prob = ((1 / odds) / overround) if odds else (
            DEFAULT_POP_WIN[pop] if pop and 1 <= pop < len(DEFAULT_POP_WIN) else 1 / field_size
        )

        if pop and pop in pop_stats:
            pop_score = pop_stats[pop]['wr']
            pop_place = pop_stats[pop].get('pr', pop_score * 3)
        elif pop and 1 <= pop < len(DEFAULT_POP_WIN):
            pop_score = DEFAULT_POP_WIN[pop]
            pop_place = pop_score * 3
        else:
            pop_score = 1 / field_size
            pop_place = 3 / field_size

        if hn and hn in hn_stats:
            hn_score = hn_stats[hn]['wr']
            hn_place = hn_stats[hn].get('pr', hn_score * 3)
        else:
            hn_score = 1 / field_size
            hn_place = 3 / field_size

        composite = (
            0.50 * mkt_prob + 0.30 * pop_score + 0.20 * hn_score if has_odds
            else 0.60 * pop_score + 0.40 * hn_score
        )
        composite_place = (
            0.45 * min(mkt_prob * 3, 1) + 0.30 * pop_place + 0.25 * hn_place if has_odds
            else 0.55 * pop_place + 0.45 * hn_place
        )
        raw.append({**h, 'mktProb': mkt_prob, 'popScore': pop_score, 'hnScore': hn_score,
                    'composite': composite, 'compositePlace': composite_place})

    total    = sum(r['composite']      for r in raw) or 1
    tot_pl   = sum(r['compositePlace'] for r in raw) or 1
    mkt_tot  = sum(r['mktProb']        for r in raw) or 1
    pop_tot  = sum(r['popScore']       for r in raw) or 1
    hn_tot   = sum(r['hnScore']        for r in raw) or 1

    ranked = [{**r,
               'prob':   r['composite']      / total,
               'probPl': r['compositePlace'] / tot_pl,
               'mktPct': r['mktProb']  / mkt_tot,
               'popPct': r['popScore'] / pop_tot,
               'hnPct':  r['hnScore']  / hn_tot,
               } for r in raw]
    ranked.sort(key=lambda x: x['prob'], reverse=True)
    return ranked


def bot_decide(ranked: list, settings: dict, balance: float):
    strategy      = settings.get('strategy', 'ai_win')
    bet_amount    = settings.get('betAmount', 1000)
    min_odds      = settings.get('minOdds', 1.0)
    max_odds      = settings.get('maxOdds', 999)
    use_kelly     = settings.get('useKelly', False)
    kelly_frac    = settings.get('kellyFraction', 0.25)
    amount        = min(bet_amount, balance)
    if amount <= 0:
        return None

    # 三連複・三連単（Harville式オッズ推定）
    if strategy in ('trio_box', 'trifecta'):
        if len(ranked) < 3:
            return None
        # 複勝スコアで再ランク
        by_place = sorted(ranked, key=lambda h: h.get('probPl', 0), reverse=True)
        top3 = by_place[:3]
        prob_map = {h['hn']: h['prob'] for h in ranked}

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
        target, bet_type = ranked[0], 'place'
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
            b = target['odds'] - 1
            f = max(0, (target['prob'] * (b + 1) - 1) / b)
            if f <= 0:
                return None
            amount = max(100, min(int(balance * f * kelly_frac), int(balance * 0.15)))
            if amount > balance:
                return None

    if not target:
        return None

    # オッズフィルター
    if bet_type == 'win' and target.get('odds'):
        if target['odds'] < min_odds or target['odds'] > max_odds:
            return None

    # useKelly フラグ有効時（kelly_win 以外）
    if use_kelly and strategy != 'kelly_win' and bet_type == 'win' and target.get('odds') and target.get('prob', 0) > 0:
        b = target['odds'] - 1
        f = max(0, (target['prob'] * (b + 1) - 1) / b)
        if f <= 0:
            return None
        amount = max(100, min(int(balance * f * kelly_frac), int(balance * 0.15)))
        if amount > balance:
            return None

    return {
        'hn':            target['hn'],
        'horses':        None,
        'pop':           target.get('pop'),
        'betType':       bet_type,
        'winOdds':       target.get('odds'),
        'estimatedOdds': None,
        'amount':        amount,
    }


def settle_trade(state: dict, trade: dict, result_map: dict):
    """pending bet を決済して history に移動する"""
    def get_pos(hn):
        return result_map.get(hn) or result_map.get(str(hn))

    decision = trade['decision']
    bet_type = decision['betType']
    won = False

    if bet_type == 'win':
        won = get_pos(decision['hn']) == 1
    elif bet_type == 'place':
        p = get_pos(decision['hn'])
        won = p is not None and p <= 3
    elif bet_type == 'trio':
        top3 = [int(h) for h, p in result_map.items() if int(p) <= 3]
        won = (len(top3) >= 3 and
               all(hn in top3 for hn in decision['horses']))
    elif bet_type == 'trifecta':
        won = all(get_pos(hn) == i + 1 for i, hn in enumerate(decision['horses']))

    if bet_type == 'place':
        eff = max(1.1, round((decision.get('winOdds') or 3) * 0.28 * 10) / 10)
    else:
        eff = decision.get('winOdds') or decision.get('estimatedOdds') or 1

    payout = int(decision['amount'] * eff) if won else 0
    profit = payout - decision['amount']
    state['balance'] = round(state['balance'] + payout)

    settled = {**trade, 'status': 'won' if won else 'lost',
               'payout': payout, 'profit': profit,
               'settledAt': datetime.now(JST).isoformat()}
    state['pending'] = [t for t in state['pending'] if t['tradeKey'] != trade['tradeKey']]
    state['history'].insert(0, settled)
    if len(state['history']) > 500:
        state['history'] = state['history'][:500]

    type_label = {'win': '単勝', 'place': '複勝', 'trio': '三連複', 'trifecta': '三連単'}.get(bet_type, bet_type)
    res_label  = f'的中! +¥{profit:,}' if won else f'外れ -¥{decision["amount"]:,}'
    add_log(state, f'{trade["venue"]}{trade["raceNo"]}R {type_label} → {res_label} (残高 ¥{state["balance"]:,})',
            'ok' if won else 'err')


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

        # ① 決済チェック（pending の結果確認）
        pending_here = [t for t in state.get('pending', [])
                        if t['venue'] == name and t['date'] == date_key]
        if pending_here:
            time.sleep(0.5)
            result_map = get_results(date_str, code)
            for trade in pending_here:
                rno = trade['raceNo']
                rr  = result_map.get(rno) or result_map.get(str(rno))
                if rr:
                    settle_trade(state, trade, rr)

        # ② 新規ベット
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
            horses = get_entries(date_str, code, rno)
            if not horses:
                continue

            ranked   = score_horses(horses, hist_stats)
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
