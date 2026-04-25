"""
地方競馬 自動データ収集スクリプト
GitHub Actions で定期実行し、data/YYYY-MM-DD.json に保存する
"""
import json
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))
BASE = 'https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo'

VENUE_CODES = {
    '帯広': 3,  '水沢': 11, '浦和': 18, '大井': 20, '金沢': 22,
    '笠松': 23, '名古屋': 24, '園田': 27, '門別': 30, '高知': 31,
    '佐賀': 32, '盛岡': 35, '船橋': 43, '川崎': 45, '姫路': 51,
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


def get_soup(url, params=None):
    res = requests.get(url, params=params, headers=HEADERS, timeout=15)
    return BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')


def get_schedule(date_str):
    """本日の開催場コードリストを返す（未知コードも discovered_codes.json に保存）"""
    soup = get_soup(f'{BASE}/TodayRaceInfoTop')

    # 既存の discovered_codes を読み込む
    data_dir = Path('data')
    discovered_path = data_dir / 'discovered_codes.json'
    discovered: dict = {}
    if discovered_path.exists():
        try:
            discovered = json.loads(discovered_path.read_text(encoding='utf-8'))
        except Exception:
            pass

    venues = []
    seen = set()
    new_discovered = False

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
                print(f'  [新規コード発見] code={code}, name={link_text}')
            venues.append({'code': code, 'name': discovered.get(code_str, link_text)})

    # 新しい未知コードがあれば保存
    if new_discovered:
        data_dir.mkdir(exist_ok=True)
        discovered_path.write_text(
            json.dumps(discovered, ensure_ascii=False, indent=2),
            encoding='utf-8'
        )

    return venues


def get_race_list(date_str, code):
    """指定場のレース番号リストを返す"""
    soup = get_soup(f'{BASE}/RaceList', params={'k_raceDate': date_str, 'k_babaCode': code})
    races = []
    seen = set()
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


def parse_entries(soup):
    """出走馬データ（馬番・人気・単勝オッズ・馬体重・騎手・馬名）を抽出
    各馬について2行のtrがある:
      1行目（horseNumセル有り）: odds_weightセルに "4.7 (3人気)" 形式
      2行目（horseNumセル無し）: odds_weightセルに "506 (+12)" 形式（体重と増減）
    """
    horses = []
    seen = set()
    all_rows = soup.find_all('tr')
    i = 0
    while i < len(all_rows):
        row = all_rows[i]
        hn_cell = row.find('td', class_='horseNum')
        if not hn_cell:
            i += 1
            continue
        try:
            hn = int(hn_cell.get_text(strip=True))
            if not (1 <= hn <= 16) or hn in seen:
                i += 1
                continue
        except (ValueError, TypeError):
            i += 1
            continue
        odds_val, pop_val = None, None
        odds_cell = row.find('td', class_='odds_weight')
        if odds_cell:
            t = odds_cell.get_text(' ', strip=True)
            m = re.search(r'([\d]+\.[\d]+)\s*[（(](\d+)人気[）)]', t)
            if m:
                odds_val = float(m.group(1))
                pop_val = int(m.group(2))
        # 騎手名（class='jocky' または 'jockey'）
        jockey = None
        jk_cell = row.find('td', class_='jocky') or row.find('td', class_='jockey')
        if jk_cell:
            jockey = jk_cell.get_text(strip=True) or None
        # 馬名（class='horseName'）
        horse_name = None
        name_cell = row.find('td', class_='horseName')
        if name_cell:
            horse_name = name_cell.get_text(strip=True) or None
        # 次の行から体重を取得
        weight = None
        weight_diff = None
        if i + 1 < len(all_rows):
            next_row = all_rows[i + 1]
            if not next_row.find('td', class_='horseNum'):
                wc = next_row.find('td', class_='odds_weight')
                if wc:
                    wt = wc.get_text(' ', strip=True)
                    wm = re.search(r'(\d{3,4})\s*(?:\(([+-]\d+)\))?', wt)
                    if wm:
                        weight = int(wm.group(1))
                        if wm.group(2):
                            weight_diff = int(wm.group(2))
        seen.add(hn)
        entry = {
            'hn': hn, 'pop': pop_val, 'odds': odds_val,
            'weight': weight, 'weightDiff': weight_diff, 'pos': None,
        }
        if jockey:     entry['jockey']    = jockey
        if horse_name: entry['horseName'] = horse_name
        horses.append(entry)
        i += 1
    horses.sort(key=lambda h: h['hn'])
    return horses


def fetch_all_results(date_str, code):
    """
    RefundMoneyList ページから全レースの着順を一括取得する。
    戻り値: {raceNo: {馬番: 着順}} の辞書
    構造: <p>1R</p> の直後のテーブル 1行目=ヘッダ, 2行目以降=着順|枠|馬番|...
    """
    try:
        soup = get_soup(
            f'{BASE}/RefundMoneyList',
            params={'k_raceDate': date_str, 'k_babaCode': code}
        )
    except Exception:
        return {}

    results = {}  # {raceNo: {hn: pos}}
    current_race = None

    for elem in soup.find_all(['p', 'table']):
        if elem.name == 'p':
            txt = elem.get_text(strip=True)
            m = re.match(r'^(\d+)R$', txt)
            if m:
                current_race = int(m.group(1))
        elif elem.name == 'table' and current_race and current_race not in results:
            pos_map = {}
            for row in elem.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) < 3:
                    continue
                # cells[0]=着順, cells[1]=枠, cells[2]=馬番
                try:
                    pos = int(cells[0].get_text(strip=True))
                    hn  = int(cells[2].get_text(strip=True))
                    if 1 <= pos <= 20 and 1 <= hn <= 16:
                        pos_map[hn] = pos
                except (ValueError, TypeError):
                    continue
            if pos_map:
                results[current_race] = pos_map
            current_race = None  # 最初のテーブル（着順表）だけ見る

    return results


def parse_race_info(soup, venue: str = ''):
    """出馬表HTMLからレース条件（馬場状態・天候・距離・コース）を抽出
    venue='帯広'（ばんえい競馬）の場合は専用解析を行う。
    """
    info = {'track': None, 'weather': None, 'distance': None, 'surface': None}
    text = soup.get_text(' ', strip=True)

    # 馬場状態（ばんえいは「軽」「やや重」「重」「不良」も使う場合あり）
    m = re.search(r'馬場[：:\s]*\S*?(不良|稍重|重|良|軽)', text)
    if m:
        raw = m.group(1)
        # 「軽」はばんえい独自表記 → 「良」相当
        info['track'] = '良' if raw == '軽' else raw

    # 天候
    m = re.search(r'天候[：:\s]*(晴|曇り?|雨|小雨|雪)', text)
    if m:
        info['weather'] = '曇' if '曇' in m.group(1) else m.group(1)

    # 距離・コース種別（全角ｍにも対応）
    m = re.search(r'(芝|ダ(?:ート)?|障(?:害)?|直線?)\s*(\d{3,4})\s*[mｍ]', text, re.IGNORECASE)
    if m:
        surf = m.group(1)
        info['distance'] = int(m.group(2))
        info['surface']  = ('芝' if '芝' in surf else '障害' if '障' in surf
                            else '直線' if '直' in surf else 'ダート')

    # ばんえい競馬（帯広）専用: 距離未取得の場合はデフォルト200m直線
    if venue == '帯広' and info['distance'] is None:
        # ばんえいは常に200m直線コース
        m2 = re.search(r'(\d{3,4})\s*[mｍ]', text)
        if m2:
            info['distance'] = int(m2.group(1))
        else:
            info['distance'] = 200
        info['surface'] = '直線'

    return info


def fetch_race_data(date_str, code, race_no, pos_map=None, venue: str = ''):
    """1レース分：エントリー取得 → 着順・レース条件を反映"""
    params = {'k_raceDate': date_str, 'k_babaCode': code, 'k_raceNo': race_no}

    entry_soup = get_soup(f'{BASE}/DebaTable', params=params)
    horses = parse_entries(entry_soup)
    if not horses:
        return None, {}

    if pos_map:
        for h in horses:
            h['pos'] = pos_map.get(h['hn'])

    race_info = parse_race_info(entry_soup, venue=venue)
    return horses, race_info


def main():
    now = datetime.now(JST)
    date_str = now.strftime('%Y/%m/%d')
    date_key = now.strftime('%Y-%m-%d')

    print(f'=== 地方競馬データ取得: {date_str} ===')

    # data/ フォルダ作成
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    out_path = data_dir / f'{date_key}.json'

    # 既存データをロード（既取得分を保持しつつ更新）
    existing_venues: dict = {}
    if out_path.exists():
        try:
            old = json.loads(out_path.read_text(encoding='utf-8'))
            existing_venues = {v['name']: v for v in old.get('venues', [])}
        except Exception:
            pass

    # スケジュール取得
    venues = get_schedule(date_str)
    print(f'開催場: {[v["name"] for v in venues]}')

    result_venues = []

    for venue in venues:
        name, code = venue['name'], venue['code']
        print(f'\n--- {name} ---')

        old_venue = existing_venues.get(name, {})
        old_races: dict = {r['raceNo']: r for r in old_venue.get('races', [])}

        try:
            race_nos = get_race_list(date_str, code)
            print(f'  レース数: {len(race_nos)}')
        except Exception as e:
            print(f'  レース一覧取得失敗: {e}')
            if old_venue:
                result_venues.append(old_venue)
            continue

        # 全レース結果を一括取得（RefundMoneyList）
        print('  結果ページ取得中...')
        all_results = fetch_all_results(date_str, code)
        print(f'  結果あり: {sorted(all_results.keys())}R')

        races = []
        for rno in race_nos:
            old_race = old_races.get(rno)

            # 既に全馬の着順が揃っているレースはスキップ
            if old_race:
                already_done = all(
                    e.get('pos') is not None
                    for e in old_race.get('entries', [])
                )
                if already_done:
                    races.append(old_race)
                    print(f'  {rno}R: スキップ（取得済み）')
                    continue

            try:
                time.sleep(0.6)
                pos_map = all_results.get(rno)
                horses, race_info = fetch_race_data(date_str, code, rno, pos_map, venue=name)
                if horses:
                    race_entry = {'raceNo': rno, 'entries': horses}
                    # レース条件をデータに追加
                    if race_info.get('track'):    race_entry['track']    = race_info['track']
                    if race_info.get('weather'):  race_entry['weather']  = race_info['weather']
                    if race_info.get('distance'): race_entry['distance'] = race_info['distance']
                    if race_info.get('surface'):  race_entry['surface']  = race_info['surface']
                    # 発走直前オッズ取得のため、最終オッズ更新日時を記録
                    race_entry['oddsUpdatedAt'] = now.isoformat()
                    # 前回取得時と比べてオッズが大きく変動した馬をログ
                    if old_race:
                        old_odds = {e['hn']: e.get('odds') for e in old_race.get('entries', []) if e.get('odds')}
                        for h in horses:
                            old_o = old_odds.get(h['hn'])
                            new_o = h.get('odds')
                            if old_o and new_o and abs(new_o - old_o) / old_o >= 0.25:
                                print(f'    ⚠ {rno}R {h["hn"]}番: オッズ大幅変動 {old_o}→{new_o}')
                    races.append(race_entry)
                    finished = [h for h in horses if h.get('pos') is not None]
                    cond_str = ' / '.join(filter(None, [
                        race_info.get('track'), race_info.get('weather'),
                        f"{race_info['distance']}m" if race_info.get('distance') else None
                    ]))
                    status = f'{len(finished)}/{len(horses)}頭 結果済' + (f' [{cond_str}]' if cond_str else '')
                    print(f'  {rno}R: {len(horses)}頭 ({status})')
                elif old_race:
                    races.append(old_race)
            except Exception as e:
                print(f'  {rno}R エラー: {e}')
                if old_race:
                    races.append(old_race)

        if races:
            result_venues.append({'name': name, 'code': code, 'races': races})

    # 保存
    total = sum(len(v['races']) for v in result_venues)
    output = {
        'date': date_key,
        'updated': now.isoformat(),
        'venues': result_venues,
    }
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'\n保存完了: {out_path}')
    print(f'合計 {total} レース')

    # ヘルスチェック: data/status.json に実行状態を保存
    warnings = []
    for v in result_venues:
        for r in v['races']:
            if not r.get('entries'):
                warnings.append(f"{v['name']}{r['raceNo']}R: エントリーなし")

    status = {
        'last_run': now.isoformat(),
        'date': date_key,
        'venues_found': len(venues),
        'venues_ok': len(result_venues),
        'total_races': total,
        'total_entries': sum(
            len(r['entries']) for v in result_venues for r in v['races']
        ),
        'warnings': warnings,
    }
    status_path = data_dir / 'status.json'
    status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'ステータス保存: {status_path}')
    if warnings:
        print(f'警告 {len(warnings)} 件:')
        for w in warnings:
            print(f'  - {w}')


if __name__ == '__main__':
    main()
