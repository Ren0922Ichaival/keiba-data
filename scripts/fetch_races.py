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


def get_soup(url, params=None):
    res = requests.get(url, params=params, headers=HEADERS, timeout=15)
    return BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')


def get_schedule(date_str):
    """本日の開催場コードリストを返す"""
    soup = get_soup(f'{BASE}/TodayRaceInfoTop')
    venues = []
    seen = set()
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
    """出走馬データ（馬番・人気・単勝オッズ）を抽出"""
    horses = []
    seen = set()
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
        odds_val, pop_val = None, None
        odds_cell = row.find('td', class_='odds_weight')
        if odds_cell:
            t = odds_cell.get_text(' ', strip=True)
            m = re.search(r'([\d]+\.[\d]+)\s*[（(](\d+)人気[）)]', t)
            if m:
                odds_val = float(m.group(1))
                pop_val = int(m.group(2))
        seen.add(hn)
        horses.append({'hn': hn, 'pop': pop_val, 'odds': odds_val, 'pos': None})
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


def parse_race_info(soup):
    """出馬表HTMLからレース条件（馬場状態・天候・距離・コース）を抽出"""
    info = {'track': None, 'weather': None, 'distance': None, 'surface': None}
    text = soup.get_text(' ', strip=True)

    # 馬場状態
    m = re.search(r'馬場[：:\s]*\S*?(不良|稍重|重|良)', text)
    if m:
        info['track'] = m.group(1)

    # 天候
    m = re.search(r'天候[：:\s]*(晴|曇り?|雨|小雨|雪)', text)
    if m:
        info['weather'] = '曇' if '曇' in m.group(1) else m.group(1)

    # 距離・コース種別
    m = re.search(r'(芝|ダ(?:ート)?|障(?:害)?|直線?)\s*(\d{3,4})\s*[mｍ]', text, re.IGNORECASE)
    if m:
        surf = m.group(1)
        info['distance'] = int(m.group(2))
        info['surface']  = ('芝' if '芝' in surf else '障害' if '障' in surf
                            else '直線' if '直' in surf else 'ダート')
    return info


def fetch_race_data(date_str, code, race_no, pos_map=None):
    """1レース分：エントリー取得 → 着順・レース条件を反映"""
    params = {'k_raceDate': date_str, 'k_babaCode': code, 'k_raceNo': race_no}

    entry_soup = get_soup(f'{BASE}/DebaTable', params=params)
    horses = parse_entries(entry_soup)
    if not horses:
        return None, {}

    if pos_map:
        for h in horses:
            h['pos'] = pos_map.get(h['hn'])

    race_info = parse_race_info(entry_soup)
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
                horses, race_info = fetch_race_data(date_str, code, rno, pos_map)
                if horses:
                    race_entry = {'raceNo': rno, 'entries': horses}
                    # レース条件をデータに追加
                    if race_info.get('track'):    race_entry['track']    = race_info['track']
                    if race_info.get('weather'):  race_entry['weather']  = race_info['weather']
                    if race_info.get('distance'): race_entry['distance'] = race_info['distance']
                    if race_info.get('surface'):  race_entry['surface']  = race_info['surface']
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
    output = {
        'date': date_key,
        'updated': now.isoformat(),
        'venues': result_venues,
    }
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'\n保存完了: {out_path}')
    total = sum(len(v['races']) for v in result_venues)
    print(f'合計 {total} レース')


if __name__ == '__main__':
    main()
