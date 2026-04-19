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


def parse_results(soup):
    """レース結果ページから 馬番→着順 マップを返す"""
    pos_map = {}
    for row in soup.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) < 2:
            continue
        hn_cell = row.find('td', class_='horseNum')
        if not hn_cell:
            continue
        try:
            hn = int(hn_cell.get_text(strip=True))
        except (ValueError, TypeError):
            continue
        if not (1 <= hn <= 16):
            continue
        # 先頭セルが着順のケースが多い
        for cell in cells[:3]:
            if cell is hn_cell:
                continue
            text = cell.get_text(strip=True).replace('着', '')
            try:
                pos = int(text)
                if 1 <= pos <= 20:
                    pos_map[hn] = pos
                    break
            except (ValueError, TypeError):
                pass
    return pos_map


def fetch_race_data(date_str, code, race_no):
    """1レース分：エントリー取得 → 結果ページも試みる"""
    params = {'k_raceDate': date_str, 'k_babaCode': code, 'k_raceNo': race_no}

    # 出走表
    entry_soup = get_soup(f'{BASE}/DebaTable', params=params)
    horses = parse_entries(entry_soup)
    if not horses:
        return None

    # 結果ページ（失敗しても無視）
    try:
        time.sleep(0.5)  # サーバー負荷軽減
        result_soup = get_soup(f'{BASE}/RaceResult', params=params)
        pos_map = parse_results(result_soup)
        if pos_map:
            for h in horses:
                h['pos'] = pos_map.get(h['hn'])
    except Exception:
        pass

    return horses


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

        races = []
        for rno in race_nos:
            # 既に結果が揃っているレースはスキップ
            old_race = old_races.get(rno)
            if old_race:
                already_done = all(
                    e.get('pos') is not None
                    for e in old_race.get('entries', [])
                )
                if already_done:
                    races.append(old_race)
                    print(f'  {rno}R: スキップ（結果取得済み）')
                    continue

            try:
                time.sleep(0.8)  # サーバー負荷軽減
                horses = fetch_race_data(date_str, code, rno)
                if horses:
                    races.append({'raceNo': rno, 'entries': horses})
                    finished = [h for h in horses if h.get('pos') is not None]
                    status = f'{len(finished)}/{len(horses)}頭 結果済'
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
