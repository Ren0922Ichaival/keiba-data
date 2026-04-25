"""
既存の data/YYYY-MM-DD.json に track/weather/distance/surface を追記するスクリプト。
keiba.go.jp の DebaTable ページを再スクレイピングして情報を補完する。
"""
import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE = 'https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo'

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


def parse_race_info(soup) -> dict:
    """出馬表 HTML からレース条件を抽出する"""
    info = {'track': None, 'weather': None, 'distance': None, 'surface': None}
    text = soup.get_text(' ', strip=True)

    # 馬場状態: 不良 > 稍重 > 重 > 良 の順で検索
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
        info['surface'] = (
            '芝'   if '芝' in surf else
            '障害'  if '障' in surf else
            '直線'  if '直' in surf else
            'ダート'
        )

    return info


def backfill_file(json_path: Path):
    data = json.loads(json_path.read_text(encoding='utf-8'))
    date_str = data['date'].replace('-', '/')   # YYYY/MM/DD 形式
    changed = False

    for venue in data.get('venues', []):
        vname = venue['name']
        code  = venue['code']

        for race in venue.get('races', []):
            rno = race['raceNo']

            # 4フィールドが全て揃っていればスキップ
            if all(k in race for k in ('track', 'weather', 'distance', 'surface')):
                print(f'  {vname} {rno}R: スキップ（既存データあり）')
                continue

            try:
                time.sleep(0.6)
                soup = get_soup(
                    f'{BASE}/DebaTable',
                    params={'k_raceDate': date_str, 'k_babaCode': code, 'k_raceNo': rno}
                )
                info = parse_race_info(soup)

                added = []
                for key in ('track', 'weather', 'distance', 'surface'):
                    if info.get(key) is not None and key not in race:
                        race[key] = info[key]
                        added.append(f'{key}={info[key]}')

                if added:
                    changed = True
                    print(f'  {vname} {rno}R: 追加 → {", ".join(added)}')
                else:
                    print(f'  {vname} {rno}R: 取得できず（ページに情報なし）')

            except Exception as e:
                print(f'  {vname} {rno}R: エラー → {e}')

    if changed:
        json_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding='utf-8'
        )
        print(f'  → {json_path.name} を更新しました')
    else:
        print(f'  → {json_path.name} に変更はありませんでした')


def main():
    data_dir = Path(__file__).parent.parent / 'data'
    json_files = sorted(data_dir.glob('20??-??-??.json'))

    if not json_files:
        print('data/ フォルダに対象 JSON ファイルが見つかりません')
        return

    print(f'対象ファイル: {[f.name for f in json_files]}')
    print()

    for jf in json_files:
        print(f'=== {jf.name} ===')
        backfill_file(jf)
        print()

    print('完了')


if __name__ == '__main__':
    main()
