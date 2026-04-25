"""
地方競馬 データ取得サーバー
keiba.go.jp の出馬表から馬番・人気・単勝オッズを取得して返す Flask API
"""
from flask import Flask, jsonify, request, send_from_directory
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from pathlib import Path

app = Flask(__name__)

@app.after_request
def add_cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
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


def fmt_date(d: str) -> str:
    """YYYY-MM-DD or YYYY/MM/DD → YYYY/MM/DD"""
    if not d:
        return datetime.now().strftime('%Y/%m/%d')
    return d.replace('-', '/')


def parse_race_info(soup: BeautifulSoup) -> dict:
    """
    出馬表HTML からレース条件（馬場状態・天候・距離・コース種別）を抽出する。
    取得できない項目は None を返す。
    """
    info = {'track': None, 'weather': None, 'distance': None, 'surface': None}
    text = soup.get_text(' ', strip=True)

    # 馬場状態: 良 / 稍重 / 重 / 不良
    m = re.search(r'馬場[：:\s]*([良稍重不](良|重|稍重|不良)?)', text)
    if m:
        raw = m.group(0)
        for t in ('不良', '稍重', '重', '良'):
            if t in raw:
                info['track'] = t
                break

    # 天候: 晴 / 曇 / 雨 / 小雨 / 雪
    m = re.search(r'天候[：:\s]*(晴|曇り?|雨|小雨|雪)', text)
    if m:
        raw = m.group(1)
        info['weather'] = '曇' if '曇' in raw else raw

    # 距離・コース種別: "ダ1400m" "芝1600m" "障1000m" "直線1000m"
    m = re.search(r'(芝|ダ(?:ート)?|障(?:害)?|直線?)\s*(\d{3,4})\s*[mｍ]', text, re.IGNORECASE)
    if m:
        surf_raw = m.group(1)
        info['distance'] = int(m.group(2))
        if '芝' in surf_raw:
            info['surface'] = '芝'
        elif '障' in surf_raw:
            info['surface'] = '障害'
        elif '直' in surf_raw:
            info['surface'] = '直線'   # ばんえい
        else:
            info['surface'] = 'ダート'

    return info


def parse_horses(soup: BeautifulSoup) -> list:
    """
    出馬表HTML から馬番・人気・単勝オッズを抽出する。
    keiba.go.jp の実際のHTML構造（class='horseNum', class='odds_weight'）に対応。
    """
    horses = []
    seen = set()

    for row in soup.find_all('tr'):
        # class='horseNum' セルから馬番を取得
        hn_cell = row.find('td', class_='horseNum')
        if not hn_cell:
            continue

        try:
            hn = int(hn_cell.get_text(strip=True))
            if not (1 <= hn <= 16):
                continue
        except (ValueError, TypeError):
            continue

        if hn in seen:
            continue

        odds_val = None
        pop_val = None

        # class='odds_weight' セルから "86.2 (7人気)" 形式を取得
        odds_cell = row.find('td', class_='odds_weight')
        if odds_cell:
            t = odds_cell.get_text(' ', strip=True)
            m = re.search(r'([\d]+\.[\d]+)\s*[（(](\d+)人気[）)]', t)
            if m:
                odds_val = float(m.group(1))
                pop_val = int(m.group(2))

        # フォールバック: 行全体から正規表現
        if odds_val is None:
            row_text = row.get_text(' ', strip=True)
            m = re.search(r'([\d]+\.[\d]+)\s*[（(](\d+)人気[）)]', row_text)
            if m:
                odds_val = float(m.group(1))
                pop_val = int(m.group(2))

        seen.add(hn)
        horses.append({
            'hn': hn,
            'pop': pop_val,
            'odds': odds_val,
        })

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
    """本日の開催競馬場一覧"""
    date = fmt_date(request.args.get('date', ''))
    try:
        res = requests.get(
            f'{BASE}/TodayRaceInfoTop',
            headers=HEADERS, timeout=10
        )
        soup = BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')

        venues = []
        seen_codes = set()
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
    race_no = request.args.get('race', '1')
    code = VENUE_CODES.get(venue)

    if not code:
        return jsonify({'error': f'競馬場「{venue}」は未対応です'}), 400

    try:
        res = requests.get(
            f'{BASE}/DebaTable',
            params={'k_raceDate': date, 'k_babaCode': code, 'k_raceNo': race_no},
            headers=HEADERS, timeout=10
        )
        soup = BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')

        horses    = parse_horses(soup)
        race_info = parse_race_info(soup)

        if not horses:
            return jsonify({
                'error': 'データが取得できませんでした。発走前または出馬表未公開の可能性があります。',
                'venue': venue, 'date': date, 'race': int(race_no)
            }), 404

        return jsonify({
            'venue': venue,
            'date': date,
            'race': int(race_no),
            'horses': horses,
            'total': len(horses),
            'raceInfo': race_info,   # 馬場状態・天候・距離・コース
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


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
        soup = BeautifulSoup(res.content, 'lxml', from_encoding='utf-8')

        all_results = {}
        current_race = None

        for elem in soup.find_all(['p', 'table']):
            if elem.name == 'p':
                txt = elem.get_text(strip=True)
                m = re.match(r'^(\d+)R$', txt)
                if m:
                    current_race = int(m.group(1))
            elif elem.name == 'table' and current_race is not None and current_race not in all_results:
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
                    all_results[current_race] = pos_map
                current_race = None  # 最初のテーブル（着順表）のみ参照

        return jsonify({
            'venue': venue,
            'date': date,
            'results': all_results,  # {raceNo(int): {hn(int): pos(int)}}
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/localdata/<date_key>')
def local_data(date_key):
    """data/YYYY-MM-DD.json をクライアントに返す（条件補完用）"""
    data_dir = Path(__file__).parent / 'data'
    filepath = data_dir / f'{date_key}.json'
    if not filepath.exists():
        return jsonify({'error': 'not found'}), 404
    return send_from_directory(str(data_dir), f'{date_key}.json',
                               mimetype='application/json')


if __name__ == '__main__':
    print('=' * 50)
    print(' 地方競馬 分析サーバー')
    print(' http://127.0.0.1:5000')
    print(' 停止: Ctrl+C')
    print('=' * 50)
    app.run(host='127.0.0.1', port=5000, debug=False)
