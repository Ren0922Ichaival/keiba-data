"""
地方競馬 自動データ収集スクリプト
GitHub Actions で定期実行し、data/YYYY-MM-DD.json に保存する
"""
import json
import re
import time
import unicodedata as _ud
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))
BASE = 'https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo'

VENUE_CODES = {
    '帯広': 3,  '水沢': 11, '浦和': 18, '大井': 20, '金沢': 22,
    '笠松': 23, '名古屋': 24, '園田': 27, '門別': 36, '高知': 31,
    '佐賀': 32, '盛岡': 10, '船橋': 19, '川崎': 45, '姫路': 51,
}
# 旧コードとの互換性（旧コードで保存されたデータも認識できるよう残す）
ALT_CODES = {30: '門別', 35: '盛岡', 43: '船橋'}
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


def get_soup(url, params=None):
    res = requests.get(url, params=params, headers=HEADERS, timeout=15)
    res.raise_for_status()
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


def _direct_tds(tr):
    """tr の直接の子の td のみ（ネスト table の td を除外）"""
    if tr is None:
        return []
    return [c for c in tr.children if getattr(c, 'name', None) == 'td']


def _extract_trainer(text):
    if not text:
        return None
    name = re.split(r'[（(]', text, maxsplit=1)[0].strip()
    return name or None


def parse_entries(soup):
    """出走馬データを抽出。1頭=5行構造:
      行0(horseNum有): 枠/馬番/馬名/騎手/オッズ
      行1: 性齢/毛色/生年
      行2: [0]父 [1]調教師(所属) [2]馬体重(増減)
      行3: [0]母 [1]生産者
      行4: [0](母父)
    親 table 内の sibling を辿り、ネスト result table を混入させない。
    """
    horses = []
    seen = set()
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

        # 行1〜4 を sibling で辿る
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
        entry = {
            'hn': hn, 'pop': pop_val, 'odds': odds_val,
            'weight': weight, 'weightDiff': weight_diff, 'pos': None,
        }
        if jockey:         entry['jockey']        = jockey
        if horse_name:     entry['horseName']     = horse_name
        if sire:           entry['sire']          = sire
        if broodmare_sire: entry['broodmareSire'] = broodmare_sire
        if trainer:        entry['trainer']       = trainer
        horses.append(entry)
    horses.sort(key=lambda h: h['hn'])
    return horses


_BET_TYPES = frozenset({'単勝', '複勝', '馬連', '枠連', '馬単', 'ワイド', '三連複', '三連単'})


def _parse_payout_table(table_elem) -> dict:
    """払戻金テーブル1枚を解析して {式別: [{horses, payout}]} を返す（テスト可能）"""
    race_payouts: dict = {}
    current_bet_type = None
    for row in table_elem.find_all('tr'):
        cells = row.find_all(['td', 'th'])
        if not cells:
            continue
        type_text = cells[0].get_text(strip=True)
        if type_text in _BET_TYPES:
            current_bet_type = type_text
        if current_bet_type and len(cells) >= 2:
            try:
                horse_txt  = cells[-2].get_text(strip=True) if len(cells) >= 3 else ''
                payout_txt = cells[-1].get_text(strip=True)
                payout_val = int(re.sub(r'[^\d]', '', payout_txt))
                if payout_val > 0:
                    if current_bet_type not in race_payouts:
                        race_payouts[current_bet_type] = []
                    race_payouts[current_bet_type].append({
                        'horses': horse_txt,
                        'payout': payout_val,
                    })
            except (ValueError, IndexError):
                pass
    return race_payouts


def parse_refund_soup(soup) -> tuple:
    """
    RefundMoneyList の BeautifulSoup から着順と払戻を解析（テスト可能なモジュール関数）。
    戻り値: (results, payouts)
      results : {raceNo(int): {hn(int): pos(int)}}
      payouts : {raceNo(int): {式別: [{horses, payout}]}}
    HTML構造に依存しない方式（テーブル位置ではなく内容で判定）。
    """
    results: dict = {}
    payouts: dict = {}
    current_race = None

    for elem in soup.find_all(['p', 'table']):
        if elem.name == 'p':
            m = re.match(r'^(\d+)R$', elem.get_text(strip=True))
            if m:
                current_race = int(m.group(1))
        elif elem.name == 'table' and current_race is not None:
            table_text = elem.get_text()
            is_payout = any(bt in table_text for bt in _BET_TYPES)

            if is_payout and current_race not in payouts:
                race_payouts = _parse_payout_table(elem)
                if race_payouts:
                    payouts[current_race] = race_payouts
                    current_race = None

            elif not is_payout and current_race not in results:
                pos_map: dict = {}
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

    return results, payouts


def fetch_all_results(date_str, code):
    """
    RefundMoneyList ページから全レースの着順と払戻金を一括取得する。
    戻り値: (results, payouts)
      results : {raceNo: {馬番: 着順}}
      payouts : {raceNo: {単勝: [{horses,payout}], 複勝: [...], ...}}
    """
    try:
        soup = get_soup(
            f'{BASE}/RefundMoneyList',
            params={'k_raceDate': date_str, 'k_babaCode': code}
        )
    except Exception as e:
        print(f'  fetch_all_results 失敗 ({date_str} code={code}): {e}')
        return {}, {}

    return parse_refund_soup(soup)


def _parse_text_for_conditions(text: str, venue: str = '') -> dict:
    # NFKC で半角カナ・全角英数・全角空白・濁点を一括正規化
    t = _ud.normalize('NFKC', text)
    info: dict = {'track': None, 'weather': None, 'distance': None, 'surface': None}

    m = re.search(r'馬場(?:状態)?[：:\s]{0,3}\S{0,4}?(不良|稍重|重|良|軽)', t)
    if m:
        info['track'] = '良' if m.group(1) == '軽' else m.group(1)

    m = re.search(r'天候[：:\s]*(晴|曇り?|雨|小雨|雪)', t)
    if m:
        info['weather'] = '曇' if '曇' in m.group(1) else m.group(1)

    # パターン1: コース種別 + 距離
    m = re.search(r'(芝|ダ(?:ート)?|障(?:害)?|直(?:線)?)\s*(\d{3,4})\s*m', t, re.IGNORECASE)
    if m:
        info['distance'] = int(m.group(2))
        info['surface'] = ('芝' if '芝' in m.group(1) else '障害' if '障' in m.group(1)
                           else '直線' if '直' in m.group(1) else 'ダート')
    # パターン2: 距離 + コース種別
    if info['distance'] is None:
        m = re.search(r'(\d{3,4})\s*m\s*(芝|ダート?|障害?|直線?)', t)
        if m:
            info['distance'] = int(m.group(1))
            info['surface'] = ('芝' if '芝' in m.group(2) else '障害' if '障' in m.group(2)
                               else '直線' if '直' in m.group(2) else 'ダート')
    # パターン3: 距離のみ
    if info['distance'] is None:
        m = re.search(r'距離[：:\s]*(\d{3,4})', t) or re.search(r'(?<!\d)(\d{3,4})\s*m(?!\d)', t)
        if m:
            info['distance'] = int(m.group(1))

    # surface フォールバック: 距離は取れたが種別未取得 → テキスト全体から探す
    if info['distance'] is not None and info['surface'] is None:
        if 'ダート' in t:
            info['surface'] = 'ダート'
        elif re.search(r'(?<![一二三四五六七八九十])芝', t):
            info['surface'] = '芝'
        elif '障害' in t:
            info['surface'] = '障害'
        elif '直線' in t:
            info['surface'] = '直線'

    # ばんえい（帯広）専用
    if venue == '帯広':
        if info['distance'] is None:
            m2 = re.search(r'(\d{3,4})\s*m', t)
            info['distance'] = int(m2.group(1)) if m2 else 200
        info['surface'] = '直線'
        # 馬場が標準カテゴリで取れない場合、馬場水分(%) から推定
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


def parse_race_info(soup, venue: str = ''):
    """出馬表HTMLからレース条件（馬場状態・天候・距離・コース）を抽出"""
    return _parse_text_for_conditions(soup.get_text(' ', strip=True), venue)


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
        except Exception as e:
            print(f'  既存JSONの読み込み失敗（無視して続行）: {out_path.name}: {e}')

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

        # 全レース結果・払戻金を一括取得（RefundMoneyList）
        print('  結果ページ取得中...')
        all_results, all_payouts = fetch_all_results(date_str, code)
        print(f'  結果あり: {sorted(all_results.keys())}R')

        races = []
        for rno in race_nos:
            old_race = old_races.get(rno)

            # 既に全馬の着順が揃っているレースはスキップ
            # ただし条件・血統・払戻が欠落している場合は再スクレイプする
            if old_race:
                already_done = all(
                    e.get('pos') is not None
                    for e in old_race.get('entries', [])
                )
                has_conditions = bool(
                    old_race.get('track') or old_race.get('distance')
                )
                has_sire = all(
                    e.get('sire') for e in old_race.get('entries', [])
                )
                if already_done and has_conditions and has_sire:
                    # スキップするが、払戻金だけは取得済みなら補完する
                    if rno in all_payouts and not old_race.get('payouts'):
                        old_race = dict(old_race)
                        old_race['payouts'] = all_payouts[rno]
                        print(f'  {rno}R: スキップ（払戻のみ補完）')
                    else:
                        print(f'  {rno}R: スキップ（取得済み）')
                    races.append(old_race)
                    continue
                elif already_done and not has_conditions:
                    print(f'  {rno}R: 条件データ欠落 → 再スクレイプ')
                elif already_done and not has_sire:
                    print(f'  {rno}R: 血統データ欠落 → 再スクレイプ')

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
                    # 払戻金（単勝・複勝等）が取得できた場合は保存
                    if rno in all_payouts:
                        race_entry['payouts'] = all_payouts[rno]
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
            len(r.get('entries', [])) for v in result_venues for r in v['races']
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
