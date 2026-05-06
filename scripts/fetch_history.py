"""
過去データを遡って取得するスクリプト。
fetch_races.py の関数を再利用し、日付を指定して実行する。

使い方:
  # 過去90日分を取得（既存スキップ）
  python fetch_history.py --days 90

  # 特定期間を取得
  python fetch_history.py --from 2026-01-01 --to 2026-03-31

  # 既存ファイルを強制上書き
  python fetch_history.py --days 30 --force

注意:
  DebaTable（出走表）を常に試みる。サイトが返さない場合は
  RefundMoneyList（着順・払戻のみ）にフォールバック。
"""
import argparse
import json
import re
import time
from datetime import date, timedelta, datetime, timezone
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent))

from fetch_races import (
    get_soup, get_race_list, fetch_race_data,
    fetch_all_results, BASE, JST, VENUE_CODES, CODE_TO_VENUE, ALT_CODES,
)

DATA_DIR = Path(__file__).parent.parent / 'data'


def get_schedule_for_date(date_str: str) -> list:
    """
    指定日（過去日付含む）の開催場リストを返す。
    1. TodayRaceInfoTop?k_raceDate= で試みる（サイトが対応している場合）
    2. 失敗 or 空の場合、全競馬場コードを RefundMoneyList で確認する
    """
    # ① TodayRaceInfoTop に日付パラメータを付けて試みる
    try:
        soup = get_soup(f'{BASE}/TodayRaceInfoTop', params={'k_raceDate': date_str})
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
            name = CODE_TO_VENUE.get(code, f'不明({code})')
            venues.append({'code': code, 'name': name})
        if venues:
            return venues
    except Exception as e:
        print(f'    スケジュールページ取得失敗: {e}')

    # ② フォールバック: 全競馬場コードを RefundMoneyList で確認
    print('    全競馬場をスキャンして開催日を確認中...')
    venues = []
    for name, code in VENUE_CODES.items():
        try:
            results, _ = fetch_all_results(date_str, code)
            if results:
                venues.append({'code': code, 'name': name})
        except Exception:
            pass
        time.sleep(0.5)
    return venues


def _is_full_race(race: dict) -> bool:
    """レースがフルデータ（オッズあり・血統あり・払戻あり）かどうか判定"""
    if race.get('_result_only'):
        return False
    entries = race.get('entries', [])
    if not any(e.get('odds') is not None for e in entries):
        return False
    # 全馬の着順が揃っているのに払戻か血統が欠落 → 再取得
    all_finished = entries and all(e.get('pos') is not None for e in entries)
    if all_finished:
        if not race.get('payouts'):
            return False
        if not all(e.get('sire') for e in entries):
            return False
    return True


def fetch_day(target_date: date, force: bool = False) -> dict:
    """
    指定日のデータを取得して data/YYYY-MM-DD.json に保存する。
    既存JSONがある場合はフルデータ済みのレースをスキップし、不足分だけ取得する。
    戻り値: {'skipped': bool, 'races': int, 'full': int, 'result_only': int}
    """
    date_key = target_date.strftime('%Y-%m-%d')
    date_str = target_date.strftime('%Y/%m/%d')
    out_path = DATA_DIR / f'{date_key}.json'

    # 既存データを読み込む（force 時は無視）
    existing_venues: dict[str, dict] = {}
    if out_path.exists() and not force:
        try:
            old = json.loads(out_path.read_text(encoding='utf-8'))
            for v in old.get('venues', []):
                existing_venues[v['name']] = v
        except Exception:
            pass

    days_ago = (date.today() - target_date).days
    print(f'\n=== {date_key} ({days_ago}日前) ===')

    venues = get_schedule_for_date(date_str)
    if not venues:
        # 開催なしでも既存データがあれば保持
        if existing_venues:
            print('  スケジュール取得できず（既存データを保持）')
            return {'skipped': True, 'races': 0, 'full': 0, 'result_only': 0}
        print('  開催なし（または取得できず）')
        return {'skipped': False, 'races': 0, 'full': 0, 'result_only': 0}

    print(f'  開催場: {[v["name"] for v in venues]}')

    result_venues = []
    total_full = 0
    total_result_only = 0
    any_fetched = False

    for venue in venues:
        name, code = venue['name'], venue['code']
        existing_venue = existing_venues.get(name, {})
        existing_races: dict[int, dict] = {r['raceNo']: r for r in existing_venue.get('races', [])}

        try:
            race_nos = get_race_list(date_str, code)
        except Exception as e:
            print(f'  --- {name}: レース一覧取得失敗: {e}')
            if existing_venue:
                result_venues.append(existing_venue)
            continue

        # フルデータ済みのレースを除いて取得対象を絞る
        need_fetch = [rno for rno in race_nos
                      if force or not _is_full_race(existing_races.get(rno, {}))]

        if not need_fetch:
            n = len(race_nos)
            print(f'  --- {name}: スキップ（{n}レース取得済み）')
            if existing_venue:  # 空dictは追加しない
                result_venues.append(existing_venue)
            total_full += n
            continue

        skip_count = len(race_nos) - len(need_fetch)
        print(f'  --- {name}: {len(need_fetch)}レース取得'
              + (f'（{skip_count}レースはスキップ）' if skip_count else ''))

        all_results, all_payouts = fetch_all_results(date_str, code)

        races = list(existing_races.values())  # 既存レースを保持
        races = [r for r in races if r['raceNo'] not in need_fetch]  # 再取得対象を除外

        for rno in need_fetch:
            pos_map = all_results.get(rno, {})

            # DebaTable を常に試みる
            try:
                time.sleep(0.8)
                horses, race_info = fetch_race_data(date_str, code, rno, pos_map, venue=name)
                if horses:
                    race_entry = {'raceNo': rno, 'entries': horses}
                    for k in ('track', 'weather', 'distance', 'surface'):
                        if race_info.get(k):
                            race_entry[k] = race_info[k]
                    if rno in all_payouts:
                        race_entry['payouts'] = all_payouts[rno]
                    races.append(race_entry)
                    total_full += 1
                    any_fetched = True
                    finished = sum(1 for h in horses if h.get('pos') is not None)
                    print(f'    {rno}R: {len(horses)}頭 ({finished}頭 結果済) [フル]')
                    continue
            except Exception as e:
                print(f'    {rno}R DebaTable失敗 → 結果のみに切替: {e}')

            # フォールバック: 結果のみで保存
            if not pos_map:
                continue
            entries = [
                {'hn': hn, 'pos': pos, 'pop': None, 'odds': None}
                for hn, pos in sorted(pos_map.items())
            ]
            race_entry = {'raceNo': rno, 'entries': entries, '_result_only': True}
            if rno in all_payouts:
                race_entry['payouts'] = all_payouts[rno]
            races.append(race_entry)
            total_result_only += 1
            any_fetched = True
            print(f'    {rno}R: {len(entries)}頭 [結果のみ]')

        if races:
            races.sort(key=lambda r: r['raceNo'])
            result_venues.append({'name': name, 'code': code, 'races': races})

    if not result_venues and not existing_venues:
        print('  有効データなし')
        return {'skipped': False, 'races': 0, 'full': total_full, 'result_only': total_result_only}

    # 既存JSONにあるが今日のスケジュールにない競馬場も保持
    for name, v in existing_venues.items():
        if not any(rv.get('name') == name for rv in result_venues):
            result_venues.append(v)

    if not any_fetched:
        return {'skipped': True, 'races': 0, 'full': total_full, 'result_only': total_result_only}

    DATA_DIR.mkdir(exist_ok=True)
    now = datetime.now(JST)
    output = {
        'date': date_key,
        'updated': now.isoformat(),
        'venues': result_venues,
    }
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    total = sum(len(v['races']) for v in result_venues)
    print(f'  保存完了: {out_path.name} (計{total}レース: フル{total_full} / 結果のみ{total_result_only})')
    return {'skipped': False, 'races': total, 'full': total_full, 'result_only': total_result_only}


def parse_date(s: str) -> date:
    return datetime.strptime(s, '%Y-%m-%d').date()


def main():
    parser = argparse.ArgumentParser(
        description='過去データを取得するスクリプト（最大180日 or 日付範囲指定）'
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--days', type=int, default=90,
        help='遡る日数（デフォルト: 90、最大: 180）'
    )
    group.add_argument(
        '--from', dest='from_date', type=parse_date, metavar='YYYY-MM-DD',
        help='取得開始日（--to と組み合わせて使用）'
    )
    parser.add_argument(
        '--to', dest='to_date', type=parse_date, metavar='YYYY-MM-DD',
        help='取得終了日（省略時: 昨日）'
    )
    parser.add_argument(
        '--force', action='store_true',
        help='既存ファイルがある日も上書きする'
    )
    args = parser.parse_args()

    today = date.today()

    if args.from_date:
        start = args.from_date
        end = args.to_date or (today - timedelta(days=1))
    else:
        days = min(max(args.days, 1), 180)
        end = today - timedelta(days=1)
        start = today - timedelta(days=days)

    if start > end:
        print('エラー: 開始日が終了日より後になっています')
        return

    target_dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    target_dates = [d for d in target_dates if d < today]

    print(f'取得期間: {start} 〜 {end} ({len(target_dates)}日間)')
    print(f'上書き: {"あり" if args.force else "なし"}')
    print(f'方針: DebaTable（フル）を優先、失敗時は結果のみにフォールバック')
    print()

    summary = {'fetched': 0, 'skipped': 0, 'total_races': 0, 'full': 0, 'result_only': 0}

    for i, target in enumerate(target_dates):  # 古い日付から取得
        result = fetch_day(target, force=args.force)
        if result['skipped']:
            summary['skipped'] += 1
        else:
            summary['fetched'] += 1
            summary['total_races'] += result['races']
            summary['full'] += result['full']
            summary['result_only'] += result['result_only']
            if i < len(target_dates) - 1:
                time.sleep(2)

    print(f'\n=== 完了 ===')
    print(f'取得: {summary["fetched"]}日 / スキップ: {summary["skipped"]}日')
    print(f'レース合計: {summary["total_races"]}件')
    print(f'  フルデータ: {summary["full"]}レース（オッズ・騎手・馬名あり）')
    print(f'  結果のみ  : {summary["result_only"]}レース（着順・払戻のみ）')


if __name__ == '__main__':
    main()
