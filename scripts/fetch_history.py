"""
過去データを遡って取得するスクリプト。
fetch_races.py の main() と同じロジックで、日付を指定して実行する。
既存のデータファイルがある日はスキップ（--force オプションで上書き）。
"""
import argparse
import json
import time
from datetime import date, timedelta, datetime, timezone
from pathlib import Path

# fetch_races.py の関数を再利用
import sys
sys.path.insert(0, str(Path(__file__).parent))

from fetch_races import (
    get_schedule, get_race_list, fetch_race_data,
    fetch_all_results, BASE, JST,
)

DATA_DIR = Path(__file__).parent.parent / 'data'


def fetch_day(target_date: date, force: bool = False) -> bool:
    """
    指定日のデータを取得して data/YYYY-MM-DD.json に保存する。
    既存ファイルがあり force=False の場合はスキップ。
    戻り値: 取得を実行した場合 True、スキップした場合 False
    """
    date_key = target_date.strftime('%Y-%m-%d')
    date_str = target_date.strftime('%Y/%m/%d')
    out_path = DATA_DIR / f'{date_key}.json'

    if out_path.exists() and not force:
        print(f'  スキップ（既存）: {date_key}')
        return False

    print(f'\n=== {date_key} ===')

    # スケジュール取得
    try:
        venues = get_schedule(date_str)
    except Exception as e:
        print(f'  スケジュール取得失敗: {e}')
        return True

    if not venues:
        print('  開催なし（または取得できず）')
        return True

    print(f'  開催場: {[v["name"] for v in venues]}')

    result_venues = []
    for venue in venues:
        name, code = venue['name'], venue['code']
        print(f'  --- {name} ---')

        try:
            race_nos = get_race_list(date_str, code)
            print(f'    レース数: {len(race_nos)}')
        except Exception as e:
            print(f'    レース一覧取得失敗: {e}')
            continue

        # 着順一括取得
        all_results = fetch_all_results(date_str, code)

        races = []
        for rno in race_nos:
            try:
                time.sleep(0.6)
                pos_map = all_results.get(rno)
                horses, race_info = fetch_race_data(date_str, code, rno, pos_map)
                if horses:
                    race_entry = {'raceNo': rno, 'entries': horses}
                    if race_info.get('track'):    race_entry['track']    = race_info['track']
                    if race_info.get('weather'):  race_entry['weather']  = race_info['weather']
                    if race_info.get('distance'): race_entry['distance'] = race_info['distance']
                    if race_info.get('surface'):  race_entry['surface']  = race_info['surface']
                    races.append(race_entry)
                    finished = [h for h in horses if h.get('pos') is not None]
                    print(f'    {rno}R: {len(horses)}頭 ({len(finished)}/{len(horses)}頭 結果済)')
            except Exception as e:
                print(f'    {rno}R エラー: {e}')

        if races:
            result_venues.append({'name': name, 'code': code, 'races': races})

    if not result_venues:
        print('  有効データなし')
        return True

    # 保存
    DATA_DIR.mkdir(exist_ok=True)
    now = datetime.now(JST)
    output = {
        'date': date_key,
        'updated': now.isoformat(),
        'venues': result_venues,
    }
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    total = sum(len(v['races']) for v in result_venues)
    print(f'  保存完了: {out_path} (計{total}レース)')
    return True


def main():
    parser = argparse.ArgumentParser(
        description='過去データを遡って取得するスクリプト（最大60日）'
    )
    parser.add_argument(
        '--days', type=int, default=30,
        help='遡る日数（デフォルト: 30、最大: 60）'
    )
    parser.add_argument(
        '--force', action='store_true',
        help='既存ファイルがある日も上書きする'
    )
    args = parser.parse_args()

    days = min(max(args.days, 1), 60)
    today = date.today()

    print(f'過去 {days} 日分のデータを取得します（{"上書きあり" if args.force else "既存スキップ"}）')

    fetched = 0
    skipped = 0

    for delta in range(1, days + 1):
        target = today - timedelta(days=delta)
        executed = fetch_day(target, force=args.force)
        if executed:
            fetched += 1
            # サーバー負荷軽減のため各日の間に2秒待機
            if delta < days:
                time.sleep(2)
        else:
            skipped += 1

    print(f'\n完了: {fetched} 日取得, {skipped} 日スキップ')


if __name__ == '__main__':
    main()
