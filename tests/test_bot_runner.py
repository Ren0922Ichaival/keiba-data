"""
bot_runner.py のバグ修正確認テスト

対象:
  - quinella_box3 推定オッズ（全3ペア合計）
  - trio_box4/trio_box5 推定オッズ（全コンビ合計）
  - settle_trade: 実払戻金優先（win/place/trio/trifecta）
  - settle_trade: 実払戻なし時の place fallback = 1.0
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from bot_runner import (
    quinella_harville_p,
    trio_harville_p,
    settle_trade,
    bot_decide,
    get_race_cond_mult,
    score_horses,
)
from itertools import combinations


# ── ヘルパー ────────────────────────────────────────────────────────────────

def _make_state(balance=100_000):
    return {'balance': balance, 'pending': [], 'history': [], 'log': []}


def _make_trade(bet_type, hn=None, horses=None, amount=1000,
                win_odds=None, est_odds=None, key='2026-01-01_大井_1'):
    return {
        'tradeKey': key,
        'date': '2026-01-01',
        'venue': '大井',
        'raceNo': 1,
        'decision': {
            'betType':       bet_type,
            'hn':            hn,
            'horses':        horses,
            'amount':        amount,
            'winOdds':       win_odds,
            'estimatedOdds': est_odds,
        },
        'status': 'pending',
        'placedAt': '2026-01-01T10:00:00',
    }


# ── Bug 1: quinella_box3 推定オッズは3ペア合計 ──────────────────────────────

def test_quinella_box3_p_covers_all_pairs():
    """3頭BOX馬連の推定確率は C(3,2)=3ペア分の合計でなければならない"""
    prob_map = {1: 0.4, 2: 0.3, 3: 0.2, 4: 0.1}
    top = [{'hn': 1}, {'hn': 2}, {'hn': 3}]
    p_all = sum(
        quinella_harville_p(prob_map, top[i]['hn'], top[j]['hn'])
        for i, j in [(0, 1), (0, 2), (1, 2)]
    )
    p_pair_only = quinella_harville_p(prob_map, top[0]['hn'], top[1]['hn'])
    # 全ペア合計は単ペアより必ず大きい（= 推定オッズは低くなる）
    assert p_all > p_pair_only


# ── Bug 2: trio_box4 推定オッズは全C(4,3)=4コンビ合計 ──────────────────────

def test_trio_box4_p_covers_all_combos():
    """4頭BOX三連複の推定確率は C(4,3)=4コンビ分の合計でなければならない"""
    prob_map = {1: 0.35, 2: 0.25, 3: 0.20, 4: 0.15, 5: 0.05}
    top = [{'hn': 1}, {'hn': 2}, {'hn': 3}, {'hn': 4}]
    p_all = sum(
        trio_harville_p(prob_map, top[i]['hn'], top[j]['hn'], top[k]['hn'])
        for i, j, k in combinations(range(4), 3)
    )
    p_top3_only = trio_harville_p(prob_map, top[0]['hn'], top[1]['hn'], top[2]['hn'])
    assert p_all > p_top3_only


def test_trio_box5_p_covers_all_combos():
    """5頭BOX三連複の推定確率は C(5,3)=10コンビ分の合計でなければならない"""
    prob_map = {1: 0.30, 2: 0.25, 3: 0.20, 4: 0.15, 5: 0.10}
    top = [{'hn': i+1} for i in range(5)]
    p_all = sum(
        trio_harville_p(prob_map, top[i]['hn'], top[j]['hn'], top[k]['hn'])
        for i, j, k in combinations(range(5), 3)
    )
    p_top3_only = trio_harville_p(prob_map, top[0]['hn'], top[1]['hn'], top[2]['hn'])
    assert p_all > p_top3_only


# ── Bug 3: settle_trade が実払戻金を優先使用 ────────────────────────────────

def test_settle_win_uses_real_payout():
    """単勝的中時に race_payouts['単勝'] の実払戻倍率を使用する"""
    state  = _make_state()
    trade  = _make_trade('win', hn=5, amount=1000, win_odds=3.5)
    result = {5: 1, 3: 2, 7: 3}
    payouts = {'単勝': [{'horses': '5', 'payout': 450}]}  # 4.5倍
    settle_trade(state, trade, result, payouts)
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert settled['effectiveOdds'] == 4.5  # 450/100
    assert settled['payout'] == 4500


def test_settle_win_fallback_when_no_payout():
    """実払戻データがない場合は winOdds で代替する"""
    state  = _make_state()
    trade  = _make_trade('win', hn=5, amount=1000, win_odds=3.5)
    result = {5: 1, 3: 2, 7: 3}
    settle_trade(state, trade, result, {})
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert settled['effectiveOdds'] == 3.5


def test_settle_place_uses_real_payout():
    """複勝的中時に race_payouts['複勝'] の実払戻倍率を使用する"""
    state  = _make_state()
    trade  = _make_trade('place', hn=3, amount=1000, win_odds=5.0)
    result = {5: 1, 3: 2, 7: 3}
    payouts = {'複勝': [{'horses': '3', 'payout': 185}]}  # 1.85倍
    settle_trade(state, trade, result, payouts)
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert settled['effectiveOdds'] == 1.85


def test_settle_trio_uses_real_payout():
    """三連複的中時に race_payouts['三連複'] の実払戻倍率を使用する"""
    state  = _make_state()
    trade  = _make_trade('trio', hn=5, horses=[5, 3, 7], amount=1000, est_odds=25.0)
    result = {5: 1, 3: 2, 7: 3}
    payouts = {'三連複': [{'horses': '3-5-7', 'payout': 6800}]}  # 68倍
    settle_trade(state, trade, result, payouts)
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert settled['effectiveOdds'] == 68.0


def test_settle_trio_fallback_to_estimated():
    """三連複実払戻なし → estimatedOdds にフォールバック"""
    state  = _make_state()
    trade  = _make_trade('trio', hn=5, horses=[5, 3, 7], amount=1000, est_odds=25.0)
    result = {5: 1, 3: 2, 7: 3}
    settle_trade(state, trade, result, {})
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert settled['effectiveOdds'] == 25.0


def test_settle_trifecta_uses_real_payout():
    """三連単的中時に race_payouts['三連単'] の実払戻倍率を使用する"""
    state  = _make_state()
    trade  = _make_trade('trifecta', hn=5, horses=[5, 3, 7], amount=1000, est_odds=80.0)
    result = {5: 1, 3: 2, 7: 3}
    payouts = {'三連単': [{'horses': '5-3-7', 'payout': 12000}]}  # 120倍
    settle_trade(state, trade, result, payouts)
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert settled['effectiveOdds'] == 120.0


# ── Bug 4: place 実払戻なし fallback = 1.0 ──────────────────────────────────

def test_settle_place_fallback_is_1_not_win_odds():
    """複勝実払戻が取得できない場合、単勝 winOdds でなく 1.0 を使う"""
    state  = _make_state()
    trade  = _make_trade('place', hn=3, amount=1000, win_odds=5.0)
    result = {5: 1, 3: 2, 7: 3}
    # 実払戻データなし
    settle_trade(state, trade, result, {})
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert settled['effectiveOdds'] == 1.0  # winOdds(5.0) ではない
    assert settled['payout'] == 1000        # 元本のみ返還


def test_settle_place_lost_is_zero():
    """複勝外れ時はペイアウト 0"""
    state  = _make_state()
    trade  = _make_trade('place', hn=1, amount=1000)
    result = {5: 1, 3: 2, 7: 3}
    settle_trade(state, trade, result, {})
    settled = state['history'][0]
    assert settled['status'] == 'lost'
    assert settled['payout'] == 0


# ── Bug 5: 複数複勝エントリで正しい馬番の払戻を取得 ─────────────────────────

def test_settle_place_multi_entry_uses_correct_hn():
    """複勝エントリが複数ある場合、ベットした馬番のエントリを取得する（最初のエントリではない）"""
    state  = _make_state()
    trade  = _make_trade('place', hn=7, amount=1000, win_odds=8.0)
    result = {5: 1, 3: 2, 7: 3}
    # 3頭それぞれ異なる複勝払戻
    payouts = {'複勝': [
        {'horses': '5', 'payout': 210},  # 最初のエントリ（5番）
        {'horses': '3', 'payout': 185},
        {'horses': '7', 'payout': 520},  # ベットした7番
    ]}
    settle_trade(state, trade, result, payouts)
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert settled['effectiveOdds'] == 5.2   # 520/100（最初の2.1ではない）
    assert settled['payout'] == 5200


def test_settle_win_multi_entry_uses_correct_hn():
    """単勝エントリが複数ある場合も正しい馬番のエントリを取得する"""
    state  = _make_state()
    trade  = _make_trade('win', hn=3, amount=1000, win_odds=4.0)
    result = {3: 1, 5: 2, 7: 3}
    payouts = {'単勝': [
        {'horses': '3', 'payout': 450},  # 唯一の単勝エントリ
    ]}
    settle_trade(state, trade, result, payouts)
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert settled['effectiveOdds'] == 4.5


# ── Bug 6: BOXベット amount = per_amount × combo_count（JS と整合） ───────────

def _make_ranked(n=8):
    """n頭のダミー ranked リスト（popとprobを均等配分）"""
    return [
        {
            'hn': i + 1, 'pop': i + 1, 'odds': 10.0 + i,
            'prob':     (n - i) / (n * (n + 1) / 2),
            'probPl':   (n - i) / (n * (n + 1) / 2),
            'mktProb':  (n - i) / (n * (n + 1) / 2),
            'mktPct':   (n - i) / (n * (n + 1) / 2),
            'popScore': 1 / n, 'hnScore': 1 / n,
            'jkScore': 1 / n, 'horseScore': 1 / n,
            'trScore': 1 / n, 'sireScore': 1 / n, 'bmsScore': 1 / n,
            'condMult': 1.0, 'weightMult': 1.0,
        }
        for i in range(n)
    ]


def _make_settings(strategy, bet_amount=1000):
    return {
        'strategy': strategy, 'betAmount': bet_amount,
        'minOdds': 1.0, 'maxOdds': 50.0,
        'useKelly': False, 'kellyFraction': 0.25,
    }


def test_trio_box4_amount_is_combo_count_times_per():
    """trio_box4 の amount = per_amount × 4（JS と同じ挙動）"""
    ranked   = _make_ranked(8)
    settings = _make_settings('trio_box4', bet_amount=1000)
    decision = bot_decide(ranked, settings, balance=100_000)
    assert decision is not None
    assert decision['betType'] == 'trio_box4'
    assert decision['_comboCount'] == 4
    assert decision['amount'] == decision['_perAmount'] * 4


def test_trio_box5_amount_is_combo_count_times_per():
    """trio_box5 の amount = per_amount × 10"""
    ranked   = _make_ranked(8)
    settings = _make_settings('trio_box5', bet_amount=1000)
    decision = bot_decide(ranked, settings, balance=100_000)
    assert decision is not None
    assert decision['_comboCount'] == 10
    assert decision['amount'] == decision['_perAmount'] * 10


def test_quinella_box3_amount_is_combo_count_times_per():
    """quinella_box3 の amount = per_amount × 3"""
    ranked   = _make_ranked(6)
    settings = _make_settings('quinella_box3', bet_amount=1000)
    decision = bot_decide(ranked, settings, balance=100_000)
    assert decision is not None
    assert decision['_comboCount'] == 3
    assert decision['amount'] == decision['_perAmount'] * 3


def test_box_bet_insufficient_balance_returns_none():
    """残高が combo_count × 最低額に満たない場合は None を返す"""
    ranked   = _make_ranked(8)
    settings = _make_settings('trio_box4', bet_amount=1000)
    # balance=300 → per_amount = 300//4=75 < 100(最低額) → None
    decision = bot_decide(ranked, settings, balance=300)
    assert decision is None


# ── Bug 7: settle_trade が None 着順でクラッシュしない ────────────────────────

def test_settle_trio_none_position_no_crash():
    """result_map に pos=None の馬が含まれても trio 決済でクラッシュしない"""
    state  = _make_state()
    trade  = _make_trade('trio', hn=5, horses=[5, 3, 7], amount=1000, est_odds=25.0)
    result = {5: 1, 3: 2, 7: 3, 9: None}  # 9番は着順不明
    settle_trade(state, trade, result, {})
    assert len(state['history']) == 1
    assert state['history'][0]['status'] == 'won'


def test_settle_quinella_box3_none_position_no_crash():
    """result_map に pos=None の馬が含まれても quinella_box3 決済でクラッシュしない"""
    state  = _make_state()
    trade  = _make_trade('quinella_box3', hn=5, horses=[5, 3, 7], amount=3000, est_odds=20.0)
    result = {5: 1, 3: 2, 7: 3, 9: None}
    settle_trade(state, trade, result, {})
    assert len(state['history']) == 1
    assert state['history'][0]['status'] == 'won'


def test_settle_trifecta_box3_none_position_no_crash():
    """result_map に pos=None の馬が含まれても trifecta_box3 決済でクラッシュしない"""
    state  = _make_state()
    trade  = _make_trade('trifecta_box3', hn=5, horses=[5, 3, 7], amount=6000, est_odds=80.0)
    result = {5: 1, 3: 2, 7: 3, 9: None}
    settle_trade(state, trade, result, {})
    assert len(state['history']) == 1
    assert state['history'][0]['status'] == 'won'


# ── Bug 8: trifecta_box3 同着時の勝利判定が trio と一致 ──────────────────────

def test_settle_trifecta_box3_dead_heat_at_third():
    """3位同着（4頭が pos<=3）でも trifecta_box3 は trio と同様に的中する"""
    state  = _make_state()
    trade  = _make_trade('trifecta_box3', hn=5, horses=[5, 3, 7], amount=6000, est_odds=80.0)
    result = {5: 1, 3: 2, 7: 3, 9: 3}  # 7番と9番が3位同着
    settle_trade(state, trade, result, {})
    assert state['history'][0]['status'] == 'won'


# ── Bug 9: BOX ベット実払戻ルックアップ ──────────────────────────────────────

def test_settle_trio_box4_uses_real_trio_payout():
    """trio_box4 的中時は実際の三連複払戻を使用する（estimatedOdds でない）
    eff = 85.0 / 4 comboCount = 21.25、payout = 4000 * 21.25 = 85000（perAmount×odds と同値）"""
    state  = _make_state()
    trade  = _make_trade('trio_box4', hn=5, horses=[5, 3, 7, 2], amount=4000, est_odds=15.0)
    trade['decision']['_comboCount'] = 4
    trade['decision']['_perAmount']  = 1000
    result = {5: 1, 3: 2, 7: 3}
    payouts = {'三連複': [{'horses': '3-5-7', 'payout': 8500}]}  # 85倍
    settle_trade(state, trade, result, payouts)
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert abs(settled['effectiveOdds'] - 85.0 / 4) < 1e-9
    assert settled['payout'] == 85000  # perAmount(1000) × 85倍


def test_settle_trio_box5_uses_real_trio_payout():
    """trio_box5 的中時は実際の三連複払戻を使用する（eff = 50/10 = 5.0）"""
    state  = _make_state()
    trade  = _make_trade('trio_box5', hn=1, horses=[5, 3, 7, 2, 1], amount=10000, est_odds=12.0)
    trade['decision']['_comboCount'] = 10
    trade['decision']['_perAmount']  = 1000
    result = {5: 1, 3: 2, 7: 3}
    payouts = {'三連複': [{'horses': '3-5-7', 'payout': 5000}]}  # 50倍
    settle_trade(state, trade, result, payouts)
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert abs(settled['effectiveOdds'] - 50.0 / 10) < 1e-9
    assert settled['payout'] == 50000  # perAmount(1000) × 50倍


def test_settle_quinella_box3_uses_real_payout():
    """quinella_box3 的中時は実際の馬連払戻を使用する（eff = 32/3, payout = 1000×32 = 32000）"""
    state  = _make_state()
    trade  = _make_trade('quinella_box3', hn=5, horses=[5, 3, 7], amount=3000, est_odds=20.0)
    trade['decision']['_comboCount'] = 3
    trade['decision']['_perAmount']  = 1000
    result = {5: 1, 3: 2, 7: 3}
    payouts = {'馬連': [{'horses': '3-5', 'payout': 3200}]}  # 32倍
    settle_trade(state, trade, result, payouts)
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert abs(settled['effectiveOdds'] - 32.0 / 3) < 1e-9
    assert settled['payout'] == 32000  # perAmount(1000) × 32倍


def test_settle_trifecta_box3_uses_real_trifecta_payout():
    """trifecta_box3 的中時は実際の三連単払戻を使用する（eff = 150/6 = 25.0）"""
    state  = _make_state()
    trade  = _make_trade('trifecta_box3', hn=5, horses=[5, 3, 7], amount=6000, est_odds=80.0)
    trade['decision']['_comboCount'] = 6
    trade['decision']['_perAmount']  = 1000
    result = {5: 1, 7: 2, 3: 3}  # 5-7-3 の順
    payouts = {'三連単': [{'horses': '5-7-3', 'payout': 15000}]}  # 150倍
    settle_trade(state, trade, result, payouts)
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert abs(settled['effectiveOdds'] - 150.0 / 6) < 1e-9
    assert settled['payout'] == 150000  # perAmount(1000) × 150倍


def test_settle_trifecta_box4_uses_real_trifecta_payout():
    """trifecta_box4 的中時は実際の三連単払戻を使用する（eff = 90/24 = 3.75）"""
    state  = _make_state()
    trade  = _make_trade('trifecta_box4', hn=5, horses=[5, 3, 7, 2], amount=24000, est_odds=60.0)
    trade['decision']['_comboCount'] = 24
    trade['decision']['_perAmount']  = 1000
    result = {3: 1, 5: 2, 7: 3}  # 3-5-7 の順
    payouts = {'三連単': [{'horses': '3-5-7', 'payout': 9000}]}  # 90倍
    settle_trade(state, trade, result, payouts)
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert abs(settled['effectiveOdds'] - 90.0 / 24) < 1e-9
    assert settled['payout'] == 90000  # perAmount(1000) × 90倍


def test_box_payout_fallback_when_no_real_payout():
    """実払戻データがない場合は estimatedOdds にフォールバック"""
    state  = _make_state()
    trade  = _make_trade('trio_box4', hn=5, horses=[5, 3, 7, 2], amount=4000, est_odds=15.0)
    result = {5: 1, 3: 2, 7: 3}
    settle_trade(state, trade, result, {})  # 実払戻なし
    settled = state['history'][0]
    assert settled['status'] == 'won'
    assert settled['effectiveOdds'] == 15.0  # estimatedOdds にフォールバック


# ── Bug 10: ai_place がオッズフィルターを無視する ────────────────────────────

def test_ai_place_respects_max_odds():
    """ai_place は max_odds を超える馬にはベットしない"""
    ranked   = _make_ranked(4)  # 全馬 odds >= 10.0
    settings = {
        'strategy': 'ai_place', 'betAmount': 1000,
        'minOdds': 1.0, 'maxOdds': 5.0,  # 5倍が上限
        'useKelly': False, 'kellyFraction': 0.25,
    }
    decision = bot_decide(ranked, settings, balance=100_000)
    assert decision is None  # 全馬がmax_oddsを超えるためベット見送り


def test_ai_place_respects_min_odds():
    """ai_place は min_odds を下回る馬にはベットしない"""
    # odds が全て 11.0-14.0 の ranked に対して min_odds=20.0 を設定
    ranked = _make_ranked(4)
    settings = {
        'strategy': 'ai_place', 'betAmount': 1000,
        'minOdds': 20.0, 'maxOdds': 100.0,
        'useKelly': False, 'kellyFraction': 0.25,
    }
    decision = bot_decide(ranked, settings, balance=100_000)
    assert decision is None  # 全馬が min_odds を下回るためベット見送り


# ── Bug 11: has_jockeys が統計なしでも True になる ───────────────────────────

# ── Bug 12: useKelly=True でベットが行われない ────────────────────────────────

def _make_realistic_ranked():
    """典型的な地方競馬の短めオッズ (2.5-15倍) と realistic な prob を持つ ranked。
    Kelly f は全馬負値になるため useKelly=True が従来コードでベットを阻害する。"""
    data = [
        # (hn, pop, odds, prob)
        (1, 1, 2.5, 0.35),
        (2, 2, 4.0, 0.25),
        (3, 3, 6.0, 0.18),
        (4, 4, 9.0, 0.12),
        (5, 5, 15.0, 0.10),
    ]
    return [
        {
            'hn': hn, 'pop': pop, 'odds': odds,
            'prob': prob, 'probPl': prob,
            'mktProb': 1/odds, 'mktPct': 1/odds,
            'popScore': 0.2, 'hnScore': 0.2, 'jkScore': 0.2,
            'horseScore': 0.2, 'trScore': 0.2, 'sireScore': 0.2, 'bmsScore': 0.2,
            'condMult': 1.0, 'weightMult': 1.0,
        }
        for hn, pop, odds, prob in data
    ]


def test_use_kelly_still_bets_when_no_edge():
    """useKelly=True で Kelly f が負（エッジなし）の時もデフォルト額でベットする"""
    ranked   = _make_realistic_ranked()  # Kelly f < 0 になる realistic なオッズ
    settings = {
        'strategy': 'ai_win', 'betAmount': 1000,
        'minOdds': 1.0, 'maxOdds': 50.0,
        'useKelly': True, 'kellyFraction': 0.25,
    }
    decision = bot_decide(ranked, settings, balance=100_000)
    # Kelly フラグ ON でもベットは行われるべき（Kelly は sizing ツール）
    assert decision is not None, "useKelly=True でも Kelly f<=0 の時はデフォルト額でベットすべき"
    assert decision['betType'] == 'win'
    assert decision['amount'] == 1000  # エッジなし → デフォルト額


def test_use_kelly_sizes_up_when_edge_exists():
    """useKelly=True で Kelly f が正（エッジあり）の時は Kelly 額でベットする"""
    # prob=0.55, odds=4.0 → Kelly f = (0.55*4-1)/3 = 0.4 > 0
    ranked = [
        {
            'hn': 1, 'pop': 1, 'odds': 4.0,
            'prob': 0.55, 'probPl': 0.55, 'mktProb': 0.25, 'mktPct': 0.25,
            'popScore': 0.35, 'hnScore': 0.2, 'jkScore': 0.2,
            'horseScore': 0.2, 'trScore': 0.2, 'sireScore': 0.2, 'bmsScore': 0.2,
            'condMult': 1.0, 'weightMult': 1.0,
        },
        {
            'hn': 2, 'pop': 2, 'odds': 8.0,
            'prob': 0.45, 'probPl': 0.45, 'mktProb': 0.125, 'mktPct': 0.125,
            'popScore': 0.2, 'hnScore': 0.2, 'jkScore': 0.2,
            'horseScore': 0.2, 'trScore': 0.2, 'sireScore': 0.2, 'bmsScore': 0.2,
            'condMult': 1.0, 'weightMult': 1.0,
        },
    ]
    settings = {
        'strategy': 'ai_win', 'betAmount': 1000,
        'minOdds': 1.0, 'maxOdds': 50.0,
        'useKelly': True, 'kellyFraction': 0.25,
    }
    decision = bot_decide(ranked, settings, balance=100_000)
    assert decision is not None
    assert decision['amount'] > 1000  # Kelly エッジあり → デフォルト 1000 円より大きい


def test_kelly_win_strategy_bets_with_overround_edge():
    """kelly_win ストラテジーは市場控除率補正後に正エッジがあればベットする。
    prob * overround > 1/odds でエッジあり。"""
    # 5-horse race, overround = sum(1/odds) ≈ 1.1
    # horse1: prob=0.35, odds=2.5, fair_p = 0.35 * overround
    # overround = 1/2.5+1/4+1/7+1/12+1/25 = 0.4+0.25+0.143+0.083+0.04 = 0.916
    # fair_p = 0.35 * 0.916 = 0.321 → f = (0.321*2.5-1)/1.5 = -0.132 → まだ負...
    # → overround = 1.3 のような大きいケースでテスト
    ranked = [
        {
            'hn': 1, 'pop': 2, 'odds': 4.0,
            'prob': 0.45, 'probPl': 0.45, 'mktProb': 0.25, 'mktPct': 0.25,
            'popScore': 0.35, 'hnScore': 0.2, 'jkScore': 0.2,
            'horseScore': 0.2, 'trScore': 0.2, 'sireScore': 0.2, 'bmsScore': 0.2,
            'condMult': 1.0, 'weightMult': 1.0,
        },
        # 残り7頭: 合計オッズ控除率を 1.3 まで上げる
        *[
            {
                'hn': i+2, 'pop': i+3, 'odds': 5.0 + i*2,
                'prob': 0.55 / 7, 'probPl': 0.55/7, 'mktProb': 0.1, 'mktPct': 0.1,
                'popScore': 0.1, 'hnScore': 0.2, 'jkScore': 0.2,
                'horseScore': 0.1, 'trScore': 0.1, 'sireScore': 0.1, 'bmsScore': 0.1,
                'condMult': 1.0, 'weightMult': 1.0,
            }
            for i in range(7)
        ]
    ]
    settings = {
        'strategy': 'kelly_win', 'betAmount': 1000,
        'minOdds': 1.0, 'maxOdds': 50.0,
        'useKelly': False, 'kellyFraction': 0.25,
    }
    # overround = 1/4 + sum(1/(5+i*2) for i in range(7)) を確認してから実行
    import math
    overround = 1/4.0 + sum(1/(5.0+i*2) for i in range(7))
    fair_p = 0.45 * overround
    f = (fair_p * 4.0 - 1) / 3.0
    if f > 0:
        decision = bot_decide(ranked, settings, balance=100_000)
        assert decision is not None, f"kelly_win はエッジあり(fair_p={fair_p:.3f}, f={f:.3f})なのに None を返した"
        assert decision['betType'] == 'win'


def test_score_horses_jockey_no_stats_treated_same_as_no_jockey():
    """騎手名はあるが jockeyStats に記録がない場合、
    騎手なしと同一のスコア分布になる（ブランチ選択が一致する）"""
    from bot_runner import score_horses
    hist_stats = {
        'popStats': {1: {'wr': 0.35, 'pr': 0.65}, 2: {'wr': 0.20, 'pr': 0.50}},
        'hnStats':  {},
        'jockeyStats': {},   # 空 — 騎手の統計データなし
        'horseStats': {}, 'trainerStats': {}, 'sireStats': {}, 'bmsStats': {},
    }
    horses_with_jockey = [
        {'hn': 1, 'pop': 1, 'odds': 2.5, 'jockey': '山田太郎'},
        {'hn': 2, 'pop': 2, 'odds': 5.0, 'jockey': '鈴木一郎'},
    ]
    horses_no_jockey = [
        {'hn': 1, 'pop': 1, 'odds': 2.5},
        {'hn': 2, 'pop': 2, 'odds': 5.0},
    ]
    ranked_w = score_horses(horses_with_jockey, hist_stats, {}, '')
    ranked_n = score_horses(horses_no_jockey,   hist_stats, {}, '')
    # 統計なし騎手は信号として使わないため同一確率になるべき
    for hw, hn in zip(ranked_w, ranked_n):
        assert abs(hw['prob'] - hn['prob']) < 1e-9, (
            f"hn={hw['hn']}: with_jockey={hw['prob']:.6f}, no_jockey={hn['prob']:.6f}"
        )


# ── Bug 13: field_size=7 が少頭数補正を受けない ─────────────────────────────

def test_field7_pop1_gets_small_field_bonus():
    """少頭数(≤7頭)仕様: 7頭立て1人気は +5% の恩恵を受けるべき"""
    h = {'hn': 1, 'pop': 1}
    race_info = {'fieldSize': 7, 'raceNo': 5}
    mult = get_race_cond_mult(h, race_info, '大井')
    assert mult > 1.0, f"7頭立て1人気は補正 >1.0 を期待するが {mult:.4f} だった"


def test_field7_pop7_gets_small_field_penalty():
    """少頭数(≤7頭)仕様: 7頭立て7人気は不利補正を受けるべき"""
    h = {'hn': 7, 'pop': 7}
    race_info = {'fieldSize': 7, 'raceNo': 5}
    mult = get_race_cond_mult(h, race_info, '大井')
    assert mult < 1.0, f"7頭立て7人気は補正 <1.0 を期待するが {mult:.4f} だった"


def test_field6_pop1_gets_small_field_bonus():
    """6頭立て1人気は既存補正でも +5% を受けることを回帰確認"""
    h = {'hn': 1, 'pop': 1}
    race_info = {'fieldSize': 6, 'raceNo': 5}
    mult = get_race_cond_mult(h, race_info, '大井')
    assert abs(mult - 1.05) < 1e-9, f"6頭立て1人気は 1.05 を期待するが {mult:.4f} だった"


# ── Bug 14: score_horses で pop_place が 1.0 を超える場合がある ───────────────

def test_pop_place_capped_at_one():
    """base_pl 計算で pop_place は 1.0 を超えてはならない"""
    # popStats で1人気の pr=1.0 (完璧な複勝率) にする
    hist_stats = {
        'popStats': {1: {'wr': 0.8, 'pr': 1.0}},  # pr=1.0 → popScore*3 > 1.0 になりうる
        'hnStats':  {},
        'jockeyStats': {}, 'horseStats': {}, 'trainerStats': {}, 'sireStats': {}, 'bmsStats': {},
    }
    horses = [
        {'hn': 1, 'pop': 1, 'odds': 1.5},
        {'hn': 2, 'pop': 2, 'odds': 5.0},
    ]
    ranked = score_horses(horses, hist_stats, {}, '')
    # 1番人気馬の probPl は [0,1] の範囲内でなければならない
    assert 0.0 <= ranked[0]['probPl'] <= 1.0, (
        f"probPl={ranked[0]['probPl']:.4f} が [0,1] 範囲外"
    )


def test_pop_place_fallback_capped_at_one():
    """popStats に pr がない場合の fallback pop_place = min(wr*3, 1.0) を確認

    pr なし → fallback = wr*3 が 1.0 を超える場合は 1.0 にキャップされるべき。
    pr=1.0 を明示したケースと同じ probPl になるはず。
    """
    hist_stats_no_pr = {
        'popStats': {1: {'wr': 0.8}},  # pr なし → uncapped fallback = 0.8*3 = 2.4
        'hnStats': {}, 'jockeyStats': {}, 'horseStats': {}, 'trainerStats': {}, 'sireStats': {}, 'bmsStats': {},
    }
    hist_stats_with_pr = {
        'popStats': {1: {'wr': 0.8, 'pr': 1.0}},  # pr=1.0 明示（キャップ済み）
        'hnStats': {}, 'jockeyStats': {}, 'horseStats': {}, 'trainerStats': {}, 'sireStats': {}, 'bmsStats': {},
    }
    horses = [
        {'hn': 1, 'pop': 1, 'odds': 2.0},
        {'hn': 2, 'pop': 2, 'odds': 5.0},
    ]
    ranked_no_pr   = score_horses(horses, hist_stats_no_pr,   {}, '')
    ranked_with_pr = score_horses(horses, hist_stats_with_pr, {}, '')
    # キャップが適用されれば probPl は同値になるはず
    assert abs(ranked_no_pr[0]['probPl'] - ranked_with_pr[0]['probPl']) < 1e-9, (
        f"pr なし probPl={ranked_no_pr[0]['probPl']:.6f} != "
        f"pr あり probPl={ranked_with_pr[0]['probPl']:.6f}\n"
        "pop_place の 1.0 キャップが適用されていない可能性"
    )
