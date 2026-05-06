"""
RefundMoneyList パーサのテスト
対象: server.parse_refund_page / fetch_races.parse_refund_soup

テストケース:
  A. 標準構造（着順テーブル → 払戻テーブル）
  B. 非標準構造（余分なテーブルが先に来る）← 旧バグ再現
  C. 複数レース（2R以上）
  D. 結果未確定（払戻なし）
  E. 複勝の複数行（rowspan ありを想定した連続行）
  F. 払戻金に3連単・馬連が含まれる
  G. 空ページ（レースなし）
"""
import sys
import os
import pytest
from bs4 import BeautifulSoup

# --- パス設定 ---------------------------------------------------------------
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, 'scripts'))

# server.py の parse_refund_page をインポート
from server import parse_refund_page

# fetch_races.py の parse_refund_soup をインポート
from fetch_races import parse_refund_soup


# ── ヘルパー ────────────────────────────────────────────────────────────────

def make_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, 'lxml')


def _results_table(rows: list[tuple]) -> str:
    """(pos, frame, hn) のリストから着順テーブル HTML を生成"""
    trs = ''.join(
        f'<tr><td>{p}</td><td>{f}</td><td>{h}</td></tr>'
        for p, f, h in rows
    )
    return f'<table>{trs}</table>'


def _payouts_table(entries: list[tuple]) -> str:
    """(式別, horses, payout) のリストから払戻テーブル HTML を生成"""
    trs = ''.join(
        f'<tr><td>{t}</td><td>{h}</td><td>{p}円</td></tr>'
        for t, h, p in entries
    )
    return f'<table>{trs}</table>'


# ── テストケース A: 標準構造 ────────────────────────────────────────────────

STANDARD_HTML = """
<p>1R</p>
{results}
{payouts}
""".format(
    results=_results_table([(1, 1, 5), (2, 2, 3), (3, 3, 7)]),
    payouts=_payouts_table([('単勝', '5', 350), ('複勝', '5', 140), ('複勝', '3', 110)]),
)


def test_standard_results():
    """標準構造: 着順テーブルから正しく着順を取得できる"""
    results, _ = parse_refund_page(make_soup(STANDARD_HTML))
    assert results[1] == {5: 1, 3: 2, 7: 3}


def test_standard_payouts():
    """標準構造: 払戻テーブルから単勝・複勝を取得できる"""
    _, payouts = parse_refund_page(make_soup(STANDARD_HTML))
    assert '単勝' in payouts[1]
    assert payouts[1]['単勝'][0]['payout'] == 350
    assert payouts[1]['単勝'][0]['horses'] == '5'
    assert len(payouts[1]['複勝']) == 2


# ── テストケース B: 非標準構造（旧バグ再現） ──────────────────────────────

# 払戻テーブルの前に余分なテーブルが存在する（table_idx==2 方式で失敗していたケース）
NON_STANDARD_HTML = """
<p>1R</p>
<table><tr><th>距離</th><td>1000m</td></tr></table>
{results}
{payouts}
""".format(
    results=_results_table([(1, 2, 5), (2, 3, 8), (3, 1, 1)]),
    payouts=_payouts_table([('単勝', '5', 480), ('三連複', '1-5-8', 2100)]),
)


def test_non_standard_results():
    """非標準構造: 余分なテーブルが先にあっても着順を正しく取得できる"""
    results, _ = parse_refund_page(make_soup(NON_STANDARD_HTML))
    assert 1 in results
    assert results[1][5] == 1
    assert results[1][8] == 2
    assert results[1][1] == 3


def test_non_standard_payouts():
    """非標準構造: 余分なテーブルが先にあっても払戻を正しく取得できる（旧バグ修正確認）"""
    _, payouts = parse_refund_page(make_soup(NON_STANDARD_HTML))
    assert 1 in payouts
    assert payouts[1]['単勝'][0]['payout'] == 480
    assert payouts[1]['三連複'][0]['horses'] == '1-5-8'
    assert payouts[1]['三連複'][0]['payout'] == 2100


# ── テストケース C: 複数レース ───────────────────────────────────────────────

MULTI_RACE_HTML = """
<p>1R</p>
{r1}{p1}
<p>2R</p>
{r2}{p2}
""".format(
    r1=_results_table([(1, 1, 3), (2, 2, 6)]),
    p1=_payouts_table([('単勝', '3', 210)]),
    r2=_results_table([(1, 3, 9), (2, 1, 2), (3, 2, 4)]),
    p2=_payouts_table([('単勝', '9', 1580), ('複勝', '9', 350), ('複勝', '2', 120)]),
)


def test_multi_race_results():
    """複数レース: 1R と 2R の着順が正しく分離される"""
    results, _ = parse_refund_page(make_soup(MULTI_RACE_HTML))
    assert results[1] == {3: 1, 6: 2}
    assert results[2][9] == 1
    assert results[2][2] == 2
    assert results[2][4] == 3


def test_multi_race_payouts():
    """複数レース: 各レースの払戻が正しく分離される"""
    _, payouts = parse_refund_page(make_soup(MULTI_RACE_HTML))
    assert payouts[1]['単勝'][0]['payout'] == 210
    assert payouts[2]['単勝'][0]['payout'] == 1580
    assert len(payouts[2]['複勝']) == 2


# ── テストケース D: 結果未確定（払戻なし） ──────────────────────────────────

NO_RESULTS_HTML = """
<p>1R</p>
<table>
  <tr><td>馬番</td><td>馬名</td><td>騎手</td></tr>
  <tr><td>1</td><td>テスト馬A</td><td>山田</td></tr>
</table>
"""


def test_no_results_returns_empty():
    """未発走/未確定レース: 結果も払戻も空"""
    results, payouts = parse_refund_page(make_soup(NO_RESULTS_HTML))
    assert results == {}
    assert payouts == {}


# ── テストケース E: 馬連・三連単を含む ─────────────────────────────────────

FULL_BET_HTML = """
<p>3R</p>
{results}
{payouts}
""".format(
    results=_results_table([(1, 2, 4), (2, 4, 11), (3, 1, 2)]),
    payouts=_payouts_table([
        ('単勝',  '4',     520),
        ('複勝',  '4',     180),
        ('複勝',  '11',    320),
        ('複勝',  '2',     110),
        ('馬連',  '4-11', 2840),
        ('三連複', '2-4-11', 8900),
        ('三連単', '4-11-2', 38500),
    ]),
)


def test_full_bet_types():
    """馬連・三連複・三連単が全て取得できる"""
    _, payouts = parse_refund_page(make_soup(FULL_BET_HTML))
    p = payouts[3]
    assert p['単勝'][0]['payout'] == 520
    assert p['馬連'][0]['horses'] == '4-11'
    assert p['三連複'][0]['payout'] == 8900
    assert p['三連単'][0]['payout'] == 38500
    assert len(p['複勝']) == 3


# ── テストケース F: 空ページ ────────────────────────────────────────────────

def test_empty_page():
    """空ページ: エラーなく空の辞書を返す"""
    results, payouts = parse_refund_page(make_soup('<html><body></body></html>'))
    assert results == {}
    assert payouts == {}


# ── fetch_races.parse_refund_soup との一貫性確認 ──────────────────────────────

def test_consistency_server_vs_fetch_races():
    """server.parse_refund_page と fetch_races.parse_refund_soup が同じ結果を返す"""
    soup = make_soup(MULTI_RACE_HTML)
    r_server,  p_server  = parse_refund_page(soup)
    r_scripts, p_scripts = parse_refund_soup(soup)
    assert r_server  == r_scripts
    assert p_server  == p_scripts


# ── 馬番・着順の境界値チェック ───────────────────────────────────────────────

def test_ignore_out_of_range_pos_hn():
    """着順 > 20 または 馬番 > 16 の行は無視される"""
    html = """
    <p>1R</p>
    <table>
      <tr><td>1</td><td>1</td><td>5</td></tr>
      <tr><td>21</td><td>1</td><td>5</td></tr>
      <tr><td>2</td><td>2</td><td>17</td></tr>
      <tr><td>3</td><td>3</td><td>7</td></tr>
    </table>
    """
    results, _ = parse_refund_page(make_soup(html))
    assert 5 in results[1]
    assert 17 not in results[1]
    assert results[1][5] == 1   # pos=21 の重複行は上書きされない
