@echo off
chcp 65001 > nul
title 地方競馬 分析サーバー

echo ========================================
echo  地方競馬 データ取得サーバー
echo ========================================
echo.

cd /d "%~dp0"

python --version > nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [エラー] Python が見つかりません。
    echo python.org からインストールしてください。
    pause
    exit /b 1
)

echo パッケージを確認中...
python -m pip install -r requirements.txt -q --disable-pip-version-check
if %ERRORLEVEL% neq 0 (
    echo [エラー] パッケージのインストールに失敗しました。
    pause
    exit /b 1
)

echo.
echo サーバーを起動しました。index.html をブラウザで開いてください。
echo 停止するにはこのウィンドウを閉じてください。
echo.

python server.py
pause
