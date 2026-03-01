"""
═══════════════════════════════════════════════════════════
  MEXC Density Scanner — Streamlit Dashboard v2.1
═══════════════════════════════════════════════════════════
"""
import io
import time
import zipfile
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from mexc_client import MexcClientSync
from analyzer import analyze_order_book, ScanResult, WallInfo
from history import DensityTracker

# ═══════════════════════════════════════════════════
# Конфиг страницы
# ═══════════════════════════════════════════════════

st.set_page_config(
    page_title="MEXC Density Scanner",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .block-container { padding-top: 0.5rem; }
    .stMetric > div { background: #1a1f2e; padding: 0.7rem; border-radius: 8px; }
    div[data-testid="stMetricValue"] { font-size: 1.4rem; }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════
# Session State
# ═══════════════════════════════════════════════════

DEFAULTS = {
    "tracker": DensityTracker(),
    "scan_results": [],
    "scan_df": pd.DataFrame(),
    "last_scan": 0.0,
    "total_pairs": 0,
    "client": MexcClientSync(),
    "detail_symbol": "",
    "nav_page": "📊 Сканер плотностей",
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ═══════════════════════════════════════════════════
# Утилиты
# ═══════════════════════════════════════════════════

def mexc_link(symbol: str) -> str:
    return f"https://www.mexc.com/exchange/{symbol.replace('USDT', '_USDT')}"


def make_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def go_to_detail(symbol: str):
    st.session_state.detail_symbol = symbol
    st.session_state.nav_page = "🔍 Детальный разбор"
    st.rerun()


def count_trades_in_window(trades: list, window_minutes: int) -> int:
    if not trades:
        return 0
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - window_minutes * 60 * 1000
    return sum(1 for t in trades if t.get("time", 0) >= cutoff)


# ───── Графики ─────

def build_candlestick_chart(klines, symbol, interval, current_price=None):
    if not klines:
        return None
    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore",
    ])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype(float)
    df["time"] = pd.to_datetime(df["open_time"], unit="ms")

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        vertical_spacing=0.03, row_heights=[0.75, 0.25],
    )
    fig.add_trace(go.Candlestick(
        x=df["time"], open=df["open"], high=df["high"],
        low=df["low"], close=df["close"],
        increasing_line_color="#00c853", decreasing_line_color="#ff1744",
        name="Цена",
    ), row=1, col=1)

    colors = ["#00c853" if c >= o else "#ff1744"
              for c, o in zip(df["close"], df["open"])]
    fig.add_trace(go.Bar(
        x=df["time"], y=df["volume"],
        marker_color=colors, opacity=0.5, name="Объём",
    ), row=2, col=1)

    if current_price:
        fig.add_hline(
            y=current_price, line_dash="dot",
            line_color="#00d2ff", line_width=1.5,
            annotation_text=f"  {current_price:.8g}",
            annotation_font_color="#00d2ff",
            annotation_font_size=11,
            row=1, col=1,
        )

    fig.update_layout(
        title=f"{symbol} — {interval}",
        template="plotly_dark", height=420,
        xaxis_rangeslider_visible=False,
        showlegend=False,
        margin=dict(l=50, r=20, t=40, b=20),
    )
    fig.update_yaxes(title_text="Цена", row=1, col=1)
    fig.update_yaxes(title_text="Объём", row=2, col=1)
    return fig


def build_orderbook_chart(bids_raw, asks_raw, current_price, depth=50):
    bid_data = [(float(p), float(p) * float(q)) for p, q in bids_raw[:depth]]
    ask_data = [(float(p), float(p) * float(q)) for p, q in asks_raw[:depth]]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=[f"{p:.8g}" for p, _ in bid_data],
        x=[v for _, v in bid_data],
        orientation="h", name="BID",
        marker_color="rgba(0,200,83,0.7)",
        hovertemplate="Цена: %{y}<br>$%{x:,.0f}<extra>BID</extra>",
    ))
    fig.add_trace(go.Bar(
        y=[f"{p:.8g}" for p, _ in ask_data],
        x=[v for _, v in ask_data],
        orientation="h", name="ASK",
        marker_color="rgba(255,23,68,0.7)",
        hovertemplate="Цена: %{y}<br>$%{x:,.0f}<extra>ASK</extra>",
    ))
    fig.add_hline(
        y=f"{current_price:.8g}",
        line_dash="dot", line_color="#00d2ff", line_width=2,
        annotation_text=f"  ← {current_price:.8g}",
        annotation_font_color="#00d2ff",
        annotation_position="top right",
    )
    fig.update_layout(
        title="📖 Стакан (USDT)",
        xaxis_title="Объём ($)",
        template="plotly_dark",
        height=max(500, depth * 14),
        barmode="relative", showlegend=True,
        yaxis=dict(type="category"),
        margin=dict(l=80, r=20, t=40, b=30),
    )
    return fig


def build_heatmap(bids_raw, asks_raw, current_price, depth=30):
    levels = []
    for p, q in bids_raw[:depth]:
        pr, vol = float(p), float(p) * float(q)
        levels.append(("BID", pr, vol))
    for p, q in asks_raw[:depth]:
        pr, vol = float(p), float(p) * float(q)
        levels.append(("ASK", pr, vol))
    if not levels:
        return None

    levels.sort(key=lambda x: x[1], reverse=True)
    max_vol = max(v for _, _, v in levels)

    fig = go.Figure()
    for side, price, vol in levels:
        intensity = vol / max_vol if max_vol > 0 else 0
        if side == "BID":
            r, g, b = 0, int(80 + 175 * intensity), 83
        else:
            r, g, b = int(80 + 175 * intensity), int(60 * (1 - intensity)), 68
        fig.add_trace(go.Bar(
            x=[vol], y=[f"{price:.8g}"],
            orientation="h",
            marker_color=f"rgba({r},{g},{b},0.85)",
            showlegend=False,
            hovertemplate=f"{side}: ${vol:,.0f}<extra>{price:.8g}</extra>",
        ))
    fig.add_hline(
        y=f"{current_price:.8g}",
        line_dash="dot", line_color="#00d2ff", line_width=2,
        annotation_text=f"  ← {current_price:.8g}",
        annotation_font_color="#00d2ff",
    )
    fig.update_layout(
        title="🔥 Хитмап плотностей",
        template="plotly_dark", height=500,
        barmode="stack",
        yaxis=dict(type="category"),
        xaxis_title="Объём (USDT)",
        margin=dict(l=80, r=20, t=40, b=30),
    )
    return fig


# ═══════════════════════════════════════════════════
# Сканирование
# ═══════════════════════════════════════════════════

def run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n):
    import config as cfg
    cfg.MIN_DAILY_VOLUME_USDT = min_vol
    cfg.MAX_DAILY_VOLUME_USDT = max_vol
    cfg.MIN_SPREAD_PCT = min_spread
    cfg.WALL_MULTIPLIER = wall_mult
    cfg.MIN_WALL_SIZE_USDT = min_wall_usd

    client = st.session_state.client
    progress = st.progress(0, "Загрузка списка пар...")

    info = client.get_exchange_info()
    if not info or "symbols" not in info:
        st.error("Не удалось загрузить список пар MEXC")
        return
    all_symbols = [
        s["symbol"] for s in info["symbols"]
        if s.get("quoteAsset") == "USDT"
        and s.get("isSpotTradingAllowed", True)
        and s.get("status") == "1"
    ]
    progress.progress(10, f"{len(all_symbols)} USDT-пар. Фильтрую...")

    tickers = client.get_all_tickers_24h()
    if not tickers:
        st.error("Не удалось загрузить тикеры")
        return
    ticker_map = {t["symbol"]: t for t in tickers if "symbol" in t}

    candidates = []
    for sym in all_symbols:
        t = ticker_map.get(sym)
        if not t:
            continue
        vol = float(t.get("quoteVolume", 0))
        if min_vol <= vol <= max_vol:
            candidates.append((sym, t))
    candidates.sort(key=lambda x: float(x[1].get("quoteVolume", 0)), reverse=True)
    progress.progress(20, f"Сканирую ({len(candidates)} пар)...")

    results = []
    total = len(candidates)
    for i, (sym, ticker) in enumerate(candidates):
        book = client.get_order_book(sym, cfg.ORDER_BOOK_DEPTH)
        if book:
            result = analyze_order_book(sym, book, ticker)
            if result and result.spread_pct >= min_spread:
                result.trade_count_24h = int(ticker.get("count", 0))
                results.append(result)
        if (i + 1) % 5 == 0 or i == total - 1:
            pct = 20 + int((i + 1) / total * 75)
            progress.progress(pct, f"{i+1}/{total} | Найдено: {len(results)}")
            time.sleep(0.02)

    new_movers = st.session_state.tracker.update(results)
    results.sort(key=lambda r: r.score, reverse=True)
    top_results = results[:top_n]

    rows = []
    for r in top_results:
        biggest = r.biggest_wall
        if not biggest:
            continue
        bid_str = " | ".join(
            f"${w.size_usdt:,.0f} ({w.multiplier}x, -{w.distance_pct}%)"
            for w in r.bid_walls[:3]) or "—"
        ask_str = " | ".join(
            f"${w.size_usdt:,.0f} ({w.multiplier}x, +{w.distance_pct}%)"
            for w in r.ask_walls[:3]) or "—"
        rows.append({
            "Скор": r.score,
            "Пара": r.symbol,
            "Спред %": round(r.spread_pct, 2),
            "Объём 24ч $": round(r.volume_24h_usdt),
            "Сделок 24ч": getattr(r, "trade_count_24h", 0),
            "BID стенки": bid_str,
            "ASK стенки": ask_str,
            "B/A": f"{len(r.bid_walls)}/{len(r.ask_walls)}",
            "🔄": "⚡" if r.has_movers else "",
            "Mid": r.mid_price,
            "Bid": r.best_bid,
            "Ask": r.best_ask,
            "Bid Depth $": round(r.total_bid_depth_usdt),
            "Ask Depth $": round(r.total_ask_depth_usdt),
        })

    st.session_state.scan_results = top_results
    st.session_state.scan_df = pd.DataFrame(rows) if rows else pd.DataFrame()
    st.session_state.last_scan = time.time()
    st.session_state.total_pairs = total

    progress.progress(100, "Готово!")
    time.sleep(0.3)
    progress.empty()
    if new_movers:
        st.toast(f"🔄 {len(new_movers)} новых переставок!", icon="⚡")


# ═══════════════════════════════════════════════════
# Сайдбар
# ═══════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## ⚙️ Настройки")
    min_vol = st.number_input("Мин. объём 24ч ($)", value=100, min_value=0, step=100)
    max_vol = st.number_input("Макс. объём 24ч ($)", value=500_000, min_value=100, step=10000)
    min_spread = st.slider("Мин. спред (%)", 0.0, 20.0, 0.5, 0.1)
    wall_mult = st.slider("Множитель (x)", 2, 50, 5)
    min_wall_usd = st.number_input("Мин. стенка ($)", value=50, min_value=1, step=10)
    top_n = st.slider("Результатов", 5, 100, 30)

    st.markdown("---")
    auto_refresh = st.checkbox("🔄 Авто-обновление (60с)")
    if auto_refresh:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=60_000, key="auto_refresh")
        except ImportError:
            st.warning("Установи `streamlit-autorefresh`")

    scan_btn = st.button("🚀 Запустить скан", use_container_width=True, type="primary")

    # ─── Скачать ВСЁ ───
    st.markdown("---")
    st.markdown("### 📥 Экспорт")

    def build_full_zip() -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            if not st.session_state.scan_df.empty:
                zf.writestr("scan_results.csv",
                            st.session_state.scan_df.to_csv(index=False))
            tracker = st.session_state.tracker
            if tracker.all_mover_events:
                m_rows = [{
                    "Время": datetime.fromtimestamp(e.timestamp).isoformat(),
                    "Пара": e.symbol, "Сторона": e.side,
                    "Старая цена": e.old_price, "Новая цена": e.new_price,
                    "Объём $": round(e.size_usdt),
                    "Сдвиг %": e.shift_pct, "Направление": e.direction,
                } for e in tracker.all_mover_events]
                zf.writestr("movers.csv",
                            pd.DataFrame(m_rows).to_csv(index=False))
            all_walls = []
            for r in st.session_state.scan_results:
                for w in r.all_walls:
                    all_walls.append({
                        "Пара": r.symbol, "Сторона": w.side,
                        "Цена": w.price, "Объём $": round(w.size_usdt),
                        "Множитель": w.multiplier,
                        "Расстояние %": w.distance_pct,
                        "Уровней": w.levels_count,
                        "Mid": r.mid_price, "Спред %": round(r.spread_pct, 2),
                    })
            if all_walls:
                zf.writestr("all_walls.csv",
                            pd.DataFrame(all_walls).to_csv(index=False))
        buf.seek(0)
        return buf.getvalue()

    has_data = bool(st.session_state.scan_results) or bool(
        st.session_state.tracker.all_mover_events)
    if has_data:
        st.download_button(
            "📦 Скачать ВСЁ (ZIP)",
            data=build_full_zip(),
            file_name=f"mexc_all_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
            mime="application/zip",
            use_container_width=True,
        )

    st.markdown("---")
    stats = st.session_state.tracker.get_stats()
    st.caption(
        f"Сканов: {stats['total_scans']} · "
        f"Пар: {stats['total_pairs_tracked']} · "
        f"Переставок: {stats['total_mover_events']}"
    )

# ═══════════════════════════════════════════════════
# Скан
# ═══════════════════════════════════════════════════

if scan_btn or (auto_refresh and time.time() - st.session_state.last_scan > 55):
    run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n)


# ═══════════════════════════════════════════════════
# Навигация — именованные вкладки
# ═══════════════════════════════════════════════════

PAGES = ["📊 Сканер плотностей", "🔍 Детальный разбор", "📈 Мониторинг переставок"]

page = st.radio(
    "nav", PAGES, horizontal=True,
    key="nav_page", label_visibility="collapsed",
)
st.markdown("---")


# ═══════════════════════════════════════════════════
# СТРАНИЦА 1 — СКАНЕР
# ═══════════════════════════════════════════════════

if page == PAGES[0]:
    results = st.session_state.scan_results
    scan_df = st.session_state.scan_df

    if not results:
        st.info("Нажми **🚀 Запустить скан** в сайдбаре")
    else:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Найдено", len(results))
        c2.metric("Проверено", st.session_state.total_pairs)
        c3.metric("Лучший", f"⭐ {results[0].score}")
        c4.metric("С переставками", sum(1 for r in results if r.has_movers))
        c5.metric("Σ сделок 24ч", f"{sum(getattr(r,'trade_count_24h',0) for r in results):,}")

        # Выбор монеты → переход к разбору
        col_s, col_g = st.columns([3, 1])
        with col_s:
            opts = [r.symbol for r in results]
            chosen = st.selectbox("🔍 Выбери пару → детальный разбор",
                                  [""] + opts, key="scanner_pick")
        with col_g:
            st.markdown("<br>", unsafe_allow_html=True)
            if chosen and st.button("➡️ Открыть", type="primary"):
                go_to_detail(chosen)

        # Таблица
        if not scan_df.empty:
            show_cols = ["Скор", "Пара", "Спред %", "Объём 24ч $",
                         "Сделок 24ч", "BID стенки", "ASK стенки", "B/A", "🔄"]
            st.dataframe(
                scan_df[show_cols],
                column_config={
                    "Скор": st.column_config.NumberColumn(format="%.1f", width="small"),
                    "Спред %": st.column_config.NumberColumn(format="%.2f"),
                    "Объём 24ч $": st.column_config.NumberColumn(format="%d"),
                    "Сделок 24ч": st.column_config.NumberColumn(format="%d"),
                    "🔄": st.column_config.TextColumn(width="small"),
                },
                hide_index=True,
                use_container_width=True,
                height=min(len(scan_df) * 38 + 40, 800),
            )
            st.download_button(
                "📥 Скачать результаты (CSV)",
                data=make_csv(scan_df),
                file_name=f"scan_{datetime.now().strftime('%H%M')}.csv",
                mime="text/csv",
            )


# ═══════════════════════════════════════════════════
# СТРАНИЦА 2 — ДЕТАЛЬНЫЙ РАЗБОР
# ═══════════════════════════════════════════════════

elif page == PAGES[1]:
    results = st.session_state.scan_results
    sym_list = [r.symbol for r in results] if results else []

    col_a, col_b = st.columns([2, 1])
    with col_a:
        idx = 0
        if st.session_state.detail_symbol in sym_list:
            idx = sym_list.index(st.session_state.detail_symbol) + 1
        target = st.selectbox("Пара", [""] + sym_list, index=idx, key="detail_sel")
    with col_b:
        manual = st.text_input("Или вручную", placeholder="XYZUSDT")

    symbol = manual.strip().upper() if manual.strip() else target
    if not symbol:
        st.info("Выбери пару из скана или введи вручную")
        st.stop()

    st.session_state.detail_symbol = symbol

    # ─── Загрузка ───
    client = st.session_state.client
    with st.spinner(f"Загружаю {symbol}..."):
        book = client.get_order_book(symbol, 500)
        ticker = client.get_ticker_24h(symbol)
        trades = client.get_recent_trades(symbol, 1000)
        kl_1h = client.get_klines(symbol, "1h", 100)
        kl_5m = client.get_klines(symbol, "5m", 100)
        kl_1m = client.get_klines(symbol, "1m", 100)

    if not book or not book.get("bids") or not book.get("asks"):
        st.error(f"Не удалось загрузить стакан {symbol}")
        st.stop()

    bids_raw = book["bids"]
    asks_raw = book["asks"]
    best_bid = float(bids_raw[0][0])
    best_ask = float(asks_raw[0][0])
    mid_price = (best_bid + best_ask) / 2
    spread_pct = (best_ask - best_bid) / best_bid * 100
    bid_depth = sum(float(p) * float(q) for p, q in bids_raw)
    ask_depth = sum(float(p) * float(q) for p, q in asks_raw)

    td = ticker if isinstance(ticker, dict) else {}
    trade_count_24h = int(td.get("count", 0))
    volume_24h = float(td.get("quoteVolume", 0))

    # Заголовок
    h1, h2 = st.columns([3, 1])
    with h1:
        st.markdown(f"## {symbol}")
    with h2:
        st.markdown(f"[🔗 Открыть на MEXC]({mexc_link(symbol)})")

    # Метрики
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Mid Price", f"{mid_price:.8g}")
    m2.metric("Спред", f"{spread_pct:.2f}%")
    m3.metric("Bid глубина", f"${bid_depth:,.0f}")
    m4.metric("Ask глубина", f"${ask_depth:,.0f}")
    m5.metric("Сделок 24ч", f"{trade_count_24h:,}")
    m6.metric("Объём 24ч", f"${volume_24h:,.0f}")

    # ─── Сделки по таймфреймам ───
    st.markdown("#### ⏱ Количество сделок")
    tc = st.columns(5)
    if trades:
        tc[0].metric("5 мин", count_trades_in_window(trades, 5))
        tc[1].metric("15 мин", count_trades_in_window(trades, 15))
        tc[2].metric("1 час", count_trades_in_window(trades, 60))
        tc[3].metric("4 часа", count_trades_in_window(trades, 240))
        tc[4].metric("24 часа", trade_count_24h)

        times = [t.get("time", 0) for t in trades if t.get("time")]
        if len(times) >= 3:
            deltas = [(times[i] - times[i + 1]) / 1000
                      for i in range(len(times) - 1) if times[i + 1] > 0]
            if deltas:
                avg_d = sum(deltas) / len(deltas)
                robot = " 🤖 **Робот!**" if avg_d < 30 and max(deltas) < 120 else ""
                st.caption(
                    f"Интервалы: ср.={avg_d:.1f}с, "
                    f"мин={min(deltas):.1f}с, макс={max(deltas):.1f}с{robot}"
                )

    # ─── Графики (вкладки) ───
    st.markdown("#### 📈 Графики")
    t1h, t5m, t1m = st.tabs(["1 час", "5 минут", "1 минута"])
    with t1h:
        fig = build_candlestick_chart(kl_1h, symbol, "1h", mid_price)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Нет данных 1h")
    with t5m:
        fig = build_candlestick_chart(kl_5m, symbol, "5m", mid_price)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Нет данных 5m")
    with t1m:
        fig = build_candlestick_chart(kl_1m, symbol, "1m", mid_price)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Нет данных 1m")

    # ─── Стакан ───
    st.markdown("#### 📖 Стакан")
    depth_v = st.select_slider("Глубина", [20, 30, 50, 100], value=50, key="ob_depth")
    fig_ob = build_orderbook_chart(bids_raw, asks_raw, mid_price, depth_v)
    st.plotly_chart(fig_ob, use_container_width=True)

    # ─── Хитмап ───
    fig_hm = build_heatmap(bids_raw, asks_raw, mid_price, 30)
    if fig_hm:
        st.plotly_chart(fig_hm, use_container_width=True)

    # ─── Последние сделки ───
    trades_df = pd.DataFrame()
    if trades:
        st.markdown("#### 📋 Последние сделки")
        t_rows = []
        for t in trades[:50]:
            p = float(t.get("price", 0))
            q = float(t.get("qty", 0))
            t_rows.append({
                "Время": pd.to_datetime(t.get("time", 0), unit="ms").strftime("%H:%M:%S"),
                "Цена": p, "Кол-во": q,
                "USDT": round(p * q, 2),
                "Сторона": "🟢 BUY" if not t.get("isBuyerMaker") else "🔴 SELL",
            })
        trades_df = pd.DataFrame(t_rows)
        st.dataframe(trades_df, hide_index=True, use_container_width=True)

    # ─── Экспорт ───
    st.markdown("---")
    st.markdown("#### 📥 Экспорт данных по паре")

    export_parts = {}
    ob_rows = []
    for side, data in [("BID", bids_raw), ("ASK", asks_raw)]:
        for p, q in data:
            ob_rows.append({"Сторона": side, "Цена": float(p),
                            "Количество": float(q),
                            "USDT": round(float(p) * float(q), 4)})
    ob_df = pd.DataFrame(ob_rows)
    export_parts["orderbook"] = ob_df
    if not trades_df.empty:
        export_parts["trades"] = trades_df
    for label, kdata in [("klines_1h", kl_1h), ("klines_5m", kl_5m), ("klines_1m", kl_1m)]:
        if kdata:
            kdf = pd.DataFrame(kdata, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trades", "taker_buy_base",
                "taker_buy_quote", "ignore"])
            kdf["time"] = pd.to_datetime(kdf["open_time"], unit="ms")
            export_parts[label] = kdf

    e1, e2 = st.columns(2)
    with e1:
        st.download_button("📥 Стакан CSV", data=make_csv(ob_df),
                           file_name=f"{symbol}_book.csv", mime="text/csv")
    with e2:
        if not trades_df.empty:
            st.download_button("📥 Сделки CSV", data=make_csv(trades_df),
                               file_name=f"{symbol}_trades.csv", mime="text/csv")

    def build_sym_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, df in export_parts.items():
                zf.writestr(f"{symbol}_{name}.csv", df.to_csv(index=False))
            meta = (f"symbol,{symbol}\nmid_price,{mid_price}\n"
                    f"spread_pct,{spread_pct:.4f}\nbid_depth,{bid_depth:.2f}\n"
                    f"ask_depth,{ask_depth:.2f}\ntrades_24h,{trade_count_24h}\n"
                    f"volume_24h,{volume_24h:.2f}\n"
                    f"timestamp,{datetime.now().isoformat()}\n")
            zf.writestr(f"{symbol}_meta.csv", meta)
        buf.seek(0)
        return buf.getvalue()

    st.download_button(
        f"📦 Скачать ВСЕ данные {symbol} (ZIP)",
        data=build_sym_zip(),
        file_name=f"{symbol}_{datetime.now().strftime('%H%M')}.zip",
        mime="application/zip",
        use_container_width=True,
    )


# ═══════════════════════════════════════════════════
# СТРАНИЦА 3 — ПЕРЕСТАВКИ
# ═══════════════════════════════════════════════════

elif page == PAGES[2]:
    tracker = st.session_state.tracker

    st.markdown("""
    **Переставляш** — плотность, которая перемещается.
    Признак робота. Для накопления данных нужно несколько сканов
    (включи авто-обновление 60с).
    """)

    movers = tracker.get_active_movers(7200)

    if not movers:
        st.info("Переставок не обнаружено. Запусти несколько сканов.")
    else:
        st.success(f"⚡ {len(movers)} переставок за 2 часа")

        m_rows = []
        for e in reversed(movers):
            m_rows.append({
                "Время": datetime.fromtimestamp(e.timestamp).strftime("%H:%M:%S"),
                "↕": "⬆️" if e.direction == "UP" else "⬇️",
                "Пара": e.symbol, "Сторона": e.side,
                "Объём $": round(e.size_usdt),
                "Было": f"{e.old_price:.8g}",
                "Стало": f"{e.new_price:.8g}",
                "Сдвиг %": round(e.shift_pct, 3),
            })
        mover_df = pd.DataFrame(m_rows)
        st.dataframe(mover_df, hide_index=True, use_container_width=True,
                     column_config={"↕": st.column_config.TextColumn(width="small")})

        col_mp, col_mg = st.columns([3, 1])
        with col_mp:
            chosen_m = st.selectbox("Выбери пару → разбор",
                                    [""] + list({e.symbol for e in movers}),
                                    key="mover_pick")
        with col_mg:
            st.markdown("<br>", unsafe_allow_html=True)
            if chosen_m and st.button("➡️ Открыть", key="mover_go"):
                go_to_detail(chosen_m)

        st.download_button(
            "📥 Переставки CSV",
            data=make_csv(mover_df),
            file_name=f"movers_{datetime.now().strftime('%H%M')}.csv",
            mime="text/csv",
        )

    top_movers = tracker.get_top_movers(15)
    if top_movers:
        st.markdown("### 🏆 Топ пар по переставкам")
        fig = go.Figure(go.Bar(
            x=[x[0] for x in top_movers],
            y=[x[1] for x in top_movers],
            marker_color="#00d2ff",
        ))
        fig.update_layout(template="plotly_dark", height=300,
                          xaxis_title="Пара", yaxis_title="Переставок")
        st.plotly_chart(fig, use_container_width=True)


# Футер
st.markdown("---")
st.caption("MEXC Density Scanner v2.1 · Не является финансовой рекомендацией")
