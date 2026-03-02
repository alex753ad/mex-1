"""
═══════════════════════════════════════════════════════════
  MEXC Density Scanner — Streamlit Dashboard v2.5
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
st.set_page_config(page_title="MEXC Density Scanner", page_icon="🔍",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown("""<style>
.block-container{padding-top:.5rem}
.stMetric>div{background:#1a1f2e;padding:.7rem;border-radius:8px}
div[data-testid="stMetricValue"]{font-size:1.4rem}
</style>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════
# Безопасные конвертации
# ═══════════════════════════════════════════════════
def sf(val, default=0.0):
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def si(val, default=0):
    try:
        return int(sf(val, default))
    except (ValueError, TypeError):
        return default

def parse_book(raw):
    out = []
    if not raw or not isinstance(raw, list):
        return out
    for e in raw:
        if not isinstance(e, (list, tuple)) or len(e) < 2:
            continue
        p, q = sf(e[0]), sf(e[1])
        if p > 0 and q > 0:
            out.append((p, q))
    return out

def extract_trade_count(td):
    if isinstance(td, list):
        td = td[0] if td else {}
    if not isinstance(td, dict):
        return 0
    for k in ("count", "tradeCount", "trades", "txcnt"):
        v = td.get(k)
        if v is None or v == "" or v == 0 or v == "0":
            continue
        r = si(v)
        if r > 0:
            return r
    return 0

def parse_klines(raw):
    if not raw or not isinstance(raw, list):
        return pd.DataFrame()
    rows = []
    for k in raw:
        if not isinstance(k, (list, tuple)) or len(k) < 6:
            continue
        rows.append({
            "open_time": sf(k[0]), "open": sf(k[1]), "high": sf(k[2]),
            "low": sf(k[3]), "close": sf(k[4]), "volume": sf(k[5]),
            "close_time": sf(k[6]) if len(k) > 6 else 0,
            "quote_volume": sf(k[7]) if len(k) > 7 else 0,
            "trades": si(k[8]) if len(k) > 8 else 0,
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df

def mexc_link(s):
    return f"https://www.mexc.com/exchange/{s.replace('USDT','_USDT')}"

def make_csv(df):
    return df.to_csv(index=False).encode("utf-8-sig")

# ═══════════════════════════════════════════════════
# Session State
# ═══════════════════════════════════════════════════
for k, v in [("tracker", DensityTracker()), ("scan_results", []),
             ("scan_df", pd.DataFrame()), ("last_scan", 0.0),
             ("total_pairs", 0), ("client", MexcClientSync()),
             ("detail_symbol", ""), ("target_page", 0)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ═══════════════════════════════════════════════════
# Графики — ВСЕ НА ЧИСЛОВЫХ ОСЯХ, БЕЗ КАТЕГОРИЙ
# ═══════════════════════════════════════════════════

def build_candlestick(df, symbol, interval, cur_price=None):
    if df is None or df.empty or len(df) < 2:
        return None
    try:
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            vertical_spacing=0.03, row_heights=[0.75, 0.25])
        fig.add_trace(go.Candlestick(
            x=df["time"], open=df["open"], high=df["high"],
            low=df["low"], close=df["close"],
            increasing_line_color="#00c853",
            decreasing_line_color="#ff1744", name="Цена"), row=1, col=1)
        colors = ["#00c853" if c >= o else "#ff1744"
                  for c, o in zip(df["close"], df["open"])]
        fig.add_trace(go.Bar(x=df["time"], y=df["volume"],
                             marker_color=colors, opacity=0.5,
                             name="Объём"), row=2, col=1)
        if cur_price and cur_price > 0:
            fig.add_hline(y=float(cur_price), line_dash="dot",
                          line_color="#00d2ff", line_width=1.5,
                          annotation_text=f"  {cur_price:.8g}",
                          annotation_font_color="#00d2ff",
                          annotation_font_size=11, row=1, col=1)
        fig.update_layout(title=f"{symbol} — {interval}",
                          template="plotly_dark", height=420,
                          xaxis_rangeslider_visible=False,
                          showlegend=False,
                          margin=dict(l=50, r=20, t=40, b=20))
        return fig
    except Exception as e:
        st.caption(f"Ошибка графика: {e}")
        return None


def build_orderbook_chart(bids, asks, cur_price, depth=50):
    """Стакан — горизонтальные бары, ЧИСЛОВАЯ ось Y"""
    try:
        b = bids[:depth]
        a = asks[:depth]
        if not b and not a:
            return None

        # Формируем данные: y — числовая цена, x — объём в $
        bid_prices = [float(p) for p, q in b]
        bid_vols = [float(p * q) for p, q in b]
        ask_prices = [float(p) for p, q in a]
        ask_vols = [float(p * q) for p, q in a]

        fig = go.Figure()
        if bid_prices:
            fig.add_trace(go.Bar(
                y=bid_prices, x=bid_vols,
                orientation="h", name="BID",
                marker_color="rgba(0,200,83,0.7)",
                hovertemplate="Цена: %{y:.8g}<br>$%{x:,.0f}<extra>BID</extra>"))
        if ask_prices:
            fig.add_trace(go.Bar(
                y=ask_prices, x=ask_vols,
                orientation="h", name="ASK",
                marker_color="rgba(255,23,68,0.7)",
                hovertemplate="Цена: %{y:.8g}<br>$%{x:,.0f}<extra>ASK</extra>"))

        if cur_price and float(cur_price) > 0:
            max_x = max(bid_vols + ask_vols) if (bid_vols or ask_vols) else 1
            fig.add_trace(go.Scatter(
                x=[0, max_x * 1.1], y=[float(cur_price), float(cur_price)],
                mode="lines", name="Текущая цена",
                line=dict(color="#00d2ff", width=2, dash="dot"),
                showlegend=False,
                hovertemplate=f"Цена: {float(cur_price):.8g}<extra></extra>"))

        fig.update_layout(
            title="📖 Стакан (USDT)", xaxis_title="Объём ($)",
            yaxis_title="Цена",
            template="plotly_dark",
            height=max(500, depth * 12),
            barmode="relative", showlegend=True,
            margin=dict(l=80, r=20, t=40, b=30))
        return fig
    except Exception as e:
        st.error(f"Ошибка стакана: {e}")
        return None


def build_heatmap(bids, asks, cur_price, depth=30):
    """Хитмап — ЧИСЛОВАЯ ось Y"""
    try:
        levels = []
        for p, q in bids[:depth]:
            levels.append(("BID", float(p), float(p * q)))
        for p, q in asks[:depth]:
            levels.append(("ASK", float(p), float(p * q)))
        if not levels:
            return None

        levels.sort(key=lambda x: x[1])
        max_vol = max(v for _, _, v in levels)
        if max_vol <= 0:
            max_vol = 1.0

        prices = []
        vols = []
        colors = []
        hovers = []
        for side, price, vol in levels:
            intensity = min(float(vol) / float(max_vol), 1.0)
            prices.append(float(price))
            vols.append(float(vol))
            if side == "BID":
                c = f"rgba(0,{int(80+175*intensity)},83,0.85)"
            else:
                c = f"rgba({int(80+175*intensity)},{int(60*(1.0-intensity))},68,0.85)"
            colors.append(c)
            hovers.append(f"{side}: ${vol:,.0f} @ {price:.8g}")

        fig = go.Figure(go.Bar(
            y=prices, x=vols, orientation="h",
            marker_color=colors,
            hovertext=hovers,
            hoverinfo="text",
            showlegend=False))

        if cur_price and float(cur_price) > 0:
            max_x = max(vols) if vols else 1
            fig.add_trace(go.Scatter(
                x=[0, max_x * 1.1], y=[float(cur_price), float(cur_price)],
                mode="lines", name="Текущая цена",
                line=dict(color="#00d2ff", width=2, dash="dot"),
                showlegend=False,
                hovertemplate=f"Цена: {float(cur_price):.8g}<extra></extra>"))

        fig.update_layout(
            title="🔥 Хитмап плотностей",
            template="plotly_dark", height=500,
            yaxis_title="Цена", xaxis_title="Объём (USDT)",
            margin=dict(l=80, r=20, t=40, b=30))
        return fig
    except Exception as e:
        st.error(f"Ошибка хитмапа: {e}")
        return None


def kline_stats(df, last_n=None):
    if df is None or df.empty:
        return {"volume": 0.0, "trades": 0}
    sub = df.tail(last_n) if last_n else df
    return {
        "volume": float(sub["quote_volume"].sum()) if "quote_volume" in sub else 0.0,
        "trades": int(sub["trades"].sum()) if "trades" in sub else 0,
    }


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
    progress = st.progress(0, "Загрузка пар...")

    try:
        info = client.get_exchange_info()
    except Exception as e:
        st.error(f"Ошибка API: {e}")
        return
    if not info or "symbols" not in info:
        err = client.last_error or "Пустой ответ"
        st.error(f"❌ Не загрузить список пар: {err}")
        st.info("Нажми **🔧 Проверить API** в сайдбаре")
        ok, msg = client.ping()
        st.caption(f"Ping: {'OK' if ok else 'FAIL'} — {msg}")
        progress.empty()
        return

    all_symbols = []
    for s in info["symbols"]:
        try:
            if s.get("quoteAsset") != "USDT":
                continue
            status = s.get("status", "")
            ok = (str(status) in ("1","ENABLED","True","true")
                  or status is True or status == 1)
            if ok and s.get("isSpotTradingAllowed", True):
                all_symbols.append(s["symbol"])
        except Exception:
            continue
    if not all_symbols:
        for s in info["symbols"]:
            try:
                if s.get("quoteAsset") == "USDT":
                    all_symbols.append(s["symbol"])
            except Exception:
                continue
    if not all_symbols:
        st.error("Нет USDT-пар")
        progress.empty()
        return

    progress.progress(10, f"{len(all_symbols)} пар...")

    try:
        tickers = client.get_all_tickers_24h()
    except Exception as e:
        st.error(f"Ошибка тикеров: {e}")
        progress.empty()
        return
    if not tickers:
        st.error(f"Нет тикеров. {client.last_error}")
        progress.empty()
        return

    ticker_map = {t["symbol"]: t for t in tickers if "symbol" in t}
    candidates = []
    for sym in all_symbols:
        t = ticker_map.get(sym)
        if t and min_vol <= sf(t.get("quoteVolume",0)) <= max_vol:
            candidates.append((sym, t))
    candidates.sort(key=lambda x: sf(x[1].get("quoteVolume",0)), reverse=True)

    if not candidates:
        st.warning("Нет пар в диапазоне")
        progress.empty()
        return

    progress.progress(20, f"Сканирую ({len(candidates)})...")
    results, errors, total = [], 0, len(candidates)
    for i, (sym, ticker) in enumerate(candidates):
        try:
            book = client.get_order_book(sym, cfg.ORDER_BOOK_DEPTH)
            if book:
                r = analyze_order_book(sym, book, ticker)
                if r and r.spread_pct >= min_spread:
                    r.trade_count_24h = extract_trade_count(ticker)
                    results.append(r)
        except Exception:
            errors += 1
        if (i+1) % 5 == 0 or i == total-1:
            progress.progress(20+int((i+1)/total*70),
                              f"{i+1}/{total} | {len(results)}")
            time.sleep(0.02)

    results.sort(key=lambda r: r.score, reverse=True)
    top = results[:top_n]

    progress.progress(92, "Сделки 24ч...")
    for r in top:
        if r.trade_count_24h == 0:
            try:
                ind = client.get_ticker_24h(r.symbol)
                tc = extract_trade_count(ind)
                if tc > 0:
                    r.trade_count_24h = tc
            except Exception:
                pass

    st.session_state.tracker.update(top)

    rows = []
    for r in top:
        if not r.all_walls:
            continue
        bs = " | ".join(f"${w.size_usdt:,.0f}({w.multiplier}x,-{w.distance_pct}%)"
                        for w in r.bid_walls[:3]) or "—"
        aks = " | ".join(f"${w.size_usdt:,.0f}({w.multiplier}x,+{w.distance_pct}%)"
                         for w in r.ask_walls[:3]) or "—"
        rows.append({"Скор": r.score, "Пара": r.symbol,
                     "Спред %": round(r.spread_pct, 2),
                     "Объём 24ч $": round(r.volume_24h_usdt),
                     "Сделок 24ч": r.trade_count_24h,
                     "BID стенки": bs, "ASK стенки": aks,
                     "B/A": f"{len(r.bid_walls)}/{len(r.ask_walls)}",
                     "🔄": "⚡" if r.has_movers else ""})

    st.session_state.scan_results = top
    st.session_state.scan_df = pd.DataFrame(rows) if rows else pd.DataFrame()
    st.session_state.last_scan = time.time()
    st.session_state.total_pairs = total
    progress.progress(100, "Готово!")
    time.sleep(0.3)
    progress.empty()
    st.toast(f"Найдено {len(top)} пар")

# ═══════════════════════════════════════════════════
PAGES = ["📊 Сканер", "🔍 Детальный разбор", "📈 Переставки"]

def go_to_detail(sym):
    st.session_state.detail_symbol = sym
    st.session_state.target_page = 1

# ═══════════════════════════════════════════════════
# Сайдбар
# ═══════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ⚙️ Настройки")
    min_vol = st.number_input("Мин. объём ($)", value=100, min_value=0, step=100)
    max_vol = st.number_input("Макс. объём ($)", value=500_000, min_value=100, step=10000)
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
            pass
    scan_btn = st.button("🚀 Запустить скан", use_container_width=True, type="primary")

    st.markdown("---")
    st.markdown("### 📥 Экспорт")
    def build_full_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            if not st.session_state.scan_df.empty:
                zf.writestr("scan_results.csv", st.session_state.scan_df.to_csv(index=False))
            tr = st.session_state.tracker
            if tr.all_mover_events:
                mr = [{"Время": datetime.fromtimestamp(e.timestamp).isoformat(),
                       "Пара": e.symbol, "Сторона": e.side,
                       "Старая": e.old_price, "Новая": e.new_price,
                       "Объём $": round(e.size_usdt),
                       "Сдвиг %": e.shift_pct, "Направление": e.direction}
                      for e in tr.all_mover_events]
                zf.writestr("movers.csv", pd.DataFrame(mr).to_csv(index=False))
            ws = []
            for r in st.session_state.scan_results:
                for w in r.all_walls:
                    ws.append({"Пара": r.symbol, "Сторона": w.side,
                               "Цена": w.price, "Объём $": round(w.size_usdt),
                               "Множитель": w.multiplier, "Расст %": w.distance_pct})
            if ws:
                zf.writestr("all_walls.csv", pd.DataFrame(ws).to_csv(index=False))
        buf.seek(0)
        return buf.getvalue()

    if st.session_state.scan_results or st.session_state.tracker.all_mover_events:
        st.download_button("📦 Скачать ВСЁ (ZIP)", data=build_full_zip(),
                           file_name=f"mexc_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
                           mime="application/zip", use_container_width=True)

    st.markdown("---")
    stats = st.session_state.tracker.get_stats()
    st.caption(f"Сканов: {stats['total_scans']} · Пар: {stats['total_pairs_tracked']} · Переставок: {stats['total_mover_events']}")

    if st.button("🔧 Проверить API", use_container_width=True):
        c = st.session_state.client
        ok, msg = c.ping()
        if ok:
            st.success(f"✅ {msg}")
        else:
            st.error(f"❌ {msg}")
            for domain in ["https://api.mexc.com", "https://www.mexc.com"]:
                try:
                    import requests as _rq
                    r = _rq.get(f"{domain}/api/v3/ping", timeout=10,
                                headers={"User-Agent": "Mozilla/5.0"})
                    if r.status_code == 200:
                        c.base_url = domain
                        st.success(f"→ {domain}")
                        break
                    else:
                        st.warning(f"❌ {domain}: {r.status_code}")
                except Exception as e:
                    st.warning(f"❌ {domain}: {e}")

# ═══════════════════════════════════════════════════
if scan_btn or (auto_refresh and time.time() - st.session_state.last_scan > 55):
    run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n)

# ═══════════════════════════════════════════════════
_idx = st.session_state.target_page
if _idx < 0 or _idx >= len(PAGES):
    _idx = 0
page = st.radio("nav", PAGES, horizontal=True, index=_idx, label_visibility="collapsed")
for i, p in enumerate(PAGES):
    if page == p:
        st.session_state.target_page = i
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
        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Найдено", len(results))
        c2.metric("Проверено", st.session_state.total_pairs)
        c3.metric("Лучший", f"⭐ {results[0].score}")
        c4.metric("С переставками", sum(1 for r in results if r.has_movers))
        tt = sum(r.trade_count_24h for r in results)
        c5.metric("Σ сделок 24ч", f"{tt:,}" if tt else "—")

        st.markdown("##### 🔍 Выбери пару")
        opts = [r.symbol for r in results]
        nc = min(10, len(opts))
        cols = st.columns(nc)
        for i, sym in enumerate(opts[:nc]):
            with cols[i]:
                if st.button(sym, key=f"c_{sym}", use_container_width=True):
                    go_to_detail(sym)
                    st.rerun()
        if len(opts) > 10:
            cs, cg = st.columns([3,1])
            with cs:
                ch = st.selectbox("Все пары", [""] + opts, key="sp")
            with cg:
                st.markdown("<br>", unsafe_allow_html=True)
                if ch and st.button("➡️", type="primary", key="od"):
                    go_to_detail(ch)
                    st.rerun()

        if not scan_df.empty:
            st.dataframe(scan_df, hide_index=True, use_container_width=True,
                         height=min(len(scan_df)*38+40, 800))
            st.download_button("📥 CSV", data=make_csv(scan_df),
                               file_name=f"scan_{datetime.now().strftime('%H%M')}.csv",
                               mime="text/csv")

# ═══════════════════════════════════════════════════
# СТРАНИЦА 2 — ДЕТАЛЬНЫЙ РАЗБОР
# ═══════════════════════════════════════════════════
elif page == PAGES[1]:
    results = st.session_state.scan_results
    sym_list = [r.symbol for r in results] if results else []
    ca, cb = st.columns([2,1])
    with ca:
        idx = 0
        ds = st.session_state.detail_symbol
        if ds and ds in sym_list:
            idx = sym_list.index(ds) + 1
        target = st.selectbox("Пара", [""] + sym_list, index=idx, key="dsel")
    with cb:
        manual = st.text_input("Вручную", placeholder="XYZUSDT")
    symbol = manual.strip().upper() if manual.strip() else target
    if not symbol:
        st.info("Выбери пару")
        st.stop()
    st.session_state.detail_symbol = symbol
    client = st.session_state.client

    with st.spinner(f"Загрузка {symbol}..."):
        try:
            book_raw = client.get_order_book(symbol, 500)
            ticker_raw = client.get_ticker_24h(symbol)
            trades_raw = client.get_recent_trades(symbol, 1000)
            kl_1m = client.get_klines(symbol, "1m", 100)
            kl_5m = client.get_klines(symbol, "5m", 100)
            kl_1h = client.get_klines(symbol, "60m", 100)
            kl_4h = client.get_klines(symbol, "4h", 100)
        except Exception as e:
            st.error(f"Ошибка: {e}")
            st.stop()

    if not book_raw or not book_raw.get("bids") or not book_raw.get("asks"):
        st.error(f"Нет стакана для {symbol}")
        st.stop()

    bids = parse_book(book_raw["bids"])
    asks = parse_book(book_raw["asks"])
    if not bids or not asks:
        st.error(f"Пустой стакан {symbol}")
        st.stop()

    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    mid = (best_bid + best_ask) / 2.0
    spread = (best_ask - best_bid) / best_bid * 100.0
    bdepth = sum(float(p)*float(q) for p, q in bids)
    adepth = sum(float(p)*float(q) for p, q in asks)

    td = ticker_raw
    if isinstance(td, list):
        td = td[0] if td else {}
    if not isinstance(td, dict):
        td = {}
    tc24 = extract_trade_count(td)
    vol24 = sf(td.get("quoteVolume", 0))

    df_1m = parse_klines(kl_1m)
    df_5m = parse_klines(kl_5m)
    df_1h = parse_klines(kl_1h)
    df_4h = parse_klines(kl_4h)

    h1, h2 = st.columns([3,1])
    with h1:
        st.markdown(f"## {symbol}")
    with h2:
        st.markdown(f"[🔗 MEXC]({mexc_link(symbol)})")

    m1,m2,m3,m4,m5,m6 = st.columns(6)
    m1.metric("Mid", f"{mid:.8g}")
    m2.metric("Спред", f"{spread:.2f}%")
    m3.metric("Bid $", f"${bdepth:,.0f}")
    m4.metric("Ask $", f"${adepth:,.0f}")
    m5.metric("Сделок 24ч", f"{tc24:,}" if tc24 else "—")
    m6.metric("Объём 24ч", f"${vol24:,.0f}")

    # Объёмы и сделки
    st.markdown("#### 📊 Объёмы и сделки")
    s5 = kline_stats(df_5m, 1)
    s15 = kline_stats(df_5m, 3)
    s60 = kline_stats(df_5m, 12)
    s4h = kline_stats(df_1h, 4)
    vc = st.columns(5)
    vc[0].metric("5м", f"${s5['volume']:,.0f}", f"{s5['trades']} сд.")
    vc[1].metric("15м", f"${s15['volume']:,.0f}", f"{s15['trades']} сд.")
    vc[2].metric("1ч", f"${s60['volume']:,.0f}", f"{s60['trades']} сд.")
    vc[3].metric("4ч", f"${s4h['volume']:,.0f}", f"{s4h['trades']} сд.")
    vc[4].metric("24ч", f"${vol24:,.0f}", f"{tc24:,} сд.")

    if trades_raw and isinstance(trades_raw, list) and len(trades_raw) > 2:
        times = [sf(t.get("time",0)) for t in trades_raw if sf(t.get("time",0)) > 0]
        if len(times) >= 3:
            deltas = [abs(times[i]-times[i+1])/1000 for i in range(len(times)-1)]
            deltas = [d for d in deltas if d >= 0]
            if deltas:
                avg = sum(deltas)/len(deltas)
                robot = " 🤖 **Робот!**" if avg < 30 and max(deltas) < 120 else ""
                st.caption(f"Интервалы: ср.={avg:.1f}с мин={min(deltas):.1f}с макс={max(deltas):.1f}с{robot}")

    # Графики
    st.markdown("#### 📈 Графики")
    t1, t2, t3, t4 = st.tabs(["1 минута", "5 минут", "1 час", "4 часа"])
    with t1:
        f = build_candlestick(df_1m, symbol, "1m", mid)
        if f: st.plotly_chart(f, use_container_width=True)
        else: st.warning("Нет данных 1m")
    with t2:
        f = build_candlestick(df_5m, symbol, "5m", mid)
        if f: st.plotly_chart(f, use_container_width=True)
        else: st.warning("Нет данных 5m")
    with t3:
        f = build_candlestick(df_1h, symbol, "1h", mid)
        if f: st.plotly_chart(f, use_container_width=True)
        else: st.warning("Нет данных 1h")
    with t4:
        f = build_candlestick(df_4h, symbol, "4h", mid)
        if f: st.plotly_chart(f, use_container_width=True)
        else: st.warning("Нет данных 4h")

    # Стакан
    st.markdown("#### 📖 Стакан")
    dv = st.select_slider("Глубина", [20,30,50,100], value=50, key="obd")
    fg = build_orderbook_chart(bids, asks, mid, dv)
    if fg:
        st.plotly_chart(fg, use_container_width=True)

    # Хитмап
    fh = build_heatmap(bids, asks, mid, 30)
    if fh:
        st.plotly_chart(fh, use_container_width=True)

    # Сделки
    trades_df = pd.DataFrame()
    if trades_raw and isinstance(trades_raw, list):
        st.markdown("#### 📋 Сделки")
        trs = []
        for t in trades_raw[:50]:
            try:
                p, q, ts = sf(t.get("price",0)), sf(t.get("qty",0)), sf(t.get("time",0))
                trs.append({"Время": pd.to_datetime(ts, unit="ms").strftime("%H:%M:%S") if ts > 0 else "—",
                            "Цена": p, "Кол-во": q, "USDT": round(p*q,2),
                            "Сторона": "🟢 BUY" if not t.get("isBuyerMaker") else "🔴 SELL"})
            except Exception:
                continue
        if trs:
            trades_df = pd.DataFrame(trs)
            st.dataframe(trades_df, hide_index=True, use_container_width=True)

    # Экспорт
    st.markdown("---")
    export = {}
    ob_rows = [{"Сторона": s, "Цена": float(p), "Кол-во": float(q), "USDT": round(float(p*q),4)}
               for s, data in [("BID", bids), ("ASK", asks)] for p, q in data]
    ob_df = pd.DataFrame(ob_rows)
    export["orderbook"] = ob_df
    if not trades_df.empty:
        export["trades"] = trades_df
    for lbl, kdf in [("1m", df_1m), ("5m", df_5m), ("1h", df_1h), ("4h", df_4h)]:
        if kdf is not None and not kdf.empty:
            export[f"klines_{lbl}"] = kdf

    e1, e2 = st.columns(2)
    with e1:
        st.download_button("📥 Стакан", data=make_csv(ob_df),
                           file_name=f"{symbol}_book.csv", mime="text/csv")
    with e2:
        if not trades_df.empty:
            st.download_button("📥 Сделки", data=make_csv(trades_df),
                               file_name=f"{symbol}_trades.csv", mime="text/csv")

    def sym_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for n, d in export.items():
                zf.writestr(f"{symbol}_{n}.csv", d.to_csv(index=False))
            zf.writestr(f"{symbol}_meta.csv",
                        f"symbol,{symbol}\nmid,{mid}\nspread,{spread:.4f}\n"
                        f"bid_depth,{bdepth:.2f}\nask_depth,{adepth:.2f}\n"
                        f"trades_24h,{tc24}\nvolume_24h,{vol24:.2f}\n"
                        f"ts,{datetime.now().isoformat()}\n")
        buf.seek(0)
        return buf.getvalue()

    st.download_button(f"📦 Всё по {symbol} (ZIP)", data=sym_zip(),
                       file_name=f"{symbol}_{datetime.now().strftime('%H%M')}.zip",
                       mime="application/zip", use_container_width=True)

# ═══════════════════════════════════════════════════
# СТРАНИЦА 3 — ПЕРЕСТАВКИ
# ═══════════════════════════════════════════════════
elif page == PAGES[2]:
    tracker = st.session_state.tracker
    st.markdown("**Переставляш** — плотность, двигающаяся по стакану. Включи авто-обновление.")
    movers = tracker.get_active_movers(7200)
    if not movers:
        st.info("Переставок нет. Запусти несколько сканов.")
    else:
        st.success(f"⚡ {len(movers)} переставок за 2ч")
        mr = [{"Время": datetime.fromtimestamp(e.timestamp).strftime("%H:%M:%S"),
               "↕": "⬆️" if e.direction == "UP" else "⬇️",
               "Пара": e.symbol, "Сторона": e.side,
               "$ ": round(e.size_usdt),
               "Было": f"{e.old_price:.8g}", "Стало": f"{e.new_price:.8g}",
               "Сдвиг %": round(e.shift_pct, 3)} for e in reversed(movers)]
        mdf = pd.DataFrame(mr)
        st.dataframe(mdf, hide_index=True, use_container_width=True)
        us = sorted({e.symbol for e in movers})
        cp, cg = st.columns([3,1])
        with cp:
            cm = st.selectbox("Пара → разбор", [""] + us, key="mp")
        with cg:
            st.markdown("<br>", unsafe_allow_html=True)
            if cm and st.button("➡️", key="mg"):
                go_to_detail(cm)
                st.rerun()
        st.download_button("📥 CSV", data=make_csv(mdf),
                           file_name=f"movers_{datetime.now().strftime('%H%M')}.csv",
                           mime="text/csv")
    tm = tracker.get_top_movers(15)
    if tm:
        st.markdown("### 🏆 Топ")
        fig = go.Figure(go.Bar(x=[x[0] for x in tm], y=[x[1] for x in tm],
                               marker_color="#00d2ff"))
        fig.update_layout(template="plotly_dark", height=300)
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
st.caption("MEXC Density Scanner v2.5")
