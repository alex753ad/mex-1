"""
═══════════════════════════════════════════════════════════
  MEXC Density Scanner v3.0
═══════════════════════════════════════════════════════════
"""
import io, time, zipfile, math
from datetime import datetime
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from mexc_client import MexcClientSync
from analyzer import analyze_order_book
from history import DensityTracker

st.set_page_config(page_title="MEXC Scanner", page_icon="🔍",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown("""<style>
.block-container{padding-top:.3rem}
.stMetric>div{background:#0d1117;padding:.6rem;border-radius:8px;border:1px solid #1e2d3d}
div[data-testid="stMetricValue"]{font-size:1.3rem}
div[data-testid="stMetricDelta"]{font-size:.8rem}
button[kind="primary"]{background:#00e676!important}
</style>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════
# Утилиты
# ═══════════════════════════════════════════════════
def sf(v, d=0.0):
    if v is None or v == "": return d
    try: return float(v)
    except: return d

def si(v, d=0):
    try: return int(sf(v, d))
    except: return d

def parse_book(raw):
    out = []
    if not raw or not isinstance(raw, list): return out
    for e in raw:
        if not isinstance(e, (list, tuple)) or len(e) < 2: continue
        p, q = sf(e[0]), sf(e[1])
        if p > 0 and q > 0: out.append((p, q))
    return out

def extract_tc(td):
    if isinstance(td, list): td = td[0] if td else {}
    if not isinstance(td, dict): return 0
    for k in ("count","tradeCount","trades","txcnt"):
        v = td.get(k)
        if v is None or v=="" or v==0 or v=="0": continue
        r = si(v)
        if r > 0: return r
    return 0

def parse_klines(raw):
    if not raw or not isinstance(raw, list): return pd.DataFrame()
    rows = []
    for k in raw:
        if not isinstance(k,(list,tuple)) or len(k)<6: continue
        rows.append({"open_time":sf(k[0]),"open":sf(k[1]),"high":sf(k[2]),
                      "low":sf(k[3]),"close":sf(k[4]),"volume":sf(k[5]),
                      "close_time":sf(k[6]) if len(k)>6 else 0,
                      "quote_volume":sf(k[7]) if len(k)>7 else 0,
                      "trades":si(k[8]) if len(k)>8 else 0})
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df

# ─── Формат цен ───
def fmt_price(price):
    """Формат: 0.00055 → 5.5·10⁻⁴, нормальные → как есть"""
    if price <= 0: return "0"
    if price >= 0.01:
        if price >= 1000: return f"{price:,.0f}"
        if price >= 1: return f"{price:.2f}"
        return f"{price:.4f}"
    # Мелкие: научная нотация
    exp = int(math.floor(math.log10(abs(price))))
    mantissa = price / (10 ** exp)
    sup = str(exp).replace("-","⁻").replace("0","⁰").replace("1","¹") \
          .replace("2","²").replace("3","³").replace("4","⁴") \
          .replace("5","⁵").replace("6","⁶").replace("7","⁷") \
          .replace("8","⁸").replace("9","⁹")
    return f"{mantissa:.2f}·10{sup}"

def fmt_price_full(price):
    """Полный формат без научной нотации (для экспорта)"""
    if price <= 0: return "0"
    d = max(2, -int(math.floor(math.log10(abs(price))))+2) if price > 0 else 8
    return f"{price:.{d}f}"

def plotly_tickfmt(price):
    if price <= 0: return ".8f"
    d = max(2, -int(math.floor(math.log10(abs(price))))+2)
    return f".{d}f"

def mexc_link(s): return f"https://www.mexc.com/exchange/{s.replace('USDT','_USDT')}"
def make_csv(df): return df.to_csv(index=False).encode("utf-8-sig")

# ═══════════════════════════════════════════════════
# Session State
# ═══════════════════════════════════════════════════
defaults = {"tracker": DensityTracker(), "scan_results": [], "scan_df": pd.DataFrame(),
            "last_scan": 0.0, "total_pairs": 0, "client": MexcClientSync(),
            "detail_symbol": "", "target_page": 0, "favorites": set()}
for k, v in defaults.items():
    if k not in st.session_state: st.session_state[k] = v

# ═══════════════════════════════════════════════════
# ГРАФИКИ
# ═══════════════════════════════════════════════════

def build_candlestick(df, symbol, interval, cur_price=None):
    if df is None or df.empty or len(df)<2: return None
    try:
        med = float(df["close"].median())
        tfmt = plotly_tickfmt(med)
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            vertical_spacing=0.03, row_heights=[0.75,0.25],
                            specs=[[{"secondary_y":True}],[{"secondary_y":False}]])
        # Свечи — МАКСИМАЛЬНО яркие цвета
        fig.add_trace(go.Candlestick(
            x=df["time"], open=df["open"], high=df["high"],
            low=df["low"], close=df["close"],
            increasing_line_color="#00FF7F",   # SpringGreen
            increasing_fillcolor="#00FF7F",
            decreasing_line_color="#FF3366",   # Ярко-розовый
            decreasing_fillcolor="#FF3366",
            name="Цена"), row=1, col=1, secondary_y=False)
        colors = ["#00FF7F" if c>=o else "#FF3366" for c,o in zip(df["close"],df["open"])]
        fig.add_trace(go.Bar(x=df["time"], y=df["volume"],
                             marker_color=colors, opacity=0.7,
                             name="Объём"), row=2, col=1)
        # % шкала
        ref = float(cur_price) if cur_price and cur_price>0 else float(df["close"].iloc[-1])
        if ref > 0:
            hi, lo = float(df["high"].max()), float(df["low"].min())
            fig.add_trace(go.Scatter(
                x=[df["time"].iloc[0], df["time"].iloc[-1]],
                y=[(hi-ref)/ref*100, (lo-ref)/ref*100],
                mode="markers", marker=dict(size=0, opacity=0),
                showlegend=False, hoverinfo="skip"),
                row=1, col=1, secondary_y=True)
            fig.update_yaxes(title_text="%", ticksuffix="%", showgrid=False,
                             zeroline=True, zerolinecolor="rgba(0,210,255,0.5)",
                             row=1, col=1, secondary_y=True)
        if cur_price and cur_price>0:
            fig.add_hline(y=float(cur_price), line_dash="dot",
                          line_color="#00BFFF", line_width=1.5,
                          annotation_text=f"  {fmt_price(float(cur_price))}",
                          annotation_font_color="#00BFFF", row=1, col=1)
        fig.update_yaxes(tickformat=tfmt, exponentformat="none",
                         row=1, col=1, secondary_y=False)
        fig.update_layout(title=f"{symbol} — {interval}",
                          template="plotly_dark", height=420,
                          xaxis_rangeslider_visible=False, showlegend=False,
                          margin=dict(l=60,r=60,t=35,b=15),
                          plot_bgcolor="#0a0e14")
        return fig
    except Exception as e:
        st.caption(f"Ошибка: {e}"); return None


def build_orderbook_chart(bids, asks, cur_price, depth=50):
    """Стакан — подписи только у крупных уровней"""
    try:
        b, a = bids[:depth], asks[:depth]
        if not b and not a: return None
        levels = []
        for p,q in b: levels.append(("BID", float(p), float(p*q)))
        for p,q in a: levels.append(("ASK", float(p), float(p*q)))
        levels.sort(key=lambda x: x[1])
        prices = [x[1] for x in levels]
        vols = [x[2] for x in levels]
        bar_colors = ["#00FF7F" if x[0]=="BID" else "#FF3366" for x in levels]
        # Подписи только у топ-10 по объёму
        vol_threshold = sorted(vols, reverse=True)[min(9, len(vols)-1)] if vols else 0
        texts = [f"${v:,.0f}" if v >= vol_threshold else "" for v in vols]
        # Метки Y — только каждый N-й + крупные уровни
        step = max(1, len(prices)//20)
        tick_vals, tick_texts = [], []
        big_prices = {levels[i][1] for i,v in enumerate(vols) if v >= vol_threshold}
        for i, p in enumerate(prices):
            if i % step == 0 or p in big_prices:
                tick_vals.append(p)
                tick_texts.append(fmt_price(p))

        fig = go.Figure(go.Bar(
            y=prices, x=vols, orientation="h",
            marker_color=bar_colors, opacity=0.9,
            text=texts, textposition="auto",
            textfont=dict(size=10, color="white"),
            hovertext=[f"{'BID' if x[0]=='BID' else 'ASK'} {fmt_price(x[1])}: ${x[2]:,.0f}"
                       for x in levels],
            hoverinfo="text"))
        if cur_price and float(cur_price) > 0:
            mx = max(vols) if vols else 1
            fig.add_trace(go.Scatter(
                x=[0, mx*1.2], y=[float(cur_price)]*2,
                mode="lines+text",
                text=["", f" {fmt_price(float(cur_price))}"],
                textposition="middle right",
                textfont=dict(color="#00BFFF", size=12),
                line=dict(color="#00BFFF", width=2.5, dash="dot"),
                showlegend=False))
        fig.update_layout(title="📖 Стакан", template="plotly_dark",
                          height=max(450, depth*10),
                          xaxis_title="Объём ($)",
                          yaxis=dict(title="Цена", tickmode="array",
                                     tickvals=tick_vals, ticktext=tick_texts,
                                     exponentformat="none"),
                          margin=dict(l=90,r=20,t=35,b=25),
                          plot_bgcolor="#0a0e14")
        return fig
    except Exception as e:
        st.error(f"Стакан: {e}"); return None


def build_heatmap(bids, asks, cur_price, depth=30):
    """Хитмап — подписи только у крупных уровней"""
    try:
        levels = []
        for p,q in bids[:depth]: levels.append(("BID", float(p), float(p*q)))
        for p,q in asks[:depth]: levels.append(("ASK", float(p), float(p*q)))
        if not levels: return None
        levels.sort(key=lambda x: x[1])
        mx = max(v for _,_,v in levels) or 1.0
        prices, vols, colors, hovers = [], [], [], []
        for side, price, vol in levels:
            i = min(vol/mx, 1.0)
            prices.append(price); vols.append(vol)
            if side=="BID":
                colors.append(f"rgba({int(0)},{int(180+75*i)},{int(80+40*i)},1)")
            else:
                colors.append(f"rgba({int(200+55*i)},{int(50*(1-i))},{int(60*(1-i))},1)")
            hovers.append(f"{side} {fmt_price(price)}: ${vol:,.0f}")
        vol_threshold = sorted(vols, reverse=True)[min(7,len(vols)-1)] if vols else 0
        texts = [f"${v:,.0f}" if v>=vol_threshold else "" for v in vols]
        step = max(1, len(prices)//15)
        big_prices = {prices[i] for i,v in enumerate(vols) if v>=vol_threshold}
        tv, tt = [], []
        for i, p in enumerate(prices):
            if i%step==0 or p in big_prices:
                tv.append(p); tt.append(fmt_price(p))
        fig = go.Figure(go.Bar(
            y=prices, x=vols, orientation="h",
            marker_color=colors,
            text=texts, textposition="auto",
            textfont=dict(size=9, color="white"),
            hovertext=hovers, hoverinfo="text", showlegend=False))
        if cur_price and float(cur_price)>0:
            mx_x = max(vols) if vols else 1
            fig.add_trace(go.Scatter(
                x=[0, mx_x*1.2], y=[float(cur_price)]*2,
                mode="lines+text",
                text=["", f" {fmt_price(float(cur_price))}"],
                textposition="middle right",
                textfont=dict(color="#00BFFF", size=12),
                line=dict(color="#00BFFF", width=2.5, dash="dot"),
                showlegend=False))
        fig.update_layout(title="🔥 Хитмап", template="plotly_dark", height=450,
                          xaxis_title="Объём ($)",
                          yaxis=dict(title="Цена", tickmode="array",
                                     tickvals=tv, ticktext=tt,
                                     exponentformat="none"),
                          margin=dict(l=90,r=20,t=35,b=25),
                          plot_bgcolor="#0a0e14")
        return fig
    except Exception as e:
        st.error(f"Хитмап: {e}"); return None


def kline_stats(df, last_n=None):
    if df is None or df.empty: return {"volume":0.0,"trades":0}
    sub = df.tail(last_n) if last_n else df
    return {"volume": float(sub["quote_volume"].sum()) if "quote_volume" in sub else 0.0,
            "trades": int(sub["trades"].sum()) if "trades" in sub else 0}


# ═══════════════════════════════════════════════════
# Сканирование — УСКОРЕННОЕ
# ═══════════════════════════════════════════════════
def run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n):
    import config as cfg
    cfg.MIN_DAILY_VOLUME_USDT = min_vol; cfg.MAX_DAILY_VOLUME_USDT = max_vol
    cfg.MIN_SPREAD_PCT = min_spread; cfg.WALL_MULTIPLIER = wall_mult
    cfg.MIN_WALL_SIZE_USDT = min_wall_usd
    client = st.session_state.client
    progress = st.progress(0, "Пары...")
    try: info = client.get_exchange_info()
    except Exception as e: st.error(f"API: {e}"); return
    if not info or "symbols" not in info:
        st.error(f"❌ {client.last_error or 'Нет ответа'}"); progress.empty(); return
    all_sym = []
    for s in info["symbols"]:
        try:
            if s.get("quoteAsset")!="USDT": continue
            st_ = s.get("status","")
            if (str(st_) in ("1","ENABLED","True","true") or st_ is True or st_==1) \
               and s.get("isSpotTradingAllowed", True):
                all_sym.append(s["symbol"])
        except: continue
    if not all_sym:
        for s in info["symbols"]:
            try:
                if s.get("quoteAsset")=="USDT": all_sym.append(s["symbol"])
            except: continue
    if not all_sym: st.error("Нет пар"); progress.empty(); return
    progress.progress(5, f"{len(all_sym)} пар")
    try: tickers = client.get_all_tickers_24h()
    except: st.error("Тикеры"); progress.empty(); return
    if not tickers: st.error(f"Тикеры: {client.last_error}"); progress.empty(); return
    tm = {t["symbol"]:t for t in tickers if "symbol" in t}
    cands = [(sym,tm[sym]) for sym in all_sym
             if sym in tm and min_vol<=sf(tm[sym].get("quoteVolume",0))<=max_vol]
    cands.sort(key=lambda x: sf(x[1].get("quoteVolume",0)), reverse=True)
    if not cands: st.warning("0 пар в диапазоне"); progress.empty(); return
    progress.progress(15, f"Скан {len(cands)} пар...")
    results, errors, total = [], 0, len(cands)
    for i, (sym, tk) in enumerate(cands):
        try:
            book = client.get_order_book(sym, cfg.ORDER_BOOK_DEPTH)
            if book:
                r = analyze_order_book(sym, book, tk)
                if r and r.spread_pct >= min_spread:
                    r.trade_count_24h = extract_tc(tk)
                    results.append(r)
        except: errors += 1
        if (i+1)%8==0 or i==total-1:
            progress.progress(15+int((i+1)/total*80), f"{i+1}/{total}|{len(results)}")
    results.sort(key=lambda r: r.score, reverse=True)
    top = results[:top_n]
    # Индивидуальные тикеры для trade count
    progress.progress(96, "Сделки...")
    for r in top[:15]:
        if r.trade_count_24h==0:
            try:
                tc = extract_tc(client.get_ticker_24h(r.symbol))
                if tc>0: r.trade_count_24h = tc
            except: pass
    st.session_state.tracker.update(top)
    rows = []
    for r in top:
        if not r.all_walls: continue
        bs = " | ".join(f"${w.size_usdt:,.0f}({w.multiplier}x)" for w in r.bid_walls[:3]) or "—"
        aks = " | ".join(f"${w.size_usdt:,.0f}({w.multiplier}x)" for w in r.ask_walls[:3]) or "—"
        rows.append({"Скор":r.score,"Пара":r.symbol,"Спред %":round(r.spread_pct,2),
                     "Объём $":round(r.volume_24h_usdt),"Сделок":r.trade_count_24h,
                     "BID":bs,"ASK":aks,"B/A":f"{len(r.bid_walls)}/{len(r.ask_walls)}",
                     "⚡":"⚡" if r.has_movers else ""})
    st.session_state.scan_results = top
    st.session_state.scan_df = pd.DataFrame(rows) if rows else pd.DataFrame()
    st.session_state.last_scan = time.time()
    st.session_state.total_pairs = total
    progress.progress(100,"✓"); time.sleep(0.2); progress.empty()


# ═══════════════════════════════════════════════════
PAGES = ["📊 Поиск","🔍 Детали","⭐ Избранное","📈 Переставки"]

def go_to_detail(sym):
    st.session_state.detail_symbol = sym
    st.session_state.target_page = 1

# ═══════════════════════════════════════════════════
# Сайдбар
# ═══════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ⚙️ Параметры")
    min_vol = st.number_input("Мин $", value=100, min_value=0, step=100)
    max_vol = st.number_input("Макс $", value=500_000, min_value=100, step=10000)
    min_spread = st.slider("Спред %", 0.0, 20.0, 0.5, 0.1)
    wall_mult = st.slider("Множитель x", 2, 50, 5)
    min_wall_usd = st.number_input("Стенка $", value=50, min_value=1, step=10)
    top_n = st.slider("Топ N", 5, 100, 30)
    st.markdown("---")
    # Автоскан — ВКЛ по умолчанию, 30 сек
    auto_on = st.checkbox("🔄 Авто-скан", value=True)
    auto_sec = st.select_slider("Интервал (с)", [15,20,30,45,60,90,120], value=30)
    if auto_on:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=auto_sec*1000, key="ar")
        except ImportError:
            st.caption("pip install streamlit-autorefresh")
    scan_btn = st.button("🚀 Скан", use_container_width=True, type="primary")
    st.markdown("---")
    # Избранное — быстрый просмотр
    fav = st.session_state.favorites
    if fav:
        st.markdown(f"⭐ **Избранное ({len(fav)})**")
        st.caption(", ".join(sorted(fav)))
    st.markdown("---")
    stats = st.session_state.tracker.get_stats()
    st.caption(f"Сканов: {stats['total_scans']} | Переставок: {stats['total_mover_events']}")
    if st.button("🔧 API", use_container_width=True):
        c = st.session_state.client
        ok, msg = c.ping()
        st.success(f"✅ {msg}") if ok else st.error(f"❌ {msg}")

# ═══════════════════════════════════════════════════
if scan_btn or (auto_on and time.time()-st.session_state.last_scan > max(auto_sec-5, 10)):
    run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n)

_idx = max(0, min(st.session_state.target_page, len(PAGES)-1))
page = st.radio("nav", PAGES, horizontal=True, index=_idx, label_visibility="collapsed")
for i,p in enumerate(PAGES):
    if page==p: st.session_state.target_page = i
st.markdown("---")


# ═══════════════════════════════════════════════════
# СТРАНИЦА 1 — ПОИСК
# ═══════════════════════════════════════════════════
if page == PAGES[0]:
    results = st.session_state.scan_results
    sdf = st.session_state.scan_df
    if not results:
        st.info("Авто-скан запустится через несколько секунд...")
    else:
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Найдено", len(results))
        c2.metric("Проверено", st.session_state.total_pairs)
        c3.metric("⭐ Лучший", f"{results[0].score}")
        c4.metric("⚡ Переставки", sum(1 for r in results if r.has_movers))

        # Таблица с кнопками
        if not sdf.empty:
            for i, row in sdf.iterrows():
                sym = row["Пара"]
                with st.container():
                    cols = st.columns([0.8, 2, 1, 1.5, 1, 3, 3, 0.8, 0.8])
                    cols[0].markdown(f"**{row['Скор']}**")
                    # Клик по паре → детали
                    if cols[1].button(sym, key=f"go_{sym}", use_container_width=True):
                        go_to_detail(sym); st.rerun()
                    cols[2].markdown(f"`{row['Спред %']}%`")
                    cols[3].markdown(f"${row['Объём $']:,}")
                    cols[4].markdown(f"{row['Сделок']}")
                    cols[5].markdown(f"🟢 {row['BID'][:40]}")
                    cols[6].markdown(f"🔴 {row['ASK'][:40]}")
                    cols[7].markdown(row.get("⚡",""))
                    # ⭐ добавить в избранное
                    fav_key = f"fav_{sym}"
                    is_fav = sym in st.session_state.favorites
                    if cols[8].button("⭐" if is_fav else "☆", key=fav_key):
                        if is_fav: st.session_state.favorites.discard(sym)
                        else: st.session_state.favorites.add(sym)
                        st.rerun()

            st.markdown("---")
            e1, e2 = st.columns(2)
            with e1:
                st.download_button("📥 CSV", data=make_csv(sdf),
                                   file_name=f"scan_{datetime.now().strftime('%H%M')}.csv",
                                   mime="text/csv")
            with e2:
                def full_zip():
                    buf = io.BytesIO()
                    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
                        zf.writestr("scan.csv", sdf.to_csv(index=False))
                    buf.seek(0); return buf.getvalue()
                st.download_button("📦 ZIP", data=full_zip(),
                                   file_name=f"mexc_{datetime.now().strftime('%H%M')}.zip",
                                   mime="application/zip")


# ═══════════════════════════════════════════════════
# СТРАНИЦА 2 — ДЕТАЛИ
# ═══════════════════════════════════════════════════
elif page == PAGES[1]:
    results = st.session_state.scan_results
    sym_list = [r.symbol for r in results] if results else []
    # Навигация
    hdr = st.columns([1, 3, 2, 1])
    with hdr[0]:
        if st.button("← Назад", key="back"):
            st.session_state.target_page = 0; st.rerun()
    with hdr[1]:
        idx = 0
        ds = st.session_state.detail_symbol
        if ds and ds in sym_list: idx = sym_list.index(ds)+1
        target = st.selectbox("Пара", [""]+sym_list, index=idx, key="dsel",
                              label_visibility="collapsed")
    with hdr[2]:
        manual = st.text_input("Ввод", placeholder="XYZUSDT", label_visibility="collapsed")
    with hdr[3]:
        sym_now = manual.strip().upper() if manual.strip() else target
        if sym_now:
            is_fav = sym_now in st.session_state.favorites
            if st.button("⭐" if is_fav else "☆ Избр.", key="fav_detail"):
                if is_fav: st.session_state.favorites.discard(sym_now)
                else: st.session_state.favorites.add(sym_now)
                st.rerun()

    symbol = manual.strip().upper() if manual.strip() else target
    if not symbol: st.info("Выбери пару или нажми ← Назад"); st.stop()
    st.session_state.detail_symbol = symbol
    client = st.session_state.client

    with st.spinner(f"{symbol}..."):
        try:
            book_raw = client.get_order_book(symbol, 500)
            ticker_raw = client.get_ticker_24h(symbol)
            trades_raw = client.get_recent_trades(symbol, 1000)
            kl_1m = client.get_klines(symbol, "1m", 100)
            kl_5m = client.get_klines(symbol, "5m", 100)
            kl_1h = client.get_klines(symbol, "60m", 100)
            kl_4h = client.get_klines(symbol, "4h", 100)
            kl_1d = client.get_klines(symbol, "1d", 100)
        except Exception as e: st.error(str(e)); st.stop()

    if not book_raw or not book_raw.get("bids") or not book_raw.get("asks"):
        st.error(f"Нет данных {symbol}"); st.stop()
    bids = parse_book(book_raw["bids"])
    asks = parse_book(book_raw["asks"])
    if not bids or not asks: st.error("Пусто"); st.stop()
    bb, ba = float(bids[0][0]), float(asks[0][0])
    mid = (bb+ba)/2; spread = (ba-bb)/bb*100
    bdepth = sum(float(p)*float(q) for p,q in bids)
    adepth = sum(float(p)*float(q) for p,q in asks)
    td = ticker_raw
    if isinstance(td, list): td = td[0] if td else {}
    if not isinstance(td, dict): td = {}
    tc24 = extract_tc(td); vol24 = sf(td.get("quoteVolume",0))
    df_1m,df_5m,df_1h,df_4h,df_1d = [parse_klines(x) for x in [kl_1m,kl_5m,kl_1h,kl_4h,kl_1d]]

    # Заголовок
    st.markdown(f"### {symbol}  ·  {fmt_price(mid)}  ·  [MEXC]({mexc_link(symbol)})")
    m1,m2,m3,m4,m5,m6 = st.columns(6)
    m1.metric("Спред", f"{spread:.2f}%")
    m2.metric("Bid $", f"${bdepth:,.0f}")
    m3.metric("Ask $", f"${adepth:,.0f}")
    m4.metric("Сделок", f"{tc24:,}" if tc24 else "—")
    m5.metric("Объём 24ч", f"${vol24:,.0f}")
    s4h = kline_stats(df_1h, 4)
    m6.metric("Объём 4ч", f"${s4h['volume']:,.0f}")

    # Объёмы
    st.markdown("#### 📊 Объёмы / сделки")
    s5,s15,s60 = kline_stats(df_5m,1), kline_stats(df_5m,3), kline_stats(df_5m,12)
    vc = st.columns(5)
    vc[0].metric("5м", f"${s5['volume']:,.0f}", f"{s5['trades']} сд.")
    vc[1].metric("15м", f"${s15['volume']:,.0f}", f"{s15['trades']} сд.")
    vc[2].metric("1ч", f"${s60['volume']:,.0f}", f"{s60['trades']} сд.")
    vc[3].metric("4ч", f"${s4h['volume']:,.0f}", f"{s4h['trades']} сд.")
    vc[4].metric("24ч", f"${vol24:,.0f}", f"{tc24:,} сд.")

    # Робот-детекция
    if trades_raw and isinstance(trades_raw, list) and len(trades_raw)>2:
        times = [sf(t.get("time",0)) for t in trades_raw if sf(t.get("time",0))>0]
        if len(times)>=3:
            deltas = [abs(times[i]-times[i+1])/1000 for i in range(len(times)-1)]
            deltas = [d for d in deltas if d>=0]
            if deltas:
                avg = sum(deltas)/len(deltas)
                robot = " 🤖 **Робот!**" if avg<30 and max(deltas)<120 else ""
                st.caption(f"Интервалы: ср.={avg:.1f}с мин={min(deltas):.1f}с макс={max(deltas):.1f}с{robot}")

    # 5 графиков
    st.markdown("#### 📈 Графики")
    tabs = st.tabs(["1м","5м","1ч","4ч","1д"])
    for tab, df_k, lbl in zip(tabs, [df_1m,df_5m,df_1h,df_4h,df_1d],
                               ["1m","5m","1h","4h","1d"]):
        with tab:
            f = build_candlestick(df_k, symbol, lbl, mid)
            if f: st.plotly_chart(f, use_container_width=True)
            else: st.warning(f"Нет {lbl}")

    # Стакан + Хитмап
    st.markdown("#### 📖 Стакан / Хитмап")
    dv = st.select_slider("Глубина", [20,30,50,100], value=50, key="obd")
    col_ob, col_hm = st.columns(2)
    with col_ob:
        fg = build_orderbook_chart(bids, asks, mid, dv)
        if fg: st.plotly_chart(fg, use_container_width=True)
    with col_hm:
        fh = build_heatmap(bids, asks, mid, 30)
        if fh: st.plotly_chart(fh, use_container_width=True)

    # Сделки
    trades_df = pd.DataFrame()
    if trades_raw and isinstance(trades_raw, list):
        st.markdown("#### 📋 Сделки")
        trs = []
        for t in trades_raw[:50]:
            try:
                p,q,ts = sf(t.get("price",0)), sf(t.get("qty",0)), sf(t.get("time",0))
                trs.append({"Время": pd.to_datetime(ts,unit="ms").strftime("%H:%M:%S") if ts>0 else "—",
                            "Цена": fmt_price(p), "Кол-во": q, "$": round(p*q,2),
                            "": "🟢" if not t.get("isBuyerMaker") else "🔴"})
            except: continue
        if trs:
            trades_df = pd.DataFrame(trs)
            st.dataframe(trades_df, hide_index=True, use_container_width=True, height=300)

    # Экспорт
    st.markdown("---")
    export = {}
    ob_df = pd.DataFrame([{"Сторона":s,"Цена":float(p),"Кол-во":float(q),"$":round(float(p*q),4)}
                           for s,data in [("BID",bids),("ASK",asks)] for p,q in data])
    export["orderbook"] = ob_df
    if not trades_df.empty: export["trades"] = trades_df
    for lbl, kdf in [("1m",df_1m),("5m",df_5m),("1h",df_1h),("4h",df_4h),("1d",df_1d)]:
        if kdf is not None and not kdf.empty: export[f"klines_{lbl}"] = kdf
    def sym_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
            for n,d in export.items(): zf.writestr(f"{symbol}_{n}.csv", d.to_csv(index=False))
        buf.seek(0); return buf.getvalue()
    st.download_button(f"📦 {symbol} ZIP", data=sym_zip(),
                       file_name=f"{symbol}_{datetime.now().strftime('%H%M')}.zip",
                       mime="application/zip", use_container_width=True)


# ═══════════════════════════════════════════════════
# СТРАНИЦА 3 — ИЗБРАННОЕ
# ═══════════════════════════════════════════════════
elif page == PAGES[2]:
    st.markdown("### ⭐ Избранное")
    fav = st.session_state.favorites

    # Импорт CSV
    st.markdown("#### 📥 Импорт")
    uploaded = st.file_uploader("Загрузи CSV со списком монет", type=["csv","txt"],
                                key="fav_import")
    if uploaded:
        try:
            content = uploaded.getvalue().decode("utf-8")
            # Поддержка: одна колонка с тикерами, или поле "Пара"/"Symbol"
            lines = [l.strip().strip('"').strip("'") for l in content.replace(",","\n").split("\n")]
            new_syms = set()
            for l in lines:
                l = l.upper().strip()
                if l and l.endswith("USDT") and len(l) > 4 and l not in ("ПАРА","SYMBOL","PAIR"):
                    new_syms.add(l)
            if new_syms:
                st.session_state.favorites.update(new_syms)
                st.success(f"Добавлено {len(new_syms)} монет: {', '.join(sorted(new_syms)[:10])}")
                st.rerun()
            else:
                st.warning("Не найдено тикеров USDT в файле")
        except Exception as e:
            st.error(f"Ошибка импорта: {e}")

    if not fav:
        st.info("Нажми ☆ рядом с парой на странице Поиска или Деталей чтобы добавить")
    else:
        st.markdown(f"**{len(fav)} монет**")

        # Список с кнопками
        for sym in sorted(fav):
            cols = st.columns([3, 1, 1, 1])
            cols[0].markdown(f"**{sym}**")
            if cols[1].button("🔍", key=f"fv_{sym}"):
                go_to_detail(sym); st.rerun()
            if cols[2].button("❌", key=f"rm_{sym}"):
                st.session_state.favorites.discard(sym)
                st.rerun()
            cols[3].markdown(f"[MEXC]({mexc_link(sym)})")

        st.markdown("---")
        # Экспорт
        st.markdown("#### 📤 Экспорт")
        fav_csv = "Пара\n" + "\n".join(sorted(fav))
        col_e1, col_e2 = st.columns(2)
        with col_e1:
            st.download_button(
                "📥 Скачать CSV",
                data=fav_csv.encode("utf-8"),
                file_name=f"favorites_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv", use_container_width=True)
        with col_e2:
            if st.button("🗑 Очистить всё", type="secondary"):
                st.session_state.favorites = set()
                st.rerun()


# ═══════════════════════════════════════════════════
# СТРАНИЦА 4 — ПЕРЕСТАВКИ
# ═══════════════════════════════════════════════════
elif page == PAGES[3]:
    tracker = st.session_state.tracker
    st.markdown("**Переставляш** — плотность двигающаяся по стакану. Признак робота.")
    movers = tracker.get_active_movers(7200)
    if not movers:
        st.info("Нет данных. Подожди несколько сканов.")
    else:
        st.success(f"⚡ {len(movers)} за 2ч")
        mr = [{"Время":datetime.fromtimestamp(e.timestamp).strftime("%H:%M:%S"),
               "↕":"⬆️" if e.direction=="UP" else "⬇️",
               "Пара":e.symbol,"Сторона":e.side,"$":round(e.size_usdt),
               "Было":fmt_price(e.old_price),"Стало":fmt_price(e.new_price),
               "%":round(e.shift_pct,3)} for e in reversed(movers)]
        mdf = pd.DataFrame(mr)
        st.dataframe(mdf, hide_index=True, use_container_width=True)
        us = sorted({e.symbol for e in movers})
        cp,cg = st.columns([3,1])
        with cp: cm = st.selectbox("→ Детали", [""]+us, key="mp")
        with cg:
            if cm and st.button("➡️", key="mg"):
                go_to_detail(cm); st.rerun()
        st.download_button("📥", data=make_csv(mdf),
                           file_name=f"movers_{datetime.now().strftime('%H%M')}.csv",
                           mime="text/csv")
    tm = tracker.get_top_movers(15)
    if tm:
        fig = go.Figure(go.Bar(x=[x[0] for x in tm], y=[x[1] for x in tm],
                               marker_color="#00BFFF"))
        fig.update_layout(template="plotly_dark", height=250, title="Топ")
        st.plotly_chart(fig, use_container_width=True)

st.caption("MEXC Scanner v3.0")
