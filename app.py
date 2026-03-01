"""
═══════════════════════════════════════════════════════════
  MEXC Density Scanner — Streamlit Dashboard v2.4
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
# Конфиг
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
# Безопасные конвертации
# ═══════════════════════════════════════════════════

def sf(val, default=0.0):
    """safe float"""
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def si(val, default=0):
    """safe int"""
    try:
        return int(sf(val, default))
    except (ValueError, TypeError):
        return default


def parse_book(raw_levels: list) -> list[tuple[float, float]]:
    """
    Преобразует сырой стакан MEXC в [(price, qty), ...].
    MEXC отдаёт строки: [["0.001","5000"], ...].
    Фильтрует нули и некорректные записи.
    """
    result = []
    if not raw_levels or not isinstance(raw_levels, list):
        return result
    for entry in raw_levels:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            continue
        p = sf(entry[0])
        q = sf(entry[1])
        if p > 0 and q > 0:
            result.append((p, q))
    return result


def extract_trade_count(ticker_data) -> int:
    """Извлечь количество сделок — handle dict or list"""
    if isinstance(ticker_data, list):
        ticker_data = ticker_data[0] if ticker_data else {}
    if not isinstance(ticker_data, dict):
        return 0
    for key in ("count", "tradeCount", "trades", "txcnt"):
        v = ticker_data.get(key)
        if v is None or v == "" or v == 0 or v == "0":
            continue
        result = si(v)
        if result > 0:
            return result
    return 0


def parse_klines(raw: list) -> pd.DataFrame:
    """Парсит свечи MEXC в DataFrame. Безопасно."""
    if not raw or not isinstance(raw, list):
        return pd.DataFrame()
    rows = []
    for k in raw:
        if not isinstance(k, (list, tuple)) or len(k) < 9:
            continue
        rows.append({
            "open_time": sf(k[0]),
            "open": sf(k[1]),
            "high": sf(k[2]),
            "low": sf(k[3]),
            "close": sf(k[4]),
            "volume": sf(k[5]),
            "close_time": sf(k[6]),
            "quote_volume": sf(k[7]),
            "trades": si(k[8]),
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df


def mexc_link(symbol: str) -> str:
    return f"https://www.mexc.com/exchange/{symbol.replace('USDT', '_USDT')}"


def make_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


# ═══════════════════════════════════════════════════
# Session State
# ═══════════════════════════════════════════════════

if "tracker" not in st.session_state:
    st.session_state.tracker = DensityTracker()
if "scan_results" not in st.session_state:
    st.session_state.scan_results = []
if "scan_df" not in st.session_state:
    st.session_state.scan_df = pd.DataFrame()
if "last_scan" not in st.session_state:
    st.session_state.last_scan = 0.0
if "total_pairs" not in st.session_state:
    st.session_state.total_pairs = 0
if "client" not in st.session_state:
    st.session_state.client = MexcClientSync()
if "detail_symbol" not in st.session_state:
    st.session_state.detail_symbol = ""
if "target_page" not in st.session_state:
    st.session_state.target_page = 0


# ═══════════════════════════════════════════════════
# Графики
# ═══════════════════════════════════════════════════

def build_candlestick(df: pd.DataFrame, symbol: str,
                      interval: str, current_price: float = None):
    """Свечной график из готового DataFrame"""
    if df.empty or len(df) < 2:
        return None
    try:
        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            vertical_spacing=0.03, row_heights=[0.75, 0.25],
        )
        fig.add_trace(go.Candlestick(
            x=df["time"], open=df["open"], high=df["high"],
            low=df["low"], close=df["close"],
            increasing_line_color="#00c853",
            decreasing_line_color="#ff1744",
            name="Цена",
        ), row=1, col=1)

        colors = ["#00c853" if c >= o else "#ff1744"
                  for c, o in zip(df["close"], df["open"])]
        fig.add_trace(go.Bar(
            x=df["time"], y=df["volume"],
            marker_color=colors, opacity=0.5, name="Объём",
        ), row=2, col=1)

        if current_price and current_price > 0:
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
    except Exception as e:
        st.caption(f"Ошибка графика: {e}")
        return None


def build_orderbook_chart(bids: list, asks: list,
                          current_price: float, depth: int = 50):
    """Горизонтальный стакан. bids/asks — уже распаршенные [(price,qty),...]"""
    try:
        bid_data = [(p, p * q) for p, q in bids[:depth]]
        ask_data = [(p, p * q) for p, q in asks[:depth]]

        fig = go.Figure()
        if bid_data:
            fig.add_trace(go.Bar(
                y=[f"{p:.8g}" for p, _ in bid_data],
                x=[v for _, v in bid_data],
                orientation="h", name="BID",
                marker_color="rgba(0,200,83,0.7)",
                hovertemplate="Цена: %{y}<br>$%{x:,.0f}<extra>BID</extra>",
            ))
        if ask_data:
            fig.add_trace(go.Bar(
                y=[f"{p:.8g}" for p, _ in ask_data],
                x=[v for _, v in ask_data],
                orientation="h", name="ASK",
                marker_color="rgba(255,23,68,0.7)",
                hovertemplate="Цена: %{y}<br>$%{x:,.0f}<extra>ASK</extra>",
            ))
        if current_price and current_price > 0:
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
    except Exception as e:
        st.error(f"Ошибка стакана: {e}")
        return None


def build_heatmap(bids: list, asks: list,
                  current_price: float, depth: int = 30):
    """Хитмап. bids/asks — уже распаршенные [(price,qty),...]"""
    try:
        levels = []
        for p, q in bids[:depth]:
            levels.append(("BID", p, p * q))
        for p, q in asks[:depth]:
            levels.append(("ASK", p, p * q))
        if not levels:
            return None

        levels.sort(key=lambda x: x[1], reverse=True)
        max_vol = max(v for _, _, v in levels)
        if max_vol <= 0:
            max_vol = 1.0

        fig = go.Figure()
        for side, price, vol in levels:
            intensity = min(vol / max_vol, 1.0)
            if side == "BID":
                r = 0
                g = int(80 + 175 * intensity)
                b = 83
            else:
                r = int(80 + 175 * intensity)
                g = int(60 * (1.0 - intensity))
                b = 68
            fig.add_trace(go.Bar(
                x=[vol], y=[f"{price:.8g}"],
                orientation="h",
                marker_color=f"rgba({r},{g},{b},0.85)",
                showlegend=False,
                hovertemplate=f"{side}: ${vol:,.0f}<extra>{price:.8g}</extra>",
            ))
        if current_price and current_price > 0:
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
    except Exception as e:
        st.error(f"Ошибка хитмапа: {e}")
        return None


def kline_stats(df: pd.DataFrame, last_n: int = None) -> dict:
    """Статистика из свечей: объём, сделки"""
    if df.empty:
        return {"volume": 0, "trades": 0}
    sub = df.tail(last_n) if last_n else df
    return {
        "volume": sub["quote_volume"].sum() if "quote_volume" in sub else 0,
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
    progress = st.progress(0, "Загрузка списка пар...")

    try:
        info = client.get_exchange_info()
    except Exception as e:
        st.error(f"Ошибка API: {e}")
        return
    if not info or "symbols" not in info:
        err = client.last_error or "Пустой ответ"
        st.error(f"❌ Не удалось загрузить список пар MEXC")
        st.warning(
            f"**Причина:** {err}\n\n"
            f"**Домен:** {client.base_url}\n\n"
            f"Возможные решения:\n"
            f"- Подожди 30 секунд и попробуй снова (rate limit)\n"
            f"- Проверь что MEXC API доступен: "
            f"https://api.mexc.com/api/v3/ping\n"
            f"- Если IP заблокирован — используй VPN/VPS"
        )
        # Попробуем ping
        ok, msg = client.ping()
        if ok:
            st.info(f"Ping OK ({msg}), но exchangeInfo не отвечает. "
                     f"Попробуй ещё раз.")
        else:
            st.error(f"Ping FAIL: {msg}")
        progress.empty()
        return

    all_symbols = []
    for s in info["symbols"]:
        try:
            if s.get("quoteAsset") != "USDT":
                continue
            # MEXC может вернуть status как: "1", 1, "ENABLED", True
            status = s.get("status", "")
            status_ok = (str(status) in ("1", "ENABLED", "True", "true")
                         or status is True or status == 1)
            trading = s.get("isSpotTradingAllowed", True)
            if status_ok and trading:
                all_symbols.append(s["symbol"])
        except Exception:
            continue

    # Если строгий фильтр ничего не дал — пробуем без фильтра статуса
    if not all_symbols:
        for s in info["symbols"]:
            try:
                if s.get("quoteAsset") == "USDT":
                    all_symbols.append(s["symbol"])
            except Exception:
                continue
        if all_symbols:
            st.warning(f"⚠️ Фильтр статуса не сработал, "
                       f"взяты все {len(all_symbols)} USDT-пар")

    if not all_symbols:
        # Может быть другой формат статуса
        sample = info["symbols"][:3] if info.get("symbols") else []
        statuses = [str(s.get("status")) for s in sample]
        st.error(f"Нет USDT-пар. Статусы первых пар: {statuses}")
        progress.empty()
        return

    progress.progress(10, f"{len(all_symbols)} USDT-пар...")

    try:
        tickers = client.get_all_tickers_24h()
    except Exception as e:
        st.error(f"Ошибка тикеров: {e}")
        progress.empty()
        return
    if not tickers:
        st.error(f"Не удалось загрузить тикеры. {client.last_error}")
        progress.empty()
        return

    ticker_map = {}
    for t in tickers:
        sym = t.get("symbol")
        if sym:
            ticker_map[sym] = t

    candidates = []
    for sym in all_symbols:
        t = ticker_map.get(sym)
        if not t:
            continue
        vol = sf(t.get("quoteVolume", 0))
        if min_vol <= vol <= max_vol:
            candidates.append((sym, t))
    candidates.sort(key=lambda x: sf(x[1].get("quoteVolume", 0)),
                    reverse=True)

    if not candidates:
        st.warning("Нет пар в заданном диапазоне")
        progress.empty()
        return

    progress.progress(20, f"Сканирую ({len(candidates)} пар)...")

    results = []
    errors = 0
    total = len(candidates)
    for i, (sym, ticker) in enumerate(candidates):
        try:
            book = client.get_order_book(sym, cfg.ORDER_BOOK_DEPTH)
            if book:
                result = analyze_order_book(sym, book, ticker)
                if result and result.spread_pct >= min_spread:
                    result.trade_count_24h = extract_trade_count(ticker)
                    results.append(result)
        except Exception:
            errors += 1
        if (i + 1) % 5 == 0 or i == total - 1:
            pct = 20 + int((i + 1) / total * 70)
            progress.progress(
                pct, f"{i+1}/{total} | Найдено: {len(results)}")
            time.sleep(0.02)

    results.sort(key=lambda r: r.score, reverse=True)
    top_results = results[:top_n]

    # Подгрузить trade count индивидуально для топа
    progress.progress(92, "Загрузка сделок 24ч...")
    for r in top_results:
        if r.trade_count_24h == 0:
            try:
                ind = client.get_ticker_24h(r.symbol)
                tc = extract_trade_count(ind)
                if tc > 0:
                    r.trade_count_24h = tc
            except Exception:
                pass

    new_movers = st.session_state.tracker.update(top_results)

    rows = []
    for r in top_results:
        if not r.all_walls:
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
            "Сделок 24ч": r.trade_count_24h,
            "BID стенки": bid_str,
            "ASK стенки": ask_str,
            "B/A": f"{len(r.bid_walls)}/{len(r.ask_walls)}",
            "🔄": "⚡" if r.has_movers else "",
        })

    st.session_state.scan_results = top_results
    st.session_state.scan_df = pd.DataFrame(rows) if rows else pd.DataFrame()
    st.session_state.last_scan = time.time()
    st.session_state.total_pairs = total

    progress.progress(100, "Готово!")
    time.sleep(0.3)
    progress.empty()
    st.toast(f"Найдено {len(top_results)} пар")
    if new_movers:
        st.toast(f"⚡ {len(new_movers)} переставок!", icon="🔄")


# ═══════════════════════════════════════════════════
# Навигация
# ═══════════════════════════════════════════════════

PAGES = ["📊 Сканер", "🔍 Детальный разбор", "📈 Переставки"]


def go_to_detail(symbol: str):
    st.session_state.detail_symbol = symbol
    st.session_state.target_page = 1


# ═══════════════════════════════════════════════════
# Сайдбар
# ═══════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## ⚙️ Настройки")
    min_vol = st.number_input("Мин. объём ($)", value=100,
                              min_value=0, step=100)
    max_vol = st.number_input("Макс. объём ($)", value=500_000,
                              min_value=100, step=10000)
    min_spread = st.slider("Мин. спред (%)", 0.0, 20.0, 0.5, 0.1)
    wall_mult = st.slider("Множитель (x)", 2, 50, 5)
    min_wall_usd = st.number_input("Мин. стенка ($)", value=50,
                                    min_value=1, step=10)
    top_n = st.slider("Результатов", 5, 100, 30)

    st.markdown("---")
    auto_refresh = st.checkbox("🔄 Авто-обновление (60с)")
    if auto_refresh:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=60_000, key="auto_refresh")
        except ImportError:
            st.warning("streamlit-autorefresh не установлен")

    scan_btn = st.button("🚀 Запустить скан",
                         use_container_width=True, type="primary")

    st.markdown("---")
    st.markdown("### 📥 Экспорт")

    def build_full_zip() -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            if not st.session_state.scan_df.empty:
                zf.writestr("scan_results.csv",
                            st.session_state.scan_df.to_csv(index=False))
            tr = st.session_state.tracker
            if tr.all_mover_events:
                m_rows = [{
                    "Время": datetime.fromtimestamp(e.timestamp).isoformat(),
                    "Пара": e.symbol, "Сторона": e.side,
                    "Старая цена": e.old_price,
                    "Новая цена": e.new_price,
                    "Объём $": round(e.size_usdt),
                    "Сдвиг %": e.shift_pct,
                    "Направление": e.direction,
                } for e in tr.all_mover_events]
                zf.writestr("movers.csv",
                            pd.DataFrame(m_rows).to_csv(index=False))
            walls = []
            for r in st.session_state.scan_results:
                for w in r.all_walls:
                    walls.append({
                        "Пара": r.symbol, "Сторона": w.side,
                        "Цена": w.price,
                        "Объём $": round(w.size_usdt),
                        "Множитель": w.multiplier,
                        "Расстояние %": w.distance_pct,
                    })
            if walls:
                zf.writestr("all_walls.csv",
                            pd.DataFrame(walls).to_csv(index=False))
        buf.seek(0)
        return buf.getvalue()

    has_data = (bool(st.session_state.scan_results) or
                bool(st.session_state.tracker.all_mover_events))
    if has_data:
        st.download_button(
            "📦 Скачать ВСЁ (ZIP)",
            data=build_full_zip(),
            file_name=f"mexc_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
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

    # Диагностика
    if st.button("🔧 Проверить API", use_container_width=True):
        c = st.session_state.client
        ok, msg = c.ping()
        if ok:
            st.success(f"✅ Ping: {msg}")
            ok2, msg2 = c.server_time()
            if ok2:
                st.success(f"✅ Server time: {msg2}")
        else:
            st.error(f"❌ Ping: {msg}")
            # Пробуем альтернативные домены
            for domain in ["https://api.mexc.com",
                           "https://www.mexc.com"]:
                try:
                    import requests as _rq
                    r = _rq.get(f"{domain}/api/v3/ping",
                                timeout=10,
                                headers={"User-Agent": "Mozilla/5.0"})
                    if r.status_code == 200:
                        st.info(f"✅ {domain} доступен!")
                        c.base_url = domain
                        st.success(f"Переключён на {domain}")
                        break
                    else:
                        st.warning(f"❌ {domain}: HTTP {r.status_code}")
                except Exception as e:
                    st.warning(f"❌ {domain}: {e}")

# ═══════════════════════════════════════════════════
# Запуск скана
# ═══════════════════════════════════════════════════

if scan_btn or (auto_refresh
                and time.time() - st.session_state.last_scan > 55):
    run_scan(min_vol, max_vol, min_spread, wall_mult,
             min_wall_usd, top_n)

# ═══════════════════════════════════════════════════
# Вкладки
# ═══════════════════════════════════════════════════

_idx = st.session_state.target_page
if _idx < 0 or _idx >= len(PAGES):
    _idx = 0

page = st.radio("nav", PAGES, horizontal=True,
                index=_idx, label_visibility="collapsed")

# Обратная синхронизация
for i, p in enumerate(PAGES):
    if page == p:
        st.session_state.target_page = i
        break

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
        c4.metric("С переставками",
                  sum(1 for r in results if r.has_movers))
        total_tc = sum(r.trade_count_24h for r in results)
        c5.metric("Σ сделок 24ч",
                  f"{total_tc:,}" if total_tc else "загрузка...")

        # Быстрые кнопки
        st.markdown("##### 🔍 Выбери пару для разбора")
        opts = [r.symbol for r in results]
        ncols = min(10, len(opts))
        btn_cols = st.columns(ncols)
        for i, sym in enumerate(opts[:ncols]):
            with btn_cols[i]:
                if st.button(sym, key=f"c_{sym}",
                             use_container_width=True):
                    go_to_detail(sym)
                    st.rerun()

        if len(opts) > 10:
            col_s, col_g = st.columns([3, 1])
            with col_s:
                chosen = st.selectbox("Полный список",
                                      [""] + opts, key="sp")
            with col_g:
                st.markdown("<br>", unsafe_allow_html=True)
                if chosen and st.button("➡️", type="primary",
                                        key="open_d"):
                    go_to_detail(chosen)
                    st.rerun()

        # Таблица
        if not scan_df.empty:
            st.dataframe(
                scan_df,
                column_config={
                    "Скор": st.column_config.NumberColumn(
                        format="%.1f", width="small"),
                    "Спред %": st.column_config.NumberColumn(
                        format="%.2f"),
                    "Объём 24ч $": st.column_config.NumberColumn(
                        format="%d"),
                    "Сделок 24ч": st.column_config.NumberColumn(
                        format="%d"),
                    "🔄": st.column_config.TextColumn(width="small"),
                },
                hide_index=True,
                use_container_width=True,
                height=min(len(scan_df) * 38 + 40, 800),
            )
            st.download_button(
                "📥 Скачать CSV",
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
        ds = st.session_state.detail_symbol
        if ds and ds in sym_list:
            idx = sym_list.index(ds) + 1
        target = st.selectbox("Пара", [""] + sym_list,
                              index=idx, key="dsel")
    with col_b:
        manual = st.text_input("Или вручную", placeholder="XYZUSDT")

    symbol = manual.strip().upper() if manual.strip() else target
    if not symbol:
        st.info("Выбери пару из скана или введи вручную")
        st.stop()

    st.session_state.detail_symbol = symbol
    client = st.session_state.client

    # ─── Загрузка данных ───
    with st.spinner(f"Загружаю {symbol}..."):
        try:
            book_raw = client.get_order_book(symbol, 500)
            ticker_raw = client.get_ticker_24h(symbol)
            trades_raw = client.get_recent_trades(symbol, 1000)
            # MEXC интервалы: 1m, 5m, 15m, 30m, 60m, 4h, 1d
            kl_1m_raw = client.get_klines(symbol, "1m", 100)
            kl_5m_raw = client.get_klines(symbol, "5m", 100)
            kl_60m_raw = client.get_klines(symbol, "60m", 100)
            kl_4h_raw = client.get_klines(symbol, "4h", 100)
        except Exception as e:
            st.error(f"Ошибка загрузки: {e}")
            st.stop()

    if not book_raw or not book_raw.get("bids") or not book_raw.get("asks"):
        st.error(f"Нет данных стакана для {symbol}")
        st.stop()

    # ─── Парсим всё в числа ───
    bids = parse_book(book_raw["bids"])
    asks = parse_book(book_raw["asks"])

    if not bids or not asks:
        st.error(f"Пустой стакан для {symbol}")
        st.stop()

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid_price = (best_bid + best_ask) / 2
    spread_pct = (best_ask - best_bid) / best_bid * 100
    bid_depth = sum(p * q for p, q in bids)
    ask_depth = sum(p * q for p, q in asks)

    td = ticker_raw
    if isinstance(td, list):
        td = td[0] if td else {}
    if not isinstance(td, dict):
        td = {}
    trade_count_24h = extract_trade_count(td)
    volume_24h = sf(td.get("quoteVolume", 0))

    # Парсим свечи
    df_1m = parse_klines(kl_1m_raw)
    df_5m = parse_klines(kl_5m_raw)
    df_1h = parse_klines(kl_60m_raw)
    df_4h = parse_klines(kl_4h_raw)

    # ─── Заголовок ───
    h1, h2 = st.columns([3, 1])
    with h1:
        st.markdown(f"## {symbol}")
    with h2:
        st.markdown(f"[🔗 MEXC]({mexc_link(symbol)})")

    # ─── Метрики ───
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Mid Price", f"{mid_price:.8g}")
    m2.metric("Спред", f"{spread_pct:.2f}%")
    m3.metric("Bid глубина", f"${bid_depth:,.0f}")
    m4.metric("Ask глубина", f"${ask_depth:,.0f}")
    m5.metric("Сделок 24ч", f"{trade_count_24h:,}" if trade_count_24h else "—")
    m6.metric("Объём 24ч", f"${volume_24h:,.0f}")

    # ─── Объёмы и сделки по таймфреймам ───
    st.markdown("#### 📊 Объёмы и сделки по таймфреймам")

    # Считаем из свечей
    s_5m = kline_stats(df_5m, 1)    # последняя 5м свеча
    s_15m = kline_stats(df_5m, 3)   # последние 3 × 5m = 15m
    s_1h = kline_stats(df_5m, 12)   # последние 12 × 5m = 60m
    s_4h = kline_stats(df_1h, 4)    # последние 4 × 1h = 4h
    s_24h_vol = volume_24h
    s_24h_trades = trade_count_24h

    vc = st.columns(5)
    vc[0].metric("5 мин",
                 f"${s_5m['volume']:,.0f}",
                 f"{s_5m['trades']} сделок")
    vc[1].metric("15 мин",
                 f"${s_15m['volume']:,.0f}",
                 f"{s_15m['trades']} сделок")
    vc[2].metric("1 час",
                 f"${s_1h['volume']:,.0f}",
                 f"{s_1h['trades']} сделок")
    vc[3].metric("4 часа",
                 f"${s_4h['volume']:,.0f}",
                 f"{s_4h['trades']} сделок")
    vc[4].metric("24 часа",
                 f"${s_24h_vol:,.0f}",
                 f"{s_24h_trades:,} сделок")

    # Тайминги сделок
    if trades_raw and isinstance(trades_raw, list) and len(trades_raw) > 2:
        times = [sf(t.get("time", 0)) for t in trades_raw
                 if sf(t.get("time", 0)) > 0]
        if len(times) >= 3:
            deltas = [(times[i] - times[i + 1]) / 1000
                      for i in range(len(times) - 1)
                      if times[i + 1] > 0]
            deltas = [d for d in deltas if d >= 0]
            if deltas:
                avg_d = sum(deltas) / len(deltas)
                robot = (" 🤖 **Робот!**"
                         if avg_d < 30 and max(deltas) < 120
                         else "")
                st.caption(
                    f"Интервалы между сделками: ср.={avg_d:.1f}с, "
                    f"мин={min(deltas):.1f}с, "
                    f"макс={max(deltas):.1f}с{robot}"
                )

    # ─── Графики свечей ───
    st.markdown("#### 📈 Графики")
    tab_1m, tab_5m, tab_1h, tab_4h = st.tabs(
        ["1 минута", "5 минут", "1 час", "4 часа"])
    with tab_1m:
        fig = build_candlestick(df_1m, symbol, "1m", mid_price)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Нет данных 1m")
    with tab_5m:
        fig = build_candlestick(df_5m, symbol, "5m", mid_price)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Нет данных 5m")
    with tab_1h:
        fig = build_candlestick(df_1h, symbol, "1h", mid_price)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Нет данных 1h (60m)")
    with tab_4h:
        fig = build_candlestick(df_4h, symbol, "4h", mid_price)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Нет данных 4h")

    # ─── Стакан ───
    st.markdown("#### 📖 Стакан")
    depth_v = st.select_slider("Глубина",
                               [20, 30, 50, 100], value=50, key="obd")
    fig_ob = build_orderbook_chart(bids, asks, mid_price, depth_v)
    if fig_ob:
        st.plotly_chart(fig_ob, use_container_width=True)

    # ─── Хитмап ───
    fig_hm = build_heatmap(bids, asks, mid_price, 30)
    if fig_hm:
        st.plotly_chart(fig_hm, use_container_width=True)

    # ─── Последние сделки ───
    trades_df = pd.DataFrame()
    if trades_raw and isinstance(trades_raw, list):
        st.markdown("#### 📋 Последние сделки")
        t_rows = []
        for t in trades_raw[:50]:
            try:
                p = sf(t.get("price", 0))
                q = sf(t.get("qty", 0))
                ts = sf(t.get("time", 0))
                t_rows.append({
                    "Время": (pd.to_datetime(ts, unit="ms")
                              .strftime("%H:%M:%S")
                              if ts > 0 else "—"),
                    "Цена": p, "Кол-во": q,
                    "USDT": round(p * q, 2),
                    "Сторона": ("🟢 BUY"
                                if not t.get("isBuyerMaker")
                                else "🔴 SELL"),
                })
            except Exception:
                continue
        if t_rows:
            trades_df = pd.DataFrame(t_rows)
            st.dataframe(trades_df, hide_index=True,
                         use_container_width=True)

    # ─── Экспорт ───
    st.markdown("---")
    st.markdown("#### 📥 Экспорт")

    export_parts = {}
    ob_rows = []
    for side, data in [("BID", bids), ("ASK", asks)]:
        for p, q in data:
            ob_rows.append({
                "Сторона": side, "Цена": p,
                "Количество": q, "USDT": round(p * q, 4),
            })
    ob_df = pd.DataFrame(ob_rows)
    export_parts["orderbook"] = ob_df
    if not trades_df.empty:
        export_parts["trades"] = trades_df
    for label, kdf in [("klines_1m", df_1m), ("klines_5m", df_5m),
                       ("klines_1h", df_1h), ("klines_4h", df_4h)]:
        if not kdf.empty:
            export_parts[label] = kdf

    e1, e2 = st.columns(2)
    with e1:
        st.download_button("📥 Стакан CSV", data=make_csv(ob_df),
                           file_name=f"{symbol}_book.csv",
                           mime="text/csv")
    with e2:
        if not trades_df.empty:
            st.download_button("📥 Сделки CSV",
                               data=make_csv(trades_df),
                               file_name=f"{symbol}_trades.csv",
                               mime="text/csv")

    def build_sym_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, edf in export_parts.items():
                zf.writestr(f"{symbol}_{name}.csv",
                            edf.to_csv(index=False))
            meta = (
                f"symbol,{symbol}\nmid_price,{mid_price}\n"
                f"spread_pct,{spread_pct:.4f}\n"
                f"bid_depth,{bid_depth:.2f}\n"
                f"ask_depth,{ask_depth:.2f}\n"
                f"trades_24h,{trade_count_24h}\n"
                f"volume_24h,{volume_24h:.2f}\n"
                f"timestamp,{datetime.now().isoformat()}\n"
            )
            zf.writestr(f"{symbol}_meta.csv", meta)
        buf.seek(0)
        return buf.getvalue()

    st.download_button(
        f"📦 Всё по {symbol} (ZIP)",
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
    **Переставляш** — плотность, перемещающаяся по стакану.
    Признак робота. Включи авто-обновление (60с) для накопления.
    """)

    movers = tracker.get_active_movers(7200)

    if not movers:
        st.info("Переставок нет. Запусти несколько сканов.")
    else:
        st.success(f"⚡ {len(movers)} переставок за 2 часа")

        m_rows = []
        for e in reversed(movers):
            m_rows.append({
                "Время": datetime.fromtimestamp(
                    e.timestamp).strftime("%H:%M:%S"),
                "↕": "⬆️" if e.direction == "UP" else "⬇️",
                "Пара": e.symbol, "Сторона": e.side,
                "Объём $": round(e.size_usdt),
                "Было": f"{e.old_price:.8g}",
                "Стало": f"{e.new_price:.8g}",
                "Сдвиг %": round(e.shift_pct, 3),
            })
        mover_df = pd.DataFrame(m_rows)
        st.dataframe(
            mover_df, hide_index=True, use_container_width=True,
            column_config={
                "↕": st.column_config.TextColumn(width="small"),
            },
        )

        unique_syms = sorted({e.symbol for e in movers})
        col_mp, col_mg = st.columns([3, 1])
        with col_mp:
            chosen_m = st.selectbox("Пара → разбор",
                                    [""] + unique_syms, key="mp")
        with col_mg:
            st.markdown("<br>", unsafe_allow_html=True)
            if chosen_m and st.button("➡️", key="mg"):
                go_to_detail(chosen_m)
                st.rerun()

        st.download_button(
            "📥 Переставки CSV",
            data=make_csv(mover_df),
            file_name=f"movers_{datetime.now().strftime('%H%M')}.csv",
            mime="text/csv",
        )

    top_movers = tracker.get_top_movers(15)
    if top_movers:
        st.markdown("### 🏆 Топ пар")
        fig = go.Figure(go.Bar(
            x=[x[0] for x in top_movers],
            y=[x[1] for x in top_movers],
            marker_color="#00d2ff",
        ))
        fig.update_layout(template="plotly_dark", height=300)
        st.plotly_chart(fig, use_container_width=True)


st.markdown("---")
st.caption("MEXC Density Scanner v2.4 · Не является финансовой рекомендацией")
