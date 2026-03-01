"""
═══════════════════════════════════════════════════════════
  MEXC Density Scanner — Streamlit Dashboard v2.0
═══════════════════════════════════════════════════════════
"""
import time
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from mexc_client import MexcClientSync
from analyzer import analyze_order_book, ScanResult, WallInfo
from history import DensityTracker

# ─── Конфиг страницы ───

st.set_page_config(
    page_title="MEXC Density Scanner",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS ───

st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
    .stMetric > div { background: #1a1f2e; padding: 0.8rem; border-radius: 8px; }
    div[data-testid="stMetricValue"] { font-size: 1.5rem; }
    .wall-bid { color: #00c853; font-weight: bold; }
    .wall-ask { color: #ff1744; font-weight: bold; }
    .mover-up { color: #00e5ff; }
    .mover-down { color: #ff9100; }
    .score-high { color: #00e676; font-weight: bold; font-size: 1.2em; }
    .score-mid { color: #ffd740; }
    .score-low { color: #757575; }
    a.mexc-link { color: #00d2ff; text-decoration: none; }
    a.mexc-link:hover { text-decoration: underline; }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════
# Инициализация состояния
# ═══════════════════════════════════════════════════

if "tracker" not in st.session_state:
    st.session_state.tracker = DensityTracker()
if "scan_results" not in st.session_state:
    st.session_state.scan_results = []
if "last_scan" not in st.session_state:
    st.session_state.last_scan = 0
if "total_pairs" not in st.session_state:
    st.session_state.total_pairs = 0
if "client" not in st.session_state:
    st.session_state.client = MexcClientSync()


# ═══════════════════════════════════════════════════
# Сайдбар — Фильтры
# ═══════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## ⚙️ Настройки сканера")

    min_vol = st.number_input(
        "Мин. объём 24ч ($)", value=100, min_value=0, step=100
    )
    max_vol = st.number_input(
        "Макс. объём 24ч ($)", value=500_000, min_value=100, step=10000
    )
    min_spread = st.slider(
        "Мин. спред (%)", 0.0, 20.0, 0.5, 0.1
    )
    wall_mult = st.slider(
        "Множитель плотности (x от медианы)", 2, 50, 5
    )
    min_wall_usd = st.number_input(
        "Мин. размер стенки ($)", value=50, min_value=1, step=10
    )
    top_n = st.slider("Кол-во результатов", 5, 100, 30)

    st.markdown("---")

    # Кнопка авто-обновления
    auto_refresh = st.checkbox("🔄 Авто-обновление (60с)", value=False)
    if auto_refresh:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=60_000, key="auto_refresh")
        except ImportError:
            st.warning("Установи `streamlit-autorefresh` для авто-обновления")

    st.markdown("---")
    scan_btn = st.button("🚀 Запустить скан", use_container_width=True, type="primary")

    st.markdown("---")
    tracker_stats = st.session_state.tracker.get_stats()
    st.caption(
        f"Сканов: {tracker_stats['total_scans']} | "
        f"Пар: {tracker_stats['total_pairs_tracked']} | "
        f"Переставок: {tracker_stats['total_mover_events']}"
    )


# ═══════════════════════════════════════════════════
# Функция сканирования
# ═══════════════════════════════════════════════════

def run_scan():
    """Выполняет полное сканирование"""
    import config as cfg
    cfg.MIN_DAILY_VOLUME_USDT = min_vol
    cfg.MAX_DAILY_VOLUME_USDT = max_vol
    cfg.MIN_SPREAD_PCT = min_spread
    cfg.WALL_MULTIPLIER = wall_mult
    cfg.MIN_WALL_SIZE_USDT = min_wall_usd

    client = st.session_state.client
    progress = st.progress(0, "Загрузка списка пар...")

    # Шаг 1: Получаем пары
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

    progress.progress(10, f"Найдено {len(all_symbols)} USDT-пар. Фильтрую по объёму...")

    # Шаг 2: Тикеры
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

    progress.progress(20, f"Сканирую стаканы ({len(candidates)} пар)...")

    # Шаг 3: Запрашиваем стаканы
    results = []
    total = len(candidates)

    for i, (sym, ticker) in enumerate(candidates):
        book = client.get_order_book(sym, cfg.ORDER_BOOK_DEPTH)
        if book:
            result = analyze_order_book(sym, book, ticker)
            if result and result.spread_pct >= min_spread:
                results.append(result)

        if (i + 1) % 5 == 0 or i == total - 1:
            pct = 20 + int((i + 1) / total * 75)
            progress.progress(
                pct,
                f"Сканирую: {i+1}/{total} | Найдено: {len(results)}"
            )
            time.sleep(0.05)  # Не блокируем UI полностью

    # Шаг 4: Обновляем трекер (детекция переставляшей)
    new_movers = st.session_state.tracker.update(results)

    results.sort(key=lambda r: r.score, reverse=True)
    st.session_state.scan_results = results[:top_n]
    st.session_state.last_scan = time.time()
    st.session_state.total_pairs = total

    progress.progress(100, "Готово!")
    time.sleep(0.3)
    progress.empty()

    if new_movers:
        st.toast(f"🔄 Обнаружено {len(new_movers)} переставок!", icon="⚡")


# ═══════════════════════════════════════════════════
# Выполнение скана
# ═══════════════════════════════════════════════════

if scan_btn or (auto_refresh and time.time() - st.session_state.last_scan > 55):
    run_scan()


# ═══════════════════════════════════════════════════
# Табы
# ═══════════════════════════════════════════════════

tab_scan, tab_book, tab_monitor = st.tabs([
    "📊 Сканер плотностей",
    "🔍 Стакан пары",
    "📈 Мониторинг переставок",
])


# ═══════════════════════════════════════════════════
# Таб 1: Сканер
# ═══════════════════════════════════════════════════

with tab_scan:
    results = st.session_state.scan_results

    if not results:
        st.info("Нажми **🚀 Запустить скан** в сайдбаре для начала работы")
    else:
        # Метрики
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Найдено пар", len(results))
        c2.metric("Проверено", st.session_state.total_pairs)
        best = results[0] if results else None
        c3.metric("Лучший скор", f"⭐ {best.score}" if best else "—")
        movers_count = sum(1 for r in results if r.has_movers)
        c4.metric("С переставками", movers_count)

        # Таблица
        rows = []
        for r in results:
            biggest = r.biggest_wall
            if not biggest:
                continue

            bid_walls_str = " | ".join(
                f"${w.size_usdt:,.0f} ({w.multiplier}x, -{w.distance_pct}%)"
                for w in r.bid_walls[:2]
            ) or "—"

            ask_walls_str = " | ".join(
                f"${w.size_usdt:,.0f} ({w.multiplier}x, +{w.distance_pct}%)"
                for w in r.ask_walls[:2]
            ) or "—"

            pair_link = r.symbol.replace("USDT", "_USDT")

            rows.append({
                "⭐": r.score,
                "Пара": r.symbol,
                "Спред %": round(r.spread_pct, 2),
                "Объём 24ч $": round(r.volume_24h_usdt),
                "🟢 BID стенки": bid_walls_str,
                "🔴 ASK стенки": ask_walls_str,
                "Стенок B/A": f"{len(r.bid_walls)}/{len(r.ask_walls)}",
                "🔄": "⚡" if r.has_movers else "",
                "MEXC": f"https://www.mexc.com/exchange/{pair_link}",
            })

        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(
                df,
                column_config={
                    "⭐": st.column_config.NumberColumn(format="%.1f", width="small"),
                    "Спред %": st.column_config.NumberColumn(format="%.2f"),
                    "Объём 24ч $": st.column_config.NumberColumn(format="%d"),
                    "MEXC": st.column_config.LinkColumn("MEXC", display_text="Открыть"),
                    "🔄": st.column_config.TextColumn(width="small"),
                },
                hide_index=True,
                use_container_width=True,
                height=min(len(rows) * 38 + 40, 800),
            )


# ═══════════════════════════════════════════════════
# Таб 2: Стакан пары
# ═══════════════════════════════════════════════════

with tab_book:
    results = st.session_state.scan_results
    symbols_list = [r.symbol for r in results] if results else []

    col_sel, col_depth = st.columns([1, 1])

    with col_sel:
        custom_sym = st.text_input(
            "Введи пару вручную", placeholder="XYZUSDT"
        )
    with col_depth:
        depth_limit = st.selectbox("Глубина стакана", [20, 50, 100, 500, 1000], index=2)

    selected = None
    if symbols_list:
        selected = st.selectbox(
            "Или выбери из результатов скана",
            options=[""] + symbols_list,
        )

    target = custom_sym.strip().upper() if custom_sym.strip() else selected

    if target:
        load_btn = st.button(f"📖 Загрузить стакан {target}", type="primary")
        if load_btn:
            client = st.session_state.client
            book = client.get_order_book(target, depth_limit)
            trades = client.get_recent_trades(target, 50)

            if not book or not book.get("bids") or not book.get("asks"):
                st.error(f"Не удалось загрузить стакан для {target}")
            else:
                bids = [(float(b[0]), float(b[1])) for b in book["bids"]]
                asks = [(float(a[0]), float(a[1])) for a in book["asks"]]

                best_bid = bids[0][0]
                best_ask = asks[0][0]
                mid = (best_bid + best_ask) / 2
                spread = (best_ask - best_bid) / best_bid * 100

                # Метрики
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Mid Price", f"{mid:.8g}")
                m2.metric("Спред", f"{spread:.2f}%")
                m3.metric("Bid глубина", f"${sum(p*q for p,q in bids):,.0f}")
                m4.metric("Ask глубина", f"${sum(p*q for p,q in asks):,.0f}")

                # ─── Визуализация стакана ───

                bid_prices = [p for p, _ in bids[:50]]
                bid_volumes = [p * q for p, q in bids[:50]]
                ask_prices = [p for p, _ in asks[:50]]
                ask_volumes = [p * q for p, q in asks[:50]]

                fig = go.Figure()

                # Биды (зелёные, справа)
                fig.add_trace(go.Bar(
                    y=[f"{p:.8g}" for p in bid_prices],
                    x=bid_volumes,
                    orientation="h",
                    name="BID (покупка)",
                    marker_color="rgba(0, 200, 83, 0.7)",
                    hovertemplate="Цена: %{y}<br>Объём: $%{x:,.0f}<extra></extra>",
                ))

                # Аски (красные, справа)
                fig.add_trace(go.Bar(
                    y=[f"{p:.8g}" for p in ask_prices],
                    x=ask_volumes,
                    orientation="h",
                    name="ASK (продажа)",
                    marker_color="rgba(255, 23, 68, 0.7)",
                    hovertemplate="Цена: %{y}<br>Объём: $%{x:,.0f}<extra></extra>",
                ))

                fig.update_layout(
                    title=f"Стакан {target} (топ-50 уровней с каждой стороны)",
                    xaxis_title="Объём (USDT)",
                    yaxis_title="Цена",
                    barmode="relative",
                    height=max(600, len(bid_prices) * 15),
                    template="plotly_dark",
                    showlegend=True,
                    yaxis=dict(type="category"),
                )

                st.plotly_chart(fig, use_container_width=True)

                # ─── Хитмап плотностей ───

                st.subheader("🔥 Хитмап плотностей")

                all_levels = []
                for p, q in bids[:30]:
                    all_levels.append(("BID", p, p * q))
                for p, q in asks[:30]:
                    all_levels.append(("ASK", p, p * q))

                if all_levels:
                    hm_df = pd.DataFrame(all_levels, columns=["Сторона", "Цена", "Объём $"])
                    max_vol_level = hm_df["Объём $"].max()

                    fig2 = go.Figure()
                    for _, row in hm_df.iterrows():
                        color = (
                            f"rgba(0,{min(255, int(row['Объём $']/max_vol_level*255))},83,0.8)"
                            if row["Сторона"] == "BID"
                            else f"rgba(255,{max(0, 255-int(row['Объём $']/max_vol_level*255))},68,0.8)"
                        )
                        fig2.add_trace(go.Bar(
                            x=[row["Объём $"]],
                            y=[f"{row['Цена']:.8g}"],
                            orientation="h",
                            marker_color=color,
                            showlegend=False,
                            hovertemplate=f"{row['Сторона']}: ${row['Объём $']:,.0f}<extra></extra>",
                        ))

                    fig2.update_layout(
                        height=500,
                        template="plotly_dark",
                        barmode="stack",
                        yaxis=dict(type="category"),
                        xaxis_title="Объём (USDT)",
                    )
                    st.plotly_chart(fig2, use_container_width=True)

                # ─── Последние сделки ───

                if trades:
                    st.subheader("📋 Последние сделки")
                    trade_rows = []
                    for t in trades[:30]:
                        trade_rows.append({
                            "Цена": float(t.get("price", 0)),
                            "Кол-во": float(t.get("qty", 0)),
                            "USDT": round(float(t.get("price", 0)) * float(t.get("qty", 0)), 2),
                            "Сторона": "🟢 BUY" if not t.get("isBuyerMaker") else "🔴 SELL",
                            "Время": pd.to_datetime(t.get("time", 0), unit="ms").strftime("%H:%M:%S"),
                        })
                    st.dataframe(
                        pd.DataFrame(trade_rows),
                        hide_index=True,
                        use_container_width=True,
                    )

                    # Анализ таймингов (для ёршей важно)
                    if len(trades) >= 3:
                        times = [t.get("time", 0) for t in trades]
                        deltas = [(times[i] - times[i+1]) / 1000
                                  for i in range(len(times)-1) if times[i+1] > 0]
                        if deltas:
                            avg_delta = sum(deltas) / len(deltas)
                            min_delta = min(deltas)
                            max_delta = max(deltas)
                            st.caption(
                                f"⏱ Интервалы между сделками: "
                                f"средний={avg_delta:.1f}с, "
                                f"мин={min_delta:.1f}с, "
                                f"макс={max_delta:.1f}с "
                                f"{'🤖 Похоже на робота!' if avg_delta < 30 and max_delta < 120 else ''}"
                            )

    else:
        st.info("Выбери пару из результатов скана или введи вручную")


# ═══════════════════════════════════════════════════
# Таб 3: Мониторинг переставок
# ═══════════════════════════════════════════════════

with tab_monitor:
    tracker = st.session_state.tracker

    st.markdown("""
    **Переставляш** — плотность, которая перемещается по стакану.
    Ключевой признак робота-маркетмейкера. Сканер сравнивает текущий
    стакан с предыдущим снимком и находит стенки, которые "переехали".

    > Для накопления данных нужно несколько сканов подряд.
    > Используй авто-обновление (60с) для автоматической детекции.
    """)

    movers = tracker.get_active_movers(window_sec=7200)  # за 2 часа

    if not movers:
        st.info(
            "Переставок пока не обнаружено. "
            "Запусти несколько сканов подряд или включи авто-обновление."
        )
    else:
        st.success(f"⚡ Обнаружено {len(movers)} переставок за последние 2 часа")

        mover_rows = []
        for e in reversed(movers):
            from datetime import datetime
            ts = datetime.fromtimestamp(e.timestamp).strftime("%H:%M:%S")
            arrow = "⬆️" if e.direction == "UP" else "⬇️"
            pair_link = e.symbol.replace("USDT", "_USDT")
            mover_rows.append({
                "Время": ts,
                "": arrow,
                "Пара": e.symbol,
                "Сторона": e.side,
                "Объём $": round(e.size_usdt),
                "Было": f"{e.old_price:.8g}",
                "Стало": f"{e.new_price:.8g}",
                "Сдвиг %": round(e.shift_pct, 3),
                "MEXC": f"https://www.mexc.com/exchange/{pair_link}",
            })

        df_movers = pd.DataFrame(mover_rows)
        st.dataframe(
            df_movers,
            column_config={
                "MEXC": st.column_config.LinkColumn("MEXC", display_text="Открыть"),
                "": st.column_config.TextColumn(width="small"),
            },
            hide_index=True,
            use_container_width=True,
        )

    # Топ пар по переставкам
    top_movers = tracker.get_top_movers(15)
    if top_movers:
        st.subheader("🏆 Топ пар по количеству переставок")
        tm_df = pd.DataFrame(top_movers, columns=["Пара", "Кол-во переставок"])
        fig_tm = go.Figure(go.Bar(
            x=[x[0] for x in top_movers],
            y=[x[1] for x in top_movers],
            marker_color="#00d2ff",
        ))
        fig_tm.update_layout(
            template="plotly_dark",
            height=300,
            xaxis_title="Пара",
            yaxis_title="Переставок",
        )
        st.plotly_chart(fig_tm, use_container_width=True)


# ─── Футер ───

st.markdown("---")
st.caption(
    "MEXC Density Scanner v2.0 | "
    "Данные получены через публичный API MEXC | "
    "Не является финансовой рекомендацией"
)
