"""
Хранилище снимков стаканов для детекции переставляшей.
Работает in-memory (для Streamlit session_state) или через SQLite (для VPS).
"""
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from analyzer import ScanResult, MoverEvent, detect_movers
import config


@dataclass
class SymbolHistory:
    """История снимков одной пары"""
    snapshots: deque = field(default_factory=lambda: deque(maxlen=config.MAX_SNAPSHOTS_PER_PAIR))
    mover_events: list[MoverEvent] = field(default_factory=list)
    first_seen: float = 0.0
    last_seen: float = 0.0
    total_scans: int = 0

    @property
    def mover_count(self) -> int:
        return len(self.mover_events)


class DensityTracker:
    """
    Отслеживает плотности во времени.
    Хранит снимки и детектит переставляшей при каждом обновлении.
    """

    def __init__(self):
        self.histories: dict[str, SymbolHistory] = defaultdict(SymbolHistory)
        self.all_mover_events: list[MoverEvent] = []
        self.scan_count = 0
        self.last_scan_time = 0.0

    def update(self, results: list[ScanResult]) -> list[MoverEvent]:
        """
        Обновляет трекер новыми результатами сканирования.
        Возвращает список новых событий переставляшей.
        """
        self.scan_count += 1
        self.last_scan_time = time.time()
        new_events = []

        for result in results:
            sym = result.symbol
            hist = self.histories[sym]

            # Сравниваем с предыдущим снимком
            if hist.snapshots:
                prev = hist.snapshots[-1]
                events = detect_movers(result, prev)
                if events:
                    result.mover_events = events
                    hist.mover_events.extend(events)
                    new_events.extend(events)

                    # Обрезаем старые события (оставляем последние 100)
                    if len(hist.mover_events) > 100:
                        hist.mover_events = hist.mover_events[-100:]

            # Сохраняем снимок
            hist.snapshots.append(result)
            hist.total_scans += 1
            hist.last_seen = result.timestamp
            if hist.first_seen == 0:
                hist.first_seen = result.timestamp

        self.all_mover_events.extend(new_events)
        if len(self.all_mover_events) > 500:
            self.all_mover_events = self.all_mover_events[-500:]

        return new_events

    def get_symbol_history(self, symbol: str) -> SymbolHistory:
        return self.histories.get(symbol, SymbolHistory())

    def get_active_movers(self, window_sec: int = 3600) -> list[MoverEvent]:
        """Возвращает переставляшей за последние N секунд"""
        cutoff = time.time() - window_sec
        return [e for e in self.all_mover_events if e.timestamp >= cutoff]

    def get_top_movers(self, n: int = 20) -> list[tuple[str, int]]:
        """Пары с наибольшим количеством переставок"""
        counts = {}
        for sym, hist in self.histories.items():
            if hist.mover_count > 0:
                counts[sym] = hist.mover_count
        sorted_pairs = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        return sorted_pairs[:n]

    def get_stats(self) -> dict:
        return {
            "total_pairs_tracked": len(self.histories),
            "total_scans": self.scan_count,
            "total_mover_events": len(self.all_mover_events),
            "pairs_with_movers": sum(
                1 for h in self.histories.values() if h.mover_count > 0
            ),
        }
