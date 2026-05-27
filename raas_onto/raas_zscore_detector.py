"""
RAAS Robust Z-score 기반 이상 감지 엔진

설계 원칙:
- 고정 임계값 대신 Rolling 30일 Robust Z-score (MAD 기반)
- 카테고리(플랫폼/채널/프로그램) 독립 baseline — 스케일 차이 자동 흡수
- 기간(일/주/월) 분리 평가
- 공휴일 당일 볼륨 알림 억제
- n < 14: 계산 건너뜀 / 14~29: YELLOW만 / 30+: RED 허용
- 분기 검토용 calibration_report() 내장
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import NamedTuple


# =============================================================================
# 지표 모니터링 설정
# =============================================================================

class MetricConfig(NamedTuple):
    field: str          # CSV 필드명
    label: str          # 한국어 레이블
    direction: str      # 'lower_is_bad' | 'higher_is_bad'
    period: str         # 'day' | 'week' | 'month'
    categories: frozenset  # {'platform', 'channel', 'program'} 중 해당 카테고리


_ALL = frozenset({'platform', 'channel', 'program'})
_PC  = frozenset({'platform', 'channel'})
_P   = frozenset({'platform'})

METRIC_CONFIGS: list[MetricConfig] = [
    # ── 일간 공통 ────────────────────────────────────────────────
    MetricConfig('dau_chg',         'DAU WoW',         'lower_is_bad',  'day',  _ALL),
    MetricConfig('deep_rate_diff',  '깊은청취율 Δ',    'lower_is_bad',  'day',  _ALL),
    MetricConfig('churn_rate_diff', '이탈률 Δ',        'higher_is_bad', 'day',  _ALL),

    # ── 일간 플랫폼+채널 ─────────────────────────────────────────
    MetricConfig('new_chg',         '신규 WoW',        'lower_is_bad',  'day',  _PC),

    # ── 일간 플랫폼 전용 ─────────────────────────────────────────
    MetricConfig('habit_rate',      '습관형성률',      'lower_is_bad',  'day',  _P),
    MetricConfig('react_rate_diff', '복귀율 Δ',        'lower_is_bad',  'day',  _P),
    MetricConfig('new_d1_ret',      '신규D1 유지율',   'lower_is_bad',  'day',  _P),
    MetricConfig('new_d7_ret',      '신규D7 유지율',   'lower_is_bad',  'day',  _P),

    # ── 주간 (월요일만 평가) ─────────────────────────────────────
    MetricConfig('dau_chg',         'WAU WoW(주간)',    'lower_is_bad',  'week', _ALL),
    MetricConfig('deep_rate_week',  '깊은청취율(주)',   'lower_is_bad',  'week', _PC),
]

# 주간 지표 평가 시 실제로 읽어올 field 매핑 (week period에서는 _week suffix 사용)
_WEEK_FIELD_OVERRIDE = {
    'dau_chg': 'dau_chg',          # WAU WoW도 동일 필드 (주간 집계 시점에만 의미)
    'deep_rate_week': 'deep_rate_week',
}


# =============================================================================
# Robust Z-score 엔진
# =============================================================================

class ZScoreDetector:
    """Robust Z-score (MAD 기반) 이상 감지 엔진."""

    PLATFORM_CODES = frozenset({'T00'})
    CHANNEL_CODES  = frozenset({'F00', 'L00', 'G00', 'P00'})
    EXCLUDE_CODES  = frozenset({'L04'})          # 분류 불가 코드 제외

    # 공휴일 당일 억제 대상 (볼륨 하락은 예상된 패턴)
    _HOLIDAY_SUPPRESS_FIELDS = frozenset({'dau_chg', 'new_chg'})

    def __init__(
        self,
        window_days: int = 30,
        z_yellow: float = 2.0,
        z_red: float = 3.0,
        min_samples: int = 14,
    ):
        self.window_days = window_days
        self.z_yellow    = z_yellow
        self.z_red       = z_red
        self.min_samples = min_samples

    # ── 카테고리 분류 ─────────────────────────────────────────────

    def _category(self, code: str) -> str:
        if code in self.PLATFORM_CODES: return 'platform'
        if code in self.CHANNEL_CODES:  return 'channel'
        return 'program'

    # ── 히스토리 수집 ─────────────────────────────────────────────

    def _collect_history(
        self,
        date_rows: dict,
        field: str,
        ref_dt: datetime,
    ) -> list[float]:
        """ref_dt 이전 window_days일 이내 유효값 반환."""
        values = []
        for d_str, row in date_rows.items():
            try:
                d_dt = datetime.strptime(d_str.replace('/', '-'), '%Y-%m-%d')
            except ValueError:
                continue
            delta = (ref_dt - d_dt).days
            if 1 <= delta <= self.window_days:
                raw = row.get(field)
                if raw is not None:
                    try:
                        v = float(raw)
                        if math.isfinite(v):
                            values.append(v)
                    except (TypeError, ValueError):
                        pass
        return values

    # ── Robust baseline ───────────────────────────────────────────

    @staticmethod
    def _robust_baseline(values: list[float]) -> dict | None:
        """median + MAD × 1.4826 (정규분포 일관성 계수)."""
        n = len(values)
        if n < 2:
            return None
        sorted_v = sorted(values)
        mid = n // 2
        median = (sorted_v[mid - 1] + sorted_v[mid]) / 2 if n % 2 == 0 else sorted_v[mid]
        deviations = sorted([abs(v - median) for v in values])
        mad = (deviations[mid - 1] + deviations[mid]) / 2 if n % 2 == 0 else deviations[mid]
        robust_std = mad * 1.4826
        return {'median': median, 'robust_std': robust_std, 'n': n}

    # ── Z-score 계산 ──────────────────────────────────────────────

    @staticmethod
    def _compute_z(value: float, baseline: dict) -> float | None:
        if baseline['robust_std'] < 1e-9:
            return None
        return (value - baseline['median']) / baseline['robust_std']

    # ── 등급 분류 ─────────────────────────────────────────────────

    def _classify(self, z: float | None, direction: str, n: int) -> str | None:
        """z-score → 'red' | 'yellow' | 'green' | None."""
        if z is None:
            return None
        red_allowed = (n >= 30)

        if direction == 'lower_is_bad':
            if z <= -self.z_red   and red_allowed: return 'red'
            if z <= -self.z_yellow:                return 'yellow'
            if z >=  self.z_red:                   return 'green'
        elif direction == 'higher_is_bad':
            if z >=  self.z_red   and red_allowed: return 'red'
            if z >=  self.z_yellow:                return 'yellow'
            if z <= -self.z_red:                   return 'green'
        return None

    # ── 메인 평가 ─────────────────────────────────────────────────

    def evaluate(
        self,
        timeline: dict,
        ref_date: str,
        is_holiday: bool = False,
    ) -> list[dict]:
        """
        ref_date 기준 전체 이상 알림 평가.

        Args:
            timeline:   {code: {date_str: row_dict}}
            ref_date:   평가 기준일 ('YYYY/MM/DD' 또는 'YYYY-MM-DD')
            is_holiday: True면 볼륨 하락 알림 억제

        Returns:
            알림 딕셔너리 리스트
            [{code, category, field, label, value, z, level,
              baseline_median, baseline_n, direction}, ...]
        """
        try:
            ref_dt = datetime.strptime(ref_date.replace('/', '-'), '%Y-%m-%d')
        except ValueError:
            return []

        is_monday = (ref_dt.weekday() == 0)
        alerts: list[dict] = []
        seen: set[tuple] = set()    # (code, field, period) 중복 방지

        for cfg in METRIC_CONFIGS:
            # 주간 지표는 월요일만
            if cfg.period == 'week' and not is_monday:
                continue

            for code, date_rows in timeline.items():
                if code in self.EXCLUDE_CODES:
                    continue

                cat = self._category(code)
                if cat not in cfg.categories:
                    continue

                dedup_key = (code, cfg.field, cfg.period)
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                # 현재값
                current_row = date_rows.get(ref_date, {})
                raw = current_row.get(cfg.field)
                if raw is None:
                    continue
                try:
                    value = float(raw)
                    if not math.isfinite(value):
                        continue
                except (TypeError, ValueError):
                    continue

                # 히스토리 & baseline
                history = self._collect_history(date_rows, cfg.field, ref_dt)
                if len(history) < self.min_samples:
                    continue

                baseline = self._robust_baseline(history)
                if baseline is None:
                    continue

                z = self._compute_z(value, baseline)
                level = self._classify(z, cfg.direction, baseline['n'])

                if level is None:
                    continue

                # 공휴일 당일: 볼륨 하락 알림 억제
                if (is_holiday
                        and cfg.field in self._HOLIDAY_SUPPRESS_FIELDS
                        and level in ('red', 'yellow')
                        and z < 0):
                    continue

                alerts.append({
                    'code':             code,
                    'category':         cat,
                    'field':            cfg.field,
                    'label':            cfg.label,
                    'period':           cfg.period,
                    'value':            round(value, 2),
                    'z':                round(z, 2),
                    'level':            level,
                    'baseline_median':  round(baseline['median'], 2),
                    'baseline_n':       baseline['n'],
                    'direction':        cfg.direction,
                })

        # 심각도 정렬: red → yellow → green, z 절댓값 내림차순
        _order = {'red': 0, 'yellow': 1, 'green': 2}
        alerts.sort(key=lambda a: (_order[a['level']], -abs(a['z'])))
        return alerts

    # ── 분기 검토용 calibration report ───────────────────────────

    def generate_calibration_report(
        self,
        timeline: dict,
        start_date: str,
        end_date: str,
    ) -> dict:
        """
        start_date ~ end_date 기간 동안 매일 evaluate()를 돌려
        알림 발생률, 지표별 통계를 집계해 반환.

        분기 검토 Claude 프롬프트에 이 결과를 넣어 파라미터 조정 판단.
        """
        try:
            s_dt = datetime.strptime(start_date.replace('/', '-'), '%Y-%m-%d')
            e_dt = datetime.strptime(end_date.replace('/', '-'), '%Y-%m-%d')
        except ValueError:
            return {}

        all_alerts: list[dict] = []
        total_days = 0
        d = s_dt
        while d <= e_dt:
            d_str = d.strftime('%Y/%m/%d')
            alerts = self.evaluate(timeline, d_str)
            if alerts or any(d_str in rows for rows in timeline.values()):
                total_days += 1
                all_alerts.extend(alerts)
            d += timedelta(days=1)

        total_evals = len(all_alerts)
        yellow_n = sum(1 for a in all_alerts if a['level'] == 'yellow')
        red_n    = sum(1 for a in all_alerts if a['level'] == 'red')
        green_n  = sum(1 for a in all_alerts if a['level'] == 'green')

        # 지표별 발생률
        field_stats: dict[str, dict] = {}
        for a in all_alerts:
            key = f"{a['field']}({a['category']})"
            fs = field_stats.setdefault(key, {'yellow': 0, 'red': 0, 'green': 0})
            fs[a['level']] += 1

        return {
            'period':       f'{start_date} ~ {end_date}',
            'total_days':   total_days,
            'total_alerts': total_evals,
            'yellow_n':     yellow_n,
            'red_n':        red_n,
            'green_n':      green_n,
            'yellow_rate':  round(yellow_n / max(total_days, 1), 2),
            'red_rate':     round(red_n    / max(total_days, 1), 2),
            'params': {
                'window_days': self.window_days,
                'z_yellow':    self.z_yellow,
                'z_red':       self.z_red,
                'min_samples': self.min_samples,
            },
            'field_stats':  field_stats,
            'alert_log':    all_alerts,
        }


# =============================================================================
# 싱글톤
# =============================================================================

_detector: ZScoreDetector | None = None


def get_detector() -> ZScoreDetector:
    global _detector
    if _detector is None:
        _detector = ZScoreDetector()
    return _detector
