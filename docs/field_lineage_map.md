# raas_kpi_latest.csv 필드 계보 매핑

`raas_kpi_latest.csv`의 각 필드가 **어느 룩업 · 정제작업(서머리인덱스)** 에서 오는지 정리한 문서.
데이터 확인 기능이 결측/이상 필드의 원인 배치를 안내하는 근거이며, 특정 필드가 이상할 때
어느 스플렁크 정제작업을 점검할지 빠르게 찾는 용도.

- **레이어 A**(필드↔룩업): `raas_kpi_save.spl` 쿼리에서 추출 (authority)
- **레이어 B**(룩업↔정제작업·실행시간·서머리인덱스): `RAAS온톨로지.xlsx` / 'SummaryIndex 및 룩업_생성정보' 시트
- 원본 데이터: [`data/field_lineage.json`](../data/field_lineage.json) — 재생성: `python build_field_lineage.py`
- 이 표는 위 JSON에서 생성한 것이라 데이터 확인 화면 안내와 동일함. `_prev`/`_chg`/`_diff`/`_share` 변형은 base 필드와 계보 동일.

---

## ① 필드 → 소스 룩업 · 정제작업(실행시간)

### 일간
| 필드 | 룩업 · 정제작업(실행) |
|---|---|
| `dau` | program_user_funnel_day · 사용자_퍼널_일 **(01:21)** |
| `dau_r7` | program_user_funnel_day · 사용자_퍼널_**주_롤링(01:35)** |
| `dau_r30` | program_user_funnel_day · 사용자_퍼널_**월_롤링(02:50)** |
| `dau_1min` `dau_10min` `real_rate` `deep_rate` | program_user_funnel_day · 사용자_퍼널_일 **(01:21)** |
| `new` | program_newuser_funnel_day · 신규_퍼널_일 **(01:20)** |
| `react` | program_reactuser_funnel_day · 복귀_퍼널_일 **(02:10)** |
| `react_rate` | 분자 program_reactuser_funnel_day**(02:10)** / 분모 **program_dormant_pool_day(01:40)** |
| `churn_rate` | 분자 program_churnuser_funnel_day**(01:58)** / 분모 program_user_funnel_day**(01:21)** |
| `d1_ret` `d7_ret` | program_user_retention_day · 유지율_일 **(01:13)** |
| `new_d1_ret` `new_d7_ret` | program_newuser_retention_day · 신규유지율_일 **(01:21)** |
| `engage_rate` | 분자 program_user_engage_day**(01:07)** / 분모 program_user_funnel_day**(01:21)** |
| `habit_rate` | program_newuser_funnel_day · 신규_퍼널_일 **(01:20)** (TYPE=3D7D_1M ÷ ALL) |

### 주간
| 필드 | 룩업 · 정제작업(실행) |
|---|---|
| `wau` `wau_1min` `wau_10min` `real_rate_week` `deep_rate_week` | program_user_funnel_week · 사용자_퍼널_주 **(월 1:35)** |
| `new_week` | program_newuser_funnel_week **(월 1:41)** |
| `react_week` | program_reactuser_funnel_week **(월 2:30)** |
| `react_rate_week` | 분자 program_reactuser_funnel_week**(월2:30)** / 분모 program_dormant_pool_week**(월1:45)** |
| `churn_rate_week` | 분자 program_churnuser_funnel_week**(월2:05)** / 분모 program_user_funnel_week**(월1:35)** |
| `w1_ret` | program_user_retention_week **(월 1:22)** |
| `new_w1_ret` | program_newuser_retention_week **(월 1:23)** |
| `engage_rate_week` | 분자 program_user_engage_week**(월1:08)** / 분모 program_user_funnel_week**(월1:35)** |
| `habit_rate_week` | program_newuser_funnel_week **(월 1:41)** (TYPE=3D7D_1M ÷ ALL) |

### 월간
| 필드 | 룩업 · 정제작업(실행) |
|---|---|
| `mau` `mau_1min` `mau_10min` `real_rate_mon` `deep_rate_mon` | program_user_funnel_mon · 사용자_퍼널_월 **(1일 02:50)** |
| `new_mon` | program_newuser_funnel_mon **(1일 02:31)** |
| `react_mon` | program_reactuser_funnel_mon **(1일 3:10)** |
| `react_rate_mon` | 분자 program_reactuser_funnel_mon**(1일3:10)** / 분모 program_dormant_pool_mon**(1일1:50)** |
| `churn_rate_mon` | 분자 program_churnuser_funnel_mon**(1일2:50)** / 분모 program_user_funnel_mon**(1일02:50)** |
| `m1_ret` | program_user_retention_mon **(1일 3:10)** |
| `new_m1_ret` | program_newuser_retention_mon **(1일 1:50)** |
| `engage_rate_mon` | 분자 program_user_engage_mon**(1일01:09)** / 분모 program_user_funnel_mon**(1일02:50)** |
| `habit_rate_mon` | program_newuser_funnel_mon · **습관형성률_월(7일 3:30)** + 신규_퍼널_월(1일02:31) |

### 편성/메타
| 필드 | 소스 |
|---|---|
| `PGM_NAME` `STIME` | BROADPLAN.csv (정적 룩업 — 스케줄 배치 아님) |
| `program_title` `guestname` `daily_corner` `weekly_corner` `view_radio_yn` | index=broadplan(프로그램편성정보, 매시간) + BROADPLAN.csv |
| `live_yn` | index=nike(편성정보, 매시간) + BROADPLAN.csv |

---

## ② 정제작업(룩업) → 영향 필드 (역방향)

배치가 실패했을 때 어떤 필드가 영향받는지 확인용.

| 룩업 · 배치(실행) | 실패 시 영향 필드 |
|---|---|
| **program_user_funnel_day.csv** 일(01:21) | `dau` `dau_1min` `dau_10min` `real_rate` `deep_rate` + `churn_rate`·`engage_rate` 분모 |
| ┗ 주_롤링(01:35) / 월_롤링(02:50) | `dau_r7` / `dau_r30` |
| **program_user_funnel_week/mon** | `wau*`·`mau*`·`real_rate_*`·`deep_rate_*` + 주/월 churn·engage 분모 |
| **program_newuser_funnel_*** | `new*` `habit_rate*` |
| **program_reactuser_funnel_*** | `react*` + `react_rate*` 분자 |
| **program_dormant_pool_*** | `react_rate*` 분모 |
| **program_churnuser_funnel_*** | `churn_rate*` 분자 |
| **program_user_engage_*** (← summary_index_14/21/26) | `engage_rate*` 분자 |
| **program_user_retention_*** | `d1_ret` `d7_ret` `w1_ret` `m1_ret` |
| **program_newuser_retention_*** | `new_d1_ret` `new_d7_ret` `new_w1_ret` `new_m1_ret` |
| **BROADPLAN.csv / index=broadplan·nike** | 편성/메타 6종 |

---

## 진단 시 주의점

- **비율 지표는 소스 2개** — `react_rate` · `churn_rate` · `engage_rate`는 분자/분모가 서로 다른 배치라
  한쪽만 실패해도 값이 틀어짐. 둘 다 점검해야 함.
- **`dau` 정상인데 `dau_r7`/`dau_r30`만 이상** → 01:35/02:50 롤링 배치만 실패한 것 (같은 룩업, 다른 배치).
- **`d7_ret`과 `new_d7_ret`은 다른 룩업** (전체 코호트 vs 신규 코호트) — 따로 깨질 수 있음.
- **1MIN/10MIN은 PERIOD 조건 필수** — 조건 누락 시 30D 롤링값이 섞여 `deep_rate`/`real_rate`가
  "없음"이 아니라 "그럴듯하게 틀림"으로 나옴 (탐지 난이도 최상).
- **참여율은 앞단에 서머리인덱스**(summary_index_14/21/26 → program_user_engage_*.csv)가 있어,
  csv가 정상인데 값이 이상하면 서머리인덱스까지 거슬러 확인.
