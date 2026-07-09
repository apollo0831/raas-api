# -*- coding: utf-8 -*-
"""종방프로그램.csv → raas_ontology_airing.ttl 생성기.
자리(채널,시작시각) 승계로 시작일 유도, 종료일은 권위(CSV). 빈 LASTDAY=현행(제외)."""
import csv, io, sys
from datetime import date, timedelta
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

SRC = r"C:\Users\USER\Downloads\종방프로그램.csv"
OUT = r"C:\Users\USER\raas_api\raas_onto\raas_ontology_airing.ttl"

CHN_CODE = {"FM": "F00", "AM": "L00"}

def parse_ymd(s):
    s = (s or "").strip()
    if len(s) != 8 or not s.isdigit():
        return None
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

rows = []
with open(SRC, encoding="utf-8-sig") as f:
    for r in csv.DictReader(f):
        end = parse_ymd(r["LASTDAY"])
        rows.append({
            "chn": CHN_CODE.get(r["CHN"].strip(), r["CHN"].strip()),
            "seq": r["SEQ"].strip(),
            "name": r["PGM_NAME"].strip(),
            "start": r["START_TIME"].strip().zfill(4),
            "end_t": r["END_TIME"].strip().zfill(4),
            "end_d": end,
            "end_raw": (r["LASTDAY"] or "").strip(),
        })

# 빈 LASTDAY = 현행 방송분(종영 아님) → 제외
current = [r for r in rows if not r["end_d"]]
ended = [r for r in rows if r["end_d"]]
print(f"총 {len(rows)}행 · 종영 {len(ended)} · 현행(빈 종료일, 제외) {len(current)}")
for r in current:
    print(f"  현행제외: {r['chn']} {r['seq']} {r['name']} {r['start']}-{r['end_t']}")

def hhmm_to_min(s):
    return int(s[:2]) * 60 + int(s[2:])

# 자리 정규화 = 정시 내림(floor-to-hour). 도메인 규칙:
#   러브FM(AM)은 정시 5분 뉴스 후 편성돼 09:05~10:00처럼 5분 늦게 시작하나,
#   청취 분석은 시간 단위(09:00~10:00)로 집계한다(5분 뉴스는 별도 분석 안 함).
#   → 실제 편성시각(slotStartTime)은 원본 보존, 분석 자리(analysis_slot)는 정시로 정규화.
#   09시/10시는 다른 시간대로 분리(정치쇼처럼 시대별 시간대 이동을 그대로 반영),
#   :05 뉴스 오프셋(16:05→16:00 등)은 정시로 흡수.
def floor_hour(hhmm):
    return hhmm[:2] + "00"   # "0905" → "0900", "2030" → "2000"

# 자리 = (채널, 분석 정시). 종료일 순 정렬 → 시작일 유도(직전 종료+1)
slots = defaultdict(list)
for r in ended:
    r["analysis_slot"] = floor_hour(r["start"])
    slots[(r["chn"], r["analysis_slot"])].append(r)

anomalies = []
for key, lst in slots.items():
    lst.sort(key=lambda r: r["end_d"])
    prev_end = None
    for r in lst:
        if prev_end is not None:
            r["start_d"] = prev_end + timedelta(days=1)
            r["start_prov"] = "derived"  # 직전 편성 종료 다음날
            gap = (r["end_d"] - r["start_d"]).days
            if gap < 0:
                anomalies.append(f"역전: {key} {r['name']} 시작{r['start_d']}>종료{r['end_d']}")
        else:
            r["start_d"] = None
            r["start_prov"] = "unknown"  # 자리 최초 기록분 — 이전 미상
        prev_end = r["end_d"]

print("\n=== 자리별 승계 체인 (정규화 자리) ===")
for key in sorted(slots.keys()):
    ch, st = key
    lst = slots[key]
    times = sorted({r["start"] for r in lst}, key=hhmm_to_min)
    tnote = "" if len(times) == 1 else f"  (원본시각 {'/'.join(times)})"
    print(f"\n[{ch} {st[:2]}:{st[2:]}] ({len(lst)}편){tnote}")
    for r in lst:
        sd = r["start_d"].isoformat() if r["start_d"] else "(미상)"
        print(f"   {sd} ~ {r['end_d'].isoformat()}  {r['name']}  <{r['seq']}>")

if anomalies:
    print("\n!! 이상:", *anomalies, sep="\n  ")
else:
    print("\n이상(시작>종료 역전) 없음")

# ---- TTL 생성 ----
PRE = """@prefix raas: <http://raas.sbs.co.kr/onto#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

#################################################################
# 프로그램 편성 이력 (ProgramAiring) — 종영분 = 불변 사실
#   출처: 종방프로그램.csv (SBS 편성). 종료일=권위, 시작일=유도(같은 자리 직전 종료+1일).
#   자리(slot)=(채널, 분석 정시). 실제 편성시각은 slotStartTime/End에 원본 보존, 분석은 정시.
#   SEQ는 시대별로 드리프트하므로 legacySeq(참고)로만 보존.
#   현행 방송분은 여기 없음 — 라이브(broadplan) 데이터가 소유(가변 종료).
#################################################################

raas:ProgramAiring a rdfs:Class ;
    rdfs:label "프로그램 편성분"@ko ;
    rdfs:comment "특정 채널의 특정 시간대 자리에서 한 프로그램이 방송된 기간. 종영분은 불변 이력."@ko .

raas:inChannel        a rdf:Property ; rdfs:label "방송 채널"@ko .
raas:airProgramName   a rdf:Property ; rdfs:label "프로그램명"@ko .
raas:slotStartTime    a rdf:Property ; rdfs:label "편성 시작시각(HHMM, 실제)"@ko ;
    rdfs:comment "실제 편성 시작시각. 러브FM은 정시 5분 뉴스 후라 :05로 시작할 수 있음(예: 0905)."@ko .
raas:slotEndTime      a rdf:Property ; rdfs:label "편성 종료시각(HHMM, 실제)"@ko .
raas:analysisSlotStart a rdf:Property ; rdfs:label "분석 자리 시각(정시)"@ko ;
    rdfs:comment "청취 분석용 정시 정규화(시작시각 정시 내림). 09:05 편성도 09:00으로 집계."@ko .
raas:airStartDate     a rdf:Property ; rdfs:label "방송 시작일(유도)"@ko ;
    rdfs:comment "같은 자리 직전 편성 종료 다음날로 추정. 자리 최초 기록분은 미상."@ko .
raas:airEndDate       a rdf:Property ; rdfs:label "방송 종료일(종영일)"@ko .
raas:startDateProvenance a rdf:Property ; rdfs:label "시작일 출처"@ko .
raas:legacySeq        a rdf:Property ; rdfs:label "원본 편성코드(참고)"@ko .

# 도메인 공리 — 시간 단위 편성 분석(정시 5분 뉴스 오프셋)
raas:HourSlotAnalysisAxiom a raas:DomainAxiom ;
    rdfs:label "시간 단위 편성 분석"@ko ;
    raas:appliesTo raas:L00 ;
    rdfs:comment "러브FM(AM)은 정시에 5분 뉴스가 편성돼 그 뒤 프로그램은 09:05~10:00처럼 5분 늦게 시작한다. 청취 분석은 시간 단위로 집계하므로 09:05 시작 편성도 09:00~10:00 자리로 처리한다(5분 뉴스 자체는 별도 분석하지 않음). 각 편성분의 실제 시작·종료시각은 slotStartTime/slotEndTime에 원본대로 보존하고, 분석 자리는 analysisSlotStart(정시)로 정규화한다. 시각이 다른 시간대로 이동한 경우(예: 정치쇼가 9시대→10시대→7시대로 이동)는 서로 다른 자리로 본다."@ko .

"""

def esc(s):
    return s.replace('\\', '\\\\').replace('"', '\\"')

lines = [PRE]
for key in sorted(slots.keys()):
    for r in slots[key]:
        iri = f"raas:Airing_{r['chn']}_{r['start']}_{r['end_raw']}"
        b = [f"{iri} a raas:ProgramAiring ;"]
        b.append(f'    rdfs:label "{esc(r["name"])}"@ko ;')
        b.append(f"    raas:inChannel raas:{r['chn']} ;")
        b.append(f'    raas:airProgramName "{esc(r["name"])}"@ko ;')
        b.append(f'    raas:slotStartTime "{r["start"]}" ;')
        b.append(f'    raas:slotEndTime "{r["end_t"]}" ;')
        b.append(f'    raas:analysisSlotStart "{r["analysis_slot"]}" ;')
        if r["start_d"]:
            b.append(f'    raas:airStartDate "{r["start_d"].isoformat()}" ;')
        b.append(f'    raas:startDateProvenance "{r["start_prov"]}" ;')
        b.append(f'    raas:airEndDate "{r["end_d"].isoformat()}" ;')
        b.append(f'    raas:legacySeq "{r["seq"]}" .')
        lines.append("\n".join(b))

with open(OUT, "w", encoding="utf-8") as f:
    f.write("\n\n".join(lines) + "\n")
print(f"\n생성: {OUT} ({len(ended)} airings)")
