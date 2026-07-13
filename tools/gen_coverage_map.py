"""커버리지 맵 생성기 — raas_metrics_registry.coverage()를 아트팩트 HTML로 렌더.

    python tools/gen_coverage_map.py [출력경로]

레지스트리 선언 + 온톨로지 인벤토리에서 자동 생성하므로, 데이터/온톨로지를 추가하면
재실행만으로 맵이 갱신된다(손으로 그리지 않는다). 기본 출력은 스크래치패드의 raas_map.html.
"""
import sys, os, html, datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import raas_metrics_registry as REG

_GRAINS = [
    ("daily", "일간 격자 · (코드 × 날짜)", "교차분석이 가능한 공통 좌표"),
    ("minute", "분 단위 · 실시간", "저장소=Splunk, 창 단위 조회만 (일간 격자와 별개)"),
    ("editorial", "편성·서술형", "숫자가 아닌 사실·연혁 (불변은 온톨로지, 당일은 라이브)"),
]


def _chips(items, cls=""):
    return "".join(f'<div class="chip {cls}">{html.escape(str(i))}</div>' for i in items)


def _band(s):
    metrics = s["onto_metrics"]
    onto = _chips(metrics, "onto") if metrics else '<div class="chip onto warn">지표 수준 정의 없음(속성)</div>'
    periods = "·".join(s["available_periods"]) or "—"
    dims = " · ".join(f'{k}({len(v)})' for k, v in s["extra_dims"].items())
    meta = html.escape(f"창 {periods}" + (f" · {dims}" if dims else ""))
    cnt = f'{len(metrics)} 지표' if metrics else "속성"
    note = f'<div class="bnote">{html.escape(s["note"])}</div>' if s.get("note") else ""
    return f'''    <div class="band">
      <div class="flowline"></div>
      <div class="band-label"><span class="name">{html.escape(s["label"])}</span><span class="cnt">{cnt}</span><span class="meta">{meta}</span></div>
      <div class="cell c-src">{_chips(s["lookups"])}{note}</div>
      <div class="cell c-onto">{onto}</div>
      <div class="cell c-prov">{_chips(s["providers"], "prov")}</div>
    </div>'''


def render(cov):
    st = cov["stats"]
    sources = cov["sources"]
    groups = ""
    for grain, title, sub in _GRAINS:
        bands = "\n".join(_band(s) for s in sources if s["grain"] == grain)
        if not bands:
            continue
        groups += f'''  <div class="grain">
    <div class="grain-tag">축 <b>{html.escape(title)}</b> — {html.escape(sub)}</div>
{bands}
  </div>
'''
    daily_nodes = [s["label"] for s in sources if s["grain"] == "daily" and s["onto_metrics"]]
    relnodes = ('<span class="redge">?—?</span>'.join(f'<span class="rnode">{html.escape(n)}</span>' for n in daily_nodes))

    warn = ""
    if st["uncovered"]:
        warn += f'<div class="note"><span class="mk m">●</span><div><b>데이터 공백</b> — 온톨로지엔 있는데 소스 미선언: {html.escape(", ".join(st["uncovered"]))}</div></div>'
    if st["undefined"]:
        warn += f'<div class="note"><span class="mk m">●</span><div><b>정의 공백</b> — 소스가 채운다는데 온톨로지에 없음(철자?): {html.escape(", ".join(st["undefined"]))}</div></div>'

    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return _TEMPLATE.format(
        stat_src=st["sources"], stat_met=st["metrics"], stat_cov=st["covered"],
        stat_rel=st["relations"], groups=groups, relnodes=relnodes, warn=warn, stamp=stamp)


_TEMPLATE = '''<title>RAAS 정보 체계 맵 — 데이터 × 온톨로지</title>
<style>
  :root{{
    --bg:#eef1f7; --surface:#ffffff; --surface-2:#f5f7fc;
    --ink:#1a2233; --ink-soft:#5c6675; --ink-faint:#8a93a4;
    --line:#d9dfea; --line-soft:#e7ecf4;
    --accent:#1f9182; --accent-soft:#1f91821a;
    --good:#2e9e6b; --warn:#bd8113; --miss:#c8503f;
    --good-bg:#2e9e6b1a; --warn-bg:#bd81131f; --miss-bg:#c8503f1a;
    --mono:ui-monospace,"SFMono-Regular","Cascadia Mono","Consolas",monospace;
    --sans:system-ui,-apple-system,"Segoe UI",Roboto,"Malgun Gothic","Apple SD Gothic Neo",sans-serif;
    --r:10px; --r-sm:7px;
  }}
  @media (prefers-color-scheme:dark){{
    :root{{
      --bg:#0d1119; --surface:#151b26; --surface-2:#1b2330;
      --ink:#e7ecf5; --ink-soft:#9aa6b8; --ink-faint:#69748a;
      --line:#28303f; --line-soft:#1f2733;
      --accent:#43c7b4; --accent-soft:#43c7b422;
      --good:#46c78a; --warn:#e0a94a; --miss:#e0705c;
      --good-bg:#46c78a1e; --warn-bg:#e0a94a1c; --miss-bg:#e0705c1c;
    }}
  }}
  :root[data-theme="dark"]{{
    --bg:#0d1119; --surface:#151b26; --surface-2:#1b2330;
    --ink:#e7ecf5; --ink-soft:#9aa6b8; --ink-faint:#69748a;
    --line:#28303f; --line-soft:#1f2733;
    --accent:#43c7b4; --accent-soft:#43c7b422;
    --good:#46c78a; --warn:#e0a94a; --miss:#e0705c;
    --good-bg:#46c78a1e; --warn-bg:#e0a94a1c; --miss-bg:#e0705c1c;
  }}
  :root[data-theme="light"]{{
    --bg:#eef1f7; --surface:#ffffff; --surface-2:#f5f7fc;
    --ink:#1a2233; --ink-soft:#5c6675; --ink-faint:#8a93a4;
    --line:#d9dfea; --line-soft:#e7ecf4;
    --accent:#1f9182; --accent-soft:#1f91821a;
    --good:#2e9e6b; --warn:#bd8113; --miss:#c8503f;
    --good-bg:#2e9e6b1a; --warn-bg:#bd81131f; --miss-bg:#c8503f1a;
  }}
  *{{box-sizing:border-box}}
  body{{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sans);line-height:1.5;-webkit-font-smoothing:antialiased;}}
  .wrap{{max-width:1080px;margin:0 auto;padding:32px 24px 72px;}}
  header.masthead{{border-bottom:1px solid var(--line);padding-bottom:20px;margin-bottom:8px;}}
  .eyebrow{{font-family:var(--mono);font-size:11px;letter-spacing:.16em;text-transform:uppercase;color:var(--accent);font-weight:600;}}
  h1{{font-size:clamp(24px,3.6vw,34px);margin:.35em 0 .15em;letter-spacing:-.01em;text-wrap:balance;font-weight:680;}}
  .sub{{color:var(--ink-soft);font-size:15px;max-width:64ch;}}
  .sub b{{color:var(--ink);font-weight:600;}}
  .stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:1px;background:var(--line);border:1px solid var(--line);border-radius:var(--r);overflow:hidden;margin:24px 0 8px;}}
  .stat{{background:var(--surface);padding:14px 16px;}}
  .stat .n{{font-family:var(--mono);font-size:26px;font-weight:600;letter-spacing:-.02em;font-variant-numeric:tabular-nums;line-height:1;}}
  .stat .k{{font-size:12px;color:var(--ink-soft);margin-top:6px;}}
  .stat.flag .n{{color:var(--miss);}} .stat.flag .k{{color:var(--miss);}}
  .legend{{display:flex;flex-wrap:wrap;gap:16px;margin:18px 0 26px;font-size:12.5px;color:var(--ink-soft);}}
  .legend span{{display:inline-flex;align-items:center;gap:6px;}}
  .dot{{width:9px;height:9px;border-radius:50%;flex:none;}}
  .dot.good{{background:var(--good);}} .dot.warn{{background:var(--warn);}} .dot.miss{{background:var(--miss);}}
  .colhead{{display:grid;grid-template-columns:150px 1fr 1fr 1fr;gap:14px;padding:0 4px 8px;}}
  .colhead div{{font-family:var(--mono);font-size:10.5px;letter-spacing:.13em;text-transform:uppercase;color:var(--ink-faint);font-weight:600;}}
  .grain{{margin:6px 0 22px;}}
  .grain-tag{{display:inline-flex;align-items:center;gap:8px;font-family:var(--mono);font-size:11px;letter-spacing:.06em;color:var(--ink-soft);background:var(--surface-2);border:1px solid var(--line);border-radius:20px;padding:4px 12px;margin-bottom:10px;}}
  .grain-tag b{{color:var(--accent);font-weight:600;}}
  .band{{display:grid;grid-template-columns:150px 1fr 1fr 1fr;gap:14px;align-items:stretch;padding:10px;border:1px solid var(--line);border-radius:var(--r);background:var(--surface);margin-bottom:10px;cursor:pointer;transition:border-color .15s,box-shadow .15s;position:relative;}}
  .band:hover{{border-color:var(--accent);}}
  .band.active{{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent),0 6px 22px -12px var(--accent);}}
  body.has-active .band:not(.active){{opacity:.42;}}
  .band-label{{display:flex;flex-direction:column;justify-content:center;gap:3px;}}
  .band-label .name{{font-weight:640;font-size:14px;letter-spacing:-.01em;}}
  .band-label .cnt{{font-family:var(--mono);font-size:11px;color:var(--ink-faint);}}
  .band-label .meta{{font-family:var(--mono);font-size:10px;color:var(--ink-faint);opacity:.85;}}
  .cell{{display:flex;flex-direction:column;gap:5px;justify-content:center;min-width:0;}}
  .chip{{font-size:12.5px;padding:5px 9px;border-radius:var(--r-sm);border:1px solid var(--line);background:var(--surface-2);color:var(--ink);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
  .chip.prov{{font-family:var(--mono);font-size:11.5px;}}
  .chip.warn{{border-color:var(--warn);background:var(--warn-bg);color:var(--warn);white-space:normal;}}
  .bnote{{font-size:11px;color:var(--ink-faint);padding:2px 2px 0;line-height:1.4;}}
  .flowline{{position:absolute;left:150px;right:0;top:50%;height:1px;background:linear-gradient(90deg,var(--accent-soft),transparent);opacity:0;pointer-events:none;}}
  .band.active .flowline{{opacity:1;}}
  .relbox{{margin-top:34px;border:1px dashed var(--miss);border-radius:var(--r);background:var(--miss-bg);padding:20px 22px;}}
  .relbox h3{{margin:0 0 4px;font-size:16px;display:flex;align-items:center;gap:9px;}}
  .relbox h3 .tag{{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--miss);border:1px solid var(--miss);border-radius:4px;padding:2px 7px;}}
  .relbox p{{margin:6px 0 16px;font-size:13.5px;color:var(--ink-soft);max-width:72ch;}}
  .relbox p b{{color:var(--ink);}}
  .relgraph{{display:flex;flex-wrap:wrap;gap:10px;align-items:center;}}
  .rnode{{font-size:12.5px;padding:6px 12px;border-radius:20px;background:var(--surface);border:1px solid var(--line);font-weight:560;}}
  .redge{{font-family:var(--mono);font-size:11px;color:var(--miss);padding:0 2px;opacity:.8;}}
  .relgraph .ex{{width:100%;font-size:12.5px;color:var(--ink-soft);margin-top:10px;font-style:italic;border-left:2px solid var(--miss);padding-left:12px;}}
  section.notes{{margin-top:40px;border-top:1px solid var(--line);padding-top:22px;}}
  section.notes h2{{font-size:14px;font-family:var(--mono);letter-spacing:.1em;text-transform:uppercase;color:var(--ink-faint);margin:0 0 12px;}}
  .note{{display:grid;grid-template-columns:22px 1fr;gap:10px;font-size:13.5px;color:var(--ink-soft);padding:9px 0;border-bottom:1px solid var(--line-soft);}}
  .note:last-child{{border-bottom:0;}} .note b{{color:var(--ink);}}
  .note .mk{{font-family:var(--mono);font-weight:600;}}
  .note .mk.g{{color:var(--good);}} .note .mk.w{{color:var(--warn);}} .note .mk.m{{color:var(--miss);}}
  .foot{{margin-top:28px;font-size:11.5px;color:var(--ink-faint);font-family:var(--mono);}}
  @media (max-width:720px){{
    .colhead{{display:none;}} .band{{grid-template-columns:1fr;}}
    .band-label{{border-bottom:1px solid var(--line-soft);padding-bottom:8px;}}
    .flowline{{display:none;}}
  }}
</style>
<div class="wrap">
  <header class="masthead">
    <div class="eyebrow">RAAS · 정보 체계 진단 · 자동 생성</div>
    <h1>데이터 × 온톨로지 커버리지 맵</h1>
    <p class="sub">레지스트리 선언과 온톨로지 인벤토리에서 <b>코드가 생성</b>한다. 밴드 하나 = 지표 패밀리 하나의 신호 사슬(소스 → 온톨로지 → 답변). 빈칸·경고색이 곧 <b>보강할 지점</b>. 데이터/온톨로지를 추가하고 재생성하면 이 맵이 갱신된다.</p>
  </header>
  <div class="stats">
    <div class="stat"><div class="n">{stat_src}</div><div class="k">데이터 소스(Feed)</div></div>
    <div class="stat"><div class="n">{stat_met}</div><div class="k">온톨로지 지표 정의</div></div>
    <div class="stat"><div class="n">{stat_cov}</div><div class="k">커버된 지표</div></div>
    <div class="stat flag"><div class="n">{stat_rel}</div><div class="k">교차지표 관계 ← 미구축</div></div>
  </div>
  <div class="legend">
    <span><i class="dot good"></i>소스·온톨로지·답변 연결됨</span>
    <span><i class="dot warn"></i>지표 아님(속성 소스)</span>
    <span><i class="dot miss"></i>비어 있음 — 보강 대상</span>
    <span style="color:var(--ink-faint)">· 밴드 클릭 시 사슬 강조</span>
  </div>
  <div class="colhead">
    <div>지표 패밀리</div><div>데이터 소스 (Splunk)</div><div>온톨로지 개념</div><div>답변 provider / scope</div>
  </div>
{groups}
  <div class="relbox">
    <h3><span class="tag">미구축</span> 교차지표 관계 레이어</h3>
    <p>패밀리들은 각자 <b>온톨로지 정의가 잘 되어 있지만</b>, 서로를 잇는 <b>관계(metric ↔ metric)</b>는 아직 {stat_rel}개다. 이 레이어가 비어 있는 한 "DAU 하락이 여성 이탈·참여 저하와 같이 갔나?" 같은 <b>패밀리를 가로지르는 질문</b>은 답할 수 없다 — 통합 계층(Phase 1~3)이 채우는 자리다.</p>
    <div class="relgraph">
      {relnodes}
      <div class="ex">예: "컬투쇼 DAU −8%가 40대 비율 상승·문자참여 −20%와 동반했는가" → 동반움직임·상관·분해로 답할 자리</div>
    </div>
  </div>
  <section class="notes">
    <h2>읽는 법 · 진단 포인트</h2>
    <div class="note"><span class="mk g">●</span><div><b>일간 격자 소스</b>는 공통 좌표(코드×날짜)를 공유 — 교차분석의 재료는 이미 갖춰짐, 잇는 계층만 없다.</div></div>
    <div class="note"><span class="mk w">●</span><div><b>속성 소스</b>(편성 연혁·오늘 게스트)는 지표가 아니라 사실/속성 — 자유질의엔 충분하나 교차분석 대상 아님(의도된 상태).</div></div>
    <div class="note"><span class="mk m">●</span><div><b>교차지표 관계 {stat_rel}개</b> — 데이터·정의는 있는데 "무엇이 무엇과 관련되나"가 없어 인사이트가 패밀리 안에 갇힘.</div></div>
    {warn}
  </section>
  <div class="foot">generated {stamp} · raas_metrics_registry.coverage() → tools/gen_coverage_map.py · 데이터/온톨로지 추가 시 재실행하면 갱신</div>
</div>
<script>
  document.querySelectorAll('.band').forEach(function(b){{
    b.addEventListener('click',function(){{
      var on=b.classList.contains('active');
      document.querySelectorAll('.band').forEach(function(x){{x.classList.remove('active');}});
      document.body.classList.toggle('has-active',!on);
      if(!on) b.classList.add('active');
    }});
  }});
</script>'''


if __name__ == "__main__":
    default_out = os.path.join(
        r"C:\Users\USER\AppData\Local\Temp\claude\C--Users-USER-raas-api",
        "c009eaf3-2c3e-411b-a3d5-b3f3510cff1c", "scratchpad", "raas_map.html")
    out = sys.argv[1] if len(sys.argv) > 1 else default_out
    cov = REG.coverage()
    with open(out, "w", encoding="utf-8") as f:
        f.write(render(cov))
    st = cov["stats"]
    print(f"맵 생성: {out}")
    print(f"  소스 {st['sources']} · 지표 {st['metrics']} · 커버 {st['covered']} · 관계 {st['relations']}")
