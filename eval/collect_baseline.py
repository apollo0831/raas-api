"""
Phase 0-2: baseline_v1.jsonl의 50개 질문을 현재 /api/query에 일괄 호출하여
답변·차트·소요시간을 baseline_v1_answers.jsonl에 저장.

특징:
- 직렬 실행 (Claude API 부담 최소화)
- 중단되어도 재개 가능 (이미 처리한 id는 skip)
- user_id="eval-baseline-v1"로 통일 (평가 후 DB에서 분리/정리 가능)
- 각 호출 1초 간격, 호출당 최대 180초 타임아웃

실행:
    python eval/collect_baseline.py
"""
import urllib.request
import json
import sys
import io
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

EVAL_DIR = Path(__file__).parent
INPUT = EVAL_DIR / "baseline_v1.jsonl"
OUTPUT = EVAL_DIR / "baseline_v1_answers.jsonl"
URL = "http://localhost:5000/api/query"
USER_ID = "eval-baseline-v1"
TIMEOUT_SEC = 180
DELAY_SEC = 1.0


def main():
    # 재개를 위해 기존 결과 로드
    done = {}
    if OUTPUT.exists():
        for line in OUTPUT.open(encoding='utf-8'):
            try:
                r = json.loads(line)
                done[r['id']] = r
            except Exception:
                pass

    # 질문 로드
    questions = [json.loads(l) for l in INPUT.open(encoding='utf-8')]
    total = len(questions)
    remaining = total - len(done)
    print(f'총 {total}건, 기존 완료 {len(done)}건, 남은 {remaining}건')
    print(f'대상 URL: {URL}')
    print(f'결과 파일: {OUTPUT}')
    print(f'{"─" * 80}')

    out_f = OUTPUT.open('a', encoding='utf-8')
    t_start = time.time()

    for i, q in enumerate(questions, 1):
        qid = q['id']
        if qid in done:
            print(f'[{i:>2}/{total}] {qid} SKIP')
            continue

        body = json.dumps({
            'question': q['question'],
            'user_id': USER_ID,
        }).encode('utf-8')
        req = urllib.request.Request(
            URL, data=body,
            headers={'Content-Type': 'application/json; charset=utf-8'},
            method='POST'
        )

        t1 = time.time()
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as r:
                data = json.loads(r.read().decode('utf-8'))
            elapsed = time.time() - t1
            answer = data.get('answer', '') or ''
            chart_data = data.get('chart_data')
            result = {
                'id': qid,
                'question': q['question'],
                'category': q['category'],
                'difficulty': q['difficulty'],
                'ok': data.get('ok', False),
                'answer': answer,
                'answer_len': len(answer),
                'chart_data': chart_data,
                'has_chart': bool(chart_data),
                'query_id': data.get('query_id'),
                'elapsed_sec': round(elapsed, 2),
                'error': None,
            }
            preview = answer[:70].replace('\n', ' ')
            chart_mark = '○' if chart_data else ' '
            print(f'[{i:>2}/{total}] {qid:<5} OK  {elapsed:>5.1f}s chart={chart_mark} '
                  f'len={len(answer):>4} | {preview}...')
        except Exception as e:
            elapsed = time.time() - t1
            result = {
                'id': qid,
                'question': q['question'],
                'category': q['category'],
                'difficulty': q['difficulty'],
                'ok': False,
                'answer': '',
                'answer_len': 0,
                'chart_data': None,
                'has_chart': False,
                'query_id': None,
                'elapsed_sec': round(elapsed, 2),
                'error': f'{type(e).__name__}: {e}',
            }
            print(f'[{i:>2}/{total}] {qid:<5} ERR {elapsed:>5.1f}s | {type(e).__name__}: {e}')

        out_f.write(json.dumps(result, ensure_ascii=False) + '\n')
        out_f.flush()
        time.sleep(DELAY_SEC)

    out_f.close()
    total_elapsed = time.time() - t_start
    print(f'{"─" * 80}')
    print(f'완료. 총 소요 {total_elapsed:.1f}s ({total_elapsed/60:.1f}분)')


if __name__ == '__main__':
    main()
