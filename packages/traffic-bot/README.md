# @team-ghost/traffic-bot

PostgreSQL에 직접 연결하지 않는 재사용 HTTP 부하 모듈입니다.
샘플 앱이나 Express에 의존하지 않습니다.
다른 앱은 호환 HTTP API를 제공하거나 `write/read` 어댑터를 주입합니다.

**지원 환경.** Node.js 22.13 이상, 그리고 ledger 잠금을 위한 Linux
`flock(1)`이 필요합니다. 컨테이너 이미지(`node:22-alpine`)에는 BusyBox
`flock`이 들어 있습니다. 대상 앱의 언어·OS와는 무관하게 HTTP로 붙습니다.
macOS 같은 다른 OS, NFS·EFS·RWX 볼륨, 노드 간 강제 detach는 보장 밖입니다.
지원되지 않는 환경에서는 쓰기 전에 `LEDGER_LOCK_UNSUPPORTED`로 멈춥니다.

## 기본 실행

```bash
BOT_BASE_URL=http://localhost:8080 \
BOT_LEDGER_DIR=./data/ledger \
BOT_RUN_ID=ghost_bot_forward_001 \
BOT_PHASE=onprem_live \
node packages/traffic-bot/bin/traffic-bot.js
```

`BOT_RUN_ID`는 필수이며 기본값이 없습니다. 팀 INSERT 워커의 `forward_NNN`·
`return_NNN`과 겹치지 않는 값을 쓰세요. 형식은 `^[A-Za-z0-9_.-]{1,200}$`입니다.

기본 초당 최대 1건을 직렬 생성·전송하고 같은 ID를 조회하여 확인합니다.
지연·재시도 중에는 실제 생성률이 설정값보다 낮아질 수 있습니다.
`SIGINT`나 `SIGTERM`은 진행 중 HTTP를 중단하고 ledger를 보존합니다.
강제 종료(crash·OOMKill·SIGKILL) 뒤에는 커널이 잠금을 즉시 풀므로, 같은
디렉터리로 바로 재시작해 남은 요청을 이어서 처리합니다.

| 환경변수 | 기본값 | 의미 |
| --- | --- | --- |
| `BOT_BASE_URL` | 필수 | 단일 공개 진입점, HTTP(S), URL 안 인증정보 금지 |
| `BOT_REQUESTS_PATH` | `/api/requests` | 저장 API 경로. `BOT_BASE_URL`과 같은 origin의 절대 경로만, `\`·공백·제어문자·`?`·`#` 금지 |
| `BOT_LEDGER_DIR` | `./data/ledger` | 영속 디렉터리. Kubernetes에서는 PVC |
| `BOT_RATE` | `1` | 초당 최대 생성 수, 0 초과 1000 이하 |
| `BOT_RUN_ID` | 필수 | writer 구분자. 같은 ledger 재시작에는 같은 값 |
| `BOT_PHASE` | `onprem_live` | 생성 당시 구간 |
| `BOT_METRICS_PORT` | `9091` | `/metrics`, `/healthz` 포트 |
| `BOT_METRICS_HOST` | `0.0.0.0` | metrics 바인딩 주소 |
| `BOT_MAX_PENDING` | `1000` | 미확인 요청 최대 수 |
| `BOT_MAX_RECORDS` | `10000` | 완료 포함 ledger 최대 기록 수 |
| `BOT_MAX_LEDGER_BYTES` | `8388608` | ledger 최대 크기(8MiB) |
| `BOT_MAX_REQUEST_AGE_MS` | `300000` | 최초 생성 후 최대 처리 시간 |
| `BOT_MAX_ATTEMPTS` | `20` | 요청별 POST 시도 상한 |
| `BOT_REQUEST_TIMEOUT_MS` | `5000` | 각 write/read 호출 제한 |
| `BOT_RETRY_DELAY_MS` | `1000` | 재시도 최소 대기 |
| `BOT_API_TOKEN` | 없음 | 저장 API Bearer 토큰. 없으면 인증 헤더 없이 보냄. 아래 절 참고 |
| `BOT_GENERATION` | `running` | 새 run의 초기 생성 상태. 저장된 제어 상태가 있으면 그 상태를 복원 |
| `BOT_CONTROL_TOKEN` | 없음 | 생성 스위치(`POST /control/generation`) Bearer 토큰. 없으면 스위치는 403 |

요약은 1초마다 JSON으로 stdout에 출력합니다. `pending`, `committed`,
`failed`, `unknown`, 시도/오류 수, 최근 1000개 커밋 지연 p95/p99/최대,
환경별 커밋 확인 비율을 포함합니다. 환경 비율은 이 프로세스에서 확인한 값입니다.
커밋 지연은 전송 시간뿐 아니라 최초 생성부터 대기·재시도까지 포함합니다.

## 생성 켜고 끄기

시연하지 않을 때 봇을 멈춰 DB 가 계속 커지지 않게 하는 스위치입니다. metrics 포트에 있습니다.

| 경로 | 동작 |
| --- | --- |
| `GET /control` | 생성 상태·누적 건수·`run_id`·`ledger_id`·요청 목적지 |
| `POST /control/generation` | `enabled`(boolean), 선택 `limit`·`expected_run_id`·`expected_ledger_id`, Bearer 인증 |

- 끄면 **새 요청만** 만들지 않습니다. 장부에 이미 있는 요청은 계속 보내고 확인합니다.
  기존 `pause()`(배달 일시정지)와 다릅니다 — 그쪽은 생성은 계속하고 보내기만 멈춥니다.
- 다시 켜면 같은 장부·run_id 로 요청 번호가 이어집니다.
- 생성 상태와 상한은 SQLite에 저장합니다. 성공 응답은 진행 중 생성과 저장이
  끝난 뒤 반환하며, 재시작해도 정지 상태를 유지합니다. `BOT_GENERATION`은
  저장된 제어 상태가 없는 run의 초기값입니다.
- `limit`은 해당 run의 누적 생성 번호 상한입니다. 0 이상의 안전한 정수이며,
  현재 `generated_total`보다 작으면 409로 거절합니다. 도달하면 자동 정지하고,
  같은 상한 재요청은 추가 요청을 만들지 않습니다. 생략하면 기존 상한을 유지합니다.
- `expected_run_id`·`expected_ledger_id`가 현재 상태와 다르면 409로 거절합니다.
  러너는 두 값을 저장해 장부 초기화나 다른 봇으로의 잘못된 재시도를 감지합니다.
- `GET /control`은 `generated_total`, `generation_limit`(무제한이면 null),
  `generation`, `pending`, `accepted_pending`, `committed`, `failed`, `unknown`,
  `paused`, `fatal`, `control_enabled`, `run_id`, `ledger_id`, `rate_per_second`,
  `request_endpoint`를 제공합니다. 생성 번호는 현재 run 기준이고 결과 건수는
  보존된 장부 전체 기준입니다. 목적지에는 인증정보·query·fragment가 없습니다.
- 이전 완료 시 `enabled=false`를 보내고 `generation=stopped`, `pending=0`을
  확인합니다. Pod와 조회 API는 유지되며 기존 요청 결과 확인은 계속됩니다.
- `BOT_CONTROL_TOKEN` 이 없으면 스위치는 403 이라 기존 배포 동작이 바뀌지 않습니다.
  토큰은 `BOT_API_TOKEN` 과 같은 형식(32자 이상 b64token)입니다.

## API 토큰

`BOT_API_TOKEN`이 있으면 모든 POST·GET에 `Authorization: Bearer <토큰>`을
붙입니다. 없으면 지금처럼 인증 헤더 없이 보내므로 인증 없는 다른 앱에도
그대로 붙습니다. 토큰은 시작할 때 검사하며, 거절하면 ledger 디렉터리·metrics
포트·HTTP 요청을 하나도 만들지 않고 종료 코드 1로 끝납니다.

| 시작 거절 코드 | 조건 |
| --- | --- |
| `BOT_API_TOKEN_INVALID` | 빈 값, 32자 미만, 또는 RFC 6750 b64token 문법 위반(허용 문자는 `A-Za-z0-9-._~+/`, `=`는 끝에만) |
| `INSECURE_TOKEN_TRANSPORT` | `BOT_BASE_URL`이 `https:`도, 정확히 `http://127.0.0.1`·`http://[::1]`도 아님. 또는 CLI 환경이나 실제 프로세스 환경에 `NODE_TLS_REJECT_UNAUTHORIZED=0`이 있음 |

loopback 판정은 URL이 정규화한 hostname의 정확 일치입니다. `localhost`,
`127.0.0.2`, `[::ffff:127.0.0.1]`, `127.0.0.1.example` 같은 이름은 거절합니다.
loopback HTTP는 같은 호스트·같은 network namespace 시험용 예외이며 RFC 6750의
"항상 TLS"와 다릅니다. 토큰은 URL·명령행·셸 이력에 넣지 말고 Secret 같은
환경 변수로만 주입하세요. CLI는 토큰과 URL을 출력하지 않습니다.

이전 미확정 POST가 없는 첫 POST의 401은 GET 없이 `failed`·`HTTP_401`이 되고
`traffic_bot_requests_total{state="failed"}`가 늘어납니다. 토큰이 틀리면
요청마다 이렇게 기록돼 레코드 상한이 빨리 차므로 이 지표를 확인하세요.
이전 POST가 미확정이면 후속 POST나 복구 GET의 401로 미저장을 확정하지 않습니다.
`pending`을 보존하며 재조회하고, 상한에서 `unknown`으로 남깁니다. 인증 변경 전
수락된 POST가 늦게 커밋될 수 있으므로 `unknown`은 수동 정합성 확인 대상입니다.
인증이 설정되지 않은 서버의 503은 일반 503처럼 GET으로 판정하고, 끝내
확인하지 못하면 `unknown`으로 남습니다.

## 모듈 사용

```js
const {createTrafficBot, createHttpAdapter} =
    require('@team-ghost/traffic-bot');

const bot = await createTrafficBot({
  adapter: createHttpAdapter({baseUrl: 'https://app.example.com'}),
  ledgerDir: '/data/ledger',
  ratePerSecond: 1,
  runId: 'ghost_bot_forward_001', // 필수, 기본값 없음
  phase: 'onprem_live',
  onSummary: (summary) => console.log(JSON.stringify(summary)),
});

void bot.start();
// await bot.pause(): HTTP 전송을 정지하고 진행 중 배치를 기다린다.
// 정지 중 새 요청은 ledger 대기열에 쌓인다.
// bot.resume(): 대기한 요청부터 처리한다.
// await bot.setGeneration(true, {limit: 100}): 누적 100건까지 생성.
// await bot.setGeneration(false): 생성 정지를 영속 저장, 기존 요청은 처리.
// await bot.stop(): 진행 중 호출 중단, ledger flush, 소유 잠금 해제.
```

직접 만든 요청은 `await bot.enqueue(record)`로 영속 등록하고
`await bot.processPending()`으로 한 번 처리할 수 있습니다.
`processPending()`은 생성 루프 없이 테스트·외부 스케줄러와도 사용할 수 있습니다.
같은 프로세스에서 동시 호출하면 진행 중 배치를 공유합니다.

```js
const adapter = {
  async write(record, {signal}) {
    const order = await otherApp.createOrder(record, {signal});
    return normalizeOrder(order);
  },
  async read(requestId, {signal}) {
    const order = await otherApp.findOrder(requestId, {signal});
    return order ? normalizeOrder(order) : null;
  },
};
```

`normalizeOrder`는 아래 모양을 반환해야 합니다. 다른 필드 이름의 API는
이 어댑터에서 변환합니다. 쓰기는 실제 커밋 후에만 이 응답을 반환해야 합니다.

```json
{
  "status": "committed",
  "request": {
    "request_id": "21d10fd2-5b3c-4ab3-9c7f-c47ee6234e59",
    "run_id": "ghost_bot_forward_001",
    "phase": "onprem_live",
    "request_no": "1",
    "payload": "synthetic demo payload",
    "created_at": "2026-11-04T00:00:00.000Z"
  },
  "environment": "onprem",
  "database": {"name": "migration_demo"}
}
```

UUID는 소문자, `request_no`는 0 이상 PostgreSQL bigint 범위 십진 문자열,
`created_at`은 UTC 밀리초 ISO 문자열입니다. `run_id`는 200자,
`phase`는 100자, `payload`는 UTF-8 64KiB 이하이며 NUL은 금지합니다.
기본 HTTP 어댑터는 200/201 응답을 읽고 조회 404를 `null`로 반환합니다.
첫 POST의 409 등 영구 4xx는 재시도하지 않습니다. 408/425/429 및 5xx는 제한 내에서
재조회·재시도하며 `Retry-After`를 존중합니다. 리다이렉트는 거부합니다.
HTTP 응답은 기본 512KiB로 제한합니다.

**커밋 판정.** POST가 2xx를 받았어도 본문이 JSON이 아니거나, 크기 제한을
넘거나, 모양이 틀리거나, 여섯 필드가 다르면 그것만으로 실패라고 단정하지
않습니다. 서버는 이미 커밋했을 수 있으므로 같은 ID로 GET해서 판정합니다.

| GET 결과 | 상태 |
| --- | --- |
| 여섯 필드가 모두 같음 | `committed` |
| 같은 ID인데 다른 필드가 있음 | `failed` (`CONTENT_CONFLICT`) |
| 404·해석 오류·HTTP 오류 | `pending`, 제한 안에서 다시 조회·전송 |
| 시도·시간 상한 소진 | `unknown` (마지막 조회로도 판정 불가) |

이전 미확정 POST가 없는 명시적 거절(POST 400·409 같은 영구 4xx)은 GET 없이
즉시 `failed`입니다. 이전 POST가 미확정이면 나중 POST의 거절은 그 시도만
거절됐다는 뜻입니다. 같은 ID를 조회하고, 확인하지 못하면 `pending`으로
보존하며 시도·시간 상한에서 `unknown`이 됩니다. 복구 GET의 401 같은 오류도
이전 POST의 미저장을 증명하지 않습니다. `unknown` 이후 새 POST는 하지 않습니다.

503의 작은 JSON 오류 본문에 정확히 `error.code=WRITES_PAUSED`가 있으면
저장하지 않았다는 명시적 응답으로 처리합니다. 이 경우 POST 시도 상한을
소모하지 않고 `Retry-After` 후 같은 ID로 다시 보냅니다. 전체 최대 처리 시간과
대기열 상한은 계속 적용합니다. 일반 503·네트워크 오류는 커밋 여부가
불명확하므로 기존 시도 상한과 조회 복구를 그대로 적용합니다.

커스텀 어댑터의 `{terminal: true}`는 해당 쓰기의 확실한 거절을 뜻하며,
이전 미확정 POST가 없을 때만 요청 전체를 영구 실패로 확정합니다.
`retryAfterMs`는 유한한 0 이상 숫자로 다음 재시도 대기를 지정합니다.
소수 밀리초는 올림하고, 음수·NaN·Infinity·숫자가 아닌 값은 무시합니다.
HTTP `Retry-After`의 초·날짜도 음수 없는 정수 밀리초로 정규화합니다.
대기는 `retryDelayMs`를 하한으로 삼되 `maxRequestAgeMs`와 안전한 정수
시각의 남은 범위를 상한으로 삼습니다. 전체 처리 시간 상한도 계속 적용합니다.
`{afterResponse: true}`는 성공 응답을 받은 뒤 본문을 쓸 수 없었다는 뜻이며,
`terminal`보다 우선해 GET 판정으로 넘어갑니다.
내부 오류 메시지는 시크릿을 포함할 수 있어 ledger에 기록하지 않습니다.
어댑터가 signal을 무시해도 봇은 제한 시간 후 결과를 기다리지 않습니다.
외부 HTTP 라이브러리도 취소할 수 있도록 signal을 전달해야 합니다.

## 내구성과 한계

전송 전 `ledger.sqlite`에 요청 한 건을 트랜잭션으로 저장합니다. SQLite WAL과
`synchronous=FULL`을 사용하고, 커밋 성공 후에만 메모리 상태와 전송 단계를
진행합니다. 완료 기록은 SQLite에 보관하며 반복 전송 루프의 메모리에는
pending만 유지합니다. 시작 시 전체 증거 검증과 run별 번호 복원은 한 번 수행합니다.
동일 디렉터리를 여러 봇에 공유하지 마세요. 기존 Linux flock 잠금도 유지합니다.
로컬 디스크 손실까지 복구하지는 않습니다.

기존 `ledger.json`은 최초 실행 시 한 트랜잭션으로 가져오고 원본을
`ledger.v1.backup.json`으로 보존합니다. 가져오기 완료 표시는 SQLite 안에 함께
커밋하므로 재시작 때 중복 가져오지 않습니다. `ledger.json`은 버전2 안내 파일로
바꿔 구버전 봇이 오래된 기록으로 요청을 다시 보내는 것을 차단합니다.
**변환 후에는 구버전 이미지로 단순 롤백할 수 없습니다.** 봇을 멈춘 상태에서
SQLite 전체 디렉터리(존재하는 WAL 포함)를 백업하고 호환 버전을 사용하세요.
원본 JSON 백업에는 변환 이후 요청이 없으므로 최신 증거로 사용하면 안 됩니다.

재시작은 pending을 먼저 재처리하며 기존 POST 시도가 있으면 먼저 조회합니다.
POST 응답 유실도 같은 ID로 조회하고 원본 요청 전체를 대조합니다.
시도/시간 상한을 넘긴 미확인 POST는 마지막 조회로 커밋을 판정합니다.
여전히 알 수 없으면 `unknown`으로 남겨 수동 확인하고 새 POST를 보내지 않습니다.
`unknown`은 실패나 데이터 손실이 확정됐다는 뜻이 아닙니다.
한 번도 보내지 않은 만료 요청, GET이 확인한 내용 충돌, 영구 4xx는
`failed`입니다.

요청 로그는 완료 후에도 남겨 이전 후 대조에 사용합니다. 설정된 전체 기록·크기
상한에 도달하면 새 생성을 중단하고 fatal 요약 및 CLI 종료 코드 1로
보고합니다. pending 상한에서는 생성을 잠시 멈추고 `generation_blocked`를
늘리며 기존 요청의 조회·재시도를 계속합니다. 대기열에 여유가 생기면 생성도
재개합니다. 자동 삭제나 조용한 누락은 없습니다. 다음 실험은 기존 ledger를
보관하고 새 디렉터리·run ID로 시작하세요. 합성 payload만 사용하세요.

기본 상한은 10000건·8MiB(요청 JSON의 논리 크기)입니다. SQLite 페이지·색인·WAL과
초기 JSON 백업 공간은 별도이므로 PVC 여유를 확인하세요. 자동 WAL 체크포인트는
128페이지마다 수행합니다. 완료 증거 자동 삭제 정책은 아직 없습니다.
건수 상한을 생성률로 나누면 실행 시간의 이론상 상한이 됩니다(10000건을 초당
1건이면 약 2시간 47분). 장시간 운영 보장으로 읽지 마세요.

같은 ledger로 재시작할 때만 같은 run ID를 씁니다. 새 run ID로 바꿔도 기존
기록은 생성 당시의 run ID·phase·본문 그대로 재처리하고, 새 run ID의
`request_no`는 1부터 셉니다. ledger를 지웠거나 잃었다면 같은 run ID를 다시
쓰지 말고 새 값으로 시작하세요.

## ledger 소유 잠금

한 ledger 디렉터리에는 한 프로세스만 씁니다. `<dir>/.owner.lock` 일반 파일을
`O_NOFOLLOW`로 열고, 그 FD를 `flock -n -x 3`에 넘겨 커널 잠금을 겁니다.
FD는 종료할 때까지 이 프로세스가 들고 있습니다. 획득 직후 같은 파일을 따로
열어 다시 잠가 보고, 그것이 거절되어야 잠금이 실제로 유지된다고 판단합니다.

*   **정상 종료**(`stop()`, SIGTERM): 남은 쓰기를 마친 뒤 FD를 닫아 풉니다.
*   **강제 종료**(crash, OOMKill, SIGKILL): 커널이 FD를 닫으면서 즉시 풉니다.
    다음 프로세스가 수동 개입 없이 잠금을 얻고 pending을 재처리합니다.
*   **정지·멈춤**(SIGSTOP, hang): 살아 있는 소유자이므로 새 프로세스를
    거절합니다. liveness 실패로 컨테이너가 죽거나 운영자가 프로세스를 끝내야
    풀립니다.
*   **판정 근거**: 커널 잠금만 봅니다. PID·hostname·시간·mtime으로 소유자의
    사망을 추정하거나 잠금을 빼앗지 않습니다. 잠금 파일의 내용(pid, hostname,
    runId, startedAt)은 진단용입니다.
*   **잠금 파일 보존**: `.owner.lock`은 지우거나 바꾸지 마세요. 매 저장 직전에
    열린 FD와 경로의 dev/inode를 비교하고, 다르면 `LEDGER_LOCK_LOST`로 멈춥니다.
    이 검사는 협력하는 writer를 전제로 합니다. 확인과 rename 사이 경합은
    막지 못합니다.

잠금 오류 코드는 네 가지입니다. CLI는 이 코드와 위 API 토큰 절의 시작 거절
코드만 출력하고 경로·원문 메시지·환경 변수·접속 문자열은 출력하지 않습니다.
그 밖의 오류는 일반 문구로만 알립니다.

| 코드 | 뜻 | 운영자 조치 |
| --- | --- | --- |
| `LEDGER_OWNED` | 다른 살아 있는 프로세스가 소유 | 이전 프로세스가 끝났는지 확인 |
| `LEDGER_LOCK_UNSUPPORTED` | `flock` 없음·잠금 미유지·symlink 등 | 지원 환경에서 실행 |
| `LEDGER_LOCK_LOST` | 실행 중 잠금 파일이 교체·삭제됨 | 원인 확인 뒤 재시작 |
| `LEDGER_LEGACY_LOCK` | 0.1.x(legacy)의 `<dir>.lock` 디렉터리가 남음 | 아래 업그레이드 절차 |

`LEDGER_LOCK_LOST`는 실행 중에만 생기며, 요약의 `fatal` 값과 종료 코드 1로
드러납니다. 나머지 셋은 시작할 때 ledger를 읽거나 쓰기 전에 거절합니다.

**0.1에서 0.2로 올리기.** 두 버전은 잠금 프로토콜이 다릅니다.

| 버전 | 소유 잠금 | `runId` |
| --- | --- | --- |
| 0.1.x (legacy) | `proper-lockfile` lease, `<dir>.lock` 디렉터리 | 선택 |
| 0.2.0 (현재) | 커널 `flock`, `<dir>/.owner.lock` 파일 | 필수 |

두 버전은 서로의 잠금을 보지 못합니다. 0.2.0은 시작할 때 `<dir>.lock`이
있으면 `LEDGER_LEGACY_LOCK`으로 거절하지만, 그 뒤에 뜬 0.1.x는 막지 못합니다.
그러므로 0.1.x와 0.2.0을 같은 ledger에 동시에 띄우지 마세요. writer가 둘이
됩니다. 0.2.0은 `<dir>.lock`을 자동으로 지우지 않습니다. 0.1.x 봇을 모두
멈추고 종료를 확인한 뒤, 그 경로가 symlink가 아닌 빈 디렉터리인지 확인하고
`rmdir`하세요. `ledger.json`과 `.owner.lock`은 건드리지 않습니다.
Kubernetes 절차는 `docs/deployment.md`를 따릅니다. 0.2.0끼리의 crash
재시작에는 이 단계가 없습니다.

**0.2.0 API 변경.** `openLedger`·`createTrafficBot`의 `lockRetries` 옵션은
없어졌습니다. crash 뒤 커널이 즉시 잠금을 풀고, 재시도는 kubelet 재시작이
맡습니다. `runId`는 이제 필수입니다. 0.1.0이 레지스트리에 게시됐는지는
확인하지 못했으므로, 이 옵션을 쓰던 외부 호출자가 있는지도 알 수 없습니다.

봇이 제공하는 증거는 HTTP 생성·조회 결과입니다. DB 전체 행 비교,
CDC 완료, 기존 writer 차단, 양쪽 DB의 원자적 전환은 별도 검증이 필요합니다.

## 커스텀 어댑터의 명시적 정지 거절

커스텀 어댑터도 저장 전에 거절했다는 보장이 있을 때만 다음 오류를 사용할 수 있습니다.
DB 장애나 커밋 여부를 알 수 없는 응답에는 사용하지 않습니다.

```js
const {HttpError} = require('@team-ghost/traffic-bot');
throw new HttpError(503, {notCommitted: true, retryAfterMs: 1000});
```

이 오류는 시도 예산을 소모하지 않지만 전체 시간·pending 상한은 유지합니다.
write_attempts/write_errors 지표에는 계획된 정지503도 실제 HTTP 결과로 포함됩니다.
정지 구간의 오류율을 장애율로 해석하지 말고 ledger의 WRITES_PAUSED와 비교하세요.

## 누적 원장 조회 비용

상태 집계는 마지막 영속 저장이 성공한 원장의 캐시를 사용하고, 전달 루프는 pending 항목만 복제한다. 이미 완료한 요청 payload 전체를 건강 검사나 10ms 전달 루프마다 복제하지 않는다. SQLite 단일 행 커밋으로 전체 JSON 재기록도 제거했다. 요청 재처리 의미와 최대 보존 한도는 유지한다.

Linux 환경에서 `node packages/traffic-bot/bench-read-views.js`로 18,400개 완료 요청의 상태 조회와 빈 전달 반복 비용을 재현할 수 있다. 임시 합성 원장만 사용하며 운영 원장은 변경하지 않는다.

## 저장 비용 재현

`node packages/traffic-bot/bench-storage.js`는 임시 디렉터리에 완료 기록 2만 건을
만든 뒤 신규10건의 생성·시도·완료 30회 영속 변경을 측정합니다. 운영 원장은
수정하지 않습니다. Linux 컨테이너 CPU0.5에서 기존6204ms, SQLite16ms였으며
운영 PVC에서의 결과나 실제 무중단 이전 검증을 대체하지 않습니다.
