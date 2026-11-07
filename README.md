# Team Ghost sample-app

SoftBank Hackathon 2026의 서비스 이전 검증 앱입니다. 현재 응답 환경과
연결 DB를 보여주며, 동일 요청을 재시도해도 한 행만 저장합니다.

## 시작하기

Node.js 22 이상과 PostgreSQL이 필요합니다.
먼저 관리자 연결로 명세의 테이블을 생성하고, 앱은 별도 최소 권한 계정으로 실행합니다.

```bash
npm ci
DATABASE_URL='postgres://admin:password@localhost:5432/demo' npm run db:init
DATABASE_URL='postgres://app:password@localhost:5432/demo' npm start
```

위 계정은 설명용입니다. 실제 시크릿은 환경변수나 Kubernetes Secret으로 공급하세요.
앱 계정은 테이블의 SELECT/INSERT만 필요하며 스키마 생성 권한은 필요하지 않습니다.

```bash
curl http://localhost:8080/api/info
curl -X POST http://localhost:8080/api/requests \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"21d10fd2-5b3c-4ab3-9c7f-c47ee6234e59","run_id":"demo","phase":"onprem_live","request_no":"1","payload":"demo","created_at":"2026-11-04T00:00:00.000Z"}'
curl http://localhost:8080/api/requests/21d10fd2-5b3c-4ab3-9c7f-c47ee6234e59
```

| 경로 | 동작 |
| --- | --- |
| `/` | 실시간 대시보드: 환경·버전·행 수·초당 유입·최근 요청·쓰기 정지 배너 |
| `/api/info` | 환경·버전·기동 시각·DB·행 수·쓰기 상태 JSON(DB 건수는 최대1초 캐시) |
| `/api/live` | 대시보드 데이터 JSON(인증 없음, payload 제외, DB 읽기 1초 캐시) |
| `POST /api/requests` | 새 저장201, 동일 요청200, 같은 ID 다른 내용409 |
| `GET /api/requests/:id` | 저장 조회200, 없음404 |
| `/healthz` | DB와 독립된 생존 검사 |
| `/readyz` | DB 접속과 테이블 조회 검사 |
| `/metrics` | HTTP/DB 요청·지연·현재 연결 DB |

위 curl 예시는 인증 env를 두지 않은 로컬 open 모드입니다. 공개 배포는
`REQUEST_API_AUTH=required`로 `/api/requests`의 POST·GET 모두 Bearer 토큰을
요구합니다. 화면·`/healthz`·`/readyz`·`/api/info`·`/api/live`·`/metrics`는 인증 없이
열려 있습니다. 토큰은 HTTPS와 헤더 파일로만 보내고 명령행에 쓰지 마세요
([배포 안내](docs/deployment.md#요청-api-인증)).

[API 계약](docs/api.md)에 입력·응답·오류 형식을 기록했습니다.

## 다른 앱에서 재사용하기

두 모듈은 npm workspaces로 관리합니다. 공개 npm에 게시하지 않았습니다.
각 폴더에서 `npm pack`으로 tarball을 만들어 다른 프로젝트에 설치할 수 있습니다.

- [request-store](packages/request-store/README.md): 프레임워크 독립 PostgreSQL 코어와
  선택적인 Express router. 다른 프레임워크는 코어의 `put/get`을 호출합니다.
- [traffic-bot](packages/traffic-bot/README.md): 독립 HTTP 봇·영속 ledger·CLI.
  다른 API에는 경로 설정 또는 `write/read` adapter를 주입합니다.

```js
const express = require('express');
const {createPostgresRequestStore} = require('@team-ghost/request-store');
const {createRequestRouter} = require('@team-ghost/request-store/express');
const store = createPostgresRequestStore({
  connectionString: process.env.DATABASE_URL,
});
const app = express();
app.use('/api/requests', createRequestRouter({
  store,
  getContext: () => ({environment: 'my-app'}),
}));
```

`getContext`는 필수이며 비어 있지 않은 `environment`를 반환해야 합니다. 없으면
router 생성 때 TypeError, 요청 때 잘못된 값이면 저장 전에 500
`CONTEXT_INVALID`입니다.

모든 언어에 동일 SDK를 제공하는 구조는 아닙니다. Node 앱에는 모듈을 설치하고,
다른 언어의 앱에는 HTTP 계약을 구현한 뒤 봇을 별도 프로세스/컨테이너로 연결합니다.

## 트래픽 봇 실행

```bash
BOT_BASE_URL=http://localhost:8080 \
BOT_LEDGER_DIR=./data/ledger BOT_RUN_ID=demo-001 \
BOT_PHASE=onprem_live BOT_RATE=1 npm run bot
```

`BOT_RUN_ID`는 필수이며 형식은 `^[A-Za-z0-9_.-]{1,200}$`입니다. 공유 DB의
다른 writer(팀 INSERT 워커의 `forward_NNN`·`return_NNN`)와 겹치지 않게
정하세요. 새 ledger에는 새 run_id를 쓰고, 지운 ledger의 run_id는 다시 쓰지
않습니다.

기본 초당 1건, 동시 1개입니다. 매 요청의 본문을 전송 전에 디스크에 저장하고,
응답 유실 시 같은 ID를 조회·재시도합니다. 재시작하면 pending부터 처리합니다.
1초마다 성공·실패·환경별 비율·지연을 출력하고, 9091의 `/metrics`를 제공합니다.

ledger를 영속 볼륨에 두세요. 한 ledger는 봇 하나만 쓰며, 커널 flock으로
강제합니다. 그래서 봇 실행에는 Linux와 flock(1)이 필요합니다. 컨테이너
이미지에는 들어 있고, macOS 호스트에서 직접 실행하면 `LEDGER_LOCK_UNSUPPORTED`로
시작을 거절합니다. 자세한 지원 범위는
[ADR-0002](docs/decisions/2026-11-04-ledger-kernel-lock-and-limits.md)에
있습니다.

기본 상한은 10000건/8MiB, 요청당 20회/5분입니다. 1건/s에서 건수 상한은 약
2시간 47분(이론값)이므로 긴 시험은 기록을 보관하고 새 ledger와 새 run_id로
나눠 실행하세요. pending 상한에서는 생성만 대기합니다. 전체 기록·크기 상한에
닿으면 실패로 알리며 기록을 자동 삭제하지 않습니다. 미확인 커밋은 `unknown`으로
남기므로 실패 건수만으로 데이터 손실을 판정하지 않습니다.

## 환경과 전환 제어

| 환경변수 | 용도 |
| --- | --- |
| `DATABASE_URL` / `DATABASE_URL_FILE` | DB 접속 시크릿. 파일은 시작 시 읽음 |
| `DB_SSL=true`, `PGSSLROOTCERT` / `DB_SSL_CA` | 인증서 검증 TLS와 CA PEM 파일/본문 |
| `DB_CONNECT_TIMEOUT_MS` / `DB_STATEMENT_TIMEOUT_MS` | 연결/쿼리 제한, 기본3000/5000ms |
| `DB_POOL_MAX` | 프로세스당 연결 수, 기본5 |
| `DEPLOY_ENV` / `DEPLOY_REGION` | 응답하는 환경·리전 표시 |
| `WRITES_PAUSED=true` | 실행 내내 강제 쓰기 정지(파일보다 우선) |
| `APP_CONTROL_FILE` | 런타임 쓰기 제어 JSON 파일 경로 |
| `FAIL_RATE` / `HEALTH_FAIL` | 명시적 장애 주입, 본선 기본0/false |
| `REQUEST_API_AUTH` | 미정의·`optional`·`required`. 공개 배포는 `required`, 그 밖 값은 closed |
| `REQUEST_API_TOKEN` | 요청 API 공유 토큰(32자 이상 b64token). Secret으로 주입, 없으면 required에서 503 |
| `BOT_API_TOKEN` | 봇이 보낼 같은 토큰. https 또는 정확한 loopback HTTP에서만 전송 |

값 없는 예시는 [.env.example](.env.example)에 있습니다. 앱은 이 파일을
자동으로 읽지 않습니다.

`APP_CONTROL_FILE`이 있으면 관련 요청마다 아래 파일을 읽습니다.
오류·누락·잘못된 형식이면 쓰기를 정지합니다. readiness는 DB 조회 가능 여부만
보므로 제어 이상으로 내려가지 않고, 읽기 API와 liveness도 계속 제공합니다.
제어 이상은 `/api/info`의 `write_control.healthy=false`와 지표
`app_write_control_healthy`로 드러납니다.

```json
{"writes_paused":true}
```

파일은 임시 파일에 쓴 후 rename으로 원자 교체하세요. false로 바꾸면 재개합니다.
단, WRITES_PAUSED=true이면 파일 값으로 해제할 수 없고 env 변경 후 재시작해야 합니다.
DB 풀의 런타임 교체는 모듈의 `pauseWrites`→`replaceConnection`→`resumeWrites`로
호스트 애플리케이션에서 호출할 수 있습니다. 공개 관리 API는 제공하지 않습니다.
샘플 서버의 DB URL 파일은 시작 시만 읽으므로 URL 변경은 재시작으로 적용합니다.

이 제어는 프로세스 단위입니다. Kubernetes ConfigMap 전파는 즉시/동시가 아니므로
쓰기 차단의 확정 증거가 아닙니다. `writes_in_flight=0`도 각 replica에서 확인해야 합니다.
DB 권한·기존 세션 차단과 CDC 최종 반영 확인은
별도의 이전 오케스트레이터가 수행해야 합니다. 새 DB 쓰기 이후 자동 주소 원복도
이 앱이 수행하지 않습니다.

## 검증

```bash
npm run check
npm test
TEST_DATABASE_URL='postgres://test:test@localhost:5432/ghost_test' \
  npm run test:integration
```

통합 테스트는 지정 DB에 테이블을 생성하고 시험 전용 run_id 행을 정리합니다.
반드시 버릴 수 있는 테스트 DB를 사용하세요. 실제 DB 없이 실행하면 PG 시험은
skip되며, `test:integration`은 DB URL이 없으면 실패합니다.

봇 ledger 잠금 시험은 flock(1)이 있는 Linux가 필요합니다. macOS에서는
`node:22-alpine` 같은 Linux Node 22 컨테이너 안에서 전체 시험을 실행하세요.
CI는 ubuntu 러너에서 실행합니다.

실제로 실행한 결과와 확인하지 않은 범위는
[구현 검증 기록](docs/verification.md)에 있습니다. `npm test`와
`test:integration`은 같은 시험 묶음을 돌리므로 두 계수를 더하지 않습니다.

## 배포

[배포 안내](docs/deployment.md)를 따릅니다. 이미지 하나에서 앱 또는 봇을
실행합니다.

- 본선 정본은 `SoftBankHackathon2026/gitops`의 `apps/sample-app`입니다.
  AWS는 base → 렌더러 생성 overlay → 사람이 관리하는 `runtime/aws` wrapper
  순서로 적용됩니다.
- `deploy.yaml`은 review-service 렌더러의 입력입니다. DB·CA·제어 파일은
  렌더러가 표현하지 못해 wrapper가 넣습니다.
- `k8s/`는 독립 적용 예시입니다. Argo가 관리하는 namespace에 함께 적용하지
  마세요. 봇 선언(`k8s/traffic-bot/`)과 업그레이드용 legacy 잠금 복구 Job은
  여기에 있습니다.

CI는 실제 PostgreSQL 시험을 통과한 커밋만 amd64·arm64로 빌드하고, PR과 main
모두 두 이미지를 검사한 뒤 main에서 Docker Hub 공개 저장소
`docker.io/kty1021/sample-app`에 커밋 SHA 태그로 게시합니다
([ADR-0003](docs/decisions/2026-11-04-ci-gate-after-db-tests.md)).
`GITOPS_TOKEN`이 있으면 게시 뒤 GitOps 저장소의 이미지 태그를 갱신합니다.
관련 gitops 변경을 먼저 검토·병합하고 DB Secret을 준비해야 합니다.

## 명세와 개발 프로필

- [Notion 근거](docs/spec-sources.md)
- [프로젝트 하네스 프로필](.claude/harness/project-profile.md)
- [하네스 정본](https://github.com/xodbs1021/kty-claude-harness-template)
## 접수 큐 전달 동시성

durable-ingress는 기본 최대 10건을 동시에 전달한다. `INGRESS_CONCURRENCY`는 1~10의 정수이며,
1로 설정하면 직렬 전달로 되돌릴 수 있다. 각 요청의 POST → GET 내용 확인 → Redis ACK 순서는 유지한다.
요청 사이 완료 순서는 보장하지 않으며, 같은 ID의 재시도는 앱의 기존 중복 방지 계약을 사용한다.
일부 실패 시 성공한 요청은 ACK하고 실패한 ID만 pending으로 남긴다.
`/pause`와 종료는 진행 중인 배치 전체가 끝날 때까지 기다리며, target 전환은 pause 완료 뒤에만 가능하다.
제어 `/status`의 `in_flight`는 실제 진행 요청 수(0~10), `concurrency`는 설정 상한이다.
앱의 PostgreSQL 풀 크기는 별도 설정이며, 동시 전달 10건이 처리량 10배를 보장하지는 않는다.
