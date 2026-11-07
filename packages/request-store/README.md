# @team-ghost/request-store

PostgreSQL에 동일 요청을 중복 없이 저장하고 조회하는 Node.js 22 모듈이다.
프레임워크와 독립된 코어와 선택적인 Express 어댑터를 제공한다.

## 설치와 연결

이 디렉터리에서 `npm pack`으로 만든 tarball을 다른 앱에 설치할 수 있다.
Express를 사용할 때만 소비자 앱에 Express 4 또는 5가 필요하다.

```js
const {
  createPostgresRequestStore,
  postgresOptionsFromEnv,
} = require('@team-ghost/request-store');
const {createRequestRouter} = require('@team-ghost/request-store/express');

const store = createPostgresRequestStore(postgresOptionsFromEnv());
app.use('/api/requests', createRequestRouter({
  store,
  getContext: () => ({environment: 'onprem', region: 'busan-local'}),
}));
```

router는 인증하지 않는다. 인증과 접근 제한 같은 노출 정책은 호스트 앱이
router 앞에 둔다. 공개 진입점에 익명 쓰기를 열어 두지 않는다.

프레임워크가 다른 경우 `store.put(record)`, `store.get(uuid)`,
`store.status()`를 호출하고 원하는 HTTP 응답으로 변환한다.
이 모듈은 샘플 앱의 환경변수나 지표 구현을 import하지 않는다.

## DB 스키마와 권한

관리자 계정으로 `schema.sql`을 먼저 적용한다. 앱 시작에서 DDL을 실행하지 않는다.
명시적 초기화 CLI는 `DATABASE_URL=... node bin/init-db.js`다.
기존 테이블이 있다면 `CREATE TABLE IF NOT EXISTS`는 스키마를 변경하지 않는다.
운영 스키마 변경은 별도 마이그레이션으로 관리한다.

```sql
CREATE TABLE public.migration_demo (
    request_id uuid PRIMARY KEY,
    run_id text NOT NULL,
    phase text NOT NULL,
    request_no bigint NOT NULL,
    payload text NOT NULL,
    created_at timestamptz NOT NULL
);
```

앱 역할에는 테이블 소유권·DDL 권한 없이 SELECT와 INSERT만 부여한다.
CDC 역할·DB writer 차단·세션 종료·전환 승인은 별도 운영 제어가 담당한다.

## 요청과 응답

```js
const record = {
  request_id: '21d10fd2-5b3c-4ab3-9c7f-c47ee6234e59',
  run_id: 'forward_001',
  phase: 'onprem_live',
  request_no: '1',
  payload: 'synthetic demo payload',
  created_at: '2026-11-04T00:00:00.000Z',
};
const result = await store.put(record);
// {created: true, request: record, database: {name: 'migration_demo'}}
const found = await store.get(record.request_id);
// {request: record, database: {name: 'migration_demo'}}
const missing = await store.get('00000000-0000-0000-0000-000000000000');
// {request: null, database: {name: 'migration_demo'}}
const ready = await store.checkReady();
// {name: 'migration_demo'}; checks table columns without counting rows
const status = await store.status();
// {name: 'migration_demo', row_count: '1'}
const live = await store.liveSnapshot(20);
// status fields + recent: newest rows' {request_id, run_id, phase, created_at}
```

동일 UUID와 동일한 모든 필드는 기존 커밋을 반환한다(`created: false`).
같은 UUID에 다른 필드가 하나라도 있으면 `REQUEST_CONFLICT`다.
INSERT와 비교 SELECT를 같은 연결에서 수행하고 COMMIT 후에만 반환한다.
반환 도중 연결이 끊어졌으면 결과가 미확정이므로 같은 ID를 조회하거나 재시도한다.

입력 규칙:

*   여섯 필드를 모두 제공하며 알 수 없는 필드는 거절한다.
*   UUID는 소문자 표준형으로 정규화한다.
*   `request_no`는 PostgreSQL signed bigint 범위의 십진 문자열이다.
    안전한 JavaScript 정수도 받아 문자열로 정규화한다.
*   `created_at`은 timezone 필수 ISO 8601, 소수 초는 최대 세 자리다.
    UTC 밀리초로 정규화한다. 이 API가 생성한 요청의 정밀도는 밀리초다.
*   `run_id`는 1–200자, `phase`는 1–100자, `payload`는 UTF-8 기준 0–65536바이트다.
    PostgreSQL에서 지원하지 않는 NUL 문자는 거절한다.

Express 어댑터는 POST `/`와 GET `/:id`를 제공한다.
POST는 신규 저장 201, 기존 커밋 200, 충돌 409, 입력 오류 400,
쓰기 정지·DB 장애 503이다. GET은 존재 200, 없음 404다.
`getContext`는 필수 함수다. 없으면 `createRequestRouter`가 `TypeError`를
던진다. 반환값에 비어 있지 않은 `environment` 문자열이 없으면 POST는
`store.put` 전에, GET은 조회 전에 500 `CONTEXT_INVALID`로 거절한다.
JSON 요청 본문은 최대 512 KiB다.
정지 상태에서는 `Retry-After: 1`을 보낸다. 실패 응답은
`{error:{code,message}}`이며 접속 정보·SQL 오류 원문을 노출하지 않는다.
성공 응답은 `status: 'committed'`, `request`, `database`, `created`(쓰기),
그리고 `getContext()`의 메타데이터를 포함한다.

## 전환 제어

```js
await store.pauseWrites();  // 새 쓰기 차단 + 진행 중 쓰기 drain
await store.replaceConnection(nextConnectionOptions);
// 검증된 새 DB에 연결됐지만 아직 쓰기 정지 상태다.
store.resumeWrites();
await store.close();
```

풀 변경에는 정지 상태가 필요하다. 후보 DB에서 테이블 조회를 먼저 확인하고
참조를 후보 풀로 바꾼 뒤 기존 풀을 best-effort로 종료한다. 기존 풀 종료가
실패해도 교체는 성공하며 `connection`/`error` 관측만 남기고 후보 풀은 닫지
않는다. 후보 검증이 실패하면 쓰기는 계속 정지하며 기존 DB로 자동 쓰기하지 않는다.
조회는 drain·후보 검증 동안만 503이고, 참조 교체 뒤에는 기존 풀 정리 중에도
후보 풀로 응답한다. 쓰기·`resumeWrites`·다른 교체는 정리가 끝날 때까지
`DB_UNAVAILABLE`이다. 정지 상태만으로는 조회가 차단되지 않는다.
`store.inFlightWrites`는 아직 완료되지 않은 쓰기 수를 읽기 전용으로 제공한다.
`pauseWrites`는 이 store 인스턴스의 쓰기만 통제한다. 여러 replica의 제어와
DB 권한 차단을 수행하지 않으며 전환 오케스트레이터를 대체하지 않는다.

## PostgreSQL 설정

`postgresOptionsFromEnv(env)`는 다음 설정을 읽는다.

| 이름 | 기본값과 동작 |
| --- | --- |
| `DATABASE_URL` | 필수 PostgreSQL 접속 URL |
| `PGSSLMODE` | `disable`; `require`, `verify-ca`, `verify-full`은 인증서와 호스트 검증 |
| `DB_SSL` | `true`이고 모드 미지정이면 TLS 검증 사용 |
| `PGSSLROOTCERT` | 검증할 CA 파일, RDS CA를 이 경로에 마운트. 인라인 값보다 우선 |
| `DB_SSL_CA` | 파일 마운트를 지원하지 않을 때 Secret에서 주입하는 CA PEM 문자열 |
| `DB_CONNECT_TIMEOUT_MS` | 3000, 최대 60000 |
| `DB_STATEMENT_TIMEOUT_MS` | 5000, 최대 60000 |
| `DB_POOL_MAX` | 5, 최대 100 |
| `DB_AUTH_MODE` | `password`(기본, URL 비밀번호) 또는 `rds-iam` |
| `AWS_REGION` | `rds-iam`일 때 필수. 토큰을 서명할 RDS 리전 |

`DB_AUTH_MODE=rds-iam`이면 비밀번호 대신 RDS IAM 인증 토큰으로 로그인한다.
`@aws-sdk/rds-signer`가 기본 자격 증명 체인(ECS 작업 역할 등)으로 새 연결마다
토큰을 서명해 pg의 `password` 콜백으로 넘긴다. 토큰 수명은 15분이지만 RDS는
로그인 때만 확인하므로 열린 연결은 끊기지 않는다. `DATABASE_URL`에는 사용자와
DB만 두고 비밀번호를 넣지 않는다. URL이나 `password` 쿼리에 비밀번호가 있거나,
사용자·`AWS_REGION`이 없거나, TLS가 꺼져 있으면 비밀번호로 넘어가지 않고
`INVALID_CONFIGURATION`으로 거절한다. 다른 `DB_AUTH_MODE` 값도 거절한다.
이 모드의 결과에는 `connectionString`이 없고 `host`·`port`·`user`·`database`와
`password` 콜백이 있다. pg가 접속 문자열과 `PGPASSWORD`로 콜백을 덮어쓰기
때문이다. URL의 나머지 쿼리 키(예: `application_name`)는 같은 이름으로 옮긴다.
테스트는 두 번째 인자 `{signerFactory}`로 서명기를 바꿔 끼울 수 있다.

URL의 `sslmode`는 helper가 제거해 명시적인 검증 설정으로 바꾼다.
다른 URL SSL 키는 거절한다. SSL 키는 `ssl`로 시작하는 모든 키(예:
`sslnegotiation`)와 `uselibpqcompat`이며 대소문자를 가리지 않는다. pg가 URL을
다시 해석할 때 이 키들이 고정한 `ssl` 옵션을 덮어써 CA와 검증 설정을 잃기
때문이다.
`DB_SSL=true` 또는 CA가 지정됐는데 TLS 모드가 `disable`이면 설정을 거절한다.
코어에 직접 options를 전달할 때는 URL에 SSL 키를 넣지 말고
`ssl: {rejectUnauthorized: true, ca}`를 전달한다. 로컬 평문은 `ssl: false`다.
코어는 처음 만들 때와 `replaceConnection` 모두 SSL 키가 있는 URL로 풀을 만들지
않는다. 교체에서는 이 거절이 `DB_UNAVAILABLE`로 보인다.

코어가 받는 `ssl`은 `true`, `false`, 미지정, 인증서를 검증하는 객체뿐이다.
`'no-verify'` 같은 문자열, `null`, 숫자, 배열은 거절한다.
`rejectUnauthorized`가 `true`가 아니거나 `checkServerIdentity`를 지정하는
객체도 거절한다. `ssl`을 생략하면 드라이버가 대신 읽는
`PGSSLMODE=no-verify`와 `pg.defaults.ssl`도 같은 규칙으로 검사한다.
초기 설정의 거절은 `INVALID_CONFIGURATION`, 교체 시 거절은 `DB_UNAVAILABLE`이다.
접속 IP도 인증서 비교 대상이므로 다른 DNS 이름으로 바꿀 수 없다.
Node가 `servername` → `host` → `socket._host` 순서로 검증 대상을 고르고
pg가 SSL 옵션을 병합하므로 `ssl.servername`·`ssl.host`·`ssl.socket`도 거절한다.
초기·교체·드라이버 기본값 모두 own/inherited 속성과 `undefined` 지정에 적용한다.
URL로 해석되지 않는 접속 문자열도
원문을 메시지·속성·cause에 남기지 않는 같은 오류로 거절한다.

`require`, `verify-ca`, `verify-full`은 모두 인증서 체인 검증과 Node 기본
호스트명 검증을 수행한다. libpq의 `require`(검증 없음)와 `verify-ca`(호스트명
미검증)보다 강한 최소 정책이며, 이 모듈은 그 약한 의미를 제공하지 않는다.
서버 인증서는 주입한 CA로 검증돼야 하고 SAN이 접속 호스트와 맞아야 한다.
RDS CA는 이미지에 넣지 않고 `PGSSLROOTCERT` 파일로 외부 주입한다. 이 설명은
정책이며 실제 운영 DB의 TLS 접속 검증을 대신하지 않는다.

선택 옵션 `writesPaused`로 시작부터 쓰기를 막을 수 있다.
`onObservation({operation,outcome,seconds})`으로 지표를 연결할 수 있다.
드라이버 오류·쿼리 timeout이 발생한 연결은 풀에서 폐기하며 TCP keepalive를
기본으로 사용한다. 정상 롤백된 업무 충돌은 연결을 재사용한다.
operation은 `write`, `read`, `status`, `readiness`, `connection`,
outcome은 `success`,
`error`다. 계측 콜백의 예외는 커밋 결과에 영향을 주지 않는다.

## 검증

저장소 루트에서 `node --test test/request-store.test.js`를 실행한다.
`TEST_DATABASE_URL`을 폐기 가능한 PostgreSQL에 지정하면
`test/request-store.integration.test.js`가 실제 중복·충돌·정지·풀 교체를 검증한다.
테스트는 전용 run_id의 행만 삭제한다. 실제 서비스 DB를 지정하지 않는다.
