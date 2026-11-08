# 이전 중 요청 접수

`node src/durable-ingress/server.js`는 이전 중 고정 접수 주소를 유지하는 별도 서비스다.
`REDIS_URL`, `INGRESS_TARGETS`가 필요하다. 대상 예시:

```json
{"onprem":{"url":"http://sample-app"},"aws":{"url":"https://app.example.com","token_env":"AWS_REQUEST_API_TOKEN"}}
```

주소 목록은 운영 설정, 인증값은 Secret 환경변수다. 토큰을 사용하는 대상은 HTTPS만 허용한다.
앱 API는 8080, 제어 API는 기본 loopback 9092에 바인딩된다. 제어 포트를 외부 Service/Ingress에 공개하지 않는다.

제어 API 환경변수 (cutover 도구가 큐를 자동 제어할 때 쓴다):

| 변수 | 기본 | 뜻 |
|---|---|---|
| `INGRESS_CONTROL_HOST` | `127.0.0.1` | 제어 API 바인드 주소. `0.0.0.0`은 9092를 러너 파드로만 제한하는 NetworkPolicy와 함께만 쓴다 |
| `INGRESS_CONTROL_TOKEN` | 없음 | 있으면 제어 API가 `Authorization: Bearer <token>`을 요구한다(401 `CONTROL_UNAUTHORIZED`). 값은 Secret에서만 |
| `INGRESS_API_PORT` / `INGRESS_CONTROL_PORT` | `8080` / `9092` | 로컬 시험용 포트 변경 |

누가 전환을 결정하나: 이 서비스는 CDC 완료나 DB 차단을 판단하지 않는다. 판단은 `migration.cutover`(Hackathon `migration/`)가 하고,
설정에 `ingress` 절이 있으면 FENCING에서 `/pause`(settled 확인) → 원본 REVOKE, SWITCHING에서 대상 GRANT → 대상 앱 readiness → `/target` → `/resume` 순으로 호출한다.
`ingress` 절이 없으면 운영자가 port-forward로 같은 순서를 수동으로 친다.

- POST `/api/requests`: 정규화된 ID·내용을 Redis Stream/AOF에 접수한 뒤 202/accepted.
- GET `/api/requests/:id`: 처리 중은 202/accepted, 처리 완료는 현재 대상 앱에서 읽은 committed 응답.
- 동일 ID의 다른 내용은 409. 기존 접수는 자동 만료·삭제하지 않는다. 저장 건수 상한은 100,000.
- 봇은 202를 committed로 세지 않는다. accepted_pending을 별도로 표시하고 최종 DB 응답까지 읽는다.
  봇 재시작·기존 재시도 만료 이후에도 accepted 요청은 조회하며 재POST하지 않는다.
  요청 생성과 처리 루프를 분리하여 DB 대기로 생성 속도가 낮아지는 현상을 줄인다.

제어 API:

| 호출 | 조건/결과 |
| --- | --- |
| GET /status | mode, target, epoch, in_flight, last_error |
| POST /pause `{}` | 새 전달 중단 후 진행 중 전달/읽기 확인이 끝날 때 반환, 접수는 지속 |
| POST /target `{"target":"aws","expected_epoch":"0"}` | pause/drain 상태·현재 epoch·미리 등록한 대상 검사 후 epoch 증가 |
| POST /resume `{"expected_epoch":"1"}` | 최신 epoch 일치 시 소비 재개 |

재시작 시 target/epoch는 복구하되 항상 paused로 시작한다. 단일 소비자 lease를 유지하며,
lease 갱신 실패·내용 충돌은 소비 중단으로 처리한다. 단일 인스턴스 데모 구성으로 HA가 아니다.
Redis 장애 시 새 접수가 실패할 수 있고, 프로세스 정지/노드 손실에 대한 DB writer 배타성은
이전 도구의 DB 권한 fencing으로 확보해야 한다. 대상을 바꾸는 API만으로 CDC 완료를 증명하지 않는다.

실제 배포·전환·복구 순서의 원본은 gitops `apps/durable-ingress/README.md`다.
현재 AWS 대상 준비 실패로 실제 이전 실행은 보류됐으며, 이 코드 테스트를 실제 이전 성공으로 세지 않는다.

검증:

```sh
# 전용 Redis만 사용한다. 이 테스트는 TEST_REDIS_URL의 DB를 초기화한다.
TEST_REDIS_URL=redis://localhost:6379 node --test test/durable-ingress.integration.test.js
# 봇은 Linux flock을 사용하므로 Linux 환경에서 실행한다.
node --test test/traffic-bot-accepted.test.js test/traffic-bot-retry.test.js test/traffic-bot.test.js
```

2026-11-06: Linux 전체 테스트 248 통과/13 생략(별도 DB·Redis 통합 조건 등),
전용 Redis 제어 통합 테스트 1 통과. 처음 Linux 실행의 IPv6 loopback 부재 실패는
IPv6 loopback을 활성화한 컨테이너에서 재실행해 해결했다. macOS의 flock 부재는
기존 봇 구현의 실행 전제로, Linux에서 검증했다.

## 접수서비스 자체를 클라우드로 이전하기

앱 전달 대상만 바꾸는 전환과 별도로, 접수서비스와 Redis 요청 기록을
다른 호스트로 옮기는 절차는 [접수서비스 인계](receipt-handoff.md)를 따른다.
