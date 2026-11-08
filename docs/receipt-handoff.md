# 접수서비스와 Redis 요청 기록 이전

`durable-ingress`의 대기 요청과 처리 기록을 별도 Redis를 사용하는 새
접수서비스로 이전한다. DB 복제와 앱 전환의 순서는 외부 러너가 책임진다.
Redis의 소비자 잠금·프로세스 상태를 복제하지 않고 요청 기록과 순서를
검증하여 새 소비자가 이어받는다. 일반적인 Redis 전체 백업 도구는 아니다.

## 전환 순서

1.  새 접수서비스를 `INGRESS_STANDBY=true`로 시작한다. 대상 Redis에는
    기존 요청이 없어야 한다. 대기 상태에서는 접수와 전달을 허용하지 않는다.
1.  원본 `/handoff/seal`로 새 접수를 닫고 진행 중 전달의 완료를 기다린다.
    접수 여부와 요청 기록은 같은 Redis Lua 실행에서 검사한다. 따라서
    HTTP 요청과 봉인이 겹쳐도 성공 응답한 요청은 스냅샷에 포함된다.
1.  외부 러너가 원본 DB 쓰기를 차단하고 DB 복제를 마친다.
1.  원본의 페이지를 대상 `/handoff/import`에 전달한다. 처리 완료 기록도
    옮겨 중복 요청을 구분한다. 원본은 계속 봉인된 상태다.
1.  클라우드 앱과 DB가 준비되면 `/handoff/activate`를 호출한다. 대상은
    전체 기록의 건수·순서·해시를 다시 읽어 확인하고 새 접수를 연다.
    전달은 계속 정지 상태이므로 `/status`의 새 epoch로 `/resume`한다.
1.  원본 `/handoff/forward`를 호출한다. 이후 이전 주소의 요청은 클라우드
    접수서비스로만 전달한다. 원본 Redis에 새 요청을 저장하지 않는다.
1.  공개 경로를 클라우드로 전환하고 `/api/receipt`의 원장 ID를 확인한다.
    원본의 `/api/receipt`는 전달 모드에서도 원본 ID를 반환하므로,
    온프렘을 경유한 응답을 클라우드 직접 응답으로 오인하지 않는다.

봉인은 자동으로 해제하지 않는다. 대상 활성화 시도 이후 원본으로 쓰기를
되돌리면 두 원장이 갈라질 수 있으므로 수동 복구가 필요하다. 공개 경로와
기존 연결의 종료를 확인하기 전에는 원본 전달 프록시를 종료하지 않는다.

## 제어 API 계약

모든 `/handoff/*`에는 `INGRESS_CONTROL_TOKEN`의 Bearer 인증이 필수다.
토큰이 없는 설치에서는 인계 API가 503으로 닫힌다. 인증값·요청 본문은
로그에 남기지 않는다. `id`는 동일 이전의 재시도 동안 고정한다.

| API | 입력 | 결과 |
| --- | --- | --- |
| `GET /handoff/status` | 없음 | 원장·인계 ID, 역할, 접수 상태, manifest, 가져온 건수·해시·cursor |
| `POST /handoff/seal` | `{id,destination_ledger_id}` | 원본 봉인 후 manifest 포함 상태 |
| `GET /handoff/export?id=...&cursor=0-0` | 첫 cursor는 `0-0` | `{id,source_ledger_id,destination_ledger_id,manifest,cursor,next_cursor,records,done}` |
| `POST /handoff/import` | export 응답 그대로 | 가져오기 진척. 동일 페이지 재전송은 중복 생성하지 않음 |
| `POST /handoff/activate` | `{id,source_ledger_id,destination_ledger_id,manifest,target}` | 전체 기록 확인 후 대상 접수 개방. 전달은 정지 상태 |
| `POST /handoff/forward` | `{id,target}` | allowlist의 새 접수서비스로 원본 요청 전달 |

`manifest`는 `{count,digest,last_cursor}`다. 해시는 순서대로 각 기록의
`[entry_id,request_id,body,status]` JSON과 직전 해시를 연결한 SHA-256 체인이다.
`records`는 최대 100개이며 JSON 크기는 768KiB 이하다. 제어 HTTP 본문의
한도는 1MiB다. 요청 불변 필드를 변경하거나 페이지를 건너뛰면 거절한다.
상태는 `none → sealing → sealed → forwarding` 또는
`none → importing → active`이고, 접수 상태는 각각
`open → sealed → forwarding`, `standby → open`이다.

Redis 스크립트의 런타임 오류는 앞선 쓰기를 롤백하지 않는다. 가져오기는
동일 stream ID·요청 ID·본문·상태를 확인하며 일부 기록만 저장된 페이지도
재시도할 수 있다. 페이지 완료 해시와 cursor는 같은 HSET으로 저장한다.
활성화 직전에는 실제 저장된 원장을 다시 읽어 manifest와 대조한다.

## 배포 설정

기존 `INGRESS_TARGETS`는 앱의 주소다. 별도 `INGRESS_FORWARD_TARGETS`는
새 접수서비스의 주소이며, 제어 요청으로 임의 URL을 지정할 수 없다.

```json
{"aws_receipt":{"url":"https://receipt.example.com","token_env":"CLOUD_RECEIPT_API_TOKEN"}}
```

- `INGRESS_STANDBY=true`: 비어 있는 대상 원장을 닫힌 상태로 시작한다.
- `INGRESS_INITIAL_TARGET=aws_eks`: 대상의 앱 경로 이름이다.
- `INGRESS_API_TOKEN`: 공개 `/api/*` 인증 토큰이다. 대기 또는 공개 제어
  모드에서는 이 토큰이 없으면 시작을 거부한다.
- `INGRESS_CONTROL_TOKEN`: 제어 API 인증 토큰이다.
- `INGRESS_EXPOSE_CONTROL=true`: 인증된 `/control/*`을 8080에도 제공한다.
  HTTPS ALB 뒤에서만 사용한다. 토큰이 없으면 시작을 거부한다.
- `INGRESS_CONTROL_HOST`: 기본 `127.0.0.1`이며 9092의 네트워크 접근 범위다.

기동 시 `HANDOFF_PROTOCOL_VERSION === 1`을 검사하면 구버전 이미지가
대기 설정을 무시한 채 접수하는 것을 막을 수 있다. 대상은 Redis의 AOF,
`appendfsync always`, `noeviction`과 영속 볼륨을 사용해야 한다.
앱·접수 API·제어 API의 토큰은 각각 Secret 환경변수로 주입한다.

## 로컬 검증

아래 Redis 주소는 다른 데이터가 없는 폐기 가능한 인스턴스여야 한다.
테스트는 두 DB를 비운다. 실제 Kubernetes·AWS·DB 이전 검증은 별도다.

```sh
TEST_HANDOFF_SOURCE_REDIS_URL=redis://127.0.0.1:6388 \
TEST_HANDOFF_TARGET_REDIS_URL=redis://127.0.0.1:6389 \
node --test test/durable-ingress-handoff.integration.test.js
```

두 Redis를 사용해 접수 봉인 경쟁, 처리/대기 기록 보존, 페이지 재시도,
부분 Redis 오류 복구, 손상 검출, 재시작, 기존 주소 전달, 원본 종료 후
대상 단독 처리, 공개/제어 토큰 분리를 확인한다.
