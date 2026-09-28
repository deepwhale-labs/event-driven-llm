# event-driven-llm

Kafka 명령을 로컬 Ollama 모델로 처리하고 결과를 Kafka 토픽에 기록하는 최소 실행 예제입니다. Spring Boot 앱이 개발용 명령 API와 워커를 함께 실행합니다. Coral 연동은 결과 토픽 뒤에 붙일 예정이며 현재 포함되어 있지 않습니다.

## 구성

`POST /api/commands` → `llm-commands` → Spring Kafka 워커 → Ollama → `llm-results`

워커 실패 시 짧게 재시도한 뒤 원본 명령을 `llm-commands.DLT`에 보냅니다. 결과 메시지는 `taskId`, `nodeId`, `output`을 담습니다. 명령을 발행한 HTTP 응답은 작업 완료가 아닌 Kafka 접수만 뜻합니다.

## Docker Compose로 실행

Docker Desktop 또는 Docker Engine과 Compose가 필요합니다. 첫 실행은 모델 다운로드가 필요합니다.

```bash
docker compose up -d broker ollama
docker compose exec ollama ollama pull llama3.2:1b
docker compose up --build -d app
```

NVIDIA GPU를 Docker에서 사용할 수 있는 호스트라면 위 명령의 `docker compose` 뒤에 `-f docker-compose.yml -f docker-compose.gpu.yml`을 붙여 GPU 설정을 적용할 수 있습니다.

PowerShell에서 명령을 발행합니다.

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/commands -ContentType 'application/json' -Body '{"prompt":"Kafka를 한 문장으로 설명해줘"}'
```

결과와 실패 메시지는 각각 아래 명령으로 확인할 수 있습니다.

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker:19092 --topic llm-results --from-beginning
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker:19092 --topic llm-commands.DLT --from-beginning
```

`OLLAMA_MODEL` 환경 변수로 모델 이름을 바꿀 수 있으며, 해당 모델을 Ollama에 먼저 받아야 합니다. JDK 21이 있는 환경에서는 `./gradlew bootRun` 또는 Windows의 `./gradlew.bat bootRun`으로 앱만 실행할 수 있습니다. 이때 Kafka와 Ollama는 로컬 기본 포트에서 실행 중이어야 합니다.

## Git 및 Docker 포함 범위

`.gitignore`는 기본적으로 모든 파일을 제외하고 Java 소스, 공용 `application.yml`, Gradle Wrapper, 명시된 빌드·Compose 파일과 README만 허용합니다. 새 문서, 테스트 리소스, CI 설정 등을 추가하면 필요한 경로만 허용 목록에 추가하세요. 로컬 환경 설정, 비밀키·인증서, 로그, 덤프, 모델, 데이터, 빌드 결과는 제외합니다. `.dockerignore`도 빌드에 필요한 파일만 전달합니다.

공용 설정과 Java 소스에는 비밀값 대신 환경 변수 참조를 사용하세요. Ignore 규칙은 파일 내용의 비밀값이나 이미 Git이 추적하는 파일을 차단하지 않습니다. 커밋 전 `git diff --cached`로 실제 포함 내용을 확인하세요.

## 범위와 다음 작업

Compose 설정은 단일 브로커와 평문 연결을 쓰는 로컬 개발용입니다. 운영 전에는 네트워크 경계, 인증과 암호화, Kafka 복제, 보관 기간을 결정해야 합니다. 워커는 메시지 하나씩 동기 처리하며 `max.poll.interval.ms`는 15분입니다. 이보다 긴 추론, 노드별 작업 라우팅, 결과 중복 제거와 Coral 멘션은 실제 업무 요구에 맞춰 설계해야 합니다. 특히 결과 토픽 발행 후 오프셋 확정 전에 워커가 종료되면 같은 `taskId`의 결과가 다시 발행될 수 있습니다.
