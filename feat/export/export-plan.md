# Feature Store Export 기능 구현 계획

## 개요
SageMaker Feature Store Offline Store에서 모든 데이터를 CSV 또는 JSON 형식으로 내보내기하는 기능을 추가합니다. Athena를 통해 데이터를 조회하고 로컬 파일로 저장할 수 있습니다.

## 명령어 설계

### 기본 명령어 구조
```bash
fs export [OPTIONS] FEATURE_GROUP_NAME OUTPUT_FILE
```

### 주요 옵션
- `--format`, `-f`: 출력 형식 (csv, json, parquet) - 기본값: csv
- `--limit`: 내보낼 레코드 수 제한 (기본값: 전체)
- `--where`: SQL WHERE 조건절 추가
- `--columns`: 내보낼 컬럼 선택 (기본값: 전체)
- `--order-by`: 정렬 기준 컬럼 지정
- `--compress/--no-compress`: 압축 여부 (기본값: True)
- `--chunk-size`: 배치 처리 크기 (기본값: 10000)
- `--s3-output-location`: Athena 쿼리 결과 임시 저장 S3 위치
- `--database`: Athena 데이터베이스 이름 (기본값: sagemaker_featurestore)
- `--dry-run`: 실제 내보내기 없이 쿼리 및 예상 결과만 표시

#### Online Store 호환성 옵션
- `--online-compatible`: Online store bulk-put 호환 형식으로 변환
- `--column-mapping`: 컬럼명 매핑 지정 (예: "event_time:EventTime,customer_id:record_id")
- `--add-event-time`: EventTime 필드 자동 추가/변환
- `--record-identifier`: 레코드 식별자 필드명 지정

### 사용 예시
```bash
# 기본 내보내기 (CSV 형식)
fs export my-feature-group data.csv

# JSON 형식으로 내보내기
fs export my-feature-group data.json --format json

# 특정 컬럼만 내보내기
fs export my-feature-group data.csv --columns "customer_id,age,balance"

# 조건부 내보내기
fs export my-feature-group recent_data.csv --where "event_time >= '2024-01-01'"

# 최대 1000건만 내보내기
fs export my-feature-group sample_data.csv --limit 1000

# 정렬된 데이터 내보내기
fs export my-feature-group sorted_data.csv --order-by "event_time DESC"

# 압축된 파일로 내보내기
fs export my-feature-group data.csv.gz --compress

# Parquet 형식으로 내보내기
fs export my-feature-group data.parquet --format parquet

# 내보내기 계획만 확인
fs export my-feature-group data.csv --dry-run

# Online Store 호환 형식으로 내보내기
fs export my-feature-group online_data.json --online-compatible

# 컬럼명 매핑하여 내보내기
fs export my-feature-group mapped_data.csv --column-mapping "event_time:EventTime,customer_id:record_id"

# EventTime 필드 자동 추가
fs export my-feature-group with_time.json --add-event-time
```

## 구현 세부사항

### 1. 파일 구조
- `src/sagemaker_fs_cli/commands/offline_export_cmd.py`: 메인 구현 파일
- CLI 명령어를 `cli.py`에 등록

### 2. 주요 기능

#### `offline_export()`
- 메인 CLI 명령어 함수
- 옵션 검증 및 파일 경로 처리
- Athena 연결 및 쿼리 실행

#### `_validate_options()`
- 옵션 조합 검증
- 출력 파일 경로 유효성 확인
- Feature Group offline store 존재 확인

#### `_build_query(feature_group_name, columns, where, order_by, limit)`
- SQL 쿼리 동적 생성
- Feature Group에 맞는 테이블 이름 찾기
- WHERE, ORDER BY, LIMIT 절 처리

#### `_find_athena_table_name(feature_group_name)`
- Feature Group에 대응하는 Athena 테이블 이름 찾기
- 다양한 테이블 명명 규칙 대응

#### `_execute_athena_query(query, output_location)`
- Athena 쿼리 실행
- 쿼리 상태 모니터링
- 결과 위치 반환

#### `_export_query_results(result_location, output_file, format, compress)`
- Athena 쿼리 결과를 S3에서 내보내기
- 형식 변환 (CSV → JSON, Parquet 등)
- 압축 처리

#### `_process_results_by_chunks(result_location, output_file, format, chunk_size)`
- 대용량 데이터를 청크 단위로 처리
- 메모리 효율적인 스트리밍 처리
- 진행률 표시

#### `_convert_format(input_data, target_format)`
- CSV → JSON, Parquet 변환
- 데이터 타입 자동 추론
- 스키마 보존

#### `_apply_online_compatibility(data, options)`
- Online store 호환성 변환
- 컬럼명 매핑 적용
- EventTime 필드 처리
- 데이터 타입 정규화

### 3. 데이터 처리 플로우

1. **전처리**
   - Feature Group 존재 및 Offline Store 활성화 확인
   - Athena 테이블 이름 조회
   - 출력 디렉토리 생성

2. **쿼리 생성**
   - 동적 SQL 쿼리 구성
   - 컬럼, 조건, 정렬, 제한 적용

3. **Athena 실행**
   - 쿼리 제출 및 실행 상태 모니터링
   - S3 결과 위치 확인

4. **데이터 내보내기**
   - S3에서 결과 파일 내보내기
   - 청크 단위 스트리밍 처리
   - 진행률 실시간 표시

5. **후처리**
   - 형식 변환 (필요시)
   - Online store 호환성 변환
   - 압축 처리
   - 임시 파일 정리

### 4. SQL 쿼리 예시

#### 기본 쿼리
```sql
SELECT * FROM "sagemaker_featurestore"."my_feature_group_1234567890123"
```

#### 조건부 쿼리
```sql
SELECT customer_id, age, balance 
FROM "sagemaker_featurestore"."my_feature_group_1234567890123"
WHERE event_time >= '2024-01-01'
ORDER BY event_time DESC
LIMIT 1000
```

#### 집계 쿼리 지원
```sql
SELECT customer_id, COUNT(*) as record_count, MAX(event_time) as latest_event
FROM "sagemaker_featurestore"."my_feature_group_1234567890123"
WHERE event_time >= '2024-01-01'
GROUP BY customer_id
ORDER BY record_count DESC
```

### 5. 출력 형식별 처리

#### CSV 형식
- 기본 Athena 출력 형식
- 헤더 포함
- UTF-8 인코딩

#### JSON 형식
- 줄바꿈으로 구분된 JSON (JSONL)
- 각 레코드를 별도 JSON 객체로 저장
- 중첩 구조 지원

#### Parquet 형식
- 컬럼형 저장 형식
- 압축 효율성 높음
- 스키마 정보 보존

### 6. Online Store 호환성 처리

#### 컬럼명 매핑
```python
# 기본 매핑 규칙
DEFAULT_COLUMN_MAPPINGS = {
    'event_time': 'EventTime',
    'eventtime': 'EventTime',
    'timestamp': 'EventTime',
    'created_at': 'EventTime'
}

# 사용자 지정 매핑
custom_mapping = {
    'customer_id': 'record_id',
    'event_time': 'EventTime'
}
```

#### EventTime 처리
- Offline store의 타임스탬프를 Unix timestamp로 변환
- 없는 경우 현재 시간으로 자동 생성
- 형식: 문자열 형태의 Unix timestamp (예: "1640995200")

#### 데이터 타입 정규화
- 모든 값을 문자열로 변환 (Online store 요구사항)
- NULL 값 처리 (빈 문자열 또는 기본값으로 변환)
- 특수 문자 및 인코딩 처리

#### 호환성 검증
```python
def validate_online_compatibility(data, feature_group_schema):
    """
    - 필수 필드 존재 확인 (record_identifier, EventTime)
    - 스키마 일치성 검증
    - 데이터 타입 호환성 확인
    """
```

### 7. 성능 최적화

#### 청크 처리
- 기본 10,000건씩 배치 처리
- 메모리 사용량 제한
- 대용량 데이터셋 지원

#### 스트리밍 내보내기
- S3 결과를 직접 스트리밍
- 임시 저장 공간 최소화
- 네트워크 효율성

#### 병렬 처리
- 다중 청크 동시 처리 (옵션)
- CPU 코어 활용 최적화

### 8. 에러 처리

1. **Feature Group 오류**
   - Offline Store 비활성화
   - 테이블 존재하지 않음

2. **Athena 쿼리 오류**
   - SQL 문법 오류
   - 권한 부족
   - 쿼리 시간 초과

3. **S3 접근 오류**
   - 버킷 권한 부족
   - 네트워크 연결 문제

4. **파일 시스템 오류**
   - 디스크 공간 부족
   - 쓰기 권한 없음

5. **호환성 오류**
   - 컬럼 매핑 실패
   - 스키마 불일치
   - 데이터 타입 변환 오류

### 9. 진행률 표시

```
📥 데이터 내보내기 시작...

✓ Feature Group 검증 완료: my-feature-group
✓ Athena 테이블 확인: sagemaker_featurestore.my_feature_group_1234567890123
✓ 쿼리 생성 완료

⠋ Athena 쿼리 실행 중...
✓ 쿼리 실행 완료 (12,345건 예상)

⠋ 데이터 내보내기 중... [████████████████████] 12,345/12,345 (100%)
✓ 데이터 변환 완료 (JSON 형식)
✓ 압축 완료 (gzip)

✅ 내보내기 완료!

내보내기 요약:
  - Feature Group: my-feature-group
  - 총 레코드 수: 12,345건
  - 출력 파일: data.json.gz (2.3MB)
  - 소요 시간: 1분 23초
```

### 10. Dry-run 출력

```
🔍 내보내기 계획 (Dry Run)

Feature Group: my-feature-group
  - Athena 테이블: sagemaker_featurestore.my_feature_group_1234567890123
  - 출력 파일: data.csv

실행할 쿼리:
  SELECT customer_id, age, balance 
  FROM "sagemaker_featurestore"."my_feature_group_1234567890123"
  WHERE event_time >= '2024-01-01'
  ORDER BY event_time DESC
  LIMIT 1000

예상 결과:
  - 컬럼 수: 3개 (customer_id, age, balance)
  - 예상 레코드 수: ~1,000건
  - 예상 파일 크기: ~50KB
  - Athena 쿼리 비용: ~$0.001

실제 내보내기를 실행하려면 --dry-run 옵션을 제거하세요.
```

## 테스트 시나리오

1. **기본 내보내기 테스트**
   - 전체 데이터 CSV 내보내기
   - 소용량/대용량 데이터셋

2. **형식 변환 테스트**
   - CSV → JSON 변환
   - CSV → Parquet 변환

3. **조건부 내보내기 테스트**
   - WHERE 절 필터링
   - 컬럼 선택
   - 정렬 및 제한

4. **에러 처리 테스트**
   - 존재하지 않는 Feature Group
   - 잘못된 SQL 조건
   - 권한 부족 상황

5. **성능 테스트**
   - 대용량 데이터 (100만건+)
   - 청크 크기 최적화
   - 메모리 사용량 모니터링

6. **압축 테스트**
   - gzip, bzip2 압축
   - 압축률 및 성능 비교

7. **Online Store 호환성 테스트**
   - offline-export → bulk-put 파이프라인
   - 컬럼명 매핑 정확성 검증
   - EventTime 변환 테스트
   - 데이터 타입 호환성 확인

## 의존성

- boto3: Athena, S3 클라이언트
- pandas: 데이터 형식 변환 (선택적)
- pyarrow: Parquet 형식 지원 (선택적)
- tqdm: 진행률 표시
- gzip/bz2: 압축 처리

## 보안 고려사항

1. **데이터 접근 권한**
   - Athena 쿼리 실행 권한
   - S3 버킷 읽기 권한
   - Glue 카탈로그 접근 권한

2. **데이터 보호**
   - 임시 파일 안전한 삭제
   - S3 전송 암호화
   - 로컬 파일 권한 설정

3. **비용 관리**
   - 쿼리 크기 제한
   - 예상 비용 표시
   - 대용량 쿼리 경고

## 향후 개선 방안

1. **고급 쿼리 지원**
   - JOIN 연산 지원
   - 집계 함수 활용
   - 복합 조건 처리

2. **스케줄링 기능**
   - 정기적 백업 내보내기
   - 증분 데이터 동기화

3. **클라우드 통합**
   - 다른 스토리지 서비스 지원
   - 데이터 파이프라인 연계