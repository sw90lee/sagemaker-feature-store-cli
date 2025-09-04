# SageMaker FeatureStore CLI

AWS SageMaker FeatureStore Online/Offline 스토어를 관리하기 위한 명령줄 도구입니다.

## 기능

### Online Store 기능
- **list**: 온라인 피처스토어 목록 조회
- **get**: 단일 레코드 조회
- **put**: 단일 레코드 업데이트
- **bulk-get**: JSON/CSV 파일을 통한 대량 데이터 조회 (--current-time 옵션으로 Time 필드를 현재 시간으로 교체 가능)
- **bulk-put**: JSON/CSV 파일을 통한 대량 데이터 업데이트

### Offline Store 기능
- **export**: 오프라인 피처스토어 데이터를 파일로 내보내기 (CSV, JSON, Parquet)

### 피처스토어 관리 기능
- **create**: 새 피처그룹 생성
- **delete**: 피처그룹 삭제
- **analyze**: 피처스토어 오프라인 스토어(S3) 용량 및 비용 분석

### 스키마 관리 기능
- **schema**: 피처 그룹의 현재 스키마 조회
- **add-features**: 새로운 feature definition과 기존 스키마 비교 분석
- **schema-template**: feature definition 템플릿 파일 생성

## 설치


### PyPI에서 설치 (패키지가 게시된 경우)

```bash
pip install sagemaker-featurestore-cli
```

### 소스에서 설치

```bash
git clone <repository-url>
cd feature_store_online_cli
pip install -e .
```

### 실행 파일 사용

릴리스 페이지에서 플랫폼에 맞는 실행 파일을 다운로드하여 사용할 수 있습니다.

## 사용법

### 기본 명령어 구조

```bash
fs batch-update s3_Bucket  --column Origin_time  --transform-function extract_time_prefix    --source-column Filename     --prefix-pattern "(\d{14})"     --time-format auto     --to-iso --no-dry-run --deduplicate

```

```bash
fs [--profile PROFILE] [--region REGION] COMMAND [OPTIONS]
```

또는 짧은 명령어:

```bash
sm-fs [--profile PROFILE] [--region REGION] COMMAND [OPTIONS]
```

### 1. 피처스토어 목록 조회

```bash
# 테이블 형태로 출력
fs list

# JSON 형태로 출력
fs list --output-format json
```

### 2. 단일 레코드 조회

```bash
# 기본 조회
fs get my-feature-group record-id-123

# 특정 피처만 조회
fs get my-feature-group record-id-123 --feature-names "feature1,feature2,feature3"

# JSON 형태로 출력
fs get my-feature-group record-id-123 --output-format json
```

### 3. 단일 레코드 업데이트

```bash
fs put my-feature-group --record '{"feature1": "value1", "feature2": "value2", "record_id": "123"}'
```

### 4. 대량 데이터 조회

```bash
# JSON 파일에서 레코드 ID 목록을 읽어 조회
fs bulk-get my-feature-group input_ids.json

# 결과를 파일로 저장
fs bulk-get my-feature-group input_ids.json --output-file results.json

# CSV 파일 사용
fs bulk-get my-feature-group input_ids.csv --output-file results.csv

# 특정 피처만 조회
fs bulk-get my-feature-group input_ids.json --feature-names "feature1,feature2"

# Time 필드를 현재 시간으로 교체하여 조회
fs bulk-get my-feature-group input_ids.json --current-time
fs bulk-get my-feature-group input_ids.json -c
```

### 5. 대량 데이터 업데이트

```bash
# JSON 파일에서 레코드들을 읽어 업데이트
fs bulk-put my-feature-group records.json

# CSV 파일 사용
fs bulk-put my-feature-group records.csv

# 배치 크기 조정으로 성능 향상
fs bulk-put my-feature-group records.csv --batch-size 500

# 결과 로그를 파일로 저장
fs bulk-put my-feature-group records.json \
  --output-file logs.txt --batch-size 200
```

**성능 최적화:**
- `--batch-size`: 배치 처리 크기 (기본값: 100, 최대 1000 권장)
- 더 큰 배치 사이즈는 처리 속도를 향상시키지만, 너무 크면 메모리 사용량이 증가합니다
- 네트워크 상황과 데이터 크기에 따라 최적값이 달라집니다

### 6. 오프라인 스토어 데이터 내보내기

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

# 압축된 파일로 내보내기
fs export my-feature-group data.csv.gz --compress

# Online Store 호환 형식으로 내보내기
fs export my-feature-group online_data.json --online-compatible

# 컬럼명 매핑하여 내보내기
fs export my-feature-group mapped_data.csv --column-mapping "event_time:EventTime,customer_id:record_id"

# 내보내기 계획만 확인
fs export my-feature-group data.csv --dry-run
```

### 7. 피처그룹 생성

```bash
# 기본 피처그룹 생성 (Online + Offline store)
fs create my-feature-group \
  --schema-file schema.json \
  --role-arn arn:aws:iam::123456789012:role/SageMakerRole \
  --s3-uri s3://my-bucket/feature-store/

# Online store만 생성
fs create my-online-feature-group \
  --schema-file schema.json \
  --role-arn arn:aws:iam::123456789012:role/SageMakerRole \
  --no-offline-store

# TTL(자동 만료) 설정과 함께 생성
fs create my-feature-group \
  --schema-file schema.json \
  --role-arn arn:aws:iam::123456789012:role/SageMakerRole \
  --s3-uri s3://my-bucket/feature-store/ \
  --ttl-duration 30 \
  --description "30일 후 자동 삭제되는 피처 그룹"

# 고급 설정으로 생성
fs create my-advanced-feature-group \
  --schema-file schema.json \
  --role-arn arn:aws:iam::123456789012:role/SageMakerRole \
  --s3-uri s3://my-bucket/feature-store/ \
  --description "고객 프로필 피처 그룹" \
  --record-identifier-name customer_id \
  --event-time-feature-name timestamp \
  --ttl-duration 365 \
  --enable-encryption \
  --kms-key-id alias/sagemaker-key \
  --table-format Glue \
  --tags environment=production \
  --tags team=ml
```

**TTL(Time To Live) 기능:**
- `--ttl-duration`: Online store 데이터의 자동 만료 기간 (1-365일)
- TTL 설정 시 Online store가 자동으로 활성화됩니다
- 설정된 기간 후 데이터가 자동으로 삭제되어 스토리지 비용을 절약할 수 있습니다

### 8. 피처그룹 삭제

```bash
# 피처그룹 삭제
fs delete my-feature-group

# 강제 삭제 (확인 없이)
fs delete my-feature-group --force
```

### 9. 피처스토어 오프라인 스토어 용량 및 비용 분석

⚠️ **주의**: 이 명령어는 오프라인 스토어(S3)만 분석합니다. 온라인 스토어(DynamoDB)는 분석하지 않습니다.

```bash
# 특정 피처그룹의 오프라인 스토어 분석
fs analyze my-feature-group

# S3 위치 직접 분석
fs analyze --bucket my-bucket --prefix path/to/data

# 결과를 CSV로 내보내기
fs analyze my-feature-group --export analysis_report.csv

# JSON 형식으로 출력
fs analyze my-feature-group --output-format json
```

### 10. 스키마 관리 기능

#### 피처 그룹의 현재 스키마 조회

```bash
# 테이블 형태로 스키마 출력
fs schema my-feature-group

# JSON 형태로 스키마 출력
fs schema my-feature-group --output-format json
```

#### 새로운 feature definition 템플릿 생성

```bash
# 기본 템플릿 생성
fs schema-template

# 특정 파일명으로 템플릿 생성
fs schema-template --output my_features.json
```

#### 새로운 feature definition과 기존 스키마 비교

```bash
# 새로운 feature definition과 현재 스키마 비교
fs add-features my-feature-group new_features.json

# 계획만 확인 (파일 생성 없음)
fs add-features my-feature-group new_features.json --dry-run
```

**스키마 관리 워크플로우:**
1. `fs schema-template` - 템플릿 파일 생성
2. 템플릿 파일을 편집하여 원하는 feature들 정의
3. `fs add-features` - 현재 스키마와 비교 분석
4. 필요시 새로운 feature group 생성 또는 데이터 마이그레이션

⚠️ **주의**: SageMaker FeatureStore는 생성된 피처 그룹의 스키마를 직접 변경할 수 없습니다. 
스키마를 변경하려면 새로운 피처 그룹을 생성하고 데이터를 마이그레이션해야 합니다.

## 파일 형식

### 입력 파일 형식 (bulk-get용)

**JSON 형식:**
```json
[
  {"record_id": "123"},
  {"record_id": "456"},
  {"record_id": "789"}
]
```

**CSV 형식:**
```csv
record_id
123
456
789
```

### 입력 파일 형식 (bulk-put용)

**JSON 형식:**
```json
[
  {
    "record_id": "123",
    "feature1": "value1",
    "feature2": 42,
    "EventTime": "1640995200"
  },
  {
    "record_id": "456",
    "feature1": "value2",
    "feature2": 84
  }
]
```

**CSV 형식:**
```csv
record_id,feature1,feature2,EventTime
123,value1,42,1640995200
456,value2,84,
```

## AWS 인증

이 도구는 AWS CLI와 동일한 인증 방식을 사용합니다:

1. AWS CLI 프로필: `--profile` 옵션 사용
2. 환경 변수: `AWS_PROFILE`, `AWS_DEFAULT_REGION`
3. IAM 역할 (EC2, Lambda 등에서 실행시)
4. AWS 자격 증명 파일

### 필요한 IAM 권한

#### Online Store 사용 시
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sagemaker:ListFeatureGroups",
        "sagemaker:DescribeFeatureGroup",
        "sagemaker-featurestore-runtime:GetRecord",
        "sagemaker-featurestore-runtime:PutRecord"
      ],
      "Resource": "*"
    }
  ]
}
```

#### Offline Store (export) 사용 시 추가 권한
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:ListTableMetadata",
        "glue:GetDatabase",
        "glue:GetTable",
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": "*"
    }
  ]
}
```

#### 피처그룹 관리 (create/delete) 시 추가 권한
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sagemaker:CreateFeatureGroup",
        "sagemaker:DeleteFeatureGroup",
        "iam:GetRole",
        "iam:PassRole"
      ],
      "Resource": "*"
    }
  ]
}
```

#### 피처스토어 오프라인 스토어 분석 (analyze) 시 추가 권한
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": [
        "arn:aws:s3:::your-feature-store-bucket",
        "arn:aws:s3:::your-feature-store-bucket/*"
      ]
    }
  ]
}
```

## 빌드

### 개발 환경 설정

```bash
# 가상환경 생성
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 또는
venv\Scripts\activate  # Windows

# 의존성 설치
pip install -r requirements.txt

# 개발 모드로 설치
pip install -e .
```

### 실행 파일 빌드

**Linux/Mac:**
```bash
./scripts/build.sh
```

**Windows:**
```bat
scripts\build.bat
```

**Python 스크립트로 빌드:**
```bash
python scripts/build.py
```

빌드된 실행 파일은 `dist/` 디렉토리에 생성됩니다.

## 예제

### 예제 파일들

프로젝트의 `examples/` 디렉토리에서 다양한 사용 예제를 찾을 수 있습니다.

### 일반적인 워크플로우

1. 사용 가능한 피처스토어 확인:
   ```bash
   fs list
   ```

2. 샘플 데이터 조회:
   ```bash
   fs get my-feature-group sample-id-123
   ```

3. 대량 데이터 조회:
   ```bash
   fs bulk-get my-feature-group record_ids.json -o results.json
   ```

4. 데이터 업데이트:
   ```bash
   fs bulk-put my-feature-group updated_records.json
   ```

## 문제 해결

### 일반적인 오류

1. **인증 오류**: AWS 자격 증명이 올바르게 설정되었는지 확인
2. **권한 오류**: IAM 정책에 필요한 권한이 포함되어 있는지 확인
3. **피처스토어 없음**: 온라인 스토어가 활성화된 피처스토어인지 확인

### 디버그 모드

더 자세한 로그를 보려면 환경 변수를 설정하세요:

```bash
export AWS_CLI_FILE_ENCODING=UTF-8
export PYTHONPATH=$PYTHONPATH:./src
```

## 기여하기

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 라이선스

MIT License

## 지원

이슈나 질문이 있으시면 GitHub Issues를 사용해 주세요.# sagemaker-feature-store-cli
