# SageMaker FeatureStore Online CLI

AWS SageMaker FeatureStore Online 스토어를 관리하기 위한 명령줄 도구입니다.

## 기능

- **list**: 온라인 피처스토어 목록 조회
- **get**: 단일 레코드 조회
- **put**: 단일 레코드 업데이트
- **bulk-get**: JSON/CSV 파일을 통한 대량 데이터 조회
- **bulk-put**: JSON/CSV 파일을 통한 대량 데이터 업데이트

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
sagemaker-fs [--profile PROFILE] [--region REGION] COMMAND [OPTIONS]
```

또는 짧은 명령어:

```bash
sm-fs [--profile PROFILE] [--region REGION] COMMAND [OPTIONS]
```

### 1. 피처스토어 목록 조회

```bash
# 테이블 형태로 출력
sagemaker-fs list

# JSON 형태로 출력
sagemaker-fs list --output-format json
```

### 2. 단일 레코드 조회

```bash
# 기본 조회
sagemaker-fs get my-feature-group record-id-123

# 특정 피처만 조회
sagemaker-fs get my-feature-group record-id-123 --feature-names "feature1,feature2,feature3"

# JSON 형태로 출력
sagemaker-fs get my-feature-group record-id-123 --output-format json
```

### 3. 단일 레코드 업데이트

```bash
sagemaker-fs put my-feature-group --record '{"feature1": "value1", "feature2": "value2", "record_id": "123"}'
```

### 4. 대량 데이터 조회

```bash
# JSON 파일에서 레코드 ID 목록을 읽어 조회
sagemaker-fs bulk-get my-feature-group input_ids.json

# 결과를 파일로 저장
sagemaker-fs bulk-get my-feature-group input_ids.json --output-file results.json

# CSV 파일 사용
sagemaker-fs bulk-get my-feature-group input_ids.csv --output-file results.csv

# 특정 피처만 조회
sagemaker-fs bulk-get my-feature-group input_ids.json --feature-names "feature1,feature2"
```

### 5. 대량 데이터 업데이트

```bash
# JSON 파일에서 레코드들을 읽어 업데이트
sagemaker-fs bulk-put my-feature-group records.json

# CSV 파일 사용
sagemaker-fs bulk-put my-feature-group records.csv
```

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
   sagemaker-fs list
   ```

2. 샘플 데이터 조회:
   ```bash
   sagemaker-fs get my-feature-group sample-id-123
   ```

3. 대량 데이터 조회:
   ```bash
   sagemaker-fs bulk-get my-feature-group record_ids.json -o results.json
   ```

4. 데이터 업데이트:
   ```bash
   sagemaker-fs bulk-put my-feature-group updated_records.json
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
