# SageMaker FeatureStore CLI 사용 예제

## 기본 설정

먼저 AWS CLI가 올바르게 설정되어 있는지 확인하세요:

```bash
aws configure list
aws sts get-caller-identity
```

## 1. 피처스토어 목록 확인

```bash
# 모든 온라인 피처스토어 목록 확인
sagemaker-fs list

# JSON 형태로 출력
sagemaker-fs list -o json
```

## 2. 단일 레코드 조회

```bash
# 기본 조회
sagemaker-fs get customer-features customer_001

# 특정 피처만 조회
sagemaker-fs get customer-features customer_001 --feature-names "age,income"

# 테이블 형태로 출력
sagemaker-fs get customer-features customer_001 -o table
```

## 3. 단일 레코드 업데이트

```bash
# 새 레코드 추가/업데이트
sagemaker-fs put customer-features --record '{"record_id": "customer_001", "age": 26, "income": 55000, "location": "Seoul"}'
```

## 4. 대량 데이터 조회

### JSON 파일 사용

```bash
# 샘플 JSON 파일로 조회
sagemaker-fs bulk-get customer-features examples/sample_record_ids.json

# 결과를 파일로 저장
sagemaker-fs bulk-get customer-features examples/sample_record_ids.json -o results.json

# 특정 피처만 조회
sagemaker-fs bulk-get customer-features examples/sample_record_ids.json --feature-names "age,income"
```

### CSV 파일 사용

```bash
# CSV 파일로 조회
sagemaker-fs bulk-get customer-features examples/sample_record_ids.csv -o results.csv
```

## 5. 대량 데이터 업데이트

### JSON 파일 사용

```bash
# JSON 파일에서 레코드 읽어서 업데이트
sagemaker-fs bulk-put customer-features examples/sample_records.json
```

### CSV 파일 사용

```bash
# CSV 파일에서 레코드 읽어서 업데이트
sagemaker-fs bulk-put customer-features examples/sample_records.csv
```

## 6. 다른 AWS 프로필/리전 사용

```bash
# 특정 프로필 사용
sagemaker-fs --profile production list

# 특정 리전 사용
sagemaker-fs --region us-west-2 list

# 프로필과 리전 모두 지정
sagemaker-fs --profile production --region us-west-2 list
```

## 일반적인 워크플로우 예제

### 1. 데이터 파이프라인에서 피처 업데이트

```bash
#!/bin/bash

# 1. 최신 피처스토어 목록 확인
echo "Available feature groups:"
sagemaker-fs list

# 2. 새로운 피처 데이터를 배치로 업데이트
echo "Updating customer features..."
sagemaker-fs bulk-put customer-features daily_features.json

# 3. 업데이트 결과 확인 (샘플 레코드 조회)
echo "Verifying updates..."
sagemaker-fs get customer-features customer_001
```

### 2. 모델 추론을 위한 피처 조회

```bash
#!/bin/bash

# 1. 추론할 고객 ID 목록으로 피처 조회
sagemaker-fs bulk-get customer-features inference_customer_ids.json -o customer_features.json

# 2. 조회 결과를 모델 입력 형태로 변환 (별도 스크립트)
python prepare_model_input.py customer_features.json model_input.json

# 3. 모델 추론 실행
python run_inference.py model_input.json predictions.json
```

### 3. A/B 테스트를 위한 피처 조회

```bash
#!/bin/bash

# A 그룹 고객들의 피처 조회
sagemaker-fs bulk-get customer-features group_a_customers.json -o group_a_features.json

# B 그룹 고객들의 피처 조회
sagemaker-fs bulk-get customer-features group_b_customers.json -o group_b_features.json

# 특정 피처만 조회하여 분석
sagemaker-fs bulk-get customer-features all_customers.json --feature-names "age,income,location" -o analysis_features.json
```

## 에러 처리 예제

### 1. 존재하지 않는 레코드 처리

```bash
# 존재하지 않는 레코드 조회 시도
sagemaker-fs get customer-features non_existent_customer

# bulk-get 사용 시 일부 레코드가 존재하지 않는 경우의 처리
sagemaker-fs bulk-get customer-features mixed_record_ids.json -o partial_results.json
```

### 2. 권한 오류 처리

```bash
# IAM 권한 확인
aws sts get-caller-identity

# 필요한 권한이 있는지 테스트
aws sagemaker list-feature-groups
aws sagemaker describe-feature-group --feature-group-name customer-features
```

## 성능 최적화 팁

### 1. 대량 데이터 처리

```bash
# 큰 파일을 처리할 때는 배치 크기를 고려하여 분할
split -l 1000 large_record_ids.json batch_

# 각 배치를 병렬로 처리
for batch in batch_*; do
    sagemaker-fs bulk-get customer-features "$batch" -o "result_$batch.json" &
done
wait
```

### 2. 특정 피처만 조회하여 네트워크 비용 절약

```bash
# 필요한 피처만 조회
sagemaker-fs bulk-get customer-features record_ids.json --feature-names "age,income" -o minimal_features.json
```

이러한 예제들을 참고하여 실제 프로젝트에서 SageMaker FeatureStore CLI를 효과적으로 활용하실 수 있습니다.