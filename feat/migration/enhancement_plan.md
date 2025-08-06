# SageMaker Feature Store CLI 개선 계획

## 현재 상황 분석

### 기존 기능
- **list**: 온라인 피처스토어만 표시 (OnlineStoreConfig가 있는 경우만)
- **get/put**: 단일 레코드 조회/저장 (온라인 스토어 대상)
- **bulk-get/bulk-put**: 대량 데이터 조회/저장 (온라인 스토어 대상)

### 코드 구조
```
src/sagemaker_fs_cli/
├── cli.py                 # 메인 CLI 엔트리포인트
├── config.py              # AWS 설정 관리
├── commands/              # 각 명령어 구현
│   ├── list_cmd.py        # 피처그룹 목록 조회
│   ├── get_cmd.py         # 단일 레코드 조회
│   ├── put_cmd.py         # 단일 레코드 저장
│   ├── bulk_get_cmd.py    # 대량 조회
│   └── bulk_put_cmd.py    # 대량 저장
└── utils/                 # 유틸리티
    ├── formatter.py       # 출력 포맷팅
    └── file_handler.py    # 파일 처리
```

## 요구사항

### 1. List 기능 개선
- **현재**: 온라인 스토어가 활성화된 피처그룹만 표시
- **변경**: 오프라인 전용 피처그룹도 표시
- **구현**: `list_cmd.py`의 필터 조건 수정

### 2. Migration 기능 추가
새로운 `migrate` 명령어 추가:
- **Offline → Online+Offline**: 오프라인 전용 → 온라인+오프라인 구조
- **Online+Offline → Online+Offline**: 기존 데이터 마이그레이션
- **옵션**: `--clear-target` 플래그로 대상 삭제 후 진행

### 3. Clear 기능 추가
새로운 `clear` 명령어 추가:
- 피처스토어의 모든 데이터 삭제
- 온라인/오프라인 스토어 선택적 삭제 지원

## 상세 구현 계획

### Phase 1: List 기능 개선
**파일**: `src/sagemaker_fs_cli/commands/list_cmd.py`

**변경사항**:
```python
# 기존: OnlineStoreConfig가 있는 경우만 포함
if fg_details.get('OnlineStoreConfig'):

# 변경: 모든 피처그룹 포함 (온라인 또는 오프라인 스토어가 있는 경우)
if fg_details.get('OnlineStoreConfig') or fg_details.get('OfflineStoreConfig'):
```

**추가 정보 표시**:
- 스토어 타입 구분 (Online Only, Offline Only, Online+Offline)
- 오프라인 전용 그룹의 추가 정보

### Phase 2: Migration 기능 구현
**새 파일**: `src/sagemaker_fs_cli/commands/migrate_cmd.py`

**주요 기능**:
1. **소스 검증**: 소스 피처그룹 존재 및 타입 확인
2. **타겟 검증**: 타겟 피처그룹 존재 및 호환성 확인
3. **스키마 검증**: 피처 정의 호환성 확인
4. **데이터 마이그레이션**:
   - 오프라인 스토어에서 데이터 읽기 (S3/Athena)
   - 배치 처리로 온라인 스토어에 저장
5. **진행상황 모니터링**

**CLI 인터페이스**:
```bash
sagemaker-fs migrate <source-feature-group> <target-feature-group> [OPTIONS]

OPTIONS:
  --clear-target    타겟 피처그룹의 기존 데이터 삭제
  --batch-size      배치 처리 사이즈 (기본: 100)
  --max-workers     동시 처리 워커 수 (기본: 4)
  --dry-run         실제 실행 없이 계획만 확인
```

### Phase 3: Clear 기능 구현
**새 파일**: `src/sagemaker_fs_cli/commands/clear_cmd.py`

**주요 기능**:
1. **온라인 스토어 클리어**: 모든 레코드 삭제
2. **오프라인 스토어 클리어**: S3 데이터 삭제
3. **안전장치**: 확인 프롬프트 및 백업 옵션

**CLI 인터페이스**:
```bash
sagemaker-fs clear <feature-group-name> [OPTIONS]

OPTIONS:
  --online-only     온라인 스토어만 삭제
  --offline-only    오프라인 스토어만 삭제
  --force           확인 없이 즉시 삭제
  --backup-s3       삭제 전 S3에 백업
```

## 구현 순서

### 1단계: List 기능 개선 ✅ 먼저 구현
- 영향도: 낮음
- 구현 복잡도: 낮음
- 다른 기능의 기반이 됨

### 2단계: Clear 기능 구현
- 영향도: 중간
- 구현 복잡도: 중간
- Migration 기능에서 활용 가능

### 3단계: Migration 기능 구현
- 영향도: 높음
- 구현 복잡도: 높음
- 다른 기능들을 활용

## 리스크 및 고려사항

### 기술적 리스크
1. **대용량 데이터 처리**: 메모리 및 처리 시간 제한
2. **API 제한**: SageMaker API 호출 제한
3. **데이터 일관성**: 마이그레이션 중 데이터 손실 방지

### 운영 리스크
1. **데이터 손실**: Clear/Migration 시 실수로 인한 데이터 손실
2. **성능 영향**: 대용량 마이그레이션 시 시스템 부하

### 해결책
1. **배치 처리**: 대용량 데이터를 작은 단위로 분할 처리
2. **재시도 로직**: API 제한 및 일시적 오류 처리
3. **백업 및 검증**: 데이터 손실 방지를 위한 안전장치
4. **Dry-run 모드**: 실제 실행 전 계획 확인

## 다음 단계

1. **1단계 구현 확인**: List 기능 개선 승인
2. **2단계 구현 확인**: Clear 기능 설계 검토
3. **3단계 구현 확인**: Migration 기능 상세 설계
4. **순차적 구현**: 각 단계별 구현 및 테스트

각 단계마다 사용자 승인을 받고 진행하겠습니다.