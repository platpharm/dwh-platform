PharmDB 파이프라인을 실행하고 Excel로 내보냅니다.

## Instructions

### Step 1: 컬렉터 실행

`pharm-db/crawler/main.py`를 사용하여 데이터를 수집합니다.

사용자가 특정 컬렉터를 지정하지 않으면 전체를 실행합니다.

```bash
cd pharm-db/crawler

# 전체 실행
python main.py -v

# 특정 컬렉터만 실행
python main.py --collectors <collector_names> -v
```

사용 가능한 컬렉터 목록 (13개):
- pharmacy_nic - 국립중앙의료원 약국 정보
- pharmacy_hira - 건강보험심사평가원 약국 정보
- hospital_nic - 국립중앙의료원 병원 정보
- hospital_hira - 건강보험심사평가원 병원 정보
- store_semas - 소상공인진흥공단 상가 정보
- medical_type - 의료기관종별 정보
- health_stat - 보건의료 통계
- building_ledger - 건축물대장 (면적 정보, bldMngNo 입력 필요)
- population_stat - 주민등록 인구/세대 통계
- business_status - 의료기관 운영상태 (폐업/휴업)
- commercial_zone - 상권 영역 정보
- medical_department - 의료기관 진료과목 상세
- prescription_stat - 의약품 처방 통계

**주의사항:**
- building_ledger는 bldMngNo 입력이 필요하여 단독 실행 불가
- health_stat 약품사용정보 API는 별도 구독 필요
- 파이프라인 실행 시 timeout을 600초(10분)로 설정

### Step 2: 실행 결과 확인

파이프라인 실행 결과를 요약하여 사용자에게 보여줍니다:
- 각 컬렉터별 성공/실패 여부
- 수집된 문서 수
- 소요 시간

### Step 3: Excel 내보내기

데이터 수집이 완료되면 Excel로 내보냅니다.

```bash
cd pharm-db
python scripts/export_to_excel.py -v
```

또는 특정 인덱스만:

```bash
python scripts/export_to_excel.py --indices <index_names>
```

### Step 4: 결과 보고

최종 결과를 사용자에게 보여줍니다:
- 생성된 Excel 파일 목록 및 크기
- 각 파일의 문서 수
- 저장 경로: `pharm-db/excel/`
