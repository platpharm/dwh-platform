# DWH Platform

플랫팜 데이터 웨어하우스 플랫폼 모노레포

## 프로젝트 구조

```
dwh-platform/
├── pharm-db/      # 약국 데이터 수집 크롤러
└── medi-db/       # 의료 트렌드 분석
```

## 모듈

### PharmDB

약국 택소노미 구축을 위한 데이터 수집 크롤러

- 10개 데이터 소스 병렬 수집
- Elasticsearch 저장
- 공공데이터 API 연동

[상세 문서](./pharm-db/README.md)

### MediDB

의료 트렌드 분석 모듈

[상세 문서](./medi-db/README.md)

## Subtree 관리

### 업데이트 받기

```bash
# pharm-db 업데이트
git subtree pull --prefix=pharm-db https://github.com/platpharm/pharm-clustering.git main --squash

# medi-db 업데이트
git subtree pull --prefix=medi-db https://github.com/platpharm/MediTrend.git main --squash
```

### 변경사항 푸시

```bash
# pharm-db에 푸시
git subtree push --prefix=pharm-db https://github.com/platpharm/pharm-clustering.git main

# medi-db에 푸시
git subtree push --prefix=medi-db https://github.com/platpharm/MediTrend.git main
```
