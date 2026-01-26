# DWH Platform

플랫팜 데이터 웨어하우스 플랫폼 모노레포

## 프로젝트 구조

```
dwh-platform/
├── pharm-clustering/    # 약국 데이터 수집 크롤러
└── MediTrend/           # 의료 트렌드 분석
```

## 모듈

### pharm-clustering

약국 택소노미 구축을 위한 데이터 수집 크롤러

- 10개 데이터 소스 병렬 수집
- Elasticsearch 저장
- 공공데이터 API 연동

[상세 문서](./pharm-clustering/README.md)

### MediTrend

의료 트렌드 분석 모듈

[상세 문서](./MediTrend/README.md)

## Subtree 관리

### 업데이트 받기

```bash
# pharm-clustering 업데이트
git subtree pull --prefix=pharm-clustering https://github.com/platpharm/pharm-clustering.git main --squash

# MediTrend 업데이트
git subtree pull --prefix=MediTrend https://github.com/platpharm/MediTrend.git main --squash
```

### 변경사항 푸시

```bash
# pharm-clustering에 푸시
git subtree push --prefix=pharm-clustering https://github.com/platpharm/pharm-clustering.git main

# MediTrend에 푸시
git subtree push --prefix=MediTrend https://github.com/platpharm/MediTrend.git main
```
