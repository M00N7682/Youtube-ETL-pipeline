# YouTube ETL Pipeline with Airflow

이 프로젝트는 YouTube Data API v3를 활용한 ETL(Extract, Transform, Load) 파이프라인입니다.  
Airflow를 통해 데이터 수집, 변환, 적재 과정을 자동화하며, 재사용성과 유지보수성을 고려하여 설계되었습니다.

## 구성 요소

- `extract.py`: YouTube Data API를 사용하여 지정된 키워드로 검색 결과를 수집하고, JSON 형식으로 저장합니다.
- `transform.py`: 추출된 JSON 데이터를 정제하고 필요한 필드를 선택하여 CSV로 변환합니다.
- `load.py`: 가공된 CSV 파일을 관계형 데이터베이스(PostgreSQL 등)에 적재합니다.
- `youtube_etl_dag.py`: 위 세 단계를 Airflow DAG으로 정의하여 자동화합니다.

Airflow 환경에서 dags/youtube_etl_dag.py를 DAG 디렉토리에 등록하고 웹 UI를 통해 수동 실행하거나 스케줄을 설정할 수 있습니다.

## 요구사항
- Python 3.9 이상
- Apache Airflow 2.10 이상
- YouTube Data API v3 Key
- PostgreSQL 또는 SQLite (SQLAlchemy 호환 DB)

