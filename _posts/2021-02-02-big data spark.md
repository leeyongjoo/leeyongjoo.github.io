---
title: 빅데이터 - Spark
category: Programmers 인공지능 데브코스
tags: [
    Big Data, Spark
]
---

# 빅데이터

- 빅데이터 정의와 예
- Hadoop
- Spark
    - 소개
    - Spark 프로그래밍 개념(RDD, Dataframe, Dataset)
    - Spark vs Pandas
    - 데이터 구조
    - 개발환경
    - 맛보기

## 빅데이터 정의와 예

- 빅데이터 정의
    - "서버 한대로 처리할 수 없는 규모의 데이터"
    - "기존의 소프트웨어로는 처리할 수 없는 규모의 데이터"
        - 오라클이나 MySQL과 같은 관계형 데이터베이스
            - 분산환경을 염두에 두지 않음
            - Scale-up 접근방식 (메모리, CPU, 디스크 추가)
    - 4V 관점
        - **Volume**: 데이터의 크기가 **대용량**인지
        - Velocity: 데이터의 처리 속도가 중요한지
        - Variety: 구조화/비구조화 데이터인지
        - Veracity: 데이터의 품질이 좋은지

- 빅데이터 예
    - 모바일 디바이스
    - 스마트 TV
    - 각종 센서 데이터 (IoT 센서)
    - 네트워킹 디바이스

## Hadoop

- 대용량 처리 기술이란?
    - **분산 환경** 기반 (1대 혹은 그 이상의 서버로 구성)
        - 분산 컴퓨팅과 분산 파일 시스템이 필요
    - **Fault Tolerance** : 소수의 서버가 고장나도 동작해야 함
    - **확장이 용이**해야 함 (Scale Out)

하둡(Hadoop)

: Doug Cutting이 구글랩 발표 논문들에 기반해 만든 오픈소스 프로젝트

1. 2003년 The Google **File System**
2. 2004년 **MapReduce**: Simplified Data processing on Large Cluster

- 처음 시작은 Nutch라는 오픈 소스 검색엔진의 하부 프로젝트
- 두 개의 서브 시스템으로 구현됨 (Hadoop 1.0)
    - `HDFS` ← 분산 파일 시스템
    - `MapReduce` ← 분산 컴퓨팅 시스템
        - 새로운 프로그래밍 방식으로 대용량 데이터 처리의 효율을 극대화하는데 맞춤

### HDFS

HDFS(Hadoop Distributed File System)

: 분산 파일 시스템

- 데이터를 블록단위로 저장
- 블록 복제 방식 (Replication)
    - **Fault tolerance**를 보장할 수 있는 방식으로 블록들이 저장됨

### MapReduce

- MapReduce 프로그래밍의 문제점
    - 작업에 따라서 복잡해짐
        - **Hive** 처럼 MapReduce로 구현된 SQL 언어들이 다시 각광을 받게 됨
    - 기본적으로 배치 작업에 최적화 (실시간 처리 ❌)

- MapReduce 프로그래밍 예제
    - Word Count : 문장이 주어지면 단어가 몇번 나오는지 카운트

🔥 하둡(Hadoop)을 이용한 데이터 시스템 구성

- 하둡은 Data Warehouse에 해당
- 워크플로우 관리에는 Airflow가 대세

📌 Hadoop 1.0 → Hadoop 2.0

![https://user-images.githubusercontent.com/46367323/106577922-4bfda500-6582-11eb-9d29-d3d2f099292e.png](https://user-images.githubusercontent.com/46367323/106577922-4bfda500-6582-11eb-9d29-d3d2f099292e.png)

*사진출처: [http://annovate.blogspot.com/2014/07/big-data-hadoop-1x-vs-hadoop-2x.html](http://annovate.blogspot.com/2014/07/big-data-hadoop-1x-vs-hadoop-2x.html)*

- 하둡 1.0
    - HDFS 위에 MapReduce 라는 분산 컴퓨팅 시스템이 도는 구조
- 하둡 2.0 (YARN 이라고 부르기도 함)
    - 아키텍처가 크게 변경됨
    - Spark는 하둡 2.0 위에서 애플리케이션 레이어로 실행됨

## Spark

Hadoop은 1세대 빅데이터 처리기술이고

Spark은 2세대 빅데이터 기술이라고 할 수 있다.

Spark

: 버클리 대학의 AMPLab에서 아파치 오픈소스 프로젝트로 2013년 시작

- 하둡의 뒤를 잇는 2세대 빅데이터 기술
    - 하둡 2.0을 분산환경으로 사용 가능 (자체 분산환경을 지원하기도 함)
- MapReduce의 단점을 대폭적으로 개선
    - Pandas와 비슷함 (Pandas는 서버 한대, Spark는 다수 서버 분산환경)

- Spark vs MapReduce
    - MapReduce
        1. 디스크 기반
        2. 하둡 위에서만 동작
        3. 키와 밸류 기반 프로그래밍
    - Spark
        1. 메모리 기반
            - 메모리가 부족해지면 디스크 사용
        2. 하둡(YARN)이외에도 다른 분산 컴퓨팅 환경 지원
        3. 다양한 방식의 컴퓨팅을 지원
            - 배치 프로그래밍, 스트리밍 프로그래밍, SQL, 머신 러닝, 그래프 분석 등

- Spark 구조
    - Driver Program : 여러개의 병렬적인 작업으로 나뉘어져

### Spark 프로그래밍 개념

- RDD(Resilient Distributed Dataset)
    - 로우레벨 프로그래밍 API로 세밀한 제어가 가능
    - 하지만 코딩의 복잡도 증가
- DataFrame, Dataset (Pandas의 데이터프레임과 유사)
    - 하이레벨 프로그래밍 API로 점점 많이 사용되는 추세
    - SparkSQL 사용 시에 사용하게 됨

- 보통 Scala, Java, Python을 사용

### Spark vs Pandas

📌 Pandas

- 파이썬으로 데이터 분석을 하는데 가장 기본이 되는 모듈 중의 하나
    - 소규모의 구조화된 데이터(테이블 형태의 데이터)를 다루는데 최적
        - 작은 크기의 데이터로 제약 (큰 데이터의 경우 Spark 사용)
        - 병렬 처리를 지원하지 않음
- Pandas로 할 수 있는 일 예시
    - 구조화된 데이터 읽어오고 저장
        - csv, json 등 다양한 포맷 지원
        - 관계형 데이터베이스에서 읽어오는 것도 가능
    - 다양한 통계 뽑기
        - 컬럼 별 평균, 표준편차, 상관관계 계산 등
    - 데이터 청소 (데이터 전처리)
        - NA 값 처리
        - 정규화(normalization)
    - 시각화(visualization)
        - matplotlib를 이용하여 다양한 형태로 시각화

### 데이터 구조

- Spark 세션
    - Spark 프로그램의 시작은 `SparkSession`을 만드는 것
    - Spark 세션을 통해 Spark이 제공하는 다양한 기능 사용
        - Spark 컨텍스트, Hive 컨텍스트, SQL 컨텍스트
        - (Spark 2.0 이전에는 기능에 따라 다른 컨텍스트를 생성해야 했음)

- Spark 데이터 구조 (3가지)
    - RDD (Resilient Distributed Dataset)
        - 로우레벨 데이터로 클러스터내의 서버에 분산된 데이터를 지칭
        - 레코드별로 존재하며 구조화된 데이터나 비구조화된 데이터 모두 지원
    - Dataframe, Dataset
        - RDD 위에 만들어지는 하이레벨 데이터로 RDD와는 달리 필드 정보를 가지고 있음 (테이블)
        - Dataset은 Dataframe과 달리 타입 정보가 존재하며 컴파일 언어에서 사용가능
            - 컴파일 언어: Scala/Java에서 사용가능
            - `PySpark` 에서는 Dataframe을 사용 (**SparkSQL을 사용하는 것이 일반적💡**)
- RDD
    - 변경이 불가능한 분산 저장된 데이터
        - RDD는 다수의 파티션으로 구성되고 Spark 클러스터 내 서버들에 나눠 저장됨
        - 로우레벨의 함수형 변환 지원 (map, filter, flatMap 등)
    - RDD가 아닌 일반 파이썬 데이터는 parallelize 함수로 RDD로 변환

- Dataframe 데이터프레임
    - RDD처럼 변경이 불가능한 분산 저장된 데이터
    - RDD와 다르게 관계형 데이터베이스 테이블처럼 **컬럼**으로 나눠 저장
        - pandas의 dataframe과 거의 유사
    - 다양한 데이터 소스 지원: 파일, Hive, 외부 데이터베이스, RDD 등

- Dataset 데이터셋 (Spark 1.6부터)
    - RDD와 SparkSQL의 최적화 엔진 두 가지 장점을 취함
    - 타입이 있는 컴파일 언어에서만 사용 가능
        - Scala와 Java 에서만 지원 (Python ❌)

### 개발 환경

1. 개인 컴퓨터에 설치하고 사용하는 방법
    - 설치가 복잡함
    - spark-submit을 이용해 실행가능
2. 각종 무료 노트북을 사용하는 방법⭐
    - 구글 **Colab** 이용⭐
    - 제플린의 무료 노트북 사용
3. AWS의 EMR 클러스터 사용하는 방법
    - 거의 프로덕션 호나경에 가까움

## 💻 Spark 실습

 PySpark, Py4J 패키지 설치

```python
!pip install pyspark==3.0.1 py4j==0.10.9
```

```python
!ls -tl
total 4
drwxr-xr-x 1 root root 4096 Feb  1 17:27 sample_data

!ls -tl sample_data
total 55504
-rw-r--r-- 1 root root 18289443 Feb  1 17:27 mnist_test.csv
-rw-r--r-- 1 root root 36523880 Feb  1 17:27 mnist_train_small.csv
-rw-r--r-- 1 root root   301141 Feb  1 17:27 california_housing_test.csv
-rw-r--r-- 1 root root  1706430 Feb  1 17:27 california_housing_train.csv
-rwxr-xr-x 1 root root     1697 Jan  1  2000 anscombe.json
-rwxr-xr-x 1 root root      930 Jan  1  2000 README.md
```

### **Spark Session**

- Spark 2.0 부터 entry point로 사용 (이전에는 SparkContext 사용)
- `SparkSession` 을 이용하여 RDD, 데이터프레임 등을 생성
- `SparkSeesion.builder` 를 호출하여 생성하며, 다양한 함수들을 통해 세부 설정 가능
    - `.master` : 클러스터 호스트를 지정
        - local → 서버에 놀고 있는 spark
        - [*] → 숫자를 입력 (*는 모두)
    - `.getOrCreate()` : appName에 해당하는 객체가 존재하면 그것을 가져오고 없으면 새로 생성

    ```python
    from pyspark.sql import SparkSession

    spark = SparkSession.builder\
            .master("local[*]")\
            .appName('PySpark_Tutorial')\
            .getOrCreate()

    spark
    # SparkSession - in-memory
    # SparkContext
    # Spark UI

    # Version
    # v3.0.1
    # Master
    # local[*]
    # AppName
    # PySpark_Tutorial
    ```

### Python 객체를 RDD로 변환

1. python 리스트 생성

    ```python
    name_list_json = [ '{"name": "keeyong"}', '{"name": "benjamin"}', '{"name": "claire"}' ]

    import json

    for n in name_list_json:
      jn = json.loads(n)
      print(jn["name"])
    # keeyong
    # benjamin
    # claire
    ```

2. python 리스트를 RDD로 변환
    - RDD로 변환되는 순간  Spark 클러스터의 서버들에 데이터가 나눠 저장됨 (파티션) → Lazy Execution 방식
    - `.parallelize()` : RDD로 변환
    - `.collect()` : 파이썬 객체로 출력

    ```python
    rdd = spark.sparkContext.parallelize(name_list_json)
    rdd
    # ParallelCollectionRDD[4] at readRDDFromFile at PythonRDD.scala:262
    rdd.count()
    # 3

    parsed_rdd = rdd.map(lambda el:json.loads(el))
    parsed_rdd
    # PythonRDD[3] at RDD at PythonRDD.scala:53

    parsed_rdd.collect()
    # [{'name': 'keeyong'}, {'name': 'benjamin'}, {'name': 'claire'}]
    ```

- python 리스트를 데이터프레임으로 변환

    ```python
    from pyspark.sql.types import StringType

    df = spark.createDataFrame(name_list_json, StringType())
    df.count()
    # 3

    df.printSchema()
    # root
    #  |-- value: string (nullable = true)

    df.select('*').collect()
    # [Row(value='{"name": "keeyong"}'),
    #  Row(value='{"name": "benjamin"}'),
    #  Row(value='{"name": "claire"}')]
    ```

    - 컬럼 이름을 설정

        ```python
        parsed_name_rdd = rdd.map(lambda el:json.loads(el)["name"])
        parsed_name_rdd.collect()
        # ['keeyong', 'benjamin', 'claire']

        from pyspark.sql import Row

        row = Row("name") # Or some other column name
        df_name = parsed_name_rdd.map(row).toDF()

        df_name.printSchema()
        # root
        # |-- name: string (nullable = true)

        df_name.select('name').collect()
        # [Row(name='keeyong'), Row(name='benjamin'), Row(name='claire')]
        ```
