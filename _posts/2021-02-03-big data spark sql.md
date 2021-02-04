---
title: 빅데이터 - Spark SQL
category: Programmers 인공지능 데브코스
tags: [
    Big Data, SQL, Spark SQL
]
---

# Spark SQL

- SQL 이란?
- SQL 실습
- Spark SQL 이란?
- Spark SQL 실습

## SQL

- 구조화된 데이터를 다루는데 사용
    - 모든 대용량 데이터 웨어하우스는 SQL 기반
        - Redshift, Snowflake, BigQuery, Hive
    - Spark도 SparkSQL을 지원
- 데이터 분야에서 반드시 익혀야할 기본 기술

### 관계형 데이터베이스

- 대표적인 관계형 데이터베이스
    - 서버 한대 - MySQL, Postgres, Oracle, ...
        - 빠른 응답 속도, 용량 한계
    - 데이터 웨어하우스 - Redshift, Snowflake, BigQuery, Hive, ...
        - 큰 용량을 지원
- 관게형 데이터베이스는 **테이블**이 존재
    - 테이블 구조
        - 테이블은 레코드로 구성
        - 레코드는 하나 이상의 필드로 구성
        - 필드(컬럼)는 이름과 타입으로 구성

(예제) 웹서비스 사용자/세션 정보

- `사용자 ID` : 보통 웹서비스에서는 등록된 사용자마다 유일한 ID를 부여
- `세션 ID` : 사용자가 외부 링크 또는 직접 방문해서 올 경우 세션을 생성
    - 세션을 만들어낸 소스를 채널이란 이름으로 기록해둠 (시간도 포함)
    - 하나의 사용자 ID 는 여러 개의 세션 ID를 가질 수 있음

👉 위 정보를 기반으로 다양한 데이터 분석과 지표 설정 가능 (마케팅, 사용자 트래픽 등)

위의 예제를 데이터베이스와 테이블로 표현

- raw_data 데이터베이스
    - `user_session_channel` 테이블
        - 컬럼명: userId, 타입: int
        - **컬럼명: sessionId, 타입: varchar(32)**
        - 컬럼명: channel, 타입: varchar(32)
    - `session_timestamp` 테이블
        - **컬럼명: sessionId, 타입: varchar(32)**
        - 컬럼명: ts, 타입: timestamp

### SQL 소개

SQL(Structured Query Language)

: 관계형 데이터베이스에 있는 데이터(테이블)를 질의하는 언어

- 두 종류의 언어로 구성(DDL, DML)

DDL(Data Definition Language)

: 테이블 구조 정의 언어

- CREATE TABLE
- DROP TABLE
- ALTER TABLE

DML(Data Manipulation Language)

: 테이블 데이터 조작 언어

- SELECT FROM

    ```sql
    SELECT 필드1, 필드2, ...
      FROM 테이블명
     WHERE 선택조건
     ORDER BY 필드지정 [ASC|DESC]
     LIMIT N;
    ```

- INSERT INTO
- UPDATE FROM
- DELETE FROM

📌 테이블 조인(JOIN)

: 두개 이상의 테이블이나 데이터베이스를 연결하여 데이터를 검색하는 방법

- INNER JOIN (교집합)
- LEFT OUTER JOIN
- RIGHT OUTER JOIN
- FULL OUTER JOIN (합집합)

## 🖥️ SQL 실습

colab에서 Redshift 기반 SQL 실습

- 데이터베이스 테이블 (위의 예제의 테이블)
    - `raw_data.session_timestamp`
    - `raw_data.user_session_channel`

- 분석할 것들
    - 월별 세션 수
    - 월별 사용자 수 (MAU; Monthly Active User)
    - 월별 채널별 사용자 수

- 주비터 SQL 엔진 설정

    SQL 엔진 로드

    ```sql
    %load_ext sql
    ```

    관계형 데이터베이스 연결 (AWS의 Redshift)

    `%sql postgresql://사용자ID:패스워드@호스트:포트번호/접속DB`

    ```sql
    # ID와 PW를 자신의 환경에 맞게 수정
    %sql postgresql://guest:Guest1!*@learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/prod
    ```

- SELECT 실행

    일별 세션ID 개수를 세션ID 개수에 대하여 내림차순으로 10개 출력

    ```sql
    %%sql

    SELECT DATE(ts) date, COUNT(sessionID)
      FROM raw_data.session_timestamp
     GROUP BY 1
     ORDER BY 2 DESC
     LIMIT 10;
    ```

    - JOIN 추가

        일별 방문 사용자 수

        ```sql
        # raw_data.user_session_channel과 raw_data.session_timestamp 테이블의 조인이 필요
        %%sql

        SELECT DATE(st.ts) date, COUNT(usc.userID)
          FROM raw_data.session_timestamp st
          JOIN raw_data.user_session_channel usc ON st.sessionID = usc.sessionID
         GROUP BY 1
         ORDER BY 1
         LIMIT 10;
        ```

        'o' 를 포함하는 채널의 개수

        ```sql
        %%sql

        SELECT distinct channel FROM raw_data.user_session_channel
        WHERE channel ilike '%o%'
        ```

        (`distinct`는 중복 제외, `ilike`는 소문자 대문자 구분 하지 않음)

### pandas와 연동

`user_session_channel` 테이블 정보 가져오기

```python
result = %sql SELECT * FROM raw_data.user_session_channel
type(result)
# sql.run.ResultSet

df = result.DataFrame()
```

```python
df.head()
# userid	sessionid	channel
# 0	779	7cdace91c487558e27ce54df7cdb299c	Instagram
# 1	230	94f192dee566b018e0acf31e1f99a2d9	Naver
# 2	369	7ed2d3454c5eea71148b11d0c25104ff	Youtube
# 3	248	f1daf122cde863010844459363cd31db	Naver
# 4	676	fd0efcca272f704a760c3b61dcc70fd0	Instagram
```

```python
df.groupby(["channel"]).size()
# channel
# Facebook     16791
# Google       16982
# Instagram    16831
# Naver        16921
# Organic      16904
# Youtube      17091
# dtype: int64

df.groupby(["channel"])["sessionid"].count()
# channel
# Facebook     16791
# Google       16982
# Instagram    16831
# Naver        16921
# Organic      16904
# Youtube      17091
# Name: sessionid, dtype: int64
```

`session_timestamp` 테이블 정보 가져오기

```python
result = %sql SELECT * FROM raw_data.session_timestamp
df_st = result.DataFrame()
```

새로운 컬럼 만들기 (date)

```python
df_st['date'] = df_st['ts'].apply(lambda x: "%d-%02d-%02d" % (x.year, x.month, x.day))
```

date 컬럼별로 세션 수를 카운트하고 date를 기준으로 내림차순 정렬

```python
df_st.groupby(["date"])["sessionid"].count().reset_index(name='count').sort_values("date", ascending=False)
```

## Spark SQL

- SparkSQL과 Spark Core의 차이점
- SparkSQL의 일반적인 사용법

SparkSQL

: 구조화된 데이터 처리를 위한 Spark 모듈

- 대화형 Spark 셸 제공
- Dataframe을 SQL로 처리 가능
    - RDD 데이터나 외부 데이터(스토리지나 관계형 데이터베이스)를 Dataframe으로 변환한 후 처리

    👉 데이터프레임은 테이블이 되고 sql 함수를 사용 가능

- SparkSQL 사용하여 외부 데이터베이스 연결
    - 외부 데이터베이스 기반으로 데이터프레임 생성
        - SparkSession의 read 함수를 사용하여 테이블 혹은 SQL 결과를 데이터프레임으로 읽어옴
        - 📌 Redshift 연결 에제
            1. SparkSession을 만들때 외부 데이터베이스에 맞는 JDBC jar 을 지정 (`.config` 에 지정)
            2. SparkSession의 read 함수를 호출
                - 로그인 정보와 읽어올 테이블 혹은 SQL 지정
                - 결과가 데이터프레임으로 리턴
            3. 리턴된 데이터프레임에 테이블 이름 지정
            4. SparkSession의 `sql()` 함수 사용

## 🖥️ Spark SQL 실습

 PySpark, Py4J 패키지 설치

```bash
!pip install pyspark==3.0.1 py4j==0.10.9
```

Redshift 관련 JAR 파일을 설치

```bash
!cd /usr/local/lib/python3.6/dist-packages/pyspark/jars && wget https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.20.1043/RedshiftJDBC42-no-awssdk-1.2.20.1043.jar
```

📌 구글 colab 에서의 pyspark의 jars 디렉토리 경로:

`/usr/local/lib/python3.6/dist-packages/pyspark/jars`

**Spark Session**

spark.jars를 통해 앞서 다운로드 받은 **Redshift 연결을 위한 JDBC 드라이버**를 사용함 (`.config("spark.jars", ...)`)

```bash
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/usr/local/lib/python3.6/dist-packages/pyspark/jars/RedshiftJDBC42-no-awssdk-1.2.20.1043.jar") \
    .getOrCreate()
```

### SparkSQL 맛보기

Pandas로 csv 파일 로드

```bash
import pandas as pd

namegender_pd = pd.read_csv("https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv")

namegender_pd.head()
# name	gender
# 0	Adaleigh	F
# 1	Amryn	Unisex
# 2	Apurva	Unisex
# 3	Aryion	M
# 4	Alixia	F
```

Pandas 데이터프레임 ➡️ Spark 데이터프레임으로 변환

```bash
namegender_df = spark.createDataFrame(namegender_pd)

namegender_df.printSchema()
# root
#  |-- name: string (nullable = true)
#  |-- gender: string (nullable = true)

namegender_df.show()
# +----------+------+
# |      name|gender|
# +----------+------+
# |  Adaleigh|     F|
# |     Amryn|Unisex|
# |    Apurva|Unisex|
# |    Aryion|     M|
# |    Alixia|     F|
# |Alyssarose|     F|
# |    Arvell|     M|
# |     Aibel|     M|
# |   Atiyyah|     F|
# |     Adlie|     F|
# |    Anyely|     F|
# |    Aamoni|     F|
# |     Ahman|     M|
# |    Arlane|     F|
# |   Armoney|     F|
# |   Atzhiry|     F|
# | Antonette|     F|
# |   Akeelah|     F|
# | Abdikadir|     M|
# |    Arinze|     M|
# +----------+------+
# only showing top 20 rows

namegender_df.groupBy(["gender"]).count().collect()
# [Row(gender='F', count=65),
#  Row(gender='M', count=28),
#  Row(gender='Unisex', count=7)]
```

📌 참고링크: [🔗](https://towardsdatascience.com/pyspark-and-sparksql-basics-6cb4bf967e53)

데이터프레임을 테이블 뷰로 만들어서 SparkSQL로 처리

- createOrReplaceTempView : SparkSession이 살아있는 동안 존재
- createGlobalTempView : Spark 드라이버가 살아있는 동안 존재

```bash
namegender_df.createOrReplaceTempView("namegender")

namegender_group_df = spark.sql("SELECT gender, count(1) FROM namegender GROUP BY 1")

namegender_group_df.collect()
# [Row(gender='F', count(1)=65),
#  Row(gender='M', count(1)=28),
#  Row(gender='Unisex', count(1)=7)]
```

Redshift와 연결해서 테이블들을 데이터프레임으로 로딩

```bash
user_session_channel_df = spark.read \
    .format("jdbc") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("url", "jdbc:redshift://learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/prod?user=guest&password=Guest1!*") \
    .option("dbtable", "raw_data.user_session_channel") \
    .load()

session_timestamp_df = spark.read \
    .format("jdbc") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("url", "jdbc:redshift://learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/prod?user=guest&password=Guest1!*") \
    .option("dbtable", "raw_data.session_timestamp") \
    .load()
```

```bash
type(session_timestamp_df)
# pyspark.sql.dataframe.DataFrame
```

테이블 뷰 생성 후 해당 뷰를 이용하여 sql 함수 실행

```bash
user_session_channel_df.createOrReplaceTempView("user_session_channel")
session_timestamp_df.createOrReplaceTempView("session_timestamp")
```

```bash
channel_count_df = spark.sql("""
    SELECT channel, count(distinct userId) uniqueUsers
    FROM session_timestamp st
    JOIN user_session_channel usc ON st.sessionID = usc.sessionID
    GROUP BY 1
    ORDER BY 1
""")
```

> 위의 sql문은 당장 실행되지 않고 아래 `.show()` 메서드를 호출되면 그때 실행된다 → ⭐Lazy Execution 방식

```bash
channel_count_df
# DataFrame[channel: string, uniqueUsers: bigint]

channel_count_df.show()
# +---------+-----------+
# |  channel|uniqueUsers|
# +---------+-----------+
# | Facebook|        889|
# |   Google|        893|
# |Instagram|        895|
# |    Naver|        882|
# |  Organic|        895|
# |  Youtube|        889|
# +---------+-----------+
```

특정 조건에 맞는 데이터 조회 SQL문 실행 (`like` 이용)
: 채널명에 'o'를 포함하고 있는 채널의 개수를 출력

```bash
channel_with_o_count_df = spark.sql("""
    SELECT COUNT(1)
    FROM user_session_channel
    WHERE channel like '%o%'
""")

channel_with_o_count_df.collect()
# [Row(count(1)=50864)]
```
