---
title: 빅데이터 - Spark MLlib 모델 튜닝, PMML
category: Programmers 인공지능 데브코스
tags: [
    Big Data, Spark MLlib, PMML
]
---

# Spark MLlib

- Spark MLlib 모델 튜닝
- ML Pipeline 기반 머신러닝 모델 만들기
- 머신러닝 모델을 API를 이용하여 서빙하기
- Spark 내용 총 정리

## Spark MLlib 모델 튜닝

(ML Tuning)

- 최적의 하이퍼 파라미터 선택
    - 최적의 모델 혹은 모델의 파라미터를 찾는 것이 아주 중요
    - 하나씩 테스트 하는 것 vs 다수를 동시에 테스트 하는 것
    - 모델 선택의 중요한 부분은 테스트 방법
        - 홀드 아웃, 교차 검증

👉 보통 ML Pipeline 과 같이 사용

📌 Spark MLlib 모델 테스트

모델 테스트 방법은 크게 2가지가 존재

- 홀드 아웃(Train-Validation Split) - `TranValidationSplit`
    - 훈련용과 테스트용 데이터 기반 테스트
    - 잘못 나눌경우 제대로 된 결과를 얻을 수 없다. → 교차 검증을 이용하여 해결
- 교차 검증(Cross Validation) - `CrossValidator`
    - 홀드 아웃(Train-Validation Split)을 반복하여 여러번 훈련 후 성능 지표를 계산하고 그것들의 평균을 내는 방법
    - K-Fold 테스트라고도 함
    - 트레이닝 셋을 K 개의 서브셋으로 나누어 총 K 번을 훈련
        - i 번째 훈련 할 때는 다음을 반복
            1. i 번째 서브셋을 빼고 훈련을 진행하여 모델 빌딩
            2. i 번째 서브셋을 이용하여 테스트 수행

    👉 홀드아웃 테스트보다 더 안정적이다 (오버피팅 문제가 감소)

Spark MLlib 모델 튜닝(Tuning)

- `TranValidationSplit` : 홀드아웃 기반 테스트 수행
- `CrossValidator` : 교차검증(K-Fold) 기반 테스트 수행
- ⭐ 3 개의 입력이 존재
    - Estimator : 머신러닝 모델 (혹은 ML Pipeline)
    - **Evaluator** : 머신러닝 모델의 성능을 나타내는 지표
    - Parameter : 훈련 반복 횟수 등의 하이퍼 파라미터
        - `ParamGridBuilder`를 이용하여 `ParamGrid` 타입의 변수 생성
        - 예) 훈련 횟수, 트리의 최대 깊이 등

👉 최종적으로 가장 결과가 좋은 모델을 리턴!

**Evaluator**

: 머신러닝 모델 성능 측정에 사용되는 지표(metrics)

- `evaluate` 함수가 제공됨
    - 테스트셋의 결과가 들어있는 **데이터프레임**(prediction 컬럼이 존재)과 **파라미터**(성능 지표 관련)를 입력
        - 보통 이 데이터프레임은 머신러닝 모델의 `transform` 함수가 리턴해준 값
- 머신러닝 알고리즘에 따라 다양한 Evaluator가 제공됨
    - RegressionEvaluator, BinaryClassificationEvaluator (AUC가 성능 지표가 됨), MulticlassClassificationEvaluator, MultilableClassificationEvaluator, RankingEvaluaotr

📌  Spark MLlib 머신러닝 모델 빌딩 전체 프로세스

1. 데이터프레임 기반 트레이닝 셋
2. ML Pipeline (Estimator)
3. 머신러닝 모델
4. ML Tuning (TrainValidationSplit or CrossValidator)
    - Estimator(ML Pipeline), Evaluator, Parameter(ParamGrid)
5. 최종 모델

## 실습🖥️: ML Pipeline 기반 머신러닝 모델 만들기

- ML pipeline 사용하여 모델 빌딩
- 다양한 Transformer 사용
    - Imputer, StringIndexer, VectorAssembler
    - **MinMaxScaler**를 사용하여 피쳐값을 0과 1 사이로 스케일링
        - 기본적으로 VectorAssembler로 벡터로 변환된 피쳐컬럼에 적용
- 머신러닝 알고리즘으로 GBTClassifier와 LogisticRegression을 사용 (2개 생성)
    - 📌 GBTClassifier(Gradient Boosted Tree Classfier)
        - 의사결정 트리(Decision Tree)의 머신러닝 알고리즘
        - Regression과 Classification에 모두 사용 가능
- 모델 튜닝으로 CrossValidation을 사용하여 모델 파라미터 선택
    - Estimator - ML Pipeline을 인자로 지정
    - Evaluator - BinaryClassificationEvaluator 사용
    - ParamGrid - ParamGridBuilder를 사용하여 생성

**ML Pipeline 사용 절차**

1. 트레이닝 셋에 수행해야하는 feature transformer들을 생성
2. 머신러닝 모델 알고리즘(Estimator)을 생성
3. 순서대로 파이썬 리스트에 추가
    - 머신러닝 알고리즘이 마지막으로 추가되어야 함
4. 파이썬 리스트를 인자로 Pipeline 개체 생성
5. Pipeline 개체를 이용하여 모델 빌딩 → 2가지 방법 존재
    - Pipeline의 `fit` 함수를 호출하여 트레이닝 셋 데이터프레임 지정
    - ML Tuning의 입력으로 지정하여 여러 하이퍼 파라미터를 테스트해보고 결과가 가장 좋은 모델을 선택
        - 이때 교차검증을 사용

(예제) ML Pipleline 사용 - 타이타닉 생존 예측 모델

### LogisticRegression 이용

1. 필요한 Transformer와 Estimator 들을 만들고 순서대로 리스트에 추가

    ```python
    from pyspark.ml.feature import Imputer, StringIndexer, VectorAssembler, MinMaxScaler

    # Gender
    stringIndexer = StringIndexer(inputCol = "Gender", outputCol = 'GenderIndexed')

    # Age
    imputer = Imputer(strategy='mean', inputCols=['Age'], outputCols=['AgeImputed'])

    # Vectorize
    inputCols = ['Pclass', 'SibSp', 'Parch', 'Fare', 'AgeImputed', 'GenderIndexed']
    assembler = VectorAssembler(inputCols=inputCols, outputCol="features")

    # MinMaxScaler
    minmax_scaler = MinMaxScaler(inputCol="features", outputCol="features_scaled")

    stages = [stringIndexer, imputer, assembler, minmax_scaler]
    ```

    - stringIndexer : 문자를 숫자로 변환
    - imputer : 비어있는 값을 처리 (strategy에 따라 처리)
    - assembler : inputCols 컬럼들을 벡터로 만들어 하나의 컬럼에 적재
    - minmax_scaler : 값 범위를 0~1 사이로 변환

    ```python
    from pyspark.ml.classification import LogisticRegression

    algo = LogisticRegression(featuresCol="features_scaled", labelCol="Survived")
    lr_stages = stages + [algo]

    lr_stages
    # [StringIndexer_1976e7f16274,
    #  Imputer_56fe707b5a4d,
    #  VectorAssembler_98d57e0b3a89,
    #  MinMaxScaler_b3dd5bb113e7,
    #  LogisticRegression_7e7d6a154cc5]
    ```

    - 마지막에 알고리즘 추가
2. 앞서 만든 리스트를 Pipeline의 인자로 지정

    ```python
    from pyspark.ml import Pipeline

    pipeline = Pipeline(stages = lr_stages)
    ```

    ```python
    # evaluator 생성
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    evaluator = BinaryClassificationEvaluator(labelCol='Survived', metricName='areaUnderROC')
    ```

    - 💡 다음과 같이 pipeline을 이용하여 바로 모델 빌드 하는것도 가능!

        ```python
        df = data.select(['Survived', 'Pclass', 'Gender', 'Age', 'SibSp', 'Parch', 'Fare'])
        train, test = df.randomSplit([0.7, 0.3])

        lr_model = pipeline.fit(train)
        lr_cv_predictions = lr_model.transform(test)

        from pyspark.ml.evaluation import BinaryClassificationEvaluator

        evaluator = BinaryClassificationEvaluator(labelCol='Survived', metricName='areaUnderROC')
        evaluator.evaluate(lr_cv_predictions)
        # 0.8671428571428581
        ```

3. ML Tuning - ParamGrid와 CrossValidator 생성

    ```python
    from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

    paramGrid = (ParamGridBuilder()
                 .addGrid(algo.maxIter, [1, 5, 10])
                 .build())

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=5
    )

    # Run cross validations.
    cvModel = cv.fit(train)
    lr_cv_predictions = cvModel.transform(test)
    evaluator.evaluate(lr_cv_predictions)
    # 0.8676819407008096
    ```

    - `cv.fit()` 는 교차 검증을 수행하고 가장 좋은 모델을 리턴
    - 어느 하이퍼 파라미터 조합이 최선의 결과를 냈는지 알고 싶다면

        ```python
        import pandas as pd

        params = [{p.name: v for p, v in m.items()} for m in cvModel.getEstimatorParamMaps()]
        pd.DataFrame.from_dict([
            {cvModel.getEvaluator().getMetricName(): metric, **ps}
            for ps, metric in zip(params, cvModel.avgMetrics)
        ])
        # 	  areaUnderROC  maxIter
        # 0	  0.826748	    1
        # 1	  0.838885	    5
        # 2	  0.845797	    10
        ```

### GBT Classifier 이용

- 위의 과정에서 알고리즘 추가 이전 과정은 동일

    ```python
    from pyspark.ml.classification import GBTClassifier

    gbt = GBTClassifier(featuresCol="features_scaled", labelCol="Survived")
    gbt_stages = stages + [gbt]

    gbt_stages
    # [StringIndexer_1976e7f16274,
    #  Imputer_56fe707b5a4d,
    #  VectorAssembler_98d57e0b3a89,
    #  MinMaxScaler_b3dd5bb113e7,
    #  GBTClassifier_2fe98abcabe9]
    ```

    ```python
    from pyspark.ml import Pipeline
    pipeline = Pipeline(stages = gbt_stages)
    ```

    ```python
    from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

    paramGrid = (ParamGridBuilder()
                 .addGrid(gbt.maxDepth, [2, 4, 6])
                 .addGrid(gbt.maxBins, [20, 60])
                 .addGrid(gbt.maxIter, [10, 20])
                 .build())

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=5
    )

    # Run cross validations.
    cvModel = cv.fit(train)
    lr_cv_predictions = cvModel.transform(test)
    evaluator.evaluate(lr_cv_predictions)
    # 0.8682479784366579
    ```

# PMML

다양한 머신러닝 개발 플랫폼들이 존재

- Scikit-Learn, PyTorch, Tensorflow 등 (Spark MLlib 포함)

다양한 머신러닝 개발 플랫폼이 공통적으로 지원해주는 파일 포맷이 있다면 머신러닝 모델 서빙환경의 통일이 가능!

👉 PMML, MLeap이 대표적인 범용 머신러닝 모델 파일포맷

> 이러한 공통 파일 포맷이 지원해주는 기능이 미약하여 복잡된 모델의 경우 지원 불가

PMML

: Machine Learning 모델을 마크업 언어로 표현해주는 XML 언어 (Predictive Model Markup Language)

- 간단한 입력 데이터 전처리와 후처리 지원 (하지만 제약사항이 많음)
- PySpark에서는 `pyspark2pmml` 을 사용
    - 내부적으로 자바 jar 파일(jpmml-sparkml) 사용
    - 너무 복잡함

## 전체적인 절차

1. ML Pipeline을 PMML 파일로 저장
    - `pyspark2pmml` 파이썬 모듈 설치 (jar 파일 설치 필요)
    - `pyspark2pmml.PMMLBuilder` 를 이용하여 ML Pipeline을 PMML 파일로 저장
2. PMML 파일을 기반으로 모델 예측 API로 론치
    - Openscoring 프레임워크 (Java)
    - AWS SageMaker
    - Flask + PyPMML
3. 이 API로 승객정보를 보내고 예측 결과를 받는 클라이언트 코드 작성

## 예제

- 머신러닝 모델을 PMML 파일로 저장하는 예제

    ```python
    from pyspark2pmml import PMMLBuilder

    pmmlBuilder = PMMLBuilder(spark.sparkContext, train_fr, cvModel)
    pmmlBuilder.buildFile("titinic.pmml")
    ```

    - `cvModel` : 머신러닝 모델 혹은 ML Pipeline
    - `train_fr` : 트레이닝셋 데이터프레임

- PMML 파일을 PyPPML로 로딩하고 호출하는 예제

    ```python
    from pypmml import Model

    # loading
    model = Model.load('single_iris_dectree.pmml')

    # predict
    model.predict({'sepal_lenght': 5.1, 'sepal_width': 3.5, 'petal_length': 1.4, 'petal_width': 0.2})
    ```
