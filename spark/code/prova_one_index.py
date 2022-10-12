from os import truncate
from urllib import response
from xml.dom.minidom import Document
import pyspark
from pyspark.sql.dataframe import DataFrame
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.conf import SparkConf
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row
from pyspark.sql.functions import from_json, lit, col
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
import pandas as pds
from pyspark.sql.functions import udf
from pyspark.sql.types  import DoubleType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from translate import Translator
import time
import json

vader = SentimentIntensityAnalyzer()
translator = Translator(from_lang = "it", to_lang="en")


def get_spark_session():
    spark_conf = SparkConf()\
        .set('es.nodes', 'elasticsearch')\
            .set('es.port', '9200')
    #spark_conf.set("es.index.auto.create", "true")
    spark_context = SparkContext(appName = 'insTAP', conf = spark_conf)
    spark_context.setLogLevel("WARN")
    spark_session = SparkSession(spark_context)
    return spark_session
    

def get_sentiment(text, daily_limit_passed = True):
    if not daily_limit_passed:
        print("traduco: ")
        print(translator.translate(text))

        value = vader.polarity_scores(translator.translate(text))
        value = value['compound']

        # 1) positive sentiment: compound score >= 0.05
        # 2) neutral sentiment: (compound score > -0.05) and (compound score < 0.05)
        # 3) negative sentiment: compound score <= -0.05
        return value
    else:
        value = vader.polarity_scores(text)
        value = value['compound']
        return value

schema = tp.StructType([
    tp.StructField("id", tp.IntegerType(), False),
    tp.StructField("user", tp.StringType(), False),
    tp.StructField("comment", tp.StringType(), False),
    tp.StructField("caption", tp.StringType(), False),
    tp.StructField("image", tp.StringType(), False),
    tp.StructField("timestamp", tp.StringType(), False),
    tp.StructField("lat", tp.FloatType(),False),
    tp.StructField("lng", tp.FloatType(),False),
])

es_mapping = {
    "mappings": {
        "properties": {
            "id":    {"type": "integer"},
            "user":   {"type": "keyword"},
            "comment":{"type":"keyword"},
            "caption":{"type":"keyword"},
            'image': {"type":"keyword"},
            "timestamp": {"type": "date"},
            "lat": {"type": "float"},
            "lng": {"type": "float"},
        }
    }
}

topic = "instap"
spark = get_spark_session()

df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', "broker:9092") \
        .option('subscribe', topic). \
            option("startingOffsets","earliest").load()            


df = df.selectExpr("CAST(value AS STRING)") \
.select(from_json("value", schema=schema).alias("data")) \
.select("data.*")

elastic_host = "http://elasticsearch:9200"
es = Elasticsearch(hosts=elastic_host, verify_certs = False)


response = es.indices.create(index="instap_index2", mappings = es_mapping, ignore = 400)

if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print("Successfully created index:", response['index'])

sentiment_udf = udf(get_sentiment, DoubleType())
df=df.withColumn("sentiment_result", sentiment_udf(col("user")))
df.writeStream.option("checkpointLocation", "./checkpoints").format("es").start("instap_index2").awaitTermination()



