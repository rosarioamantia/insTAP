from os import truncate
from urllib import response
from xml.dom.minidom import Document
import pyspark
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
from pyspark.sql.functions import from_json
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.functions import udf
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from translate import Translator
import time
import json

vader = SentimentIntensityAnalyzer()
translator = Translator(from_lang = "it", to_lang="en")


def get_spark_session():
    spark_conf = SparkConf().set('es.nodes', 'elastic_search_AM').set('es.port', '9200')
    #spark_conf.set("es.index.auto.create", "true")
    spark_context = SparkContext(appName = 'insTAP', conf = spark_conf)
    spark_session = SparkSession(spark_context)
    return spark_session
    

def get_sentiment(text, daily_limit_passed = False):
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
    tp.StructField("user", tp.StringType(), False),
    tp.StructField("caption", tp.StringType(), False)
])

es_mapping = {
    "mappings": {
        "properties": {
            "user":    {"type": "keyword"},
            "caption":     {"type": "keyword"},
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

print("-----------------------------------------------------------------------------------------------------")
print(df.printSchema())
print("-----------------------------------------------------------------------------------------------------")

elastic_host = "http://10.0.100.51:9200"

es = Elasticsearch(
    elastic_host,
    verify_certs=False
)

query=df.writeStream \
        .option("checkpointLocation", "./checkpoints") \
        .format("es") \
        .start("elastic_index" + "/_doc")\
        #.show()
        #.show()
query.awaitTermination()