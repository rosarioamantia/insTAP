from os import truncate
from urllib import response
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


'''
print("OOOOOOOOKKKKKKKKKKKKKKKK")
print(get_sentiment("oggi sono andato in ufficio e ne sono tanto felice"))
print(get_sentiment("sono un gorilla e mi piace odorare la gente"))
print(get_sentiment("bene"))
print(get_sentiment("benissimo"))
print(get_sentiment("oggi è andata peggio di ieri, domani andrà meglio, evviva!"))
'''


# JSON schema from Kafka
schema = tp.StructType([
    tp.StructField(name= 'primocampo', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'secondocampo',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'terzocampo',       dataType= tp.StringType(),  nullable= True)
])

spark = get_spark_session()
spark.sparkContext.setLogLevel("ERROR")

"""
configurazioni per kafka
topic=nome topic di kafka
elastic_host = "elastic_search_AM" (è nome container elastic search)
elastic_index = INDEX 
kafkaServer = "kafka_server_AM:9092" (è nome container kafka e porta)
"""
# prima colonna frase
data = [("recor1primacolonna", 10, "ciao ciau"),
    ("recor2primacolonna", 10, "sto male"),
    ("recor3primacolonna", 20, "sto bene")
  ]

#inizializzo dataframe
df = spark.createDataFrame(data = data, schema = schema)
df.printSchema()
df.show()

#sembra che lo faccia per portare il suo campo a string
#df=df.alias("ciao").select("ciao.terzocampo").where("ciao.secondocampo > 11")

#df =  df.select(from_json("secondocampo",schema=schema))

#df.printSchema()
#df.show()

sentiment_res = udf(get_sentiment, tp.DoubleType())
df=df.withColumn("sentiment_result", sentiment_res("terzocampo")) #necessario portare a double? (non esiste in python)
#print(df.select("sentiment_result"))
df.printSchema()
df.show()

es_mapping = {
    "mappings": {
        "properties": {
            "primocampo":   {"type": "keyword"},
            "secondocampo":    {"type": "integer"},
            "terzocampo":     {"type": "keyword"},
            "sentiment":    {"type":"double"}
        }
    }
}

addr = "http://10.0.100.51:9200"

es = Elasticsearch(hosts=[addr])

'''index = "iiiindexxx"
#response = es.index(index="okokindex", body=es_mapping)
response = es.indices.create(
    index=index, 
    body=es_mapping, 
    ignore=400
)'''
response = es.indices.create(
    index="elastic_indesssss", 
    body=es_mapping,
    ignore = 400
)



print(type(response))
print(response)

print("OKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK")

if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print ("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])
elif 'error' in response:
    print ("ERROR:", response['error']['root_cause'])
    print ("TYPE:", response['error']['type'])


'''query=df.writeStream.option("checkpointLocation", "./checkpoints").format("es").start(index + "/_doc")
        #.show()
        #.show()
query.awaitTermination()'''