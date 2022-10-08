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
from pyspark.sql.functions import from_json
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.functions import udf
import time
import json


def get_spark_session():
    spark_conf = SparkConf()\
        .set('es.nodes', 'elasticsearch_INSTAP')\
            .set('es.port', '9200')
    #spark_conf.set("es.index.auto.create", "true")
    spark_context = SparkContext(appName = 'insTAP', conf = spark_conf)
    spark_context.setLogLevel("WARN")
    spark_session = SparkSession(spark_context)
    return spark_session

#kafka message structure
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

fields_to_visualize = ["id", "user", "comment", "caption", "image", "lat", "lng", "prediction"]

training_set_schema = tp.StructType([
    tp.StructField(name= 'id', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'subjective',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'positive',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'negative',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'ironic',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'lpositive',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'lnegative',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'top',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'comment',       dataType= tp.StringType(),   nullable= True)
])

es_mapping = {
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

'''
response = es.indices.create(index="instap_index5", mappings = es_mapping, ignore = 400)
print(response)

if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print("Successfully created index:", response['index'])
'''
        
training_set = spark.read.csv('../tap/spark/dataset/training_set_sentipolc16.csv',
                         schema=training_set_schema,
                         header=True,
                         sep=',')
# define stage 1: tokenize the tweet text    
stage_1 = RegexTokenizer(inputCol= 'comment' , outputCol= 'tokens', pattern= '\\W')
# define stage 2: remove the stop words
stage_2 = StopWordsRemover(inputCol= 'tokens', outputCol= 'filtered_words')
# define stage 3: create a word vector of the size 100
stage_3 = Word2Vec(inputCol= 'filtered_words', outputCol= 'vector', vectorSize= 100)
# define stage 4: Logistic Regression Model
model = LogisticRegression(featuresCol= 'vector', labelCol= 'positive')
# setup the pipeline
pipeline = Pipeline(stages= [stage_1, stage_2, stage_3, model])


# fit the pipeline model with the training data
pipelineFit = pipeline.fit(training_set)

print(type(training_set))
print(training_set)

def elaborate(batch_df: DataFrame, batch_id: int):
    batch_df.show(truncate=False)
    if not batch_df.rdd.isEmpty():        
        #pipelineFit: modello istruito
        data = pipelineFit.transform(batch_df)
        print("********************************************************************************************************")
        print(data)
        print("********************************************************************************************************")
        clean_data = data.select(fields_to_visualize)
        
        for idx, row in enumerate(clean_data.collect()):
            doc_to_write = row.asDict()
            id = f'{batch_id}-{idx}'
            resp = es.index(index = "instap_index5", id = id, document = doc_to_write)
            print(resp)
            
##prof_method
def alt_elaborate(data2: DataFrame):
    print("******************************************************************************************************************************************************************************************************************************")
    data2.show()
    print("******************************************************************************************************************************************************************************************************************************")
    data2.select(fields_to_visualize) \
    .write \
    .format("org.elasticsearch.spark.sql") \
    .mode('overwrite') \
    .option("es.mapping.id","id") \
    .option("es.nodes", elastic_host).save("instap_index5")
    
    print("SAVED ******************************************************************************************************************************************************************************************************************************")

df.writeStream \
    .foreachBatch(elaborate) \
    .start() \
    .awaitTermination() #metodo prof
    
    





