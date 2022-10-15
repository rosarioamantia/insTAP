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
        .set('es.nodes', 'elasticsearch')\
            .set('es.port', '9200')
    spark_context = SparkContext(appName = 'insTAP', conf = spark_conf)
    spark_context.setLogLevel("WARN")
    spark_session = SparkSession(spark_context)
    return spark_session

spark = get_spark_session()
topic = "instap"
kafkaServer = "broker:9092"
elastic_index = "instap"
elastic_host = "http://elasticsearch:9200"


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

training_set = spark.read.csv('../tap/spark/dataset/training_set_sentipolc16.csv',
                         schema=training_set_schema,
                         header=True,
                         sep=',')
# define stage 1: tokenize the Instagram comments text    
stage_1 = RegexTokenizer(inputCol= 'comment' , outputCol= 'tokens', pattern= '\\W')
# define stage 2: remove the stop words
stage_2 = StopWordsRemover(inputCol= 'tokens', outputCol= 'filtered_words')
# define stage 3: create a word vector of the size 50
stage_3 = Word2Vec(inputCol= 'filtered_words', outputCol= 'vector', vectorSize= 50)
# define stage 4: Logistic Regression Model
model = LogisticRegression(featuresCol= 'vector', labelCol= 'positive')
# setup the pipeline
pipeline = Pipeline(stages= [stage_1, stage_2, stage_3, model])


#trained model
pipelineFit = pipeline.fit(training_set)

#Create DataFrame representing the stream of input lines from Kafka
df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', kafkaServer) \
        .option('subscribe', topic). \
            option("startingOffsets","earliest").load()
            
df = df.selectExpr("CAST(value AS STRING)") \
.select(from_json("value", schema=schema).alias("data")) \
.select("data.*")

es = Elasticsearch(hosts=elastic_host, verify_certs = False)

def process_batch(batch_df: DataFrame, batch_id: int):
    batch_df.show(truncate=False)
    if not batch_df.rdd.isEmpty():        
        #pipelineFit = trained model
        data = pipelineFit.transform(batch_df)
        print("********************************************************************************************************")
        print(data)
        print("********************************************************************************************************")
        analyzed_data = data.select(fields_to_visualize)
        
        for idx, row in enumerate(analyzed_data.collect()):
            doc_to_write = row.asDict()
            id = f'{batch_id}-{idx}'
            resp = es.index(index = elastic_index, id = id, document = doc_to_write)
            print(resp)
            
            if 'acknowledged' in resp:
                if resp['acknowledged'] == True:
                    print("Successfully created index:", resp['index'])
            
##prof_method
def alt_process_batch(data2: DataFrame):
    print("******************************************************************************************************************************************************************************************************************************")
    data2.show()
    print("******************************************************************************************************************************************************************************************************************************")
    data2.select(fields_to_visualize) \
    .write \
    .format("org.elasticsearch.spark.sql") \
    .mode('overwrite') \
    .option("es.mapping.id","id") \
    .option("es.nodes", elastic_host).save(elastic_index)
    
    print("SAVED ******************************************************************************************************************************************************************************************************************************")

df.writeStream \
    .foreachBatch(process_batch) \
    .start() \
    .awaitTermination()
    
    





