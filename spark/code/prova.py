from os import truncate
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from translate import Translator
import time

#
vader = SentimentIntensityAnalyzer()
translator = Translator(from_lang = "it", to_lang="en")
# 

#
def get_spark_session():
    spark_conf = SparkConf().set
    spark = SparkSession.builder.appName("TapDataFrame").getOrCreate()
    return spark

def get_sentiment(text):
    print("traduco: ")
    print(translator.translate(text))

    value = vader.polarity_scores(translator.translate(text))
    value = value['compound']

    # 1) positive sentiment: compound score >= 0.05
    # 2) neutral sentiment: (compound score > -0.05) and (compound score < 0.05)
    # 3) negative sentiment: compound score <= -0.05
    return value


#print("OLEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
print("OOOOOOOOKKKKKKKKKKKKKKKK")
print(get_sentiment("oggi sono andato in campagna e ne sono tanto felice"))
print(get_sentiment("bene"))
print(get_sentiment("benissimo"))
print(get_sentiment("oggi è andata peggio di ieri, domani andrà meglio, evviva!"))


# JSON schema from Kafka
schema = tp.StructType([
    tp.StructField(name= 'id', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'subjective',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'positive',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'negative',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'ironic',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'lpositive',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'lnegative',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'top',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'tweet',       dataType= tp.StringType(),   nullable= True)
])

#training_set = spark.read.csv("/opt/tap/spark/dataset/training_set_sentipolc16.csv", schema = schema, header = True, sep = ',')



spark = get_spark_session()
spark.sparkContext.setLogLevel("ERROR")

kafka_server = ""


df = spark.readStream \
    