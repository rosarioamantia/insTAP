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
import time

spark = SparkSession.builder.appName("TapDataFrame").getOrCreate()
#time.sleep(500)

#creazione dataframe tipizzato
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

training_set = spark.read.csv("/opt/tap/spark/dataset/training_set_sentipolc16.csv", schema = schema, header = True, sep = ',')

training_set.groupBy("positive").count().show()
#training_set.show(200, 1000)

stage_1 = RegexTokenizer(inputCol= 'tweet' , outputCol= 'tokens', pattern= '\\W')
stage_2 = StopWordsRemover(inputCol= 'tokens', outputCol= 'filtered_words')
stage_3 = Word2Vec(inputCol= 'filtered_words', outputCol= 'vector', vectorSize= 100)
model = LogisticRegression(featuresCol= 'vector', labelCol= 'positive')
pipeline = Pipeline(stages= [stage_1, stage_2, stage_3, model])

print("----------------------------------------------------------------PipelineFit-------------------------------------------------------------")
print("inizio pipelineFit")
training_set.show(20, 1000)
pipelineFit = pipeline.fit(training_set)
print(type(pipelineFit))
print("fine pipelineFit")
#pipelineFit.select(all).show(truncate = False)

tweetDf = spark.createDataFrame(["bello", "brutto"], tp.StringType()).toDF("tweet")
tweetDf.show(truncate=False)

pipelineFit.transform(tweetDf).select('tweet', 'prediction', ).show(100)

#modelSummary = pipelineFit.stages[-1].summary #stampandolo si hanno dei risultati del modello



#print(spark)
print("ciau")