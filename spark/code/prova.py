from ast import Str
from functools import partial
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.types import StructType,StructField, StringType
from elasticsearch import Elasticsearch
from datetime import datetime
from time import sleep
from pyspark.sql.functions import from_json
import pyspark.sql.types as tp
from pyspark.sql import types as st

from pyspark.ml import PipelineModel


spark = SparkSession.builder.appName("APP_NAME").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print(spark)

model = PipelineModel.load("model")

kafka_struct = tp.StructType([
        tp.StructField('countryCode',               dataType= tp.StringType()),
        tp.StructField('totalProduction',           dataType= tp.DoubleType())  
        
])

df = 