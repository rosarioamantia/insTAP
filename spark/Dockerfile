FROM openjdk:8-jre

ENV SPARK_VERSION=3.1.1
ENV SPARK_DIR=/opt/spark
ENV PATH $SPARK_DIR/bin:$PATH

# ADD will automatically extract the file
ADD setup/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz /opt

RUN apt-get update && apt-get -y install bash python3 python3-pip netcat file 

RUN pip3 install pyspark numpy 'elasticsearch==8.2.0' vaderSentiment translate kafka-python pandas
# Create Sym Link 
RUN ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop2.7 ${SPARK_DIR} 

# Add dataset
ADD dataset /opt/tap/spark/dataset

# Add Python Code
ADD code/*  /opt/tap/

# Add Java Code
#ADD apps /opt/tap/apps

# Add Spark Manager
#ADD spark-manager.sh $SPARK_DIR/bin/spark-manager

WORKDIR ${SPARK_DIR}

ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.2.0", "--master", "local[*]", "../tap/prova_multi_index.py"]