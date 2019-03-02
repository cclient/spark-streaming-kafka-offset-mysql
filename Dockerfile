FROM gettyimages/spark:2.3.0-hadoop-2.8
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
ADD jars /usr/spark-2.3.0/jars
ADD target/scala-2.11/ /usr/spark-2.3.0/