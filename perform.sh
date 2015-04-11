#!/bin/bash

set -x
#
VCOREREFERENCE=http://support.elasticmapreduce.s3.amazonaws.com/spark/vcorereference.tsv
CONFIGURESPARK=http://support.elasticmapreduce.s3.amazonaws.com/spark/configure-spark.bash

#--- Now use configure-spark.bash to set values

wget $CONFIGURESPARK

bash configure-spark.bash  spark.io.compression.codec=lzf


bash configure-spark.bash spark.executor.extraJavaOptions         -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSPermGenSweepingEnabled -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=5000m
bash configure-spark.bash spark.driver.extraJavaOptions         -Dspark.driver.log.level=INFO -XX:+UseConcMarkSweepGC -XX:+CMSPermGenSweepingEnabled -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=5000m
bash configure-spark.bash spark.io.compression.codec      lzf


#bash configure-spark.bash spark.serializer=org.apache.spark.serializer.KryoSerializer spark.speculation=true

#MISSING  JARS
wget http://www.java2s.com/Code/JarDownload/play/play_2.10.jar.zip
wget http://www.java2s.com/Code/JarDownload/play-iteratees/play-iteratees_2.10.jar.zip
wget http://www.us.apache.org/dist/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
wget http://central.maven.org/maven2/org/elasticsearch/elasticsearch-spark_2.10/2.1.0.Beta3/elasticsearch-spark_2.10-2.1.0.Beta3.jar
wget http://central.maven.org/maven2/org/elasticsearch/elasticsearch-hadoop/2.1.0.Beta3/elasticsearch-hadoop-2.1.0.Beta3.jar

unzip play_2.10.jar.zip
unzip play-iteratees_2.10.jar.zip

mkdir -p sparkmain/target
tar -zxf apache-maven-3.0.5-bin.tar.gz

bash configure-spark.bash
#/usr/share/aws/emr/scripts/configure-hadoop -y yarn.nodemanager.resource.cpu-vcores=2
#/usr/share/aws/emr/scripts/configure-hadoop -z yarn.scheduler.capacity.maximum-applications=40000
#/usr/share/aws/emr/scripts/configure-hadoop -y yarn.scheduler.capacity.default.maximum-applications=500
#/usr/share/aws/emr/scripts/configure-hadoop -z yarn.scheduler.capacity.maximum-am-resource-percent=100

exit 0