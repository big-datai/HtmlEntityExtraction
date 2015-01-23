#!/bin/bash
# Configures spark-default.conf for dedicate/maximum cluster use
#   Set num executors to number to total instance count at time of creation (spark.executor.instances)
#   Set vcores per executor to be all the vcores for the instance type of the core nodes (spark.executor.cores)
#   Set the memory per executor to the max available for the node (spark.executor.memory)
#   Set the default parallelism to the total number of cores available across all nodes at time of cluster creation  (spark.default.parallelism)
#
# Limitations:
#   Assumes a homogenous cluster (all core and task instance groups the same instance type)
#   Is not dynamic with cluster resices
# https://github.com/awslabs/emr-bootstrap-actions/tree/master/spark
set -x
#
VCOREREFERENCE=http://support.elasticmapreduce.s3.amazonaws.com/spark/vcorereference.tsv
CONFIGURESPARK=http://support.elasticmapreduce.s3.amazonaws.com/spark/configure-spark.bash
#
echo "Configuring Spark default configuration to the max memory and vcore setting given configured number of cores nodes at cluster creation"

#Set the default yarn min allocation to 256 to allow for most optimum memory use
/usr/share/aws/emr/scripts/configure-hadoop -y yarn.scheduler.minimum-allocation-mb=256

#Gather core node count
NUM_NODES=$(grep /mnt/var/lib/info/job-flow.json -e "instanceCount" | sed 's/.*instanceCount.*:.\([0-9]*\).*/\1/g')
NUM_NODES=$(expr $NUM_NODES - 1)

if [ $NUM_NODES -lt 2 ]
then
#set back to default to be safe
NUM_NODES=2
fi

SLAVE_INSTANCE_TYPE=$(grep /mnt/var/lib/info/job-flow.json -e "slaveInstanceType" | cut -d'"' -f4 | sed  's/\s\+//g')

if [ "$SLAVE_INSTANCE_TYPE" == "" ]
then
SLAVE_INSTANCE_TYPE="m3.xlarge"
fi

wget $VCOREREFERENCE

NUM_VCORES=$(grep vcorereference.tsv -e $SLAVE_INSTANCE_TYPE | cut -f2)

MAX_YARN_MEMORY=$(grep /home/hadoop/conf/yarn-site.xml -e "yarn\.scheduler\.maximum-allocation-mb" | sed 's/.*<value>\(.*\).*<\/value>.*/\1/g')

EXEC_MEMORY=$(echo "($MAX_YARN_MEMORY - 1024 - 384) - ($MAX_YARN_MEMORY - 1024 - 384) * 0.07 " | bc | cut -d'.' -f1)
EXEC_MEMORY+="M"

PARALLEL=$(expr $NUM_VCORES \*2 \* $NUM_NODES)

#--- Now use configure-spark.bash to set values

wget $CONFIGURESPARK

bash configure-spark.bash spark.executor.instances=$NUM_NODES spark.executor.cores=$NUM_VCORES spark.executor.memory=$EXEC_MEMORY

if [ $PARALLEL -gt 2 ]
then
#only set/change this if it looks reasonable
bash configure-spark.bash spark.default.parallelism=$PARALLEL
fi

echo "spark.serializer        org.apache.spark.serializer.KryoSerializer" >>~/spark/conf/spark-defaults.conf
echo "spark.kryo.registrator        um.re.es.emr.MyRegistrator" >>~/spark/conf/spark-defaults.conf

#MISSING  JARS
wget http://www.java2s.com/Code/JarDownload/play/play_2.10.jar.zip
wget http://www.java2s.com/Code/JarDownload/play-iteratees/play-iteratees_2.10.jar.zip
wget http://www.us.apache.org/dist/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
wget http://central.maven.org/maven2/org/elasticsearch/elasticsearch-spark_2.10/2.1.0.Beta3/elasticsearch-spark_2.10-2.1.0.Beta3.jar
wget http://central.maven.org/maven2/org/elasticsearch/elasticsearch-hadoop/2.1.0.Beta3/elasticsearch-hadoop-2.1.0.Beta3.jar

unzip play_2.10.jar.zip
unzip play-iteratees_2.10.jar.zip
tar -zxf apache-maven-3.0.5-bin.tar.gz
/usr/share/aws/emr/scripts/configure-hadoop -y yarn.nodemanager.resource.cpu-vcores=2

exit 0