or d in $(cat ~/domains.list); do
nohup ~/spark/bin/spark-submit --jars $(echo /home/hadoop/*.jar | tr ' ' ',') --class "um.re.models.GBTPerDomain" --master yarn-cluster /home/hadoop/sparkmain/target/sparkmain-1.0-SNAPSHOT.jar $d &
done