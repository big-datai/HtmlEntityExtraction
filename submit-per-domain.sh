i = 0
for d in $(cat ~/domains.list); do
i=$((i+1))
nohup ~/spark/bin/spark-submit --jars $(echo /home/hadoop/*.jar | tr ' ' ',') --class "um.re.domain.models.GBTPerDomain" --master yarn-cluster /home/hadoop/sparkmain/target/sparkmain-1.0-SNAPSHOT.jar $d &
if (($i == 10)); then
sleep 20
i=0
fi
done
