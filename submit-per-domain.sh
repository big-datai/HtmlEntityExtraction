i = 0
for d in $(cat ~/domains.list); do
i=$((i+1))
nohup ~/spark/bin/spark-submit --jars $(echo /home/hadoop/*.jar | tr ' ' ',') --class "um.re.domain.models.GBTPerDomain" --master yarn-client --num-executors 4 --driver-memory 1g --executor-memory 2g --executor-cores 2 /home/hadoop/sparkmain/target/sparkmain-1.0-SNAPSHOT.jar $d &
sleep 60
echo $i"   "$d
if (($i == 30)); then
sleep 7000
i=0
fi
done
