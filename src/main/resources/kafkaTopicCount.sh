while [[ $# > 0 ]]
do
key="$1"

case $key in
    -e|--topic)
    TOPIC="$2"
    shift # past argument
    ;;
esac
shift # past argument or value
done


sum=0
for line in $(bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --time -1 --topic $TOPIC)
do
count=`echo $line| cut -d':' -f 3`
sum=$((sum+count)) 
done
echo $sum