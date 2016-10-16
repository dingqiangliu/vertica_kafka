#!/bin/sh

curDir=$(pwd)
scriptDir=$(cd $(dirname $0); pwd)


# run cmd on host through ssh
runcmd_on() {
  node=$1; shift
  if [ "localhost" = "${node}" ] ; then
	runcmd "$@"
	return $?
  fi

  command="$1"; shift
  # replace '\' with '\\' for arguments on ssh
  A_ARGS=()
  while [ "" != "$1" ] ; do
    A_ARGS[${#A_ARGS[@]}]=$(sed "s/\\\\/\\\\\\\\/g"<<<"$1")
    shift
  done

  if((dryRun)) ; then
	echo ssh ${node} ${command} "${A_ARGS[@]}"
  else
	ssh ${node} ${command} "${A_ARGS[@]}"
  fi
}

# run cmd on localhost
runcmd() {
  if((dryRun)) ; then
	msg="$@"
	if (grep "bin/vsql"<<<"${msg}" >/dev/null) && !(grep "\-c"<<<"${msg}" >/dev/null) && !(grep "\-f"<<<"${msg}" >/dev/null) ; then
	  echo "$@<<-'EOF'"
	  cat -
	  echo "EOF"
	else
	  echo "$@"
	fi
  else
	command="$1"; shift
	${command} "$@"
  fi
}

runscript_on() {
  node=$1; shift
  if [ "localhost" = "${node}" ] ; then
	runscript "$@"
	return $?
  fi

  # replace '\' with '\\' for arguments on ssh
  scpt=$(sed "s/\\\\/\\\\\\\\/g"<<<"$@")
  
  if((dryRun)) ; then
	echo ssh ${node} "echo ${scpt} | sh"
  else
	ssh ${node} "echo ${scpt} | sh"
  fi
}

# run runscript on localhost
runscript() {
  if((dryRun)) ; then
	echo "echo \"$@\" | sh"
  else
	echo "$@" | sh
  fi
}


#stop kafka service
kafka_stop() {
  echo stop kafka service...
  arrBrokerHosts=( $(sed 's/\,/\n/g'<<<"${BROKERS}"| sed 's/:.*//g' | sort -u) )
  for bhost in ${arrBrokerHosts} ; do
    runcmd_on ${bhost} $KAFKA_HOME/bin/kafka-server-stop.sh &
  done
  wait

  runcmd sleep 5

  arrZookeeperHosts=( $(sed 's/\,/\n/g'<<<"${ZOOKEEPERS}"| sed 's/:.*//g' | sort -u) )
  for zhost in ${arrZookeeperHosts} ; do
    runcmd_on ${zhost} $KAFKA_HOME/bin/zookeeper-server-stop.sh &
  done
  wait
}


# clean kafka logs, storages...
kafka_clean() {
  kafka_stop
  
  echo clean kafka logs, storages...

  arrZookeepers=( $(sed 's/\,/ /g'<<<${ZOOKEEPERS}) )
  for (( n=0 ; n<${#arrZookeepers[*]} ; n++ )) ; do
    zookeeper=${arrZookeepers[$n]}

    zid=${n}
	zhost=$(cut -d : -f 1 <<<${zookeeper})
    zport=$(cut -d : -f 2 <<<${zookeeper})
    runcmd_on ${zhost} rm -rf /tmp/zookeeper${zid}/ &
  done
  wait

  arrBrokers=( $(sed 's/\,/ /g'<<<${BROKERS}) )
  for (( n=0 ; n<${#arrBrokers[*]} ; n++ )) ; do
    broker=${arrBrokers[$n]}

    bid=${n}
	bhost=$(cut -d : -f 1 <<<${broker})
    bport=$(cut -d : -f 2 <<<${broker})
    runcmd_on ${bhost} rm -rf /tmp/kafka-logs${bid}/ &
  done
  wait
}

#start kafka service
kafka_start() {
  echo starting kafka...
  arrZookeepers=( $(sed 's/\,/ /g'<<<${ZOOKEEPERS}) )
  for (( n=0 ; n<${#arrZookeepers[*]} ; n++ )) ; do
    zookeeper=${arrZookeepers[$n]}

    zid=${n}
	zhost=$(cut -d : -f 1 <<<${zookeeper})
    zport=$(cut -d : -f 2 <<<${zookeeper})
    runcmd_on ${zhost} cp $KAFKA_HOME/config/zookeeper.properties /tmp/zookeeper${n}.properties
    runcmd_on ${zhost} sed -i "s/^clientPort=.*$/clientPort=${zport}/" /tmp/zookeeper${n}.properties
    runcmd_on ${zhost} sed -i "s/^dataDir=.*$/dataDir=\/tmp\/zookeeper${zid}/" /tmp/zookeeper${n}.properties
    runcmd_on ${zhost} $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon /tmp/zookeeper${n}.properties &
  done
  wait
  runcmd sleep 5

  arrBrokers=( $(sed 's/\,/ /g'<<<${BROKERS}) )
  for (( n=0 ; n<${#arrBrokers[*]} ; n++ )) ; do
    broker=${arrBrokers[$n]}

    bid=${n}
	bhost=$(cut -d : -f 1 <<<${broker})
    bport=$(cut -d : -f 2 <<<${broker})
    runcmd_on ${bhost} cp $KAFKA_HOME/config/server.properties /tmp/kfk-server${n}.properties
    runcmd_on ${bhost} sed -i "s/^broker\.id=.*$/broker.id=${bid}/" /tmp/kfk-server${n}.properties
    runcmd_on ${bhost} sed -i "s/^#*host\.name=.*$/host.name=${bhost}/" /tmp/kfk-server${n}.properties
    runcmd_on ${bhost} sed -i "s/^port=.*$/port=${bport}/" /tmp/kfk-server${n}.properties
    runcmd_on ${bhost} sed -i "s/^zookeeper\.connect=.*$/zookeeper.connect=${ZOOKEEPERS}/" /tmp/kfk-server${n}.properties
	
    runcmd_on ${bhost} sed -i "s/^log\.dirs=.*$/log.dirs=\/tmp\/kafka-logs${bid}/" /tmp/kfk-server${n}.properties
    runcmd_on ${bhost} sed -i "s/^log\.retention\.check.interval\.ms=.*$/log.retention.check.interval.ms=1000/" /tmp/kfk-server${n}.properties
	runcmd_on ${bhost} sed -i "1s/^/log.retention.bytes=$((200*1024*1024))\n/" /tmp/kfk-server${n}.properties
	runcmd_on ${bhost} sed -i "1s/^/log.segment.bytes=$((50*1024*1024))\n/" /tmp/kfk-server${n}.properties
    runcmd_on ${bhost} $KAFKA_HOME/bin/kafka-server-start.sh -daemon /tmp/kfk-server${n}.properties &
  done
  wait
  runcmd sleep 5
}

#int test
test_init() {
  echo creating topic...
  runcmd $KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEEPERS --create --topic $TOPIC_NAME --partitions $NUM_PARTS --replication-factor $REP_FACTOR 
  runcmd $KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEEPERS --describe --topic $TOPIC_NAME 

  echo config on vertica ...
  # install kafka connector for Vertica
  if !($VSQL -c "\df KafkaSource" | grep -i KafkaSource >/dev/null) ; then
	runcmd $VSQL -f /opt/vertica/packages/kafka/ddl/install.sql
  fi
  
	runcmd $VSQL <<-EOF
	  drop table if exists kafka_online_sales_fact cascade;
	
	  CREATE TABLE kafka_online_sales_fact
	  (
	      sale_date_key int NOT NULL ENCODING DELTAVAL,
	      ship_date_key int NOT NULL ENCODING DELTAVAL,
	      product_key int NOT NULL ENCODING DELTAVAL,
	      product_version int NOT NULL ENCODING BLOCKDICT_COMP,
	      customer_key int NOT NULL ENCODING DELTAVAL,
	      call_center_key int NOT NULL ENCODING RLE,
	      online_page_key int NOT NULL ENCODING RLE,
	      shipping_key int NOT NULL ENCODING DELTAVAL,
	      warehouse_key int NOT NULL ENCODING DELTAVAL,
	      promotion_key int NOT NULL ENCODING DELTAVAL,
	      pos_transtool_number int NOT NULL ENCODING DELTAVAL,
	      sales_quantity int ENCODING BLOCKDICT_COMP,
	      sales_dollar_amount float ENCODING BLOCKDICT_COMP,
	      ship_dollar_amount float ENCODING BLOCKDICT_COMP,
	      net_dollar_amount float ENCODING BLOCKDICT_COMP,
	      cost_dollar_amount float ENCODING BLOCKDICT_COMP,
	      gross_profit_dollar_amount float ENCODING BLOCKDICT_COMP,
	      transtool_type varchar(16) ENCODING RLE
	  )
	  ORDER BY online_page_key,
	            call_center_key,
	            transtool_type,
	            pos_transtool_number
	  SEGMENTED BY hash(online_page_key) ALL NODES KSAFE;
	
	EOF

  runcmd /opt/vertica/packages/kafka/bin/vkconfig scheduler --create --username dbadmin --password "" --config-schema stream_config --frame-duration "$FRAME_DURATION" --resource-pool general --operator dbadmin
  runcmd /opt/vertica/packages/kafka/bin/vkconfig cluster --create --username dbadmin --password "" --config-schema stream_config --cluster testKafkaCluster --hosts $BROKERS 
  runcmd /opt/vertica/packages/kafka/bin/vkconfig source --create --username dbadmin --password "" --config-schema stream_config --cluster testKafkaCluster --source $TOPIC_NAME --partitions $NUM_PARTS 
  runcmd /opt/vertica/packages/kafka/bin/vkconfig target --create --username dbadmin --password "" --config-schema stream_config --target-schema public --target-table kafka_online_sales_fact 
  runcmd /opt/vertica/packages/kafka/bin/vkconfig load-spec --create --username dbadmin --password "" --config-schema stream_config --load-spec loadspec1  --parser delimited --filters $"FILTER KafkaInsertDelimiters(delimiter = E'\n')" --load-method direct
  runcmd /opt/vertica/packages/kafka/bin/vkconfig microbatch --create --username dbadmin --password "" --config-schema stream_config --microbatch microbatch1  --target-schema public --target-table kafka_online_sales_fact --load-spec loadspec1 --add-source $TOPIC_NAME --add-source-cluster testKafkaCluster --rejection-schema public --rejection-table kafka_online_sales_fact_rej

	runcmd $VSQL <<-EOF
	  select * from stream_config.stream_scheduler;
	  select * from stream_config.stream_clusters;
	  select * from stream_config.stream_sources;
	  select * from stream_config.stream_targets;
	  select * from stream_config.stream_load_specs;
	  select * from stream_config.stream_microbatches;
	  select * from stream_config.stream_microbatch_source_map;
	  select * from stream_config.stream_lock;
	  select batch_start,batch_end-batch_start,end_offset,(end_offset-start_offset) as msgs,end_reason from stream_config.stream_microbatch_history order by batch_start desc limit 1;
	EOF

}

#clean test
test_clean() {
  echo droping config on vertica ...
  runcmd /opt/vertica/packages/kafka/bin/vkconfig microbatch --delete --username dbadmin --password "" --config-schema stream_config --microbatch microbatch1 
  runcmd /opt/vertica/packages/kafka/bin/vkconfig load-spec --delete --username dbadmin --password "" --config-schema stream_config --load-spec loadspec1 
  runcmd /opt/vertica/packages/kafka/bin/vkconfig target --delete --username dbadmin --password "" --config-schema stream_config --target-schema public --target-table kafka_online_sales_fact 
  runcmd /opt/vertica/packages/kafka/bin/vkconfig source --delete --username dbadmin --password "" --config-schema stream_config --cluster testKafkaCluster --source $TOPIC_NAME 
  runcmd /opt/vertica/packages/kafka/bin/vkconfig cluster --delete --username dbadmin --password "" --config-schema stream_config --cluster testKafkaCluster
  runcmd /opt/vertica/packages/kafka/bin/vkconfig scheduler --drop --username dbadmin --password "" --config-schema  stream_config

  ## uninstall kafka connector for Vertica
  #runcmd $VSQL -c "drop schema if exists stream_config cascade;"
  runcmd $VSQL -c "drop table if exists kafka_online_sales_fact cascade;"
  runcmd $VSQL -c "drop table if exists kafka_online_sales_fact_rej cascade;"
 
  echo droping kafka topic...
  runcmd $KAFKA_HOME/bin/kafka-topics.sh --delete --topic $TOPIC_NAME --zookeeper $ZOOKEEPERS
}

# start producer
producer_start() {
  echo producer data rows=${ROWS_COUNT}...

  for((n=0; n<PROD_COUNT; n++)) ; do
	cat <<-EOF > /tmp/testkafkaproducer${n}.sh
	  while true; do
	    maxKafkaTopicOffset=\$($KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $BROKERS --topic $TOPIC_NAME --time -1| awk -F : '{print \$3}' | sort | tail -1)
	    maxVerticaTopicOffset=\$($VSQL_F -XAqtc "select max(end_offset) from stream_config.stream_microbatch_history where source_cluster='testkafkacluster' and source_name='$TOPIC_NAME';")
	    
	    # make sue the gap betwwen vertica and kafka is not too large to avoid messages lost before comsumering, that will cause vertica connector marking time!
	    if(( maxVerticaTopicOffset + $ROWS_COUNT > maxKafkaTopicOffset )) ; then
	      ${scriptDir}/vmart_gen --time_file ${scriptDir}/Time.txt --seed \${RANDOM} --online_sales_fact $((ROWS_COUNT/PROD_COUNT)) --tablename online_sales_fact --outputfilename - 2>/dev/null | $KAFKA_HOME/bin/kafka-console-producer.sh --topic $TOPIC_NAME --broker-list $BROKERS --metadata-expiry-ms 1000	
	    else
	      sleep 1
	    fi
	  done
	EOF

	runcmd chmod a+x /tmp/testkafkaproducer${n}.sh
	runcmd nohup /tmp/testkafkaproducer${n}.sh &
  done
}

# stop producer
producer_stop() {
  echo "stoping producer..."

  for((n=0; n<PROD_COUNT; n++)) ; do
	runscript "ps ax | grep testkafkaproducer${n}.sh | grep -v "$0" | grep -v grep | awk '{print \$1}' | xargs kill -9"
  done
}

# start consumer
consumer_start() {
  echo loading data ...
  runcmd nohup /opt/vertica/packages/kafka/bin/vkconfig launch --username dbadmin --password "" --config-schema stream_config --instance-name scheduler0 &
}

# stop consumer
consumer_stop() {
  echo "stoping consumer..."
  runcmd /opt/vertica/packages/kafka/bin/vkconfig shutdown --username dbadmin --password "" --config-schema stream_config 
}

# mon
mon() {
  echo "monitoring..."
  kafka_topic_status="$($KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $BROKERS --topic $TOPIC_NAME --time -1 | tr "\\r\\n" ";" |sed -s 's/:/, /g')"
  target_row_count=$($VSQL_F -F ", " -XAqtc "select count(*) from kafka_online_sales_fact;")
  microbatch_history=$($VSQL_F -F ", " -XAqtc "select batch_start,batch_end-batch_start,end_offset,(end_offset-start_offset) as msgs,end_reason from stream_config.stream_microbatch_history order by batch_start desc limit 1;")
  running_copy_count=$($VSQL_F -F ", " -XAqtc "select count(*) from sessions where current_statement like 'COPY%KafkaSource%';")
  
	cat<<-EOF
	  kafka topic status    : $kafka_topic_status
	  target_table_row_count: $target_row_count
	  microbatch_history    : $microbatch_history
	  running_copy_cmd_count: $running_copy_count
	EOF
}


##################################################
#  parameters
#

usage() {
	cat <<-EOF
		testing kafka & vertica integration.
		Usage: $(basename ${0})
		  -t tool: mon, kafka_clean, kafka_start, test_init, producer_start, consumer_start, producer_stop, consumer_stop, test_clean, kafka_stop, 
		           services_reset = producer_stop; consumer_stop; kafka_stop; kafka_clean; kafka_start; test_clean; kafka_stop; kafka_start; test_init; kafka_stop
		           services_start = producer_stop; consumer_stop; kafka_stop
		           services_stop = kafka_start; consumer_start; producer_start
		           services_restart = producer_stop; consumer_stop; kafka_stop; kafka_start; consumer_start; producer_start
		  [-d dryrun, default off.]
	EOF
  exit 1
}

while getopts "h d t:" opt; do
	case $opt in
		t ) tool="$OPTARG";;
		d ) dryRun=1;;
		h ) usage;;
		? ) usage;;
		* ) usage;;
	esac
done;

VSQL=${VSQL:-"/opt/vertica/bin/vsql"}
VSQL_F=${VSQL/-e/}; VSQL_F=${VSQL_F/-a/}
KAFKA_HOME=${KAFKA_HOME:-"/usr/local/kafka_2.10-0.8.2.1"}
# KAFKA_HOME=${KAFKA_HOME:-"/usr/local/kafka_2.10-0.9.0.1"}
ZOOKEEPERS=${ZOOKEEPERS:-"v001:2181"}
#BROKERS=${BROKERS:-"v001:9092,v001:9093,v001:9094"}
BROKERS=${BROKERS:-"v001:9092"}
REP_FACTOR=1
TOPIC_NAME=online_sales_fact
FRAME_DURATION="00:00:01"

arrBrokers=( $(sed 's/\,/ /g'<<<${BROKERS}) )
NUM_PARTS=$(( ${#arrBrokers[*]} * 1 ))

ROWS_COUNT=1000000
PROD_COUNT=1

case ${tool} in
	kafka_start ) kafka_start;;
	kafka_stop ) kafka_stop;;
	kafka_clean ) kafka_clean;;
	test_init ) test_init;;
	producer_start ) producer_start;;
	producer_stop ) producer_stop;;
	consumer_start ) consumer_start;;
	consumer_stop ) consumer_stop;;
	test_clean ) test_clean;;
	mon ) mon;;
	services_reset ) producer_stop; consumer_stop; kafka_stop; kafka_clean; kafka_start; test_clean; kafka_stop; kafka_start; test_init; kafka_stop;;
	services_stop ) producer_stop; consumer_stop; kafka_stop;;
	services_start ) kafka_start; consumer_start; producer_start;;
	services_restart ) producer_stop; consumer_stop; kafka_stop; kafka_start; consumer_start; producer_start;;
	* ) usage;exit 1;;
esac


