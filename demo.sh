#!/usr/bin/env bash

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
	echo ssh ${node} "echo \"${scpt}\" | bash"
  else
	ssh ${node} "echo \"${scpt}\" | bash"
  fi
}

# run runscript on localhost
runscript() {
  if((dryRun)) ; then
	echo "echo \"$@\" | bash"
  else
	echo "$@" | bash
  fi
}


#stop kafka service
kafka_stop() {
  echo stop kafka service...
  arrBrokerHosts=( $(sed 's/\,/\n/g'<<<"${BROKERS}"| sed 's/:.*//g' | sort -u) )
  for bhost in ${arrBrokerHosts[*]} ; do
    runscript_on ${bhost} "sed -i 's/xargs kill/xargs -r kill/g' $KAFKA_HOME/bin/kafka-server-stop.sh"
    runcmd_on ${bhost} $KAFKA_HOME/bin/kafka-server-stop.sh &
  done
  wait

  runcmd sleep 5

  arrZookeeperHosts=( $(sed 's/\,/\n/g'<<<"${ZOOKEEPERS}"| sed 's/:.*//g' | sort -u) )
  for zhost in ${arrZookeeperHosts[*]} ; do
    runscript_on ${zhost} "sed -i 's/xargs kill/xargs -r kill/g' $KAFKA_HOME/bin/zookeeper-server-stop.sh"
    runcmd_on ${zhost} $KAFKA_HOME/bin/zookeeper-server-stop.sh &
  done
  wait

  # sometimes zoopker can not be stopped, just kill it roughly
  runcmd sleep 2
  for zhost in ${arrZookeeperHosts[*]} ; do
    runscript_on ${zhost} "ps ax | grep -i 'zookeeper' | grep -v grep | xargs | cut -d ' ' -f 1 | xargs -r kill -9" &
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
    runcmd_on ${zhost} rm -rf ${KAFKA_DATA}/zookeeper${zid}/ &
  done
  wait

  arrBrokers=( $(sed 's/\,/ /g'<<<${BROKERS}) )
  for (( n=0 ; n<${#arrBrokers[*]} ; n++ )) ; do
    broker=${arrBrokers[$n]}

    bid=${n}
	bhost=$(cut -d : -f 1 <<<${broker})
    bport=$(cut -d : -f 2 <<<${broker})
    runcmd_on ${bhost} rm -rf ${KAFKA_DATA}/kafka-logs${bid}/ &
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
    runcmd_on ${zhost} cp $KAFKA_HOME/config/zookeeper.properties $KAFKA_HOME/config/zookeeper${n}.properties
    runscript_on ${zhost} "sed -i 's/^clientPort=.*$/clientPort=${zport}/' $KAFKA_HOME/config/zookeeper${n}.properties"
    runscript_on ${zhost} "sed -i 's/^dataDir=.*$/dataDir=$(sed -e 's/\//\\\//g' <<< ${KAFKA_DATA})\/zookeeper${zid}/' $KAFKA_HOME/config/zookeeper${n}.properties"
    runcmd_on ${zhost} $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper${n}.properties &
  done
  wait
  runcmd sleep 5

  arrBrokers=( $(sed 's/\,/ /g'<<<${BROKERS}) )
  for (( n=0 ; n<${#arrBrokers[*]} ; n++ )) ; do
    broker=${arrBrokers[$n]}

    bid=${n}
	bhost=$(cut -d : -f 1 <<<${broker})
    bport=$(cut -d : -f 2 <<<${broker})
    runcmd_on ${bhost} cp $KAFKA_HOME/config/server.properties $KAFKA_HOME/config/kfk-server${n}.properties
    runscript_on ${bhost} "sed -i 's/^broker\.id=.*$/broker.id=${bid}/' $KAFKA_HOME/config/kfk-server${n}.properties"
    runscript_on ${bhost} "sed -i 's/^#*host\.name=.*$/host.name=${bhost}/' $KAFKA_HOME/config/kfk-server${n}.properties"
    runscript_on ${bhost} "sed -i 's/^port=.*$/port=${bport}/' $KAFKA_HOME/config/kfk-server${n}.properties"
    runscript_on ${bhost} "sed -i 's/^zookeeper\.connect=.*$/zookeeper.connect=${ZOOKEEPERS}/' $KAFKA_HOME/config/kfk-server${n}.properties"
	
    runscript_on ${bhost} "sed -i 's/^log\.dirs=.*$/log.dirs=$(sed -e 's/\//\\\//g' <<< ${KAFKA_DATA})\/kafka-logs${bid}/' $KAFKA_HOME/config/kfk-server${n}.properties"
    runscript_on ${bhost} "sed -i 's/^log\.retention\.check.interval\.ms=.*$/log.retention.check.interval.ms=1000/' $KAFKA_HOME/config/kfk-server${n}.properties"
    runscript_on ${bhost} "sed -i '1s/^/log.retention.bytes=$((200*1024*1024))\n/' $KAFKA_HOME/config/kfk-server${n}.properties"
    runscript_on ${bhost} "sed -i '1s/^/log.segment.bytes=$((50*1024*1024))\n/' $KAFKA_HOME/config/kfk-server${n}.properties"
    runcmd_on ${bhost} $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/kfk-server${n}.properties &
  done
  wait
  runcmd sleep 5
}

#int test
test_init() {
  echo creating topic...
  for((n=0; n<TOPIC_COUNT; n++)) ; do  
    runcmd $KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEEPERS --create --topic ${TOPIC_NAME}${n}  --partitions $NUM_PARTS --replication-factor $REP_FACTOR 
    runcmd $KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEEPERS --describe --topic ${TOPIC_NAME}${n} 
  done

  echo config on vertica ...
  # install kafka connector for Vertica
  if !($VSQL -c "\df KafkaSource" | grep -i KafkaSource >/dev/null) ; then
	runcmd $VSQL -f /opt/vertica/packages/kafka/ddl/install.sql
  fi
  
	runcmd $VSQL <<-EOF
	  create resource pool stream_default_pool;

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

  runcmd /opt/vertica/packages/kafka/bin/vkconfig scheduler --create --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --frame-duration "$FRAME_DURATION" --resource-pool stream_default_pool --operator "$VER_SCHED_USER"
  runcmd /opt/vertica/packages/kafka/bin/vkconfig cluster --create --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --cluster testKafkaCluster --hosts $BROKERS --kafka_conf '{"api.version.request":false}'
  runcmd /opt/vertica/packages/kafka/bin/vkconfig target --create --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --target-schema public --target-table kafka_online_sales_fact
  runcmd /opt/vertica/packages/kafka/bin/vkconfig load-spec --create --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --load-spec loadspec1  --parser delimited --filters $"FILTER KafkaInsertDelimiters(delimiter = E'\n')" --load-method auto --uds-kv-parameters '{"api.version.request":false}'
  runcmd /opt/vertica/packages/kafka/bin/vkconfig load-spec --read --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --load-spec loadspec1

  for((n=0; n<TOPIC_COUNT; n++)) ; do
    runcmd /opt/vertica/packages/kafka/bin/vkconfig source --create --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --cluster testKafkaCluster --source ${TOPIC_NAME}${n} --partitions $NUM_PARTS --kafka_conf '{"api.version.request":false}'
    runcmd /opt/vertica/packages/kafka/bin/vkconfig microbatch --create --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --microbatch microbatch${n}  --target-schema public --target-table kafka_online_sales_fact --load-spec loadspec1 --add-source ${TOPIC_NAME}${n} --add-source-cluster testKafkaCluster --rejection-schema public --rejection-table kafka_online_sales_fact_rej
  done

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
  for((n=0; n<TOPIC_COUNT; n++)) ; do
    runcmd /opt/vertica/packages/kafka/bin/vkconfig microbatch --delete --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --microbatch microbatch${n}
    runcmd /opt/vertica/packages/kafka/bin/vkconfig source --delete --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --cluster testKafkaCluster --source ${TOPIC_NAME}${n}
  done
  runcmd /opt/vertica/packages/kafka/bin/vkconfig load-spec --delete --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --load-spec loadspec1
  runcmd /opt/vertica/packages/kafka/bin/vkconfig target --delete --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --target-schema public --target-table kafka_online_sales_fact
  runcmd /opt/vertica/packages/kafka/bin/vkconfig cluster --delete --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --cluster testKafkaCluster
  runcmd /opt/vertica/packages/kafka/bin/vkconfig scheduler --drop --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema  stream_config

  ## uninstall kafka connector for Vertica
  #runcmd $VSQL -c "drop schema if exists stream_config cascade;"
  runcmd $VSQL -c "drop table if exists kafka_online_sales_fact cascade;"
  runcmd $VSQL -c "drop table if exists kafka_online_sales_fact_rej cascade;"
 
  echo droping kafka topic...
  for((n=0; n<TOPIC_COUNT; n++)) ; do
    runcmd $KAFKA_HOME/bin/kafka-topics.sh --delete --topic ${TOPIC_NAME}${n} --zookeeper $ZOOKEEPERS
  done
}

# start producer
producer_start() {
  echo producing data...

  for((n=0; n<TOPIC_COUNT; n++)) ; do
	cat <<-EOF > /tmp/testkafkaproducer${n}.sh
#!/usr/bin/env bash
	  while true; do
	    maxKafkaTopicOffset=\$($KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $BROKERS --topic ${TOPIC_NAME}${n} --time -1| awk -F : '{print \$3}' | sort | tail -1)
	    maxVerticaTopicOffset=\$($VSQL_F -XAqtc "select max(end_offset) from stream_config.stream_microbatch_history where source_cluster='testkafkacluster' and source_name='${TOPIC_NAME}${n}';")
	    
	    # make sue the gap betwwen vertica and kafka is not too large to avoid messages lost before comsumering, that will cause vertica connector marking time!
	    if(( maxVerticaTopicOffset + ${ROWS_COUNT}/${TOPIC_COUNT} > maxKafkaTopicOffset )) ; then
	      ${scriptDir}/vmart_gen --time_file ${scriptDir}/Time.txt --seed \${RANDOM} --online_sales_fact $((ROWS_COUNT/TOPIC_COUNT)) --tablename online_sales_fact --outputfilename - 2>/dev/null | $KAFKA_HOME/bin/kafka-console-producer.sh --topic ${TOPIC_NAME}${n} --broker-list $BROKERS --metadata-expiry-ms 1000	
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

  for((n=0; n<TOPIC_COUNT; n++)) ; do
	runscript "ps ax | grep testkafkaproducer${n}.sh | grep -v "$0" | grep -v grep | awk '{print \$1}' | xargs -r kill -9"
	runscript "ps ax | grep kafka.tools.ConsoleProducer | grep -v grep | awk '{print \$1}' | xargs -r kill -9"
  done
}

# start consumer
consumer_start() {
  echo loading data ...
  runcmd nohup /opt/vertica/packages/kafka/bin/vkconfig launch --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config --instance-name scheduler0 &
}

# stop consumer
consumer_stop() {
  echo "stoping consumer..."
  runcmd /opt/vertica/packages/kafka/bin/vkconfig shutdown --username "$VER_SCHED_USER" --password "$VER_SCHED_PWD" --config-schema stream_config
}

# mon
mon() {
  kafka_status=$($scriptDir/kafkacat -L -b $BROKERS -q 2>/dev/null|sed '2,$s/^/                            /g')
  vertica_status=$($VSQL_F -XAqtc 'select version();' 2>/dev/null)

  kafka_offsets_of_topics="$(test -n "$kafka_status" && for((n=0; n<TOPIC_COUNT; n++)) ; do $KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $BROKERS --topic ${TOPIC_NAME}${n} --time -1; done | tr "\\r\\n" ";" |sed -s 's/:/, /g')"
  target_row_count=$(test -n "$vertica_status" && $VSQL_F -F ", " -XAqtc "select count(*) from kafka_online_sales_fact;")
  scheduler_is_running=$(test -n "$vertica_status" && $VSQL_F -F ", " -XAqtc "SELECT count(*) as cnt FROM v_monitor.locks AS l JOIN v_catalog.tables AS t ON l.object_id = t.table_id WHERE t.table_schema ilike 'stream_config' AND t.table_name = 'stream_lock' AND l.lock_scope ilike '%transaction%' AND l.lock_mode ilike '%s%';")
  running_producers_count=$(ps ax | grep testkafkaproducer.*.sh | grep -v grep | awk '{print $6}' | sort -u | wc -l)
  running_copy_count=$(test -n "$vertica_status" && $VSQL_F -F ", " -XAqtc "select count(*) from sessions where current_statement like 'COPY%KafkaSource%';")
  latest_microbatch=$(test -n "$vertica_status" && $VSQL_F -F ", " -XAqtc "select row(microbatch,source_name,batch_start,(batch_end-batch_start) as duration,end_offset,(end_offset-start_offset) as msgs,end_reason) from stream_config.stream_microbatch_history limit 1 over(partition by microbatch,source_name order by batch_start desc);"|sed '2,$s/^/                           /g')
  
	cat<<-EOF
	  status...
	  ----------------------------------------------------------------------------
	  vertica status         : $vertica_status
	  microbatch history     : $latest_microbatch
	  scheduler is running   : $scheduler_is_running
	  running producers count: $running_producers_count
	  running copy cmds count: $running_copy_count
	  kafka offsets of topics: $kafka_offsets_of_topics
	  target table rows count: $target_row_count
	  kafka status           : $kafka_status
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
		           services_reset = producer_stop; consumer_stop; kafka_stop; kafka_start; test_clean; kafka_stop; kafka_clean; kafka_start; test_init; kafka_stop
		           services_stop = producer_stop; consumer_stop; kafka_stop
		           services_start = kafka_start; consumer_start; producer_start
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
VER_SCHED_USER="dbadmin"
VER_SCHED_PWD=""
KAFKA_HOME=${KAFKA_HOME:-"/usr/local/kafka_2.10-0.9.0.1"}
KAFKA_DATA=/tmp/kafka
ZOOKEEPERS=${ZOOKEEPERS:-"localhost:2181"}
#sample: BROKERS=${BROKERS:-"v001:9092,v001:9093,v001:9094"}
BROKERS=${BROKERS:-"localhost:9092"}
REP_FACTOR=1
TOPIC_NAME=online_sales_fact

arrBrokers=( $(sed 's/\,/ /g'<<<${BROKERS}) )
NUM_PARTS=$(( ${#arrBrokers[*]} * 1 ))

ROWS_COUNT=1000000
FRAME_DURATION="00:00:10"
TOPIC_COUNT=1

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
	services_reset ) producer_stop; consumer_stop; kafka_stop; kafka_start; test_clean; kafka_stop; kafka_clean; kafka_start; test_init; kafka_stop;;
	services_stop ) producer_stop; consumer_stop; kafka_stop;;
	services_start ) kafka_start; consumer_start; producer_start;;
	services_restart ) producer_stop; consumer_stop; kafka_stop; kafka_start; consumer_start; producer_start;;
	* ) usage;exit 1;;
esac

