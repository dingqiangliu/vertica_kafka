<html lang="zn_CN"> <head> <meta charset='utf-8'> <title>Vertica and Kafka Integration Demo</title> </head> <body>

Vertica and Kafka integration demo
==========
This is simple CLI demo, **demo.sh**, for integraton between Vertica and Kafka.

It provide some "tools" to startup/stop kafka cluster, config Vertica microbatch, start/stop producer/consumer, and monitor.

 * -t toolName
 * -d . just show the commands, will not run them really. 


Usage: ./demo.sh -h
----------
![help](./imgs/help.png)

Monitoring: ./demo.sh -t mon
----------
![monitoring](./imgs/monitoring.png)

Note: 
----------
 * maybe you need re-compile **vmart_gen** in **generator** directory if your system is not RHEL6.x-64.
 * before use **-t mon** to get status of kafka, you'd install libs/librdkafka-0.9.1.tgz first.


</body> </html>



