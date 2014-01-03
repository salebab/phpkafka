#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>

#include "librdkafka/rdkafka.h"  /* for Kafka driver */


ZEND_BEGIN_ARG_INFO(arginfo_kafka_produce, 0)
    ZEND_ARG_INFO(0, string)
ZEND_END_ARG_INFO();

/* Function names */
ZEND_FUNCTION(kafka_produce);

zend_function_entry kafka_produce_functions[] =
{
    ZEND_FE(kafka_produce, arginfo_kafka_produce)
    {NULL,NULL,NULL} /* Marks the end of function entries */
};


zend_module_entry kafka_module_entry = {
    STANDARD_MODULE_HEADER,
    "kafka",
    kafka_produce_functions, /* Function entries */
    NULL, /* Module init */
    NULL, /* Module shutdown */
    NULL, /* Request init */
    NULL, /* Request shutdown */
    NULL, /* Module information */
    "0.1", /* Replace with version number for your extension */
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_KAFKA
ZEND_GET_MODULE(kafka)
#endif

PHP_FUNCTION(kafka_produce)
{
    char* str;
    int str_len;
    
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &str, &str_len) == FAILURE) {
        return;
    }
	
	
	static rd_kafka_t *rk;
	
	rd_kafka_topic_t *rkt;
	char *brokers = "localhost:9092";
	char mode = 'C';
	char *topic = NULL;
	int partition = RD_KAFKA_PARTITION_UA;
	int opt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
	const char *debug = NULL;
	int64_t start_offset = 0;

	/* Kafka configuration */
	conf = rd_kafka_conf_new();

	/* Topic configuration */
	topic_conf = rd_kafka_topic_conf_new();
	
	//rd_kafka_conf_set_dr_cb(conf, msg_delivered);

	/* Create Kafka handle */
	if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
            fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
            exit(1);
	}


	/* Add brokers */
	if (rd_kafka_brokers_add(rk, brokers) == 0) {
            fprintf(stderr, "%% No valid brokers specified\n");
            exit(1);
	}

	/* Create topic */
	rkt = rd_kafka_topic_new(rk, topic, topic_conf);
	
	
	rd_kafka_produce(rkt, partition,
		 RD_KAFKA_MSG_F_COPY,
		 /* Payload and length */
		 str, str_len,
		 /* Optional key and its length */
		 NULL, 0,
		 /* Message opaque, provided in
		  * delivery report callback as
		  * msg_opaque. */
		 NULL);
	 
	 rd_kafka_poll(rk, 10);
    
    RETURN_STRING(str, 1);
}