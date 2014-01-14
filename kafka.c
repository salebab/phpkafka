#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>
#include <syslog.h>
#include <time.h>
#include <php.h>
#include <php_kafka.h>
#include <librdkafka/rdkafka.h>


static int run = 1;
static rd_kafka_t *rk;

static void stop(int sig) {
    run = 0;
    fclose(stdin); /* abort fgets() */
}

static void err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
    openlog("phpkafka", 0, LOG_USER);
    syslog(LOG_INFO, "phpkafka - ERROR CALLBACK: %s: %s: %s\n", 
            rd_kafka_name(rk), rd_kafka_err2str(err), reason);
    
    stop(err);
}


static PHP_FUNCTION(kafka_produce);

ZEND_BEGIN_ARG_INFO(arginfo_kafka_produce, 0)
    ZEND_ARG_INFO(0, string)
    ZEND_ARG_INFO(0, string)
    ZEND_ARG_INFO(0, string)
ZEND_END_ARG_INFO();


zend_function_entry kafka_functions[] = {
    ZEND_FE(kafka_produce, arginfo_kafka_produce)
    {NULL,NULL,NULL} /* Marks the end of function entries */
};

zend_module_entry kafka_module_entry = {
    STANDARD_MODULE_HEADER,
    "kafka",
    kafka_functions, /* Function entries */
    PHP_MINIT(kafka), /* Module init */
    PHP_MSHUTDOWN(kafka), /* Module shutdown */
    PHP_RINIT(kafka), /* Request init */
    PHP_RSHUTDOWN(kafka), /* Request shutdown */
    NULL, /* Module information */
    "0.1", /* Replace with version number for your extension */
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_KAFKA
ZEND_GET_MODULE(kafka)
#endif

PHP_RSHUTDOWN_FUNCTION(kafka)
{
    /* Destroy the handle */
    rd_kafka_wait_destroyed(1);
    rd_kafka_destroy(rk);
    rk = NULL;
    //fprintf(stdout, "%% Request shutdown\n");
    return SUCCESS;
}
PHP_RINIT_FUNCTION(kafka)
{
    
    //fprintf(stdout, "%% Request init\n");
    return SUCCESS;
}


PHP_MSHUTDOWN_FUNCTION(kafka)
{
    //fprintf(stdout, "%% Module shutdown");
    return SUCCESS;
}
PHP_MINIT_FUNCTION(kafka)
{
    //fprintf(stdout, "%% Module init\n");
    return SUCCESS;
}
static void msg_delivered (rd_kafka_t *rk,
                           void *payload, size_t len,
                           int error_code,
                           void *opaque, void *msg_opaque) {
    if (error_code) {
        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - Message delivery failed: %s",
                rd_kafka_err2str(error_code));
    }
    
    
}

static void setup(char* host)
{
    if(rk == NULL) {
        char errstr[512];
        rd_kafka_conf_t *conf;
        
            /* Kafka configuration */
        conf = rd_kafka_conf_new();

        /* Set up a message delivery report callback.
         * It will be called once for each message, either on successful
         * delivery to broker, or upon failure to deliver to broker. */
        rd_kafka_conf_set_dr_cb(conf, msg_delivered);
        rd_kafka_conf_set_error_cb(conf, err_cb);
        rd_kafka_conf_set(conf, "queued.min.messages", "1000", NULL, 0);

        
        if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
                openlog("phpkafka", 0, LOG_USER);
                syslog(LOG_INFO, "phpkafka - failed to create new producer: %s", errstr);
                exit(1);
        }
        
        /* Add brokers */
        if (rd_kafka_brokers_add(rk, host) == 0) {
                openlog("phpkafka", 0, LOG_USER);
                syslog(LOG_INFO, "php kafka - No valid brokers specified");
                exit(1);
        }
    }
    
}

PHP_FUNCTION(kafka_produce)
{
    char* host_port;
    int host_port_len;
    char* topic;
    int topic_len;
    char* msg;
    int msg_len;
    rd_kafka_topic_conf_t *topic_conf;
        
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sss", 
            &host_port, &host_port_len,
            &topic, &topic_len,
            &msg, &msg_len        
            ) == FAILURE) {
        return;
    }
    
    signal(SIGINT, stop);
    signal(SIGPIPE, stop);
      
    rd_kafka_topic_t *rkt;
    int partition = 0;

    setup(host_port);
    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, rd_kafka_topic_conf_new());

    topic_conf = rd_kafka_topic_conf_new();
    rd_kafka_topic_conf_set(topic_conf, "message.timeout.ms", "500",
                                NULL, 0);
    /* Send/Produce message. */
    //fprintf(stdout, "%% Producing...\n");
    rd_kafka_produce(rkt, partition,
                     RD_KAFKA_MSG_F_COPY,
                     /* Payload and length */
                     msg, msg_len,
                     /* Optional key and its length */
                     NULL, 0,
                     /* Message opaque, provided in
                      * delivery report callback as
                      * msg_opaque. */
                     NULL);
    
    int r = rd_kafka_poll(rk, -1);
    
    fprintf(stdout, "%% pool: %i\n", r);
    RETURN_LONG(1);
}