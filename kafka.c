#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php_kafka.h"
#include "librdkafka/rdkafka.h"  /* for Kafka driver */


static PHP_FUNCTION(kafka_produce);
static rd_kafka_t *rk;

ZEND_BEGIN_ARG_INFO(arginfo_kafka_produce, 0)
    ZEND_ARG_INFO(0, string)
    ZEND_ARG_INFO(0, string)
    ZEND_ARG_INFO(0, string)
ZEND_END_ARG_INFO();


zend_function_entry kafka_functions[] =
{
    ZEND_FE(kafka_produce, arginfo_kafka_produce)
    {NULL,NULL,NULL} /* Marks the end of function entries */
};


zend_module_entry kafka_module_entry = {
    STANDARD_MODULE_HEADER,
    "kafka",
    kafka_functions, /* Function entries */
    PHP_MINIT(kafka), /* Module init */
    PHP_MSHUTDOWN(kafka), /* Module shutdown */
    NULL, /* Request init */
    NULL, /* Request shutdown */
    NULL, /* Module information */
    "0.1", /* Replace with version number for your extension */
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_KAFKA
ZEND_GET_MODULE(kafka)
#endif

PHP_MSHUTDOWN_FUNCTION(kafka)
{
    if(rk != NULL) {
        rd_kafka_wait_destroyed(100);
        rd_kafka_destroy(rk);
    }
    fprintf(stderr, "%% Module shutdown");
    return SUCCESS;
}
PHP_MINIT_FUNCTION(kafka)
{
    
    
    return SUCCESS;
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
        //rd_kafka_conf_set_dr_cb(conf, msg_delivered);
        
        
        if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                                errstr, sizeof(errstr)))) {
                fprintf(stderr,
                        "%% Failed to create new producer: %s\n",
                        errstr);
                exit(1);
        }
        
        /* Add brokers */
        if (rd_kafka_brokers_add(rk, host) == 0) {
                fprintf(stderr, "%% No valid brokers specified\n");
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
    
    int result = -1;
    
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sss", 
            &host_port, &host_port_len,
            &topic, &topic_len,
            &msg, &msg_len        
            ) == FAILURE) {
        return;
    }

    rd_kafka_topic_t *rkt;
    int partition = 0;

    setup(host_port);
    
    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, rd_kafka_topic_conf_new());

    /* Send/Produce message. */
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
    
    RETURN_LONG(result);
}