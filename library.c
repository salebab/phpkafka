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
#include "library.h"
#include "librdkafka/rdkafka.h"

static int run = 1;
static rd_kafka_t *rk;

void kafka_stop(int sig) {
    run = 0;
    fclose(stdin); /* abort fgets() */
    rd_kafka_destroy(rk);
    rk = NULL;
}

void kafka_err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
    openlog("phpkafka", 0, LOG_USER);
    syslog(LOG_INFO, "phpkafka - ERROR CALLBACK: %s: %s: %s\n", 
            rd_kafka_name(rk), rd_kafka_err2str(err), reason);
    
    kafka_stop(err);
}

void kafka_msg_delivered (rd_kafka_t *rk,
                           void *payload, size_t len,
                           int error_code,
                           void *opaque, void *msg_opaque) {
    if (error_code) {
        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - Message delivery failed: %s",
                rd_kafka_err2str(error_code));
    }
}

void kafka_setup(char* brokers)
{
    if(rk == NULL) {
        char errstr[512];
        rd_kafka_conf_t *conf;
        
            /* Kafka configuration */
        conf = rd_kafka_conf_new();

        /* Set up a message delivery report callback.
         * It will be called once for each message, either on successful
         * delivery to broker, or upon failure to deliver to broker. */
        rd_kafka_conf_set_dr_cb(conf, kafka_msg_delivered);
        rd_kafka_conf_set_error_cb(conf, kafka_err_cb);
        rd_kafka_conf_set(conf, "queued.min.messages", "1000", NULL, 0);

        
        if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
                openlog("phpkafka", 0, LOG_USER);
                syslog(LOG_INFO, "phpkafka - failed to create new producer: %s", errstr);
                exit(1);
        }
        
        /* Add brokers */
        if (rd_kafka_brokers_add(rk, brokers) == 0) {
                openlog("phpkafka", 0, LOG_USER);
                syslog(LOG_INFO, "php kafka - No valid brokers specified");
                exit(1);
        }
    }
}

void kafka_destroy()
{
    if(rk != NULL) {
        rd_kafka_wait_destroyed(1000);
        rd_kafka_destroy(rk);
        rk = NULL;
    }
}

void kafka_produce(char* topic, char* msg, int msg_len)
{
    signal(SIGINT, kafka_stop);
    signal(SIGPIPE, kafka_stop);
    
    rd_kafka_topic_conf_t *topic_conf;
    rd_kafka_topic_t *rkt;
    int partition = 0;
    
    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, rd_kafka_topic_conf_new());

    topic_conf = rd_kafka_topic_conf_new();
    
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
    
    rd_kafka_poll(rk, 0);
}
