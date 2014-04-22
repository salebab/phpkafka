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

        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - using: %s", brokers);


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
        rd_kafka_destroy(rk);
        rd_kafka_wait_destroyed(1000);
        rk = NULL;
    }
}

void kafka_produce(char* topic, char* msg, int msg_len)
{

    signal(SIGINT, kafka_stop);
    signal(SIGPIPE, kafka_stop);

    rd_kafka_topic_t *rkt;
    int partition = RD_KAFKA_PARTITION_UA;

    rd_kafka_topic_conf_t *topic_conf;

    /* Topic configuration */
    topic_conf = rd_kafka_topic_conf_new();

    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, topic_conf);

    if (rd_kafka_produce(rkt, partition,
                     RD_KAFKA_MSG_F_COPY,
                     /* Payload and length */
                     msg, msg_len,
                     /* Optional key and its length */
                     NULL, 0,
                     /* Message opaque, provided in
                      * delivery report callback as
                      * msg_opaque. */
                     NULL) == -1) {
      openlog("phpkafka", 0, LOG_USER);
      syslog(LOG_INFO, "phpkafka - %% Failed to produce to topic %s "
          "partition %i: %s",
          rd_kafka_topic_name(rkt), partition,
          rd_kafka_err2str(
            rd_kafka_errno2err(errno)));
      rd_kafka_poll(rk, 0);
    }

    /* Poll to handle delivery reports */
    rd_kafka_poll(rk, 0);

    /* Wait for messages to be delivered */
    while (run && rd_kafka_outq_len(rk) > 0)
      rd_kafka_poll(rk, 100);

    rd_kafka_topic_destroy(rkt);
}
