"""Copy messages from Kafka topic to directory
"""

#import warnings
#import json
#import yaml
import argparse
import logging
#import sys
#from urllib.parse import urlparse
#import pymysql.cursors
from confluent_kafka import Consumer, Producer, KafkaError

n_written = 0

def consume(conf, log, alerts, consumer=None):
    "fetch a batch of alerts from kafka, return number of alerts consumed"

    log.debug('called consume with config: ' + str(conf))
    
    c = consumer

    n = 0
    n_error = 0
    try:
        while n < conf['batch_size']:
            # Poll for messages
            msg = c.poll(conf['timeout'])
            if msg is None:
                # stop when we get to the end of the topic
                log.info('reached end of topic')
                break
            elif not msg.error():
                log.debug("Got message with offset " + str(msg.offset()))
                alert = msg.value()
                alerts.append(alert)
                n += 1
            else:
                n_error += 1
                log.warning(str(msg.error()))
                try:
                    if msg.error().fatal():
                        break
                except:
                    pass
                if conf['max_errors'] < 0:
                    continue
                elif conf['max_errors'] < n_error:
                    log.error("maximum number of errors reached")
                    break
                else:
                    continue
        log.info("consumed {:d} alerts".format(n))
        if n > 0:
            n_produced = write(conf, log, alerts)
            if n_produced != n:
                raise Exception("Failed to write all alerts in batch: expected {}, got {}".format(n, n_produced))
            c.commit(asynchronous=False)
    except KafkaError as e:
        # TODO handle this properly
        log.warning(str(e))
    return n

def write(conf, log, alerts):
    "write a batch of alerts to the target directory, return number of alerts produced"

    log.debug('called write with config: ' + str(conf))

    # produce alerts
    n = 0
    try:
        while alerts:
            alert = alerts.pop(0)
            #p.produce(conf['output_topic'], value=alert)
            with open("{}/{}.{}".format(conf['output_dir'], n_written, conf['file_ext'])) as f:
                f.write(alert)
                f.close()
                n += 1
                n_written += 1
    finally:
        pass
    log.info("wrote {:d} alerts".format(n))
    return n

def run(conf, log):
    settings = {
        'bootstrap.servers': conf['broker'],
        'group.id': conf['group'],
        'session.timeout.ms': 30000,
        'max.poll.interval.ms': 300000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'enable.auto.commit': False
    }
    consumer = Consumer(settings, logger=log)
    consumer.subscribe([conf['input_topic']])

    batches = conf['max_batches']
    while batches != 0:
        batches -= 1
        alerts = []
        n = consume(conf, log, alerts, consumer)
        if n==0 and conf['stop_at_end']:
            consumer.close()
            break
         


if __name__ == '__main__':
    # parse cmd line arguments
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-b', '--broker', type=str, help='address:port of Kafka broker(s)')
    parser.add_argument('-g', '--group', type=str, default='sherlock-dev-1', help='group id to use for Kafka')
    parser.add_argument('-t', '--timeout', type=int, default=30, help='kafka consumer timeout in s') # 10s is probably a sensible minimum
    parser.add_argument('-e', '--stop_at_end', action='store_true', default=False, help='stop when no more messages to consume')
    parser.add_argument('-i', '--input_topic', type=str, help='name of input topic')
    parser.add_argument('-n', '--batch_size', type=int, default=1000, help='number of messages to process per batch')
    parser.add_argument('-l', '--max_batches', type=int, default=-1, help='max number of batches to process')
    parser.add_argument('-m', '--max_errors', type=int, default=-1, help='maximum number of non-fatal errors before aborting') # negative=no limit
    parser.add_argument('-q', '--quiet', action="store_true", default=None, help='minimal output')
    parser.add_argument('-v', '--verbose', action="store_true", default=None, help='verbose output')
    parser.add_argument('-d', '--output_dir', type=str, default='out', help='name of output directory')
    parser.add_argument('-x', '--file_ext', type=str, default='avro', help='file extension')
    parser.add_argument('--debug', action="store_true", default=None, help='debugging output')
    conf = vars(parser.parse_args())

    # set up a logger
    if conf['quiet']:
        logging.basicConfig(level=logging.ERROR)
    elif conf['verbose']:
        logging.basicConfig(level=logging.INFO)
    elif conf['debug']:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    log = logging.getLogger("sherlock_wrapper") 

    # print options on debug
    log.debug("config options:\n"+json.dumps(conf,indent=2))

    # check that required options are set
    if not conf.get('broker'):
        log.error("broker not set")
        sys.exit(2)
    if not conf.get('input_topic'):
        log.error("input topic not set")
        sys.exit(2)
    if not conf.get('output_topic'):
        log.error("output topic not set")
        sys.exit(2)

    run(conf, log)

