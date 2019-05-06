from google.cloud import pubsub
from datetime import datetime

import json
import os
import time
import argparse

MESSAGE_TIME = 0.011

PARAMS = None

instance = {
    "V23": -0.471709469790029,
    "V22": -0.581270019133044,
    "V21": 3.7536560833085604,
    "V20": -1.7910765472128303,
    "V27": 0.699600694529817,
    "V26": 0.795361353517608,
    "V25": -0.459130000870193,
    "V24": 0.0153454352970634,
    "V28": 0.339586330147973,
    "V1": 0.10741590340147,
    "V2": 0.8113232283170401,
    "V3": -2.7792543222248702,
    "V4": -1.17726972753825,
    "V5": -0.46540477371290995,
    "V6": -0.496894626457315,
    "V7": 0.380197905618243,
    "V8": -4.88379215131891,
    "V9": -1.10742940467555,
    "Amount": 274.8,
    "key": 8821738702745408944,
    "Time": 126481.0,
    "V18": 0.49129158267520895,
    "V19": -1.3922975162365498,
    "V12": 0.279855357730141,
    "V13": 0.377550214586591,
    "V10": 0.924402052309218,
    "V11": -1.16198047670576,
    "V16": -2.09567581478598,
    "V17": 0.21285085432553197,
    "V14": 0.794771759648585,
    "V15": -1.01132120214707
}

def generate_instance():


def send_message(publisher, topic, index):

    source_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    source_id = str(abs(hash(str(index)+str(instance)+str(source_timestamp)+str(os.getpid()))) % (10 ** 10))
    message = json.dumps(instance)
    publisher.publish(topic=topic, data=message, source_id=source_id, source_timestamp=source_timestamp)
    return message


def simulate_stream_data():

    print("Data points to send: {}".format(PARAMS.stream_sample_size))
    print("PubSub topic: {}".format(PARAMS.pubsub_topic))
    print("Messages per second: {}".format(PARAMS.frequency))
    print("Sleep time between each data point: {} seconds".format(sleep_time_per_msg))

    time_start = datetime.utcnow()
    print(".......................................")
    print("Simulation started at {}".format(time_start.strftime('%Y-%m-%d %H:%M:%S')))
    print(".......................................")

    #[START simulate_stream]
    publisher = pubsub.PublisherClient()
    event_type = publisher.topic_path(PARAMS.project_id, PARAMS.pubsub_topic)
    try:
      publisher.get_topic(event_type)
      print('Reusing pub/sub topic {}'.format(PARAMS.pubsub_topic))
    except:
      publisher.create_topic(event_type)
      print('Creating pub/sub topic {}'.format(PARAMS.pubsub_topic))

    for index in range(PARAMS.stream_sample_size):

        message = send_message(publisher, event_type, index)

        # for debugging
        if PARAMS.show_message:
            print "Message {} was sent: {}".format(index+1, message)
            print ""

        time.sleep(sleep_time_per_msg)
    #[END simulate_stream]

    time_end = datetime.utcnow()

    print(".......................................")
    print("Simulation finished at {}".format(time_end.strftime('%Y-%m-%d %H:%M:%S')))
    print(".......................................")
    time_elapsed = time_end - time_start
    time_elapsed_seconds = time_elapsed.total_seconds()
    print("Simulation elapsed time: {} seconds".format(time_elapsed_seconds))
    print("{} data points were sent to: {}.".format(PARAMS.stream_sample_size, event_type))
    print("Average frequency: {} per second".format(round(PARAMS.stream_sample_size/time_elapsed_seconds, 2)))


if __name__ == '__main__':

    args_parser = argparse.ArgumentParser()

    args_parser.add_argument(
        '--project_id',
        help="""
        Google Cloud project id\
        """,
        default='ksalama-gcp-playground',
    )

    args_parser.add_argument(
        '--pubsub-topic',
        help="""
        Cloud Pub/Sub topic to send the messages to\
        """,
        default='babyweights',
    )

    args_parser.add_argument(
        '--frequency',
        help="""
        Number of messages per seconds to be sent to the topic in streaming pipelines\
        """,
        default=50,
        type=int
    )

    args_parser.add_argument(
        '--stream-sample-size',
        help="""
        Total number of messages be sent to the topic in streaming pipelines\
        """,
        default=10,
        type=int
    )

    args_parser.add_argument(
        '--show-message',
        action='store_true',
        default=False
    )

    PARAMS = args_parser.parse_args()

    print ''
    print 'Simulation Parameters:'
    print PARAMS
    print ''

    total_msg_time = MESSAGE_TIME * PARAMS.frequency  # time to send the required messages per second
    total_sleep_time = 1 - total_msg_time  # total sleep time per second (in order not to send more)
    sleep_time_per_msg = total_sleep_time / PARAMS.frequency

    simulate_stream_data()

