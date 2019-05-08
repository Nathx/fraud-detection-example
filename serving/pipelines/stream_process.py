import apache_beam as beam
import logging
from apache_beam.transforms.window import FixedWindows
import argparse

from model import inference
from datetime import datetime
import json

from google.cloud import pubsub
from google.cloud import bigquery


BQ_SCHEMA_DEF = {
  "Time": 'INTEGER',
  "Amount": 'FLOAT',
  "key": 'INTEGER'
}
BQ_SCHEMA_DEF.update(dict(('V{}'.format(i + 1), 'FLOAT') for i in range(28)))


def prepare_steaming_source(project_id, pubsub_topic, pubsub_subscription):

    # create pubsub topic

    publisher = pubsub.PublisherClient()
    project_path = publisher.project_path(project_id)
    topic_path = publisher.topic_path(project_id, pubsub_topic)

    if topic_path in [topic.name for topic in publisher.list_topics(project_path)]:
        print('Deleting pub/sub topic {}...'.format(pubsub_topic))
        publisher.delete_topic(topic_path)

    print('Creating pub/sub topic {}...'.format(pubsub_topic))
    topic = publisher.create_topic(topic_path)
    print('Pub/sub topic {} is up and running'.format(pubsub_topic))

    subscriber = pubsub.SubscriberClient()
    project_path = subscriber.project_path(project_id)
    subscription_path = subscriber.subscription_path(
        project_id, pubsub_subscription)
    if subscription_path in [sub.name for sub in subscriber.list_subscriptions(project_path)]:
        print('Deleting pub/sub subscription {}...'.format(pubsub_subscription))
        subscriber.delete_subscription(subscription_path)

    print('Creating pub/sub subscription {}...'.format(pubsub_subscription))
    subscriber.create_subscription(subscription_path, topic_path)
    print('Pub/sub topic {} is up and running'.format(pubsub_subscription))

    print("")


def prepare_steaming_sink(project_id, bq_dataset, bq_table):

    # create BigQuery table
    schema = [bigquery.SchemaField(field_name, data_type)
              for field_name, data_type in BQ_SCHEMA_DEF.items()]

    bq_client = bigquery.Client(project=project_id)
    dataset_ref = bq_client.dataset(bq_dataset)
    try: 
        dataset = bq_client.get_dataset(dataset_ref)
    except:
        print('Creating BQ Dataset {}...'.format(bq_dataset))
        dataset = bigquery.Dataset(bq_dataset)
        dataset = bq_client.create_dataset(dataset)
    
    table_ref = dataset.table(bq_table)
    try: 
        table = bq_client.get_table(table_ref)
    except:
        print('Creating BQ Table {}...'.format(bq_table))
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table)

    print('BQ Table {} is up and running'.format(bq_table))
    print("")


def estimate(messages, inference_type):

    if not isinstance(messages, list):
        messages = [messages]

    instances = list(
        map(lambda message: json.loads(message),
            messages)
    )

    source_ids = list(
        map(lambda instance: instance.pop('source_id'),
            instances)

    )

    source_timestamps = list(
        map(lambda instance: instance.pop('source_timestamp'),
            instances)
    )

    if inference_type == 'local':
        estimated_weights = inference.estimate_local(instances)
    elif inference_type == 'cmle':
        estimated_weights = inference.estimate_cmle(instances)
    else:
        estimated_weights = 'NA'

    for i in range(len(instances)):
        instance = instances[i]
        instance['estimated_weight'] = estimated_weights[i]
        instance['source_id'] = source_ids[i]
        instance['source_timestamp'] = source_timestamps[i]
        instance['predict_timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    logging.info(instances)

    return instances


def run_pipeline(inference_type, project, pubsub_topic, pubsub_subscription, bq_dataset, bq_table, runner, args=None):

    prepare_steaming_source(project, pubsub_topic, pubsub_subscription)

    prepare_steaming_sink(project, bq_dataset, bq_table)

    pubsub_subscription_url = "projects/{}/subscriptions/{}".format(project, pubsub_subscription)

    options = beam.pipeline.PipelineOptions(flags=[], **args)

    pipeline = beam.Pipeline(runner, options=options)

    (

            pipeline
            | 'Read from PubSub' >> beam.io.ReadStringsFromPubSub(subscription=pubsub_subscription_url, id_label="source_id")
            | 'Estimate Targets - {}'.format(inference_type) >> beam.FlatMap(lambda message: estimate(message, inference_type))
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(project=project,
                                                             dataset=bq_dataset,
                                                             table=bq_table)
    )

    pipeline.run()


#[START micro-batching]
def run_pipeline_with_micro_batches(inference_type, project,
                                    pubsub_topic, pubsub_subscription,
                                    bq_dataset, bq_table,
                                    window_size, runner, args=None):

    prepare_steaming_source(project, pubsub_topic, pubsub_subscription)
    prepare_steaming_sink(project, bq_dataset, bq_table)
    pubsub_subscription_url = "projects/{}/subscriptions/{}".format(project, pubsub_subscription)
    options = beam.pipeline.PipelineOptions(flags=[], **args)

    pipeline = beam.Pipeline(runner, options=options)
    (
            pipeline
            | 'Read from PubSub' >> beam.io.ReadStringsFromPubSub(subscription=pubsub_subscription_url, id_label="source_id")
            | 'Micro-batch - Window Size: {} Seconds'.format(window_size) >> beam.WindowInto(FixedWindows(size=window_size))
            | 'Estimate Targets - {}'.format(inference_type) >> beam.FlatMap(lambda messages: estimate(messages, inference_type))
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(project=project,
                                                             dataset=bq_dataset,
                                                             table=bq_table
                                                             )
    )

    pipeline.run()
#[END micro-batching]

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
        '--pubsub-subscription',
        help="""
        Cloud Pub/Sub subscription to receive the messages to\
        """,
        default='',
        type=int
    )
    PARAMS = args_parser.parse_args()

    prepare_steaming_source(PARAMS.project_id, PARAMS.pubsub_topic, PARAMS.pubsub_subscription)

