from google.cloud import pubsub

PROJECT_ID='my-project-1470763762469'
TOPIC = 'new-transactions'
SUBSCRIPTION='transactions-inference'

client = pubsub.PublisherClient()
topic = client.get_topic(publisher.topic_path(PROJECT_ID, PARAMS.pubsub_topic))

subscription = topic.subscription(name=SUBSCRIPTION)
if not subscription.exists():
    print('Creating pub/sub subscription {}...'.format(SUBSCRIPTION))
    subscription.create(client=client)

print ('Pub/sub subscription {} is up and running'.format(SUBSCRIPTION))
print("")


subscription = topic.subscription(SUBSCRIPTION)
message = subscription.pull(return_immediately=True)

print("source_id", message[0][1].attributes["source_id"])
print("source_timestamp:", message[0][1].attributes["source_timestamp"])
print("")
print(message[0][1].data)

