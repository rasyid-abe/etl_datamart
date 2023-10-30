from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
from helper import getKafkaBrokerConf

def get_message(topic='', consumer='', max_message=20000, timeout=10, id="id_transaction"):
    consumer = Consumer(getKafkaBrokerConf(consumer))
    # get topic
    consumer.subscribe([topic])

    # consume the messages
    messages = consumer.consume(num_messages=max_message, timeout=timeout)
    transaction_ids = []
    deleted_transaction_ids = []

    for message in messages:
        try:
            transaction = json.loads(message.value().decode("utf-8"))
        except:
            continue

        if transaction['after']:
            transaction_ids.append(str(transaction['after'][id]))
        else:
            deleted_transaction_ids.append(str(transaction['before'][id]))

    return transaction_ids, deleted_transaction_ids, consumer

def parseOffset(offset:list) -> list:
    """
        Parse list of confluent_kafka.TopicPartition into list of dict.
        The value of dict will be partition:offset 
        ref : https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.TopicPartition
    """
    temps = []
    for _ in offset:
        temps.append({_.partition : _.offset})
    return temps

def get_message_in_tuple(topic='', consumer='', max_message=20000, timeout=10, id="id_transaction", additional_key = [], cast_to_string = True) -> list:
    """
        Return tuple of (new trx, deleted trx, consumer)
        - new trx contains list of tuple which you defined keys parameter. the tuple will be in ordered manner as the parameter 
        - ^ this also applied to deleted_ids
    """
    consumer = Consumer(getKafkaBrokerConf(consumer))
    # get topic
    consumer.subscribe([topic])

    # consume the messages
    messages = consumer.consume(num_messages=max_message, timeout=timeout)
    transaction_ids = []
    deleted_transaction_ids = []

    for message in messages:
        try:
            transaction = json.loads(message.value().decode("utf-8"))
        except:
            continue
        items = []
        if transaction['after']:
            items.append(str(transaction['after'][id]) if cast_to_string else transaction['after'][id])
            for key in additional_key:
                items.append(str(transaction['after'][key]) if cast_to_string else transaction['after'][key])
            transaction_ids.append(tuple(items))
        else:
            items.append(str(transaction['before'][id]) if cast_to_string else transaction['before'][id])
            for key in additional_key:
                items.append(str(transaction['before'][key]) if cast_to_string else transaction['before'][key])
            deleted_transaction_ids.append(tuple(items))

    return transaction_ids, deleted_transaction_ids, consumer