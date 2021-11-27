from redis import Redis
from random import randint
from redis_batch.consumer import Consumer

STREAM = f'test_stream'
GROUP = 'test_group'


def test_end_to_end():

    redis_conn = Redis()
    if redis_conn.xlen(name=STREAM):
        redis_conn.xtrim(STREAM, maxlen=0)
    test_dataset = [{"test": "data1"}, {"test": "data2"}]
    for test_data in test_dataset:
        print(f'Add {test_data}')
        redis_conn.xadd(name=STREAM, fields=test_data)
    redis_consumer = Consumer(redis_conn=redis_conn, stream=STREAM,
                              consumer_group=GROUP, batch_size=len(test_dataset))
    messages = redis_consumer.get_items()
    assert redis_consumer._get_no_of_messages_already_assigned() == len(test_dataset)
    assert len(messages) != len(test_dataset)
    for message in messages:
        assert message.content in test_dataset
        redis_consumer.remove_item_from_stream(item_id=message.msgid)
test_end_to_end()
