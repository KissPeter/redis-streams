from redis import Redis

from redis_batch.consumer import Consumer

STREAM = 'test_stream'
GROUP = 'test_group'


def test_end_to_end():

    redis_conn = Redis()
    redis_conn.xdel(STREAM, *['-', '+'])
    test_dataset = [{"test": "data1"}, {"test": "data2"}]
    for test_data in test_dataset:
        redis_conn.xadd(name=STREAM, fields=test_data)
    redis_consumer = Consumer(redis_conn=redis_conn, stream=STREAM,
                              consumer_group=GROUP)
    messages = redis_consumer.get_new_messages_from_group(
        requested_messages=len(test_dataset))
    assert redis_consumer.get_no_of_messages_already_assigned() == len(test_dataset)
    assert len(messages) == len(test_dataset)
    for message in messages:
        assert message.content in test_dataset
        redis_consumer.remove_item_from_stream(item_id=message.msgid)
