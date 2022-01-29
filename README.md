#Redis Batch
This package builds on [Redis Streams](https://redis.io/topics/streams-intro) and provides an easy to use interface for collection and batch processing.
Designed for a highly available, scalable and distributed environment, it thus offers, in addition to the main functionality, monitoring and scale capabilities. 

The main idea is that Redis Streams supports several message producers. The messages  then organized into consumer groups where multiple consumers can collect a batch of items, process them and acknowledge the successfully processed ones.
If processing fails, the message has not been acknowledged will be part of the next batch. In case of consumer failure the monitor component will re-assign the related messages to a healthy consumer this way messages don't get lost.
Optional scaling component monitors incoming/processed message rate and suggests consumer scale if necessary

##Installation

Latest version:
```
pip3 install redis-batch
```

## Components
Overview of the components
![Redis Streams](https://tgrall.github.io/assets/images/redis-streams-101-img-1-2968c7ae8874c27aa176d161aa05a1d1.png "Redis Stream")
*Image source: [tgrall.github.io](https://tgrall.github.io/blog/2019/09/02/getting-with-redis-streams-and-java)*

### Provider
As its name suggests, this component is responsible for providing the messages in the stream. Redis supports multiple providers.
#### Example code
```python
redis_conn = Redis()
sample_data = {"message": "stuff goes here"}
redis_conn.xadd(name=STREAM, fields=sample_data)
```
### Consumer
The consumer registers in the consumer group and start fetching for available messages. Once a preconfigured batch size is reached, it gives back the list of items to the caller which then can acknowledge this way remove from the Stream the message.
The consumer implementation returns after the preconfigured maximum weight time, even if the lot is not full. This way the items won't wait long in the stream
#### Example code
```python
# It is crucial to enable "decode_response" feature of Redis
redis_conn = Redis(decode_responses=True)
consumer = Consumer(
        redis_conn=redis_conn,
        stream=STREAM,
        consumer_group=GROUP,
        batch_size=10,
        max_wait_time_ms=30000
    )
while True:
    messages = consumer.get_items()
    total_no_of_messages = len(messages)
    for i, item in enumerate(messages):
        print(f"Pocessing {i}/{total_no_of_messages} message:{item}")
        process_message(item=item)
        consumer.remove_item_from_stream(item_id=item.msgid)
```
### Monitor
Periodically check the activity of the consumers warns if they are idle  - not fetching message from the Stream for longer than the preconfigured inactivity threshold or have more assigned messages than the batch size. Automatic or on-demand cleanup are also supported.
#### Example code
```python
    monitor = Monitor(
        redis_conn=Redis(),
        stream=STREAM,
        consumer_group=GROUP,
        batch_size=10,   # batch size has to be tha same as for consumers 
    )
    monitor.collect_monitoring_data(auto_cleanup=True)
    monitor.print_monitoring_data()
```
Output
```
+-------------------------+-------------+-----------------+----------------------------------+
|             Consumer id |   Idle time |   Pending items | Status                           |
+=========================+=============+=================+==================================+
| b'29102140026848155456' |         923 |               7 | OK                               |
+-------------------------+-------------+-----------------+----------------------------------+
| b'29104139791624517440' |      294191 |               5 | WARNING - idle for long time     |
+-------------------------+-------------+-----------------+----------------------------------+
| b'29144140168467982144' |      361502 |               8 | WARNING - idle for long time     |
+-------------------------+-------------+-----------------+----------------------------------+
| b'29304140033034540864' |        8658 |              11 | WARNING - too many pending items |
+-------------------------+-------------+-----------------+----------------------------------+
| b'29312139940580673344' |       11734 |              58 | WARNING - too many pending items |
+-------------------------+-------------+-----------------+----------------------------------+
| b'29314139867734665024' |       14216 |               1 | OK                               |
+-------------------------+-------------+-----------------+----------------------------------+
```
### Scaler
By checking the number of messages waiting to be assigned and the number of pending items, utilization ratio can be calculated. Once this rate crosses a lower (scale in) or higher (scale out) the code will give a suggestion of scale in / out. 
#### Example code
```python
    scaler = Scaler(
        redis_conn=Redis(decode_responses=True),
        stream=STREAM,
        consumer_group=GROUP
    )
    scaler.collect_metrics()
    rate, suggestion = scaler.get_scale_decision(
        scale_out_rate=60, scale_in_rate=20
    )
    print(
        f"Consumers should be {suggestion} as stream length "
        f"({scaler.stream_lenght}) / pending ({scaler.stream_pending}) "
        f"rate is {rate}%"
    )
```
Output
```
Consumers should be IN as stream length (11) / pending (83) rate is 13.253%
Consumers should be NO_SCALE as stream length (18) / pending (79) rate is 22.7848%

```
## License
 This project is licensed under the terms of the GPL3.0 license.

### Runnel
[Runnel](https://runnel.dev/guide.html?highlight=batch#batching) is a great project with batch collection capability however threatenss the whole batch as one entity therefore may acknowledges all the items even they are not processed

