from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

import os
import logging

log = logging.getLogger(__name__)

class Producer:
    def __init__(self, schema_cls, topic, flush_interval=10000):
        self.topic = topic 
        self.flush_interval = flush_interval

        schema_registry_conf = {'url': os.environ.get('SCHEMA_REGISTRY_URL')}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        protobuf_serializer = ProtobufSerializer(
                schema_cls,
                schema_registry_client,
                {'use.deprecated.format': True})

        producer_conf = {
            'bootstrap.servers': os.environ.get('BOOTSTRAP_SERVER'),
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': protobuf_serializer,
        }

        self.producer = SerializingProducer(producer_conf)
        self.product = 0

    def produce(self, key, value):
        self.producer.produce(
            topic=self.topic,
            partition=-1,
            key=key,
            value=value,
            on_delivery=self.delivery_report
        )
        self.product += 1
        if self.product % self.flush_interval == 0:
            self.poll()

    def poll(self):
        self.producer.poll()
        self.producer.flush()

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            log.error('Delivery failed for User record {}: {}'.format(msg.key(), err))
        else:
            log.info(
                'User record {} successfully produced to {} [{}] at offset {}'.format(
                    msg.key(), msg.topic(), msg.partition(), msg.offset()
                )
            )
