from logging import Handler
from kafka import KafkaProducer
# import pykafka


class KafkaLoggingHandler(Handler):

    def __init__(self, hosts_list, topic, key=None):

        Handler.__init__(self)

        self.hosts_list = hosts_list
        self.topic = topic
        self.key = key

        self.producter = KafkaProducer(bootstrap_servers=self.hosts_list)

    def emit(self, record):
        try:
            msg = self.format(record)
            self.producter.send(topic=self.topic, value=record, key=self.key)
        except Exception:
            self.handleError(record)

    def close(self):
        Handler.close(self)
        self.producter.close()
