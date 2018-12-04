from logging import Handler
from kafka import KafkaProducer


class KafkaLoggingHandler(Handler):

    def __init__(self, hosts_list, topic, key=None, partition=None, **kargs):
        """
        Kafka logging handler init function
        :param hosts_list: ‘host[:port]’ string (or list of ‘host[:port]’ strings)
        :param topic: kafka topic
        :param key: the key for kafka productor send msg
        :param partition: partition for kafka productor send msg
        :param kargs: Keyword Arguments from KafkaProducer, except bootstrap_servers
        """
        Handler.__init__(self)

        self.hosts_list = hosts_list
        self.topic = topic
        self.key = key
        self.partition = partition
        self.producter = KafkaProducer(bootstrap_servers=self.hosts_list, **kargs)

    def emit(self, record):
        try:
            msg = self.format(record)
            self.producter.send(topic=self.topic, value=bytes(msg), partition=self.partition, key=self.key)
        except Exception as ex:
            print(ex)
            self.handleError(record)

    def close(self, timeout=None):
        Handler.close(self)
        self.producter.close(timeout)

    def flush(self, timeout=None):
        self.producter.flush(timeout)
