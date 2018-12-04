# KafkaLoggingHandler

将日志输出来Kafka的 Python 的 logging handler

## 使用

```python
import time
import logging
from kafka_logging_handler import KafkaLoggingHandler


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 第一个参数是kafka的主机列表, 第二个参数是topic
kafka_handler = KafkaLoggingHandler(["10.110.147.45:9092"], "test")
kafka_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
kafka_handler.setFormatter(formatter)
logger.addHandler(kafka_handler)

logger.debug('this is a logger debug message')
logger.info('this is a logger info message')
logger.warning('this is a logger warning message')
logger.error('this is a logger error message')
logger.critical('this is a logger critical message')
```