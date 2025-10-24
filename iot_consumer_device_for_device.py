import asyncio
import json
from kafka import KafkaConsumer
from json import loads
import logger
import deviceManager
import genericMessageProducer
import configUtils
import kafkaUtils

log = logger.setup_logger("iot_consumer_device_for_device")

ACTION_TOPIC_NAME = 'action'
DEVICE_STATUS_TOPIC_NAME = "device-status"

async def consume_message(message):        
    if ( not configUtils.isConfigReady()):
        log.error("missing configuration - forcing configuration load - message will not be processed")
        kafkaUtils.generateConfigFile()
        return

    alias = message["alias"]
    operation = message["operation"].lower()
    log.info(f"Device: {alias}, operation: {operation}")
    try:
        await deviceManager.turn_the_device_on_or_off(alias=alias, operation= operation)        
        device_status = {
            "device": alias,
            "status": operation
        }
        genericMessageProducer.publish_message(DEVICE_STATUS_TOPIC_NAME,device_status)
    except Exception as e:
        log.error(e)
        genericMessageProducer.publish_message("errors",e)


async def main():
    log.info("Comsumer Action started")    
    consumer = KafkaConsumer(
        ACTION_TOPIC_NAME,        
        bootstrap_servers=['rp-queue2:29092'],
        # auto_offset_reset='earliest',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        # group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    for message in consumer:
        message = message.value
        log.info(f"message: {message}")
        await consume_message(message=message)

asyncio.run(main())