import os
import asyncio
import logging
from datetime import datetime, UTC
import tinytuya
from database import get_db_connection
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DEVICE_ID = os.environ.get("DEVICE_ID") or "no_id"
DEVICE_IP = os.environ.get("DEVICE_IP") or "no_ip"
DEVICE_KEY = os.environ.get("DEVICE_KEY") or "no_key"
FREQUENCY = int(os.environ.get("FREQUENCY") or 10)

d = tinytuya.OutletDevice(DEVICE_ID, DEVICE_IP, DEVICE_KEY, version=3.3)
logger = logging.getLogger('uvicorn.error')

async def poll_device():
    logger.info('Polling device started')
    db_conn = get_db_connection()
    while True:
        await asyncio.sleep(FREQUENCY)
        data = d.status()
        current = data["dps"]["18"] / 1000
        power = data["dps"]["19"] / 10
        voltage = data["dps"]["20"] / 10
        poll_datetime = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"{poll_datetime} | CUR:{current}A VOL:{voltage}V POW:{power}W")

        query = f"""
        INSERT INTO power (poll_datetime, voltage, current, power)
        VALUES ('{poll_datetime}', {voltage}, {current}, {power})
        """
        db_conn.execute(query)

        payload = d.generate_payload(tinytuya.UPDATEDPS)
        d.send(payload)
