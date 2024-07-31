import os
import asyncio
import logging
from datetime import datetime, UTC, timedelta
import tinytuya
from database import Database
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DEVICE_ID = os.environ.get("DEVICE_ID") or "no_id"
DEVICE_IP = os.environ.get("DEVICE_IP") or "no_ip"
DEVICE_KEY = os.environ.get("DEVICE_KEY") or "no_key"
FREQUENCY = int(os.environ.get("FREQUENCY") or 60)

db = Database()
d = tinytuya.OutletDevice(DEVICE_ID, DEVICE_IP, DEVICE_KEY, version=3.3)
logger = logging.getLogger('uvicorn.error')

def get_device_data() -> dict[str, str|float] | None:
    try:
        # Signal to update status
        payload = d.generate_payload(tinytuya.UPDATEDPS)
        d.send(payload)
        # Get status
        data = d.status()
        current = data["dps"]["18"] / 1000
        power = data["dps"]["19"] / 10
        voltage = data["dps"]["20"] / 10
        poll_datetime = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
        return {
            "poll_datetime": poll_datetime,
            "current": current,
            "voltage": voltage,
            "power": power
        }
    except:
        logger.warning(f"Could not get response from device!")
        return None

async def transfer_data_to_hourly():
    while True:
        _datetime = datetime.now(UTC).strftime('%Y-%m-%d %H:00:00')
        data = db.get_average_power_usage_per_hour_before(_datetime)
        output = []
        for entry in data:
            output.append({
                "date": entry[0],
                "time": entry[1],
                "power": entry[2],
            })
        if output:
            logger.info('Transferring  polling data to hour average')
            db.insert_into_power_hourly_table(output)
            db.delete_power_usage_before(_datetime)
        await asyncio.sleep(3600)

async def poll_device():
    while True:
        logger.info('Polling device...')
        data = get_device_data()
        if data:
            logger.info(f"CUR:{data['current']}A VOL:{data['voltage']}V POW:{data['power']}W")
            db.insert_into_power_table(data)
        await asyncio.sleep(FREQUENCY)

def generate_full_day_hours(date_str):
    base_time = datetime.strptime(date_str, "%Y-%m-%d")
    return [(base_time + timedelta(hours=i)).strftime('%H:%M:%S') for i in range(24)]

async def generate_missing_data():
    while True:
        dates = db.get_distinct_dates()
        for date_tuple in dates[:-1]:
            values_to_insert = []
            date = date_tuple[0]

            # Fetch all time entries for the current date
            times = db.get_distinct_times_for_date(date)
            existing_times = set([row[0] for row in times])
            if len(existing_times) == 24:
                continue

            # Generate all possible hourly times for the date
            all_times = generate_full_day_hours(date)

            # Identify missing times
            missing_times = set(all_times) - existing_times
            missing_times = sorted(missing_times)
            for missing_time in missing_times:
                values_to_insert.append({
                    'date': date,
                    'time': missing_time,
                    'power': 0.0,
                })
            db.insert_into_power_hourly_table(values_to_insert)
        await asyncio.sleep(3600 * 24)
