import os
import asyncio
import logging
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from dotenv import load_dotenv, find_dotenv
import tinytuya
import sqlitecloud
from datetime import datetime, UTC, timedelta
import numpy as np

load_dotenv(find_dotenv())

# Load env
DEVICE_ID = os.environ.get("DEVICE_ID") or "no_id"
DEVICE_IP = os.environ.get("DEVICE_IP") or "no_ip"
DEVICE_KEY = os.environ.get("DEVICE_KEY") or "no_key"
FREQUENCY = int(os.environ.get("FREQUENCY") or 10)
DATABASE_URL = os.environ.get("DATABASE_URL") or ""

#Setup connections
conn = sqlitecloud.connect(DATABASE_URL)
d = tinytuya.OutletDevice(DEVICE_ID, DEVICE_IP, DEVICE_KEY, version=3.3)

# Initialize fastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Actions to be taken on startup
    task = asyncio.create_task(poll_device())
    yield

    # Actions to be taken on shutdown
    task.cancel()
    conn.close()
    logger.info('Polling device ended')

app = FastAPI(lifespan=lifespan)
logger = logging.getLogger('uvicorn.error')


async def poll_device():
    logger.info('Polling device started')
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
        conn.execute(query)

        payload = d.generate_payload(tinytuya.UPDATEDPS)
        d.send(payload)


@app.get("/")
async def root():
    return {"message": "root"}

@app.get("/api/data")
def read_data(start_date: datetime | None = None, end_date: datetime | None = None):
    try:
        cursor = conn.cursor()
        query = "SELECT * FROM power "
        if start_date and end_date:
            query += f"WHERE poll_datetime >= '{start_date}' AND poll_datetime <= '{end_date}'"
        elif start_date:
            query += f"WHERE poll_datetime >= '{start_date}'"
        elif end_date:
            query += f"WHERE poll_datetime <= '{end_date}'"

        cursor.execute(query)
        rows = cursor.fetchall()

        result = []
        for row in rows:
            result.append({
                "poll_datetime": row[1],
                "voltage": row[2],
                "current": row[3],
                "power": row[4],
            })
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/power-usage")
def power_usage():
    try:
        cursor = conn.cursor()
        query = f"""
        SELECT
            DATE(poll_datetime) AS day,
            AVG(power) AS average_power
        FROM
            power
        GROUP BY
            day
        ORDER BY
            day;
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
          return []

        result = []
        for row in rows:
            day = row[0]
            average_power_watts = float(row[1])
            average_power_kw = average_power_watts / 1000
            total_energy_kwh = average_power_kw * 24
            result.append({
                "day": day,
                "energy_kwh": total_energy_kwh,
            })
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# To run the server, use: uvicorn your_script_name:app --reload
