from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
from database import get_db_connection
from auth import get_current_user

router = APIRouter()

@router.get("/")
async def root():
    return {"message": "root"}

@router.get("/api/data", dependencies=[Depends(get_current_user)])
def read_data(start_date: datetime | None = None, end_date: datetime | None = None):
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor()
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

@router.get("/api/power-usage", dependencies=[Depends(get_current_user)])
def power_usage():
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor()
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

def get_router():
  return router