from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
from auth import get_current_user
from database import Database
from polling import get_device_data
import calendar

router = APIRouter()
database = Database()

def get_router():
    return router

@router.get("/")
async def root():
    return {"message": "root"}

@router.get("/api/data", dependencies=[Depends(get_current_user)])
def read_data(start_date: datetime | None = None, end_date: datetime | None = None):
    try:
        rows_power_usage = database.get_power_data(start_date, end_date)
        result = [
            {
                "poll_datetime": row[1],
                "voltage": row[2],
                "current": row[3],
                "power": row[4],
            }
            for row in rows_power_usage
        ]
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/power-usage/hourly", dependencies=[Depends(get_current_user)])
def power_usage_hourly():
    try:
        rows_power_usage = database.get_power_usage_hourly()
        rows_prices = database.get_energy_price_monthly()
        if not rows_power_usage or not rows_prices:
            return []

        prices = dict(rows_prices)
        result = []

        for row in rows_power_usage:
            date_org = row[0]
            time = row[1]
            power_w = row[2]
            cost = prices[row[0][:7] + '-01'] * row[2] / 1000

            result.append({
                "date": date_org,
                "hour": time[:2],
                "energy_w": round(power_w, 2),
                "cost": round(cost, 2),
            })

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/power-usage/daily", dependencies=[Depends(get_current_user)])
def power_usage_by_daily():
    try:
        rows_power_usage = database.get_power_usage_daily()
        rows_prices = database.get_energy_price_monthly()
        if not rows_power_usage or not rows_prices:
            return []

        prices = dict(rows_prices)
        result = []

        for row in rows_power_usage:
            date = row[0]
            average_power_w = float(row[1])
            total_energy_kwh = average_power_w / 1000 * 24
            cost = prices[row[0][:7] + '-01'] * total_energy_kwh

            result.append({
                "date": date,
                "energy_kwh": round(total_energy_kwh, 2),
                "cost": round(cost, 2),
            })

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/power-usage/monthly", dependencies=[Depends(get_current_user)])
def power_usage_monthly():
    try:
        rows_power_usage = database.get_power_usage_monthly()
        rows_prices = database.get_energy_price_monthly()
        if not rows_power_usage or not rows_prices:
            return []

        prices = dict(rows_prices)
        result = []

        for row in rows_power_usage:
            year_month = row[0]
            date_str = year_month + '-01'
            date = datetime.strptime(date_str, '%Y-%m-%d')
            average_power_w = float(row[1])
            month_energy_kwh = average_power_w / 1000 * 24 * calendar.monthrange(date.year, date.month)[1]
            cost = prices[date_str] * month_energy_kwh

            result.append({
                "date": date_str,
                "energy_kwh": round(month_energy_kwh, 2),
                "cost": round(cost, 2),
            })

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/power-usage/yearly", dependencies=[Depends(get_current_user)])
def power_usage_yearly():
    try:
        rows_power_usage = database.get_power_usage_monthly()
        rows_prices = database.get_energy_price_monthly()
        if not rows_power_usage or not rows_prices:
            return []

        prices = dict(rows_prices)
        result = []
        year_energy_kwh = 0
        year_cost = 0

        for idx, row in enumerate(rows_power_usage):
            year_month = row[0]
            year = row[0][:4]
            date_str = year_month + '-01'
            date = datetime.strptime(date_str, '%Y-%m-%d')
            average_power_watts = float(row[1])
            average_power_kw = average_power_watts / 1000
            month_energy_kwh = average_power_kw * 24 * calendar.monthrange(date.year, date.month)[1]
            month_cost = prices[date_str] * month_energy_kwh
            year_energy_kwh += month_energy_kwh
            year_cost += month_cost

            if (idx + 1) >= len(rows_power_usage) or rows_power_usage[idx + 1][0][:4] != year:
                result.append({
                    "date": str(date.year) + '-01-01',
                    "energy_kwh": round(year_energy_kwh, 2),
                    "cost": round(year_cost, 2),
                })
                year_energy_kwh = 0
                year_cost = 0

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/power-usage/now", dependencies=[Depends(get_current_user)])
def power_usage_now():
    try:
        return get_device_data()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/energy-price/monthly")
def energy_prices_monthly():
    try:
        rows_prices = database.get_energy_price_monthly()
        if not rows_prices:
            return []

        result = [
            {
                "date": row[0],
                "price": float(row[1]),
            }
            for row in rows_prices
        ]
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/energy-price/day")
def energy_price_for_day(date: datetime):
    try:
        price = database.get_energy_price_for_day(date)
        if not price:
            return []

        result = {
            "date": date.strftime("%Y-%m-%d"),
            "price": float(price[0]),
        }
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))