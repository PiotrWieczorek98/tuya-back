from datetime import datetime
import os
import sqlitecloud
import logging
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DATABASE_URL = os.environ.get("DATABASE_URL") or ""
TABLE_POWER = "power"
TABLE_POWER_HOURLY = "power_hourly"
TABLE_USER = "users"
TABLE_PRICES = "prices"

class SingletonMeta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class Database(metaclass=SingletonMeta):
    def __init__(self):
        self.logger = logging.getLogger('uvicorn.error')
        self._connection = sqlitecloud.connect(DATABASE_URL)

    def _attempt_execution(self, query: str, params = None):
        cursor = self._connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor


    def execute(self, query: str, params=None):
        try:
            return self._attempt_execution(query, params)
        except:
            try:
                self._connection = sqlitecloud.connect(DATABASE_URL)
                return self._attempt_execution(query, params)
            except Exception as e:
                self.logger.error(f"Query execution failed after retry: {e}")
                raise


    def get_user(self, username: str):
        query = f"SELECT name, password FROM {TABLE_USER} WHERE name = ?"
        user = self.execute(query, (username,)).fetchone()
        if user:
            return {
                "username": user[0],
                "password": user[1]
            }
        return None

    def insert_into_power_table(self, data: dict):
        query = f"""
        INSERT INTO {TABLE_POWER} (poll_datetime, voltage, current, power)
        VALUES ('{data['poll_datetime']}', {data['voltage']}, {data['current']}, {data['power']})
        """
        self.execute(query)

    def insert_into_power_hourly_table(self, data: list):
        query = f"INSERT INTO {TABLE_POWER_HOURLY} (date, time, power) VALUES "
        values_list = []
        for entry in data:
            values = f"('{entry['date']}', '{entry['time']}', {entry['power']})"
            values_list.append(values)

        query += ", ".join(values_list)
        self.execute(query)

    # POWER
    def get_power_data(self, start_date: datetime | None = None, end_date: datetime | None = None):
        query = f"SELECT * FROM {TABLE_POWER} "
        if start_date and end_date:
            query += f"WHERE poll_datetime >= '{start_date}' AND poll_datetime <= '{end_date}'"
        elif start_date:
            query += f"WHERE poll_datetime >= '{start_date}'"
        elif end_date:
            query += f"WHERE poll_datetime <= '{end_date}'"

        return self.execute(query).fetchall()

    def get_power_usage_daily(self):
        query = f"""
        SELECT
            DATE(date) AS day,
            AVG(power) AS average_power
        FROM
            {TABLE_POWER_HOURLY}
        GROUP BY
            day
        ORDER BY
            day;
        """
        return self.execute(query).fetchall()

    def get_average_power_usage_per_hour_before(self, datetime: str):
        query = f"""
        SELECT
            DATE(poll_datetime) AS date,
            strftime('%H:00:00', poll_datetime) AS time,
            AVG(power) AS power
        FROM
            {TABLE_POWER}
        WHERE poll_datetime < '{datetime}'
        GROUP BY
            date, time
        ORDER BY
            date, time
        """
        return self.execute(query).fetchall()

    def get_power_usage_hourly(self):
        query = f"""
        SELECT
            date,
            time,
            power
        FROM {TABLE_POWER_HOURLY}
        ORDER BY
            date, time
        """
        return self.execute(query).fetchall()

    def get_power_usage_monthly(self):
        query = f"""
        SELECT
            strftime('%Y-%m', date) AS month,
            AVG(power) AS average_power
        FROM
            {TABLE_POWER_HOURLY}
        GROUP BY
            month
        ORDER BY
            month;
        """

        return self.execute(query).fetchall()

    def get_power_usage_yearly(self):
        query = f"""
        SELECT
            strftime('%Y', date) AS year,
            AVG(power) AS average_power
        FROM
            {TABLE_POWER_HOURLY}
        GROUP BY
            year
        ORDER BY
            year;
        """

        return self.execute(query).fetchall()

    def delete_power_usage_before(self, datetime: str):
        query = f"""
        DELETE FROM
            {TABLE_POWER}
        WHERE poll_datetime < '{datetime}'
        """
        self.execute(query)

    #PRICES

    def get_energy_price_for_day(self, date: datetime):
        month = date.strftime('%Y-%m-') + '01'
        query = f"SELECT price FROM {TABLE_PRICES} WHERE datetime = '{month}';"
        return self.execute(query).fetchone()

    def get_energy_price_monthly(self):
        query = f"SELECT datetime, price FROM {TABLE_PRICES};"
        return self.execute(query).fetchall()

    # MISC

    def get_distinct_dates(self):
        query = f"SELECT DISTINCT date FROM {TABLE_POWER_HOURLY}"
        return self.execute(query).fetchall()

    def get_distinct_times_for_date(self, date: str):
        query = f"SELECT time FROM {TABLE_POWER_HOURLY} WHERE date = '{date}'"
        return self.execute(query).fetchall()