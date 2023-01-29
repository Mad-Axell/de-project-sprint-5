from typing import List, Optional

from datetime import date, datetime, time
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class TimestampDdsObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date


class TimestampDdsRepository:
    def insert_dds_timestamp(self, conn: Connection, timestamp: TimestampDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s);
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "time": timestamp.time,
                    "date": timestamp.date
                },
            )


    def get_timestamp(self, conn: Connection, dt: datetime) -> Optional[TimestampDdsObj]:
        with conn.cursor(row_factory=class_row(TimestampDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, ts, year, month, day, time, date
                    FROM dds.dm_timestamps
                    WHERE ts = %(dt)s;
                """,
                {"dt": dt},
            )
            obj = cur.fetchone()
        return obj