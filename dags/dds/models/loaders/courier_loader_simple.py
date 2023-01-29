from lib import PgConnect


class CouriersLoader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_couriers(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
insert into de.dds.dm_couriers (courier_id, name)
select 
	replace(replace(object_value, '"', ''), '''','"')::JSON->>'_id' as courier_id,
	replace(replace(object_value, '"', ''), '''','"')::JSON->>'name' as name
from de.stg.deliverysystem_couriers dc;
"""
                )
