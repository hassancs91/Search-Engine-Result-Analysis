# db.py
# db_params = {
#     'dbname': 'studies',
#     'user': 'RfA2g1JHO0jNDWak',
#     'password': 'vJ6&bG2$hJ1]kN4:',
#     'host': 'studies-cl9xf-postgresql.external.kinsta.app',
#     'port': 32346
# }


import asyncpg

pool = None

async def init_db_pool():
    global pool
    pool = await asyncpg.create_pool(
        user='RfA2g1JHO0jNDWak',
        password='vJ6&bG2$hJ1]kN4:',
        database='studies',
        host='studies-cl9xf-postgresql.external.kinsta.app',
        port= 32346
    )

async def get_db_pool():
    global pool
    if pool is None:
        await init_db_pool()
    return pool


async def close_db_pool():
    global pool
    if pool:
        await pool.close()