from config import DB_TYPE

if DB_TYPE == "postgres":
    from database_pg import *  # noqa: F401,F403
else:
    from database import *  # noqa: F401,F403
