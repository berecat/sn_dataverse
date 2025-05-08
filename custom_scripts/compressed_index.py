from storage.miner.sqlite_miner_storage import SqliteMinerStorage
from common import constants, utils

if __name__ == "__main__":
    storage = SqliteMinerStorage()
    
    storage.refresh_compressed_index(
        time_delta=constants.MINER_CACHE_FRESHNESS
    )