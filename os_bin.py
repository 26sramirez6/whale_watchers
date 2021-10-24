import requests
import json
import decimal
import pickle
from multiprocessing import Pool, Process, Manager, Lock
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from proxy import request_direct, request_through_proxy_pool


CONTRACT = "0xee0ba89699a3dd0f08cb516c069d81a762f65e56"
GWEI_CONVERSION = decimal.Decimal("1000000000000000000")


def get_os_bin(tid, contract_address, tokenids, bins):
    OPENSEA_ASSETS = "https://api.opensea.io/api/v1/assets"
    LIMIT_SIZE = 30
    MAX_ITER = 10
    remaining = set(tokenids)
    processed = set()
    thread_bins = {}
    for attempt in range(MAX_ITER):
        remaining.difference_update(processed)
        remaining_list = list(remaining)
        batch_count = (len(remaining) // LIMIT_SIZE) + 1
        remaining_batches = [remaining_list[i*LIMIT_SIZE:(i+1)*LIMIT_SIZE] 
                   for i in range(batch_count)] 
        for batch in remaining_batches:
            params = {
              "asset_contract_address": contract_address,
              "limit": min(50, len(batch)),
              "token_ids": batch}
            try:
                response = request_through_proxy_pool(OPENSEA_ASSETS, params=params)
                loads = json.loads(response.text)
            except Exception as e:
                continue
    
            if response.status_code == 200:
                assets = loads.get("assets", None)
                if assets is not None:
                    for asset in assets:
                        tokenid = asset["token_id"]
                        processed.add(int(tokenid))
                        sell_orders = asset.get("sell_orders", None)
                        if sell_orders is not None and len(sell_orders)>0:
                            sell_order = sell_orders[0]
                            current_price_gwei = decimal.Decimal(sell_order["current_price"])
                            current_price_eth = current_price_gwei / GWEI_CONVERSION
                            thread_bins[tokenid] = current_price_eth
            else:
                print("status code", response.status_code)
    bins.update(thread_bins)        
            
def generate_bins(process_count, top_n, rankings, metadatas):
    processes = []
    batch_size = top_n / process_count
    token_ids = [metadatas[rankings[i][0]][0] for i in range(top_n)]
    manager = Manager()
    bins = manager.dict()
    for j in range(process_count):
        start = int(j*(batch_size))
        end = int((j+1)*(batch_size))
        p = Process(target=get_os_bin, args=(j, CONTRACT, token_ids[start:end], bins))
        processes.append(p)
      
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    
    return bins

def print_bins(top_n, metadatas, rankings, bins):
    for i, rank in enumerate(rankings[:top_n]):
        token_name = rank[0]
        token_id, _ = metadatas[token_name]
        if str(token_id) in bins:
            print("Rank {}: {}, {}, BIN: {} eth".
                  format(i, token_name, rank[2], bins.get(str(token_id), "--")))

if __name__ == "__main__":
    process_count = 4
    top_n = 500
    trait_archives = pickle.load(open("trait_archives.pickle", "rb"))
    metadatas = pickle.load(open("metadatas.pickle", "rb"))
    rankings = pickle.load(open("rankings.pickle", "rb"))
    bins = generate_bins(process_count, top_n, rankings, metadatas)
    print_bins(top_n, metadatas, rankings, bins)