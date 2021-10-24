import requests
import json
import decimal
import pickle
CONTRACT = "0xf7143ba42d40eaeb49b88dac0067e54af042e963"
GWEI_CONVERSION = decimal.Decimal("1000000000000000000")

def get_os_bin(tid, contract_address, tokenids):
    OPENSEA_ASSETS = "https://api.opensea.io/api/v1/assets"
    params = {"asset_contract_address": contract_address,
              "limit": 50,
              "token_ids": tokenids}
    
    response = requests.request("GET", OPENSEA_ASSETS, params=params)
    print(response.status_code)
    loads = json.loads(response.text)
    ret = {}
    if response.status_code == 200:
        assets = loads.get("assets", None)
        if assets is not None:
            for asset in assets:
                tokenid = asset["token_id"]
                sell_orders = asset.get("sell_orders", None)
                if sell_orders is not None and len(sell_orders)>0:
                    sell_order = sell_orders[0]
                    current_price_gwei = decimal.Decimal(sell_order["current_price"])
                    current_price_eth = current_price_gwei / GWEI_CONVERSION
                    ret[tokenid] = current_price_eth
    return ret
            
def generate_bins(process_count, top_n):
    processes = []
    batch_size = top_n / process_count
    token_ids = [v[0] for k, v in metadatas.items()]
    for j in range(process_count):
        start = int(j*(batch_size))
        end = len(token_ids) if j==process_count-1 else int((j+1)*(batch_size))
        p = Process(target=get_os_bin, args=(j, CONTRACT, token_ids[start:end]))
        processes.append(p)
      
    for p in processes:
        p.start()
    for p in processes:
        p.join()
        

if __name__ == "__main__":
    trait_archives = pickle.load(open("trait_archives.pickle", "rb"))
    metadatas = pickle.load(open("metadatas.pickle", "rb"))
    rankings = pickle.load(open("rankings.pickle", "rb"))
    process_count = 4
    top_n = 20
    generate_bins(process_count, top_n, metadatas)