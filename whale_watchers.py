'''
Created on Oct 4, 2021

@author: 26sra
'''
# -*- coding: utf-8 -*-
import requests
import json
import time
import decimal
import pprint
import datetime

ETHERSCAN_API_KEY = "4IV18GY7DMT29HR6PMGXB9EZUZ1KZYJGX2"
OS_URL = "https://api.opensea.io/api/v1/events"
ETH_URL = "https://api.etherscan.io/api"
QRY_STR = {"only_opensea":"false","offset":"0","limit":"20", "event_type":"successful"}
HEADERS = {"Accept": "application/json"}
GWEI_CONVERSION = decimal.Decimal("1000000000000000000")

BLUECHIP_CONTRACTS = {
    "CryptoPunks": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
    "BAYC:": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
    "CoolCats": "0x1a92f7381b9f03921564a437210bb9396471050c",
    "CyberKongs": "0x57a204aa1042f6e66dd7730813f4024114d74f37",
    "Cryptoadz": "0x1CB1A5e65610AEFF2551A50f76a87a7d3fB649C6"}

BLUECHIP_SCORES = {
    "CryptoPunks": 5,
    "BAYC:": 4,
    "CoolCats": 2,
    "CyberKongs": 4,
    "Cryptoadz": 1}

CONTRACT_TO_SCORE = {v:BLUECHIP_SCORES[k]  for k, v in BLUECHIP_CONTRACTS.items()}

class WhaleTransaction:
    def __init__(self):
        self.purchase_time = datetime.datetime.now()

class BuyerScan:
    def __init__(self, addr, eth_balance, is_whale, bluechip_score):
        self.eth_balance = eth_balance
        self.addr = addr
        self.is_whale = is_whale
        self.bluechip_score = bluechip_score
        
        
def process_event(event):
    asset = event["asset"]
    if asset is None: return
    payment_token = event["payment_token"]
    eid = event["id"]
    is_eth = payment_token is not None and payment_token["symbol"]=="ETH"
    usd_price = -1 if not is_eth else float(payment_token["usd_price"])
    permalink = asset["permalink"]
    contract = asset["asset_contract"]
    contract_address = contract["address"]
    collection = asset["collection"]
    buyer_address = event["winner_account"]["address"]
    trade_print = decimal.Decimal(event["total_price"])
    eth_amount = trade_print / GWEI_CONVERSION
    usd_amount = eth_amount * decimal.Decimal(usd_price)
    
    if is_eth:
        buyer = scan_buyer(buyer_address, ETHERSCAN_API_KEY)
        if buyer.is_whale:
            print("WHALE SIGHTING:", datetime.datetime.now())
            print("eth balance:", buyer.eth_balance)
            print("collection name:", collection["name"])
            print("tokenid:", asset["token_id"])
            print("eventid:", eid)
            print("permalink:", permalink)      
            print("contract_address:", contract_address)
            print("buyer_address:", buyer_address)
            print("eth amount:", eth_amount)
            print("usd amount:", usd_amount)
    return eid

def scan_for_bluechips(addr):
    url = "https://api.opensea.io/api/v1/assets"
    params = {"owner": addr, "limit":20, "asset_contract_addresses":[v for _,v in BLUECHIP_CONTRACTS.items()]}
    response = requests.request("GET", url, headers=HEADERS, params=params)
    loads = json.loads(response.text)
    assets = loads["assets"]
    score = 0
    if len(assets) > 0:
        for asset in assets:
            contract_address = asset["asset_contract"]["address"]
            score += CONTRACT_TO_SCORE[contract_address]
    return score


def scan_buyer(addr, api_key):
    MAX_ITER = 10
    
    for i in range(MAX_ITER):
        params = {"module": "account", "action":"balance","address":addr,"tag":"latest","apikey":api_key}
        response = requests.request("GET", ETH_URL, headers=HEADERS, params=params)
        loads = json.loads(response.text)
        if loads["message"]=="OK":
            eth_balance = decimal.Decimal(loads["result"]) / GWEI_CONVERSION
            bluechip_score = scan_for_bluechips(addr)
            if eth_balance > 100 or (eth_balance>50 and bluechip_score>10):
                return BuyerScan(addr, eth_balance, True, bluechip_score)
            else:
                break            
        else:
            time.sleep(.1)
            
    return BuyerScan(addr, -1, False, -1)
    
if __name__ == "__main__":
    recents = {}
    while True:
        cycle_start = datetime.datetime.now()
        response = requests.request("GET", OS_URL, headers=HEADERS, params=QRY_STR)
        loads = json.loads(response.text)
        events = loads["asset_events"]
        for event in events:
            eid = event["id"]
            if eid not in recents:
                process_event(event)
                recents[eid] = 0
        
        removes = []
        for eid in recents:
            if recents[eid] > 100:
                removes.append(eid)
            recents[eid] += 1
        
        for eid in removes:
            recents.pop(eid) 
            
        print("{}: Processed event batch".format(cycle_start))