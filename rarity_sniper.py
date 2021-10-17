# -*- coding: utf-8 -*-
'''
Created on Oct 11, 2021

@author: 26sra
'''
import sys
import requests
import json
import time
import os
import decimal
import numpy as np
import pprint
from collections import OrderedDict
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import datetime
from lxml.html import fromstring
from multiprocessing import Pool, Process, Manager, Lock
import pickle
from web3 import Web3
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ETHERSCAN_API_KEY = "4IV18GY7DMT29HR6PMGXB9EZUZ1KZYJGX2"
OS_URL = "https://api.opensea.io/api/v1/events"
ETH_URL = "https://api.etherscan.io/api"
QRY_STR = {"only_opensea":"false","offset":"0","limit":"20", "event_type":"successful"}
HEADERS = {"Accept": "application/json"}
CONTRACT = "0x219b8ab790decc32444a6600971c7c3718252539" #"0xf7143ba42d40eaeb49b88dac0067e54af042e963" #"0xf7143ba42d40eaeb49b88dac0067e54af042e963" #"0x9A95eCEe5161b888fFE9Abd3D920c5D38e8539dA"
WEB3_URL = "https://mainnet.infura.io/v3" #"wss://mainnet.infura.io/ws/v3"
WEB3_API_KEY = "76e9e5d5de124620a24a3430699db0c3"
SCRAPER_API_KEY = "40b2cc4259dffdd12466035827b4c3e5"
SCRAPER_API_BASE = "http://api.scraperapi.com"
SCRAPER_API_PROXY_POOL = "http://scraperapi.{}:{}@proxy-server.scraperapi.com:8001"


def request_through_proxy_pool(url, premium=False):
    params = {"country_code": "us"} #, "premium":"true"}
    if premium: params["premium"] = "true"

    params_str = ".".join(["{}={}".format(k,v) for k, v in params.items()])
    proxies = {"http": SCRAPER_API_PROXY_POOL.format(params_str, SCRAPER_API_KEY)}
    response = requests.request("GET", url, proxies=proxies, verify=False)
    return response

def request_direct(url):
    params = {"api_key": SCRAPER_API_KEY, "url": url, "premium":"true"}
    response = requests.request("GET", url, params=params, verify=False)
    return response
        

def scan_batch(tid, indices, base_uri, token_idx_format, is_ipfs):
    trait_archives, metadatas = {}, {}
    failures = 0
    remaining = set(indices)
    processed = set()
    
    min_timeout = .25
    max_timeout = .5
    max_iter = 10
    
    ipfs_rotator = IPFSRotator(base_uri)

    dt = (max_timeout - min_timeout)/max_iter
    for major_attempt in range(max_iter):
        remaining.difference_update(processed)
        print("major attempt {}. Remaining: {}".format(major_attempt, len(remaining)))
        for index in remaining:
            
            if is_ipfs:
                base_uri = ipfs_rotator.get_gateway()
            
            url = "{}/{}".format(base_uri, token_idx_format.format(index))
            try:
                response = request_through_proxy_pool(url)
                loads = json.loads(response.text)                
                traits = loads['attributes']
                name = loads['name']
            except Exception as e:
                print("FAILED on index {}, response code {}, error {}, host {}".format(index, response.status_code if "response" in locals() else "--", e, url))
                failures += 1
                continue
            
            # for when the mess up metadata
            if isinstance(name, list):
                name = name[0]
                
            metadatas[name] = traits
            for i, trait in enumerate(traits):
                trait_type = trait['trait_type']
                trait_value = trait['value']
                if trait_type not in trait_archives:
                    trait_archives[trait_type] = {}
                trait_values_dict = trait_archives[trait_type]
                if trait_value not in trait_values_dict:
                    trait_values_dict[trait_value] = 0
                trait_values_dict[trait_value] += 1
            print("processed index", index)
            processed.add(index)
        
    pickle.dump(
        trait_archives,
        open('trait_archives{}.pickle'.format(tid), 'wb'),
        protocol=pickle.HIGHEST_PROTOCOL)
    pickle.dump(
        metadatas,
        open('metadatas{}.pickle'.format(tid), 'wb'),
        protocol=pickle.HIGHEST_PROTOCOL)

    print("batch completed")

def generate_metadatas(base_uri, token_idx_format, is_ipfs, collection_size, process_count):
    processes = []
    batch_size = collection_size / process_count
    for j in range(process_count):
        start = 1 + int(j*(batch_size))
        end = collection_size if j==process_count-1 else int((j+1)*(batch_size))
        p = Process(target=scan_batch, args=(j, range(start, end), base_uri, token_idx_format, is_ipfs))
        processes.append(p)
      
    for p in processes:
        p.start()
    for p in processes:
        p.join()
        
    trait_archives = {}
    metadatas = {}
    for i in range(process_count):
        thread_traits_archive = pickle.load(open('trait_archives{}.pickle'.format(i), 'rb'))
        thread_metadatas = pickle.load(open('metadatas{}.pickle'.format(i), 'rb'))
        
        metadatas.update(thread_metadatas)
        for k,v in thread_traits_archive.items():
            if k not in trait_archives:
                trait_archives[k] = {}
            trait_archive = trait_archives[k]
            for name, count in v.items():
                if name not in trait_archive:
                    trait_archive[name] = 0
                trait_archive[name] += count
    
    # identify unique traits, correct the metadatas by adding missing global categories
    trait_categories = trait_archives.keys()
    for trait, options in trait_archives.items():
        trait_presence_count = sum(options.values())
        missing = collection_size - trait_presence_count
        options["None"] = missing
    for token, attributes in metadatas.items():
        token_attributes = set([attr["trait_type"] for attr in attributes])
        for category in trait_categories:
            if category not in token_attributes:
                attributes.append({"trait_type" : category, "value": "None"})
    
    pickle.dump(
        trait_archives,
        open('trait_archives.pickle', 'wb'),
        protocol=pickle.HIGHEST_PROTOCOL)
    pickle.dump(
        metadatas,
        open('metadatas.pickle', 'wb'),
        protocol=pickle.HIGHEST_PROTOCOL)

    print("*******COMPLETED METADATA SCAN*******")
    

def get_contract_abi(contract_address, etherscan_api_key):
    params = {"module": "contract", 
              "action":"getabi",
              "address":contract_address,
              "tag":"latest",
              "apikey":ETHERSCAN_API_KEY}
    
    response = requests.request("GET", ETH_URL, params=params)
    loads = json.loads(response.text)
    return loads["result"]


class IPFSRotator
    IPFS_GATEWAY = [
         "https://cloudflare-ipfs.com/ipfs/",
         "http://ipfs.io/ipfs/",
         "https://ipfs.infura.io:5001/api/v0/cat?arg=",
         "https://ipfs.tubby.cloud/ipfs/",
         "https://ravencoinipfs-gateway.com/ipfs/",
         "https://ipfs.adatools.io/ipfs/",
         "https://ipfs.eth.aragon.network/ipfs/",
         "https://gateway.pinata.cloud/",
         "https://gateway.ipfs.io/ipfs/",
         "https://gateway.originprotocol.com/ipfs/",
    ]

    def __init__(self, base_uri, probs=[1./len(IPFS_GATEWAY) for _ in IPFS_GATEWAY]):
        self.hash = base_uri.split("/")[-2] if "ipfs" in base_uri else ""
        self.probs = probs
        self.success = OrderedDict([(gateway,1) for gateway in self.IPFS_GATEWAY])
        self.failure = OrderedDict([(gateway,1) for gateway in self.IPFS_GATEWAY])

    def get_base_uri(self):
        success_rates = np.array([float(self.success[g]) / float(self.success[g]+self.failure[g]) for g in self.IPFS_GATEWAY])
        success_rates *= success_rates
        probs = success_rates / success_rates.sum()
        gateway = np.random.choice(self.IPFS_GATEWAY, p=probs)
        return "{}{}".format(gateway, self.hash)
    
    def register_result(gateway, succes):
        if success:
            self.success[gateway] += 1
        else:
            self.failure[gateway] += 1


def get_token_uri(contract_address, token_id, abi): 
    w3 = Web3(Web3.HTTPProvider("{}/{}".format(WEB3_URL, WEB3_API_KEY)))
    contract_address = Web3.toChecksumAddress(contract_address)
    contract = w3.eth.contract(contract_address, abi=abi)
    token_uri = contract.functions.tokenURI(token_id).call()
    is_ipfs = "ipfs" in token_uri
    
    base_uri = "/".join(token_uri.split("/")[:-1])
    
    split = token_uri.split("/")
    base_uri = "/".join(split[:-1])
    token_idx_format = split[-1].replace(str(token_id), "{}")
    return base_uri, token_uri, token_idx_format, is_ipfs


def spin_until_reveal(contract_abi):
    is_live = False
    while not is_live:
        base_uri, token_uri, token_idx_format, is_ipfs = get_token_uri(CONTRACT, np.random.randint(1,20), contract_abi)
        try:
            response = request_direct(token_uri)
            loads = json.loads(response.text)
            traits = loads['attributes']
            name = loads['name']
            is_live = True
        except Exception:
            print("{}: reveal not live, sleeping".format(datetime.datetime.now()))
            time.sleep(5)
            
    print("{} REVEAL LIVE on {}, BEGINNING SCAN".format(datetime.datetime.now(), token_uri))
    return base_uri, token_uri, token_idx_format, is_ipfs

def generate_rankings():
    specific_trait_rarities = {}
    average_trait_rarities = {}
    for trait, options in trait_archives.items():
        option_size = len(options)
        option_pool_size = sum([v for v in options.values()])
        avg_inner_rarity = 0
        inner_rarities = {}
        for specific_trait_value, count in options.items():
            inner_rarity = float(count) / float(option_pool_size)
            inner_rarities[specific_trait_value] = inner_rarity
            avg_inner_rarity += inner_rarity
        specific_trait_rarities[trait] = inner_rarities
        average_trait_rarities[trait] = avg_inner_rarity / option_pool_size
   
   
    token_rarities = []
    for name, attributes in metadatas.items():
        power_score = 1
        min_rarity_trait_score = np.inf
        for trait in attributes:
            rarity = specific_trait_rarities[trait['trait_type']][trait['value']]
            power_score *= rarity
            if rarity < min_rarity_trait_score:
                min_rarity_trait_score = rarity
                rarest_trait = (trait['trait_type'], trait['value'])
        token_rarities.append((name, power_score, rarest_trait))
   
    power_ranking = sorted(token_rarities, key=lambda x: x[1])
   
    rankings = ["Power Rank {}: {}, Rarest trait: {}".format(i, power_ranking[i][0], power_ranking[i][2]) for i in range(len(power_ranking))]
    for rank in rankings:
        print(rank.encode("utf-8"))
    with open("rankings.txt", "wb") as f:
        f.write("\n".join(rankings).encode("utf-8"))
    print("Runtime: {}".format((datetime.datetime.now() - start).total_seconds()))


def map_against_os():
    return
    
    
if __name__ == "__main__":
    collection_size = 8888
    start = datetime.datetime.now()
    contract_abi = get_contract_abi(CONTRACT, ETHERSCAN_API_KEY)
    base_uri, token_uri, token_idx_format, is_ipfs = spin_until_reveal(contract_abi)
    process_count = 100
    generate_metadatas(base_uri, token_idx_format, is_ipfs, collection_size, process_count)
    generate_rankings()
    
    
