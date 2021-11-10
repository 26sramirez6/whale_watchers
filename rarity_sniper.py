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
from proxy import request_direct, request_through_proxy_pool
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TESTS = {
    "metasaur": "0xf7143ba42d40eaeb49b88dac0067e54af042e963",
    "svs_bat":  "0xee0ba89699a3dd0f08cb516c069d81a762f65e56",
    "svs": "0x219b8ab790decc32444a6600971c7c3718252539",
    "basement": "0x9A95eCEe5161b888fFE9Abd3D920c5D38e8539dA",
    "mekaverse": "0x9a534628b4062e123ce7ee2222ec20b86e16ca8f",
    "boonji": "0x4cd0ea8b1bDb5ab9249d96cCF3d8A0d3aDa2Bc76",
    "divine": "0xc631164B6CB1340B5123c9162f8558c866dE1926"}

ETHERSCAN_API_KEY = "4IV18GY7DMT29HR6PMGXB9EZUZ1KZYJGX2"
OS_URL = "https://api.opensea.io/api/v1/events"
ETH_URL = "https://api.etherscan.io/api"
QRY_STR = {"only_opensea":"false","offset":"0","limit":"20", "event_type":"successful"}
HEADERS = {"Accept": "application/json"}
CONTRACT = TESTS["divine"] 
WEB3_URL = "https://mainnet.infura.io/v3" #"wss://mainnet.infura.io/ws/v3"
WEB3_API_KEY = "76e9e5d5de124620a24a3430699db0c3"

class IPFSRotator:
    GATEWAYS = [
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

    def __init__(self, ipfs_with_hash, probs=None):
        self.probs = [1./len(self.GATEWAYS) for _ in self.GATEWAYS] if probs is None else probs            
        self.hash = ipfs_with_hash.split("/")[-1] if "ipfs" in ipfs_with_hash else ""
        self.success = OrderedDict([(gateway,1) for gateway in self.GATEWAYS])
        self.failure = OrderedDict([(gateway,1) for gateway in self.GATEWAYS])
        self.last_gateway = self.GATEWAYS[0]
        
    def get_base_uri(self):
        success_rates = np.array([float(self.success[g]) / float(self.success[g]+self.failure[g]) for g in self.GATEWAYS])
        success_rates *= success_rates
        probs = success_rates / success_rates.sum()
        gateway = np.random.choice(self.GATEWAYS, p=probs)
        self.last_gateway = gateway
        return "{}{}".format(gateway, self.hash)
    
    def register_result(self, success):
        if success:
            self.success[self.last_gateway] += 1
        else:
            self.failure[self.last_gateway] += 1
            

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
        for token_id in remaining:
            
            if is_ipfs: 
                final_base = ipfs_rotator.get_base_uri()
            else:
                final_base = base_uri
            
            url = "{}/{}".format(final_base, token_idx_format.format(token_id))
            print("hitting {}".format(url))
            try:
                response = request_through_proxy_pool(url)
                loads = json.loads(response.text)                
                traits = loads['attributes']
                name = loads['name']
            except Exception as e:
                print("FAILED on token_id {}, response code {}, error {}, host {}".format(token_id, response.status_code if "response" in locals() else "--", e, url))
                failures += 1
                ipfs_rotator.register_result(False)
                continue
            ipfs_rotator.register_result(True)
            # for when the mess up metadata
            if isinstance(name, list):
                name = name[0]
            metadatas[name] = (token_id, traits)
            traits.append({"trait_type": "trait_count", "value" : len(traits)})
            for i, trait in enumerate(traits):
                trait_type = trait['trait_type']
                trait_value = trait['value']
                if trait_type not in trait_archives:
                    trait_archives[trait_type] = {}
                trait_values_dict = trait_archives[trait_type]
                if trait_value not in trait_values_dict:
                    trait_values_dict[trait_value] = 0
                trait_values_dict[trait_value] += 1
            print("processed token_id", token_id)
            processed.add(token_id)
        
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
    for _, (_, attributes) in metadatas.items():
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
    
    return trait_archives, metadatas
    

def get_contract_abi(contract_address, etherscan_api_key):
    params = {"module": "contract", 
              "action":"getabi",
              "address":contract_address,
              "tag":"latest",
              "apikey":ETHERSCAN_API_KEY}
    
    response = requests.request("GET", ETH_URL, params=params)
    loads = json.loads(response.text)
    return loads["result"]



def get_token_uri(contract_address, token_id, abi): 
    w3 = Web3(Web3.HTTPProvider("{}/{}".format(WEB3_URL, WEB3_API_KEY)))
    contract_address = Web3.toChecksumAddress(contract_address)
    contract = w3.eth.contract(contract_address, abi=abi)
    token_uri = contract.functions.tokenURI(token_id).call()
    is_ipfs = "ipfs" in token_uri
    if is_ipfs:
        if "http" not in token_uri:
            token_uri = np.random.choice(IPFSRotator.GATEWAYS[:2]) + token_uri.strip("ipfs://")
    
    
    base_uri = "/".join(token_uri.split("/")[:-1])
    
    split = token_uri.split("/")
    base_uri = "/".join(split[:-1])
    token_idx_format = split[-1].replace(str(token_id), "{}")
    return base_uri, token_uri, token_idx_format, is_ipfs

np.random.seed(86648)
def spin_until_reveal(contract_abi):
    is_live = False
    attempts = 1
    while not is_live:
        rand_token = (attempts + 1) % 20
        try:
            base_uri, token_uri, token_idx_format, is_ipfs = get_token_uri(CONTRACT, rand_token, contract_abi)
        except Exception as e:
            print("exception on token query, skipping", e, rand_token)
            time.sleep(5)
            continue
            
        try:
            response = request_direct(token_uri)
            loads = json.loads(response.text)
            traits = loads['attributes']
            name = loads['name']
            is_live = len(traits) > 1
        except Exception:
            print("{}: reveal not live, sleeping".format(datetime.datetime.now()))
            time.sleep(5)
            
    print("{} REVEAL LIVE on {}, BEGINNING SCAN".format(datetime.datetime.now(), token_uri))
    return base_uri, token_uri, token_idx_format, is_ipfs

def generate_rankings(trait_archives, metadatas):
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
    for name, (_, attributes) in metadatas.items():
        power_score = 1000000
        min_rarity_trait_score = np.inf
        for trait in attributes:
            rarity = specific_trait_rarities[trait['trait_type']][trait['value']]
            power_score *= rarity
            if rarity < min_rarity_trait_score:
                min_rarity_trait_score = rarity
                rarest_trait = (trait['trait_type'], trait['value'])
        token_rarities.append((name, power_score, rarest_trait, attributes))
   
    power_ranking = sorted(token_rarities, key=lambda x: x[1])
   
    printout = ["Power Rank {}: {}, Rarest trait: {}".format(i, power_ranking[i][0], power_ranking[i][2]) for i in range(len(power_ranking))]
    for rank in printout:
        print(rank.encode("utf-8"))
    with open("rankings.txt", "wb") as f:
        f.write("\n".join(printout).encode("utf-8"))
    pickle.dump(
        power_ranking,
        open('rankings.pickle', 'wb'),
        protocol=pickle.HIGHEST_PROTOCOL)
    print("Runtime: {}".format((datetime.datetime.now() - start).total_seconds()))
    
    return power_ranking

def map_against_os(rankings, metadatas):
    return
    
    
if __name__ == "__main__":
    collection_size = 100
    start = datetime.datetime.now()
    contract_abi = get_contract_abi(CONTRACT, ETHERSCAN_API_KEY)
    base_uri, token_uri, token_idx_format, is_ipfs = spin_until_reveal(contract_abi)
    process_count = 4
    trait_archives, metadatas = generate_metadatas(base_uri, token_idx_format, is_ipfs, collection_size, process_count)
    generate_rankings(trait_archives, metadatas)
    
    
