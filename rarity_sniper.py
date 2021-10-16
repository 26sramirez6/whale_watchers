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
from multiprocessing import Pool, Process, Manager
import pickle
from web3 import Web3
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ETHERSCAN_API_KEY = "4IV18GY7DMT29HR6PMGXB9EZUZ1KZYJGX2"
OS_URL = "https://api.opensea.io/api/v1/events"
ETH_URL = "https://api.etherscan.io/api"
QRY_STR = {"only_opensea":"false","offset":"0","limit":"20", "event_type":"successful"}
HEADERS = {"Accept": "application/json"}
CONTRACT = "0xf7143ba42d40eaeb49b88dac0067e54af042e963" #"0xf7143ba42d40eaeb49b88dac0067e54af042e963" #"0x9A95eCEe5161b888fFE9Abd3D920c5D38e8539dA"
WEB3_URL = "https://mainnet.infura.io/v3" #"wss://mainnet.infura.io/ws/v3"
WEB3_API_KEY = "76e9e5d5de124620a24a3430699db0c3"
SCRAPER_API_KEY = "40b2cc4259dffdd12466035827b4c3e5"
SCRAPER_API_BASE = "http://api.scraperapi.com"

HEADERS = [
    # Firefox 77 Mac
    OrderedDict([
        ("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0"),
        ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"),
        ("Accept-Language", "en-US,en;q=0.5"),
        ("Referer", "https://www.google.com/"),
        ("DNT", "1"),
        ("Connection", "keep-alive"),
        ("Upgrade-Insecure-Requests", "1")
    ]),
    # Firefox 77 Windows
    OrderedDict([
        ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0"),
        ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"),
        ("Accept-Language", "en-US,en;q=0.5"),
        ("Accept-Encoding", "gzip, deflate, br"),
        ("Referer", "https://www.google.com/"),
        ("DNT", "1"),
        ("Connection", "keep-alive"),
        ("Upgrade-Insecure-Requests", "1")
    ]),
    # Chrome 83 Mac
    OrderedDict([
        ("Connection", "keep-alive"),
        ("DNT", "1"),
        ("Upgrade-Insecure-Requests", "1"),
        ("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"),
        ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"),
        ("Sec-Fetch-Site", "none"),
        ("Sec-Fetch-Mode", "navigate"),
        ("Sec-Fetch-Dest", "document"),
        ("Referer", "https://www.google.com/"),
        ("Accept-Encoding", "gzip, deflate, br"),
        ("Accept-Language", "en-GB,en-US;q=0.9,en;q=0.8")
    ]),
    # Chrome 83 Windows
    OrderedDict([
        ("Connection", "keep-alive"),
        ("Upgrade-Insecure-Requests", "1"),
        ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"),
        ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"),
        ("Sec-Fetch-Site", "same-origin"),
        ("Sec-Fetch-Mode", "navigate"),
        ("Sec-Fetch-User", "?1"),
        ("Sec-Fetch-Dest", "document"),
        ("Referer", "https://www.google.com/"),
        ("Accept-Encoding", "gzip, deflate, br"),
        ("Accept-Language", "en-US,en;q=0.9")
    ])
]

def get_header():
    return np.random.choice(HEADERS)


class ProxyRotator:
    @staticmethod
    def parse_sslproxies():
        response = requests.get("https://sslproxies.org/") 
        soup = BeautifulSoup(response.content, 'html5lib')
        proxies = list(
            map(lambda x:x[0]+':'+x[1], list(zip(map(lambda x:x.text, soup.findAll('td')[::8]), 
                                                  map(lambda x:x.text, soup.findAll('td')[1::8])))))            
        return [None] if len(proxies)==0 else proxies
   
    @staticmethod
    def parse_freeproxy():
        response = requests.get("https://free-proxy-list.net", headers=get_header())
        parser = fromstring(response.text)
        proxies = []
        for i in parser.xpath('//tbody/tr')[:100]:
            if i.xpath('.//td[7][contains(text(),"yes")]'):
                proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.append(proxy)
        return [None] if len(proxies)==0 else proxies
        
    @staticmethod
    def none():
        return [None]
   
    PROVIDERS = {
        None : none,
        "https://www.sslproxies.org/": parse_sslproxies,
        "https://free-proxy-list.net/": parse_freeproxy}

    def __init__(self, p={None:.06, "https://www.sslproxies.org/":.47, "https://free-proxy-list.net/":.47}):
        self.tables_by_provider = {}
        self.full_tables = []
        self.consecutive_failures_by_provider = {}
        self.consecutive_failures_by_proxy = {k: {} for k in ProxyRotator.PROVIDERS}
        self.full_consecutive_failures = {}
        self.total_failures_by_provider = {}
        self.total_attempts_by_provider = {}
        self.total_attempts_by_proxy = {}
        self.total_failures_by_proxy = {}
        self.last_proxy = None
        self.last_provider = None
        self.last_success = True
        for k,v in ProxyRotator.PROVIDERS.items():
            provider_proxies = v.__func__()                
            self.tables_by_provider[k] = provider_proxies
            self.full_tables.extend(provider_proxies)
            self.consecutive_failures_by_provider[k] = 0
            self.total_attempts_by_provider[k] = 0
            self.total_failures_by_provider[k] = 0
            for proxy in provider_proxies:
                self.consecutive_failures_by_proxy[proxy] = 0
                self.total_attempts_by_proxy[proxy] = 0
                self.total_failures_by_proxy[proxy] = 0
            
        self.providers = list(self.tables_by_provider)
        self.probabilities = [p[provider] for provider in self.providers]
        
    def get_proxy(self):
        if self.last_success:
            return self.last_proxy, self.last_provider
        
        provider = np.random.choice(self.providers, p=self.probabilities)
        proxy = np.random.choice(self.tables_by_provider[provider])
        return proxy, provider
    
    def register_results(self, proxy, provider, success):
        if success:
            self.consecutive_failures_by_provider[provider] = 0
            self.consecutive_failures_by_proxy[proxy] = 0
        else:
            self.consecutive_failures_by_provider[provider] += 1
            self.consecutive_failures_by_proxy[proxy] += 1
            self.total_failures_by_provider[provider] += 1
            self.total_failures_by_proxy[proxy] += 1
            
        self.total_attempts_by_provider[provider] += 1
        self.total_attempts_by_proxy[proxy] += 1
        
        self.last_proxy = proxy
        self.last_provider = provider
        self.last_success = success

    
    def print_results(self):
        print("*********ROTATOR RESULTS*********")
        for k,v in self.total_attempts_by_provider.items():
            print(k, "success rate", "0 attempts" if v==0 else (v - self.total_failures_by_provider[k]) / v)


def scan_batch_ipfs(tid, indices, base_uri):
    trait_archives, metadatas = {}, {}
    failures = 0
    remaining = set(indices)
    processed = set()
    
    min_timeout = .25
    max_timeout = .5
    max_iter = 10
    dt = (max_timeout - min_timeout)/max_iter
    for major_attempt in range(max_iter):
        remaining.difference_update(processed)
        print("major attempt {}. Remaining: {}".format(major_attempt, len(remaining)))
        for index in remaining:
            url = "{}/{}".format(base_uri, index)
                
            try:
                params = {"api_key": SCRAPER_API_KEY, "url": url, "premium":"true"}
                proxies = {"http": "http://scraperapi.country_code=us:{}@proxy-server.scraperapi.com:8001".format(SCRAPER_API_KEY)}
                response = requests.request("GET", url, proxies=proxies, verify=False)
                
                loads = json.loads(response.text)
                
                traits = loads['attributes']
                name = loads['name']
            except Exception as e:
                print("FAILED on index {}, response code {}, error {}, host {}".format(index, response.status_code if "response" in locals() else "--", e, url))
                failures += 1
                continue
                
            
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
    print("batch completed, successes: {}, failures: {}".format(len(indices)-failures, failures))

def run_batches(base_uri, collection_size, process_count):
    processes = []
    batch_size = collection_size / process_count
    for j in range(process_count):
        start = 1 + int(j*(batch_size))
        end = collection_size if j==process_count-1 else int((j+1)*(batch_size))
        p = Process(target=scan_batch_ipfs, args=(j, range(start, end), base_uri))
        processes.append(p)
      
    for p in processes:
        p.start()
    for p in processes:
        p.join()

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
        
def url_from_ipfs(ipfs_address):

    if ( "http" in ipfs_address ):
        return ipfs_address

    IPFS_GATEWAY = np.random.choice(
        [
#         "https://cloudflare-ipfs.com/ipfs/",
        "https://ipfs.io/ipfs/",
#         "https://ipfs.infura.io:5001/api/v0/cat?arg=",
#         "https://ipfs.tubby.cloud/ipfs/",
#         "https://ravencoinipfs-gateway.com/ipfs/",
#         "https://ipfs.adatools.io/ipfs/",
#         "https://ipfs.eth.aragon.network/ipfs/",
#         "https://gateway.pinata.cloud/",
#         "https://gateway.ipfs.io/ipfs/",
#         "https://gateway.originprotocol.com/ipfs/",
        ]
    )

    url = IPFS_GATEWAY + ipfs_address.strip("ipfs://")
    
    return url

def get_token_uri(contract_address, token_id, abi): 
    w3 = Web3(Web3.HTTPProvider("{}/{}".format(WEB3_URL, WEB3_API_KEY)))
    contract_address = Web3.toChecksumAddress(contract_address)
    contract = w3.eth.contract(contract_address, abi=abi)
    token_uri = contract.functions.tokenURI(token_id).call()
    if "ipfs" in token_uri:
        token_uri = url_from_ipfs(token_uri)
    
    base_uri = "/".join(token_uri.split("/")[:-1])
    
    return base_uri, token_uri


def spin_until_reveal(contract_abi):
    main_rotator = ProxyRotator()
    is_live = False
    while not is_live:
        proxy, provider = main_rotator.get_proxy()
        base_uri, token_uri = get_token_uri(CONTRACT, np.random.randint(1,20), contract_abi)
        if proxy is None:
            proxies = {}
        else:
            proxies = {"http":proxy}
        try:
            response = requests.request(
                    "GET",
                    token_uri,
                    headers=get_header(), 
                    timeout=1,
                    proxies=proxies)
            
            loads = json.loads(response.text)
            traits = loads['attributes']
            name = loads['name']
            is_live = True
        except Exception:
            print("{}: reveal not live, sleeping".format(datetime.datetime.now()))
            main_rotator.register_results(proxy, provider, False)
            time.sleep(5)
            
    print("{} REVEAL LIVE on {}, BEGINNING SCAN".format(datetime.datetime.now(), token_uri))
    return base_uri, token_uri
    
if __name__ == "__main__":
    
    collection_size = 9999
    start = datetime.datetime.now()
    contract_abi = get_contract_abi(CONTRACT, ETHERSCAN_API_KEY)
    base_uri, token_uri = spin_until_reveal(contract_abi)
    process_count = 50
    run_batches(base_uri, collection_size, process_count)
    
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