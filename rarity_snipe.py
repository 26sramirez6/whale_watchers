'''
Created on Oct 11, 2021

@author: 26sra
'''
# -*- coding: utf-8 -*-
import requests
import json
import time
import os
import decimal
import numpy as np
import pprint
import datetime
from multiprocessing import Pool, Process, Manager
import pickle
BASE_URL = "https://ipfs.io/ipfs/"
ipfs_hash = "bafybeic26wp7ck2bsjhjm5pcdigxqebnthqrmugsygxj5fov2r2qwhxyqu"
ETHERSCAN_API_KEY = "4IV18GY7DMT29HR6PMGXB9EZUZ1KZYJGX2"
OS_URL = "https://api.opensea.io/api/v1/events"
ETH_URL = "https://api.etherscan.io/api"
QRY_STR = {"only_opensea":"false","offset":"0","limit":"20", "event_type":"successful"}
HEADERS = {"Accept": "application/json"}


def url_from_ipfs(ipfs_address):

    if ( "http" in ipfs_address ):
        return ipfs_address

    IPFS_GATEWAY = np.random.choice(
        ["https://cloudflare-ipfs.com/ipfs/",
        "https://ipfs.io/ipfs/",
        "https://ipfs.infura.io:5001/api/v0/cat?arg=",
        "https://ipfs.tubby.cloud/ipfs/",
        "https://ravencoinipfs-gateway.com/ipfs/",
        "https://ipfs.adatools.io/ipfs/",
        "https://ipfs.eth.aragon.network/ipfs/",
#         "https://gateway.pinata.cloud/",
        "https://gateway.ipfs.io/ipfs/",
        "https://gateway.originprotocol.com/ipfs/",
        ]
        )

    url = IPFS_GATEWAY + ipfs_address.strip("ipfs://")

    return url

def scan_batch_ipfs(tid, indices, hash):
    trait_archives, metadatas = {}, {}
    for index in indices:
        hashed = url_from_ipfs(hash)
        url = "{}/{}".format(hashed, index)
        print(url)
        response = requests.request("GET", url, headers=HEADERS)
            
        loads = json.loads(response.text)
        traits = loads['attributes']
        metadatas[loads['name']] = traits
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
    pickle.dump(
        trait_archives, 
        open('trait_archives{}.pickle'.format(tid), 'wb'), 
        protocol=pickle.HIGHEST_PROTOCOL)
    pickle.dump(
        metadatas, 
        open('metadatas{}.pickle'.format(tid), 'wb'), 
        protocol=pickle.HIGHEST_PROTOCOL)
    print("batch completed")
        
if __name__ == "__main__":

    collection_size = 1000
    batch_size = collection_size / os.cpu_count()
    processes = []
    for j in range(os.cpu_count()):
        indices = [i for i in range(1 + int(j*(batch_size)), int((j+1)*(batch_size)))]
        p = Process(target=scan_batch_ipfs, args=(j, indices, ipfs_hash))
        processes.append(p)
    
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    
    print("COMPLETED")
    trait_archives = {}
    metadatas = {}
    for i in range(os.cpu_count()):
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
    
    
    print(specific_trait_rarities)
    print(average_trait_rarities)   
    token_rarities = []
    for name, attributes in metadatas.items():
        power_score = 1
        min_rarity_trait_score = np.inf
        for trait in attributes:
            try:
                rarity = specific_trait_rarities[trait['trait_type']][trait['value']]
            except:
                pause = True
            power_score *= rarity
            min_rarity_trait_score = min(min_rarity_trait_score, rarity)
        token_rarities.append((name, power_score, min_rarity_trait_score))
    
    power_ranking = sorted(token_rarities, key=lambda x: x[1])
    min_rarity_trait_ranking = sorted(token_rarities, key=lambda x: x[2])
    
    for i in range(len(power_ranking)):
        print("Rank {}: {} ({})".format(i, power_ranking[i][0], power_ranking[i][1]))