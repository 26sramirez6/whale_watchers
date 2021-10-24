'''
Created on Oct 23, 2021

@author: 26sra
'''
import requests
SCRAPER_API_KEY = "40b2cc4259dffdd12466035827b4c3e5"
SCRAPER_API_BASE = "http://api.scraperapi.com"
SCRAPER_API_PROXY_POOL = "http://scraperapi.{}:{}@proxy-server.scraperapi.com:8001"

def request_through_proxy_pool(url, params={}, premium=False):
    api_params = {"country_code": "us"}
    if premium: api_params["premium"] = "true"
    
    params_str = ".".join(["{}={}".format(k,v) for k, v in api_params.items()])
    proxies = {"http": SCRAPER_API_PROXY_POOL.format(params_str, SCRAPER_API_KEY)}
    response = requests.request("GET", url, proxies=proxies, params=params, verify=False)
    return response

def request_direct(url, params={}, premium=False):
    params = {"api_key": SCRAPER_API_KEY, "url": url}
    if premium: params["premium"] = "true"
    response = requests.request("GET", url, params=params, verify=False)
    return response