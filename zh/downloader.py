import asyncio
import json
import re
from typing import Iterable
from urllib.parse import unquote
import aiohttp
import aiohttp_socks

import os
import random
from aiohttp_socks import ProxyType, ProxyConnector, ChainProxyConnector
from aiohttp import TCPConnector
from hashlib import sha512

import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
db = client['zhihu']
dbzhwikihow = client['wikihowZH']
collection_paths = dbzhwikihow['path']
collection_char = dbzhwikihow['char']

cwkh = db['wkh']

rate_limit_token = '''If you are a web crawler or bot, 0100100001100101011011000110110001101111! Well, you obviously can read human languages if you're on wikiHow.'''

# 代理池
spyons = [
    None,
]
proxyi = 0

def get_proxy():
    global proxyi
    if proxyi >= len(spyons):
        proxyi %= len(spyons)
    p = spyons[proxyi]
    if p and not p.startswith('socks'):
        p = 'http://' + p
    proxyi += 1
    return p


async def fetch(url: str, header_only=False, return_resp=False):
    prox = get_proxy()
    conn = ProxyConnector.from_url(prox) if prox is not None and prox.startswith('socks') else None
    async with aiohttp.ClientSession(
        connector=conn,
        headers={
            "accept-encoding":"gzip, deflate, br",
            "user-agent":"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
        }
    ) as session:
        async with session.get(url,
            proxy=prox if conn is None else None,
            # allow_redirects=False
        ) as response:
            if header_only:
                return response.headers
            if return_resp:
                return response, await response.text()
            print('request', url)
            return response.headers, await response.text()

async def migrator():
    cur = cwkh.find({})
    async for i in cur:
        ui = unquote(unquote(i.pop('_id')))
        await collection_paths.find_one_and_update(
            {'_id': ui}, 
            {'$set': i}, 
            upsert=True)

# async def search_fetch():

async def search_fetch():
    from expander import dump_urls, insert_without_exception

    li = []
    async for i in collection_char.find({'vis':{'$nin': True}}):
        li.append(i['_id'])
    
    async def subtask(i):
        s = set()
        ctr = 0
        err = 1
        while 1:
            try:
                resp, body = await fetch(f'https://zh.wikihow.com/wikiHowTo?search={i}&start={ctr}', return_resp=True)
            except Exception as e:
                print(e)
                break
            ctr += 15
            if rate_limit_token in body:
                print('触发反爬')
                break
                # exit(1)
            if resp.status == 404:
                err = 0
                break
            if resp.status != 200:
                print(resp.status, body)
                break
            for i in re.findall(r'class="result_link" href=https://zh.wikihow.com/(.*?) >', body):
                ui = unquote(unquote(i))
                if ui not in s:
                    print(ui)
                    s.add(ui)
            await asyncio.sleep(0.3)
        await asyncio.gather(*(asyncio.ensure_future(insert_without_exception(k)) for k in s))
        if err == 0:
            await collection_char.update_one({'_id': i}, {'$set': {'vis': True}})
        print(i, 'visited')

    tasks = []
    for p, i in enumerate(li):
        tasks.append(asyncio.ensure_future(subtask(i)))
        if p % 16 == 15:
            await asyncio.gather(*tasks)
            await asyncio.sleep(16.66)
            tasks.clear()

    await asyncio.gather(*tasks)
    
        

async def random_fetch():
    from expander import dump_urls, insert_without_exception
    async def random_fetch1():
        try:
            resp, html = await fetch('https://zh.wikihow.com/Special:Randomizer', return_resp=True)
            if rate_limit_token in html:
                print('触发反爬')
                exit(1)
                raise NameError('触发反爬')
        except Exception as e:
            print(e)
            return
        c = await collection_paths.find_one({'_id': resp.url.name})
        if not c:
            print('add', resp.url.name)
            await collection_paths.insert_one({'_id': resp.url.name, 'html': html, 'date': resp.headers.get('last-modified', ''), 'scanned': True})
        s = {}
        await dump_urls(html, s)
        tasks = [insert_without_exception(k, v) for k, v in s.items()]
        await asyncio.gather(*tasks)

    for i in range(7200):
        await asyncio.gather(*[random_fetch1() for _ in range(32)])
        await asyncio.sleep(10)
        print(i)

async def cache_fetch(path, forced_fetch=False):
    fpath = unquote(path.replace(':', '-').replace('/','、')).replace('"','“') + '.htm'
    # dbcontent = await cwkh.find_one({'_id': path})
    if forced_fetch or not os.path.exists(fpath):
        try:
            headers, cont = await fetch('https://zh.wikihow.com/' + path)
            if rate_limit_token in cont:
            # if len(cont) < 4096:
                print('limit triggered')
                raise NameError('触发反爬')
            await collection_paths.find_one_and_update({'_id': path}, {'$set': {'date': headers.get('last-modified', ''), 'html': cont}}, upsert=True)
        except Exception as e:
            print(fpath, e)
            return 0
        # with open(fpath, 'w', encoding='utf-8') as f:
            # f.write(cont)
        return 0
        # return cont
    else:
        return 1
        # with open(fpath, 'r', encoding='utf-8') as f:
            # cont = f.read()
            # return cont

indexes = {}


def cat(*args): return '/'.join(args)

tmpfiledir = 'tmpdownload'

async def main():
    tasks = []
    cur = collection_paths.find({'html': {'$in': [None]}})
    cid = 0
    async for x in cur:
        cid += 1
        tasks.append(asyncio.ensure_future(cache_fetch(x['_id'], True)))
        if len(tasks) >= 256:
            res = await asyncio.gather(*tasks)
            print('processing...', cid)
            if not all(res):
                await asyncio.sleep(3)
            tasks.clear()
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    if not os.path.exists(tmpfiledir):
        os.mkdir(tmpfiledir)

    asyncio.run(main())