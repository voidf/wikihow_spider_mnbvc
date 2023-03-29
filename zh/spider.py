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
cwkh = db['wkh']


spyons = [
    None,
]
proxyi = 0

def get_proxy():
    global proxyi
    if proxyi >= len(spyons):
        proxyi %= len(spyons)
    p = spyons[proxyi]
    if p:
        p = 'http://' + p
    proxyi += 1
    return p


async def fetch(url: str):
    async with aiohttp.ClientSession(
        # connector=conn,
        headers={
            "accept-encoding":"gzip, deflate, br",
            "user-agent":"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
        }
    ) as session:
        async with session.get(url,
            proxy=get_proxy(),
            allow_redirects=False
        ) as response:
            print('request', url)
            return await response.text()

indexes = {}


def cat(*args): return '/'.join(args)

tmpfiledir = 'tmpdownload'

downloadnum = 0
async def cache_fetch(path):
    global downloadnum

    fpath = unquote(path.replace(':', '-')) + '.htm'
    if not os.path.exists(fpath):
        myid = downloadnum # 单线程
        downloadnum += 1
        cont = await fetch('https://zh.wikihow.com/' + path)
        with open(tmppath := cat(tmpfiledir, myid), 'w', encoding='utf-8') as f:
            f.write(cont)
        os.rename(tmppath, fpath)
        indexes[path] = cont
        return cont
    else:
        with open(fpath, 'r', encoding='utf-8') as f:
            cont = f.read()
            indexes[path] = cont
            return cont

async def cache_fetch2(path):
    fpath = sha512(path.encode('utf-8')).hexdigest()
    if not os.path.exists(tmppath := cat(tmpfiledir, fpath)):
        cont = await fetch('https://zh.wikihow.com/' + path)
        with open(tmppath, 'w', encoding='utf-8') as f:
            f.write(cont)
        indexes[path] = cont
        return cont
    else:
        with open(tmppath, 'r', encoding='utf-8') as f:
            cont = f.read()
            indexes[path] = cont
            return cont

cache_method = cache_fetch2

async def nocache_fetch(path):
    cont = await fetch('https://zh.wikihow.com/' + path)
    indexes[path] = cont
    return cont

async def pagination_extend(cates: list[str]) -> list[str]:
    tasks = []
    for x in cates:
        pagesurl = re.findall(r'href="/Category:(.*?\?pg=[0-9]*?)"', x)
        for pg in pagesurl:
            if pg not in indexes:
                tasks.append(cache_method('Category:'+pg))
    return await asyncio.gather(*tasks)

def scan_sitemap():
    with open('Special-Sitemap.htm', 'r', encoding='utf-8') as f:
        res = re.findall('href="/Category:(.*?)"', f.read())
    print(res)
    return ['Category:'+x for x in set(res)]

async def download_category(categories: Iterable[str]):
    tasks = []
    for category in categories:
        tasks.append(asyncio.ensure_future(cache_method(category)))
    res = await asyncio.gather(*tasks)
    await pagination_extend(res)
    # assert all(res)

async def put_to_mongo():
    for k, v in indexes.items():
        r = set(re.findall(r'<a href="https://zh.wikihow.com/(.*?)">', v))
        for ri in r:
            if not ri.startswith('Category:') and ri not in indexes:
                try:
                    await cwkh.insert_one({'_id': ri})
                except Exception as e:
                    print(e)

async def main():
    cates = scan_sitemap()
    await download_category(cates)
    await put_to_mongo()

if __name__ == "__main__":
    if not os.path.exists(tmpfiledir):
        os.mkdir(tmpfiledir)

    asyncio.run(main())