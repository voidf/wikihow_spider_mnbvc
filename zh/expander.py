"""非联网工具"""

import asyncio
import json
import re
from typing import Iterable
from urllib.parse import unquote

import os
import random
from hashlib import sha512

import motor.motor_asyncio
import pymongo
client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
db = client['zhihu']
dbzhwikihow = client['wikihowZH']
collection_paths = dbzhwikihow['path']
collection_char = dbzhwikihow['char']

cwkh = db['wkh']




def cat(*args): return '/'.join(args)

tmpfiledir = 'tmpdownload'

async def insert_without_exception(k, v=None):
    try:
        await collection_paths.insert_one({'_id': k}) 
    except:
        pass

def prefilter(s: str):
    return (not s.endswith('?amp=1')) and not s.endswith('.js') and not s.endswith('.css') and '.php' not in s and s != 'feed.rss'

async def dump_urls(_: str, s: set):
    for i in re.findall(r'href="/(.?)"', _):
        ui = unquote(unquote(i))
        if ui not in s and prefilter(ui):
            print(ui)
            s[ui] = i
    for i in re.findall(r'href="https://zh.wikihow.com/(.*?)"', _):
        ui = unquote(unquote(i))
        if ui not in s and prefilter(ui):
            print(ui)
            s[ui] = i

async def expand_using_char():
    cur = collection_paths.find({}, projection={'_id':1})
    li = set()
    async for i in cur:
        for j in i['_id']:
            li.add(j)
            print(len(li))
    await asyncio.gather(*(collection_char.insert_one({'_id': x}) for x in li))

async def expand_in_dir(file_dir=r'D:\wikihow_zh2\tmpdownload'):
    s = {}
    for i in os.listdir(file_dir):
        if 1 or i.endswith('.htm'):
            try:
                with open(cat(file_dir, i), 'r', encoding='utf-8') as f:
                    html = f.read()
                await dump_urls(html, s)
            except Exception as e:
                print(e, i)
    tasks = [collection_paths.insert_one({'_id': k, 'unquoted': v}) for k, v in s.items()]
    await asyncio.gather(*tasks)

async def expand_in_mongo():
    li = []
    ids = []
    cur = collection_paths.find({
        'scanned': {'$nin': [True]},
        'html': {'$nin': [None]},
        '$expr':{
            '$gt':
            [{'$strLenCP':'$html'}, 4096]
        }
    })
    s = {}

    async for _ in cur:
        li.append(_['html'])
        ids.append(_['_id'])

    for idd, _ in zip(ids, li):
        await dump_urls(_, s)
        await collection_paths.update_one({'_id': idd}, {'$set': {'scanned': True}})
    for i in ids:
        s.pop(i, None)
    tasks = [insert_without_exception(si, ui) for si, ui in s.items()]
    await asyncio.gather(*tasks)
        
"""
db.path.deleteMany({_id: /\?amp=1$/})
"""


async def main():
    cached = set()
    cur = cwkh.find({})

    async for cc in cur:
        cached.add(cc['_id'])

    cid = 0
    for i in os.listdir('.'):
        if i.endswith('.htm') and i != 'Special-Sitemap.htm':
            with open(i, 'r', encoding='utf-8') as f:
                rawtext = f.read()
            related = re.findall(r'<a class="related-wh" href="/(.*?)">', rawtext)
            if len(related) != 18:
                print(i, len(related))
                # assert len(related) > 0
            try:
                for j in related:
                    if j in cached:
                        continue
                    await cwkh.insert_one({'_id': j})
                    cid += 1
                    print(cid)
            except pymongo.errors.DuplicateKeyError as e:
                # print(e)
                pass


if __name__ == "__main__":
    if not os.path.exists(tmpfiledir):
        os.mkdir(tmpfiledir)

    asyncio.run(expand_in_mongo())
    asyncio.run(main())
    asyncio.run(expand_in_dir())
    asyncio.run(expand_using_char())