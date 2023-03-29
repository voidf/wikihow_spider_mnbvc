import asyncio
import json
import re
from typing import Iterable
from urllib.parse import unquote

import os
import random
from hashlib import sha512
from urllib.parse import urlparse
import motor.motor_asyncio
import pymongo
client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
dbenwikihow = client['wikihowEN']
collection_paths = dbenwikihow['path']
collection_char = dbenwikihow['char']


def cat(*args): return '/'.join(args)

tmpfiledir = 'tmpdownload'

async def insert_without_exception(k, v=None):
    try:
        await collection_paths.insert_one({'_id': k}) 
    except:
        pass
filter_in = [
    '?amp=1',
    '.js',
    '.css',
    '.php',
    '.jpg',
    '.png',
    '.mp4',
    '.ico',
    '.rss',
    '.xml',
    '.pdf',
]

filter_prefix = [
    'wikiHowTo',
    'Special:',
    'Template:',
    'User_talk:',
    'wikiHow:',
    'wikiHow_talk:',
    'User:',
    'index.php',
    'Author/',
    'Course/',
    'Questions/',
    'Relationships/',
    'Sample/',
    'Watch/',
    'extensions/',
    'Games/',
    '‘/wikiHow:Content-Management’'
]

def prefilter(s: str):
    for _ in filter_in:
        # if s.endswith(_):
        if _ in s:
            return False
    for _ in filter_prefix:
        if s.startswith(_):
            return False
    return True
# {_id:/^Author\//}
# db.path.deleteMany({_id:/^Course\//})
# db.path.deleteMany({_id:/^Questions\//})
async def dump_urls(_: str, s: set):
    for i in re.findall(r'href="/(.?)"', _):
        # ui = unquote(unquote(i))
        ui = urlparse(ui).path.removesuffix('.html').removesuffix('.htm')
        if ui not in s and prefilter(ui):
            print(ui)
            s[ui] = i
    for i in re.findall(r'href="https://www.wikihow.com/(.*?)"', _):
        # ui = unquote(unquote(i))
        ui = urlparse(ui).path.removesuffix('.html').removesuffix('.htm')
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

async def expand_in_dir(file_dir=r'D:\wikihow_en'):
    s = {}
    for i in os.listdir(file_dir):
        if 1 or i.endswith('.htm'):
            try:
                with open(cat(file_dir, i), 'r', encoding='utf-8', errors='ignore') as f:
                    html = f.read()
                await dump_urls(html, s)
            except Exception as e:
                print(e, i)
    tasks = [insert_without_exception(k) for k, v in s.items()]
    await asyncio.gather(*tasks)

async def expand_in_mongo():
    li = []
    ids = []
    cur = collection_paths.find({
        'scanned': {'$exists': False},
        'html': {'$exists': True},
        # '$expr':{
        #     '$gt':
        #     [{'$strLenCP':'$html'}, 4096]
        # }
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

async def clear_db():
    for _ in (await collection_paths.find({
        't404': True
    }).to_list(None)):
        if not prefilter(iid := _['_id']):
            await collection_paths.delete_one({'_id': iid})
            print('delete', iid)



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

    asyncio.run(clear_db())