
#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
MIT License

Copyright (c) 2022 Rhythm-2019

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
Footer
© 2022 GitHub, Inc.
Footer navigation
Terms
Privacy

-------------------------------------------------
@File    :   calculater.py
@Time    :   2023/02/06 13:55:06
@Author  :   Rhythm-2019 
@Version :   1.0
@Contact :   rhythm_2019@163.com
@Desc    :   Redis 对象内存占用计算器
-------------------------------------------------
Change Activity: 
- 完成对象内存占用计算
-------------------------------------------------
'''
__author__ = 'Rhythm-2019'

import sys
from functools import reduce

def str_bytes(s):
    return len(s.encode('utf-8'))
def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

class Field(object):
    def __init__(self, name, bytes, desc=None) -> None:
        self._name = name
        self._bytes = bytes
        self._desc = desc
    
    @property
    def name(self):
        return self._name
    @property
    def bytes(self):
        return self._bytes
    
class RedisStruct(object):
    
    def __init__(self, name, fields, refs=None) -> None:
        self._name = name
        self._fields = fields
        self._refs = [] if refs is None else refs
    
    @property
    def name(self):
        return self._name
    
    def add_ref(self, ref):
        self._refs.append(ref)
        return self
    
    def add_refs(self, refs):
        self._refs += refs
        return self
    
    def bytes(self):
        bytes = reduce(lambda x, y: x + y, map(lambda field: field.bytes, self._fields))
        if len(self._refs) != 0:
            bytes += reduce(lambda x, y: x + y, map(lambda ref: ref.bytes(), self._refs))
        return bytes
    
    def print(self, deepth=0):
        print('\t' * deepth, self._name)
        for field in self._fields:
            print('\t' * deepth, field.name, field.bytes)
        for ref in self._refs:
            ref.print(deepth + 1)
        return self
            
class Array(object):
    def __init__(self, bytes) -> None:
        self._bytes = bytes
    
    def bytes(self):
        return self._bytes
    def print(self , deepth=0):
        print('\t' * (deepth - 1), 'Array usage', self._bytes, 'bytes')
        return self
    
class RedisObject(RedisStruct):
    def __init__(self) -> None:
        super().__init__('RedisObject', [
            Field('type', 4),
            Field('encoding', 4),
            Field('refcount', 4),
            Field('lru', 4),
            Field('ptr', 8),
        ])

# SDS
class SDS(RedisStruct):
    def __init__(self) -> None:
        super().__init__('SDS', [
            Field('free', 4),
            Field('len', 4),
            Field('buf', 8)
        ])        

# AdList
class AdListNode(RedisStruct):
    def __init__(self) -> None:
        super().__init__('AdListNode', [
            Field('prev', 8),
            Field('next', 8),
            Field('value', 8),
        ])        
class AdList(RedisStruct):
    def __init__(self) -> None:
        super().__init__('AdList', [
            Field("head", 8),
            Field("tail", 8),
            Field("dup", 8),
            Field("free", 8),
            Field("match", 8),
            Field("len", 8),
        ])  
        
# ZipList      
class Ziplist(RedisStruct):
    def __init__(self) -> None:
        super().__init__('Ziplist', [
            Field('zlbytes', 4),
            Field('zltail', 4),
            Field('zllen', 2),
            Field('zlend', 1),
        ])    
class ZipListNode(RedisStruct):
    def __init__(self, previous_enrty_length, encoding, content) -> None:
        super().__init__('ZiplistNode', [
            Field('previous_enrty_length', previous_enrty_length),
            Field('encoding', encoding),
            Field('content', content),
        ])  

def create_ZipListNodeList(l):
    zn_list = []
    for i in range(len(l)):
        if i == 0 or str_bytes(l[i - 1]) <= 2**8:
            previous_enrty_length = 1
        else:
            previous_enrty_length = 5
        
        if l[i].isnumeric() or str_bytes(l[i]) < 2**8:
            encoding = 1
        elif str_bytes(l[i]) < 2**14:
            encoding = 2
        else:
            encoding = 5
            
        zn_list.append(ZipListNode(previous_enrty_length, encoding, str_bytes(l[i])))
    return zn_list


# Dict
class DictType(RedisStruct):
    def __init__(self) -> None:
        super().__init__('DictType', [
            Field('hashFunction', 8),
            Field('keyDup', 8),
            Field('valDup', 8),
            Field('keyCompare', 8),
            Field('keyDestructor', 8),
            Field('valDestructor', 8),
        ])    
class DictHt(RedisStruct):
    def __init__(self) -> None:
        super().__init__('DictHt', [
            Field('size', 8),
            Field('sizemask', 8),
            Field('used', 8),
            Field('table', 8),
        ]) 
class Dict(RedisStruct):
    def __init__(self) -> None:
        super().__init__('Dict', [
            Field('type', 8),
            Field('privdata',8),
            Field('ht',8),
            Field('rehashidx',8),
            Field('iterators',4),
        ]) 
class DictEntry(RedisStruct):
    def __init__(self) -> None:
        super().__init__('DictEntry', [
            Field('key', 8),
            Field('value', 8),
            Field('next', 8),
        ]) 

def create_DictEntryArray(kv_dict):
    sz = len(kv_dict)
    
    # 计算 ht 大小
    while True:
        tmp = sz
        sz &= (sz - 1)
        if sz == 0:
            break
    dict_sz = tmp << 1
    
    es = []
    for k, v in kv_dict.items():
        e = DictEntry()
        e.add_ref(string_obj(k))
        if v is not None and not isfloat(v):
            e.add_ref(string_obj(v))
        es.append(e)
    for _ in range(len(kv_dict), dict_sz):
        es.append(DictEntry())
    return es
            
# Intset
class Intset(RedisStruct):
    def __init__(self) -> None:
        super().__init__('Intset', [
            Field('encoding', 4),
            Field('length', 4), 
            Field('content', 8),
        ]) 

def create_IntsetArray(int_set):
    encoding = 1
    for s in int_set:
        i = int(s)
        if i > 2**32:
            encoding = 8
        elif i > 2**16:
            encoding = 4
        elif i > 2**8:
            encoding = 2
    return Array(encoding * len(int_set)) 

# Skiplist
class ZskiplistLevel(RedisStruct):
    def __init__(self) -> None:
        super().__init__('ZskiplistLevel', [
                Field('forward', 8),
                Field('span', 4),
        ])
class zskiplistNode(RedisStruct):
    def __init__(self) -> None:
        super().__init__('zskiplistNode', [
                Field('obj', 8),
                Field('score', 8),
                Field('zskiplistNode', 8),
                Field('level', 8),
        ])
class ZSkiplist(RedisStruct):
    def __init__(self) -> None:
        super().__init__('ZSkiplist', [
            Field('head', 8),
            Field('tail', 8),
            Field('length', 8),
            Field('level', 4),
        ]) 
def create_SkiplistNodeList(ms_dict):
    # Dummy node    
    nl = [zskiplistNode().add_refs(32 * [ZskiplistLevel()])]
    for m in ms_dict.keys():
        #  ZSET 中 Dict 和 SkipList 的 member 是共享的，m 被放入到 Dict 中
        nl.append(zskiplistNode().add_refs(32 * [ZskiplistLevel()]))
    return nl

def string_obj(s):
    if s.isnumeric():
        return RedisObject()
    else:
        return RedisObject().add_ref(SDS().add_ref(Array(str_bytes(s))))
       
def list_obj(l):
    def is_use_ziplist(l):
        if len(l) >= 64:
            return False
        for s in l:
            # TODO 数字过大需要使用字符串存储
            if not s.isnumeric() and len(s) > 256:
                return False
        return True
    
    if is_use_ziplist(l):
        return RedisObject().add_ref(Ziplist().add_refs(create_ZipListNodeList(l)))
    else:
        return RedisObject().add_ref(().add_refs([string_obj(s) for s in l]))

def hash_obj(kv_dict):
    def is_use_ziplist(kv_dict):
        if len(kv_dict) >= 64:
            return False
        for k, v in kv_dict.items():
            if not k.isnumeric() and len(k) > 256:
                return False
            if not v.isnumeric() and len(v) > 256:
                return False
        return True
    
    if is_use_ziplist(kv_dict):
        l = []
        for k, v in kv_dict.items():
            l.append(k)
            l.append(v)
        return RedisObject().add_ref(Ziplist().add_refs(create_ZipListNodeList(l)))    
    else:
        return RedisObject().add_ref(Dict().add_refs([DictHt().add_refs(create_DictEntryArray(kv_dict)), DictHt()]))
    
def set_obj(set):
    def is_use_intset(set):
        if len(set) > 512:
            return False
        for s in set:
            if not s.isnumeric():
                return False
        return True
    if is_use_intset(set):
        return RedisObject().add_ref(Intset().add_ref(create_IntsetArray(set)))
    else:
        kv_dict = {}
        for s in set:
            kv_dict[s] = None
        return RedisObject().add_ref(Dict().add_refs([DictHt().add_refs(create_DictEntryArray(kv_dict)), DictHt()]))


def zset_obj(ms_dict):
    def is_use_ziplist(ms_dict):
        if len(ms_dict) >= 128:
            return False
        for k in ms_dict.keys():
            if len(k) >= 64:
                return False
        return True
    if is_use_ziplist(ms_dict):
        l = []
        for m, s in ms_dict.items():
            l.append(m)
            l.append(s)
        return RedisObject().add_ref(Ziplist().add_refs(create_ZipListNodeList(l)))    
    else:
        return (RedisObject().add_ref(ZSkiplist().add_refs(create_SkiplistNodeList(ms_dict)))
                    .add_ref(Dict().add_refs([DictHt().add_refs(create_DictEntryArray(ms_dict)), DictHt()])))
def print_help():
    print('python redis-calculater.py [-p] type key value...')                        
    
    
def handle(show_detial, type, key, values): 
    krobj, vrobj = string_obj(key), None
    match type:
        case 'string':
            if len(values) < 1:
                print("please input value")
                return
            vrobj = string_obj(values[0])
        case 'list':
            vrobj = list_obj(values)
        case 'hash':
            kv_dict = {}
            for i in range(0, len(values), 2):
                if i > len(values) - 2:
                    break
                kv_dict[values[i]] = values[i + 1]
            vrobj = hash_obj(kv_dict)    
        case 'set':
            vrobj = set_obj(values)
        case 'zset':
            kv_dict = {}
            for i in range(0, len(values), 2):
                if i > len(values) - 2:
                    break
                if not isfloat(values[i + 1]):
                    print('score %s no expected'.format(values[i + 1]))
                    return
                kv_dict[values[i]] = values[i + 1]
            vrobj = zset_obj(kv_dict)
        case _:
            print_help()
            return
        
    if show_detial:
        print("Memory usage detail: ")
        print("================ KEY ================")
        krobj.print()
        print("================ VALUE ================")
        vrobj.print()
        
    print("KEY memory usage " + str(krobj.bytes()) + "bytes")
    print("VALUE memory usage " + str(vrobj.bytes()) + "bytes")
    print("total:  " + str(krobj.bytes() + vrobj.bytes()) + " bytes")
    
if __name__ == '__main__':
    
    argvs = sys.argv[1:]
    if len(argvs) < 3:
        print_help()
        sys.exit()
    
    # 是否显示详细
    show_detial = False
    if argvs[0] == '-p':
        show_detial = True
        argvs = argvs[1:]
    
    type = argvs[0]
    key = argvs[1]
    values = argvs[2:]
    handle(show_detial, type, key, values)           