#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: lqk  
@contact: 798244092@qq.com 
@site: https://github.com/lqkweb 
@file: bloomfilter.py 
@time: 2019/7/8 4:43 PM 
"""
import hashlib


class HashMap(object):
    def __init__(self, m, seed):
        self.m = m
        self.seed = seed

    def hash(self, value):
        """
        Hash Algorithm
        :param value: Value
        :return: Hash Value
        """
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return (self.m - 1) & ret


class BloomFilter(object):
    def __init__(self, server, key, bit=30, blockNum=1):
        """
        Initialize BloomFilter
        :param server: Redis Server
        :param key: BloomFilter Key
        :param bit: m = 2 ^ bit
        :param hash_number: the number of hash function
        """
        # default to 1 << 30 = 10,7374,1824 = 2^30 = 128MB, max filter 2^30/hash_number = 1,7895,6970 fingerprints
        self.m = 1 << bit
        self.seeds = [5, 7, 11, 13, 31, 37, 61]
        self.server = server
        self.key = key
        self.blockNum = blockNum
        self.hashfunc = []
        for seed in self.seeds:
            self.hashfunc.append(HashMap(self.m, seed))

    def exists(self, value):
        """
        if value exists
        :param value:
        :return:
        """
        if not value:
            return False
        # m5 = md5()
        # m5.update(value.encode())
        value = hashlib.sha256(value.encode("utf-8")).hexdigest()
        ret = True
        name = self.key + str(int(value[0:2], 16) % self.blockNum)
        for f in self.hashfunc:
            loc = f.hash(value)
            ret = ret & self.server.getbit(name, loc)
        return ret

    def insert(self, value):
        """
        add value to bloom
        :param value:
        :return:
        """
        value = hashlib.sha256(value.encode("utf-8")).hexdigest()
        name = self.key + str(int(value[0:2], 16) % self.blockNum)
        for f in self.hashfunc:
            loc = f.hash(value)
            self.server.setbit(name, loc, 1)


if __name__ == '__main__':
    import redis

    redis_client = redis.StrictRedis(host='127.0.0.1', password='', port='6379', db=1)
    filter_key = "filterKey"
    bit_key = 15
    block_num_key = 1
    bf = BloomFilter(redis_client, filter_key, bit_key, block_num_key)
    url = "http://www.baidu.com"
    filter_data = hashlib.md5(url.encode("utf-8")).hexdigest()
    if bf.exists(filter_data):
        print("已存在")
    else:
        bf.insert(filter_data)
        print("加入去重")
