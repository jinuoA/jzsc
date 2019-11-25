#!/usr/bin/env/ python
# -*- coding:utf-8 -*-
# Author:Mr.Xu

import redis
from spider.db.config import *


class RedisClient(object):

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD):
        """
        初始化
        :param host: Redis 地址
        :param port: Redis 端口
        :param password: Redis 密码
        """
        if password:
            self._db = redis.StrictRedis(host=host, port=port, db=4, password=password, decode_responses=True)
        else:
            self._db = redis.StrictRedis(host=host, port=port, db=4, decode_responses=True)

    def flush(self):
        """
        清空数据库
        flush db
        :return:
        """
        self._db.flushall()

    def add(self, idx=None, rediskey=None, score=INITIAL_SCORE):
        """
        添加ID,设置分数为初始分数10
        :param comp_id: comp_id
        :param score: 分数
        :return: 添加结果
        """
        if not self._db.zscore(rediskey, idx):
            print('save ', idx)
            return self._db.zadd(rediskey, {idx: score})
        else:
            print(idx, ' already exists')
            return None

    def add_one(self, idx=None, rediskey=None):
        """
        添加ID,设置分数为初始分数10
        :param comp_id: comp_id
        :param score: 分数
        :return: 添加结果
        """
        if not self._db.zscore(rediskey, idx):
            print('save ', idx)
            return self._db.zadd(rediskey, idx)
        else:
            print(idx, ' already exists')
            return None


    def deletes(self, idx=None, rediskey=None, ):
        return self._db.zrem(rediskey, idx)

    def exists(self, idx, rediskey=None):
        """
        判断是否存在代理
        :param proxy: 代理
        :return: 是否存在
        """
        score = self._db.zscore(rediskey, idx)
        if score is not None:
            return True
        else:
            return False
        # return self._db.zscore(self._rediskey, idx) == None

    def count(self, rediskey=None):
        """
        获取ID总的数量
        :return: 数量
        """
        return self._db.zcard(rediskey)

    def all(self, rediskey=None):
        """
        获取全部代理
        :return: 全部代理列表
        """
        return self._db.zrangebyscore(rediskey, MIN_SCORE, MAX_SCORE)

    def batch(self, rediskey=None, start=0, stop=100):
        """
        批量获取
        :param start: 开始索引
        :param stop: 结束索引
        :return: idx列表
        """
        return self._db.zrevrange(rediskey, start, stop)


if __name__ == '__main__':
    conn = RedisClient()
    result = conn.batch()
    print(result)
