# Twitter 的雪花算法，适合分布式群集生成唯一 ID

import time


class Snowflake(object):
    """
    雪花算法
    """
    epoch = 1288834974657  # 起点时间
    worker_id_bits = 5  # 机器 ID 位数
    data_center_id_bits = 5  # 机房 ID 位数
    max_worker_id = -1 ^ (-1 << worker_id_bits)  # 最大机器ID
    max_data_center_id = -1 ^ (-1 << data_center_id_bits)  # 最大机房ID
    sequence_bits = 12  # 序列ID占用位数
    worker_id_shift = sequence_bits  # 机器 ID 偏移量
    data_center_shift = sequence_bits + worker_id_bits  # 机房 ID 偏移量
    #  时间戳偏移量
    timestamp_shift = sequence_bits + worker_id_bits + data_center_id_bits
    sequence_mask = -1 ^ (-1 ^ sequence_bits)  # 最大序列
    sequence = 0  # 当前序列号
    last_timestamp = 0  # 上一次时间戳

    def __init__(self, worker_id, data_center_id):
        """
        设置机房 ID 和机器 ID
        Args:
            worker_id: 机器 ID
            data_center_id: 机房ID
        """
        self.worker_id = worker_id
        self.data_center_id = data_center_id

    def next_id(self):
        """
        获取 ID
        Returns:
            一个不重复的递增ID
        """
        timestamp = self.get_now()
        if timestamp < self.last_timestamp:
            raise "Clock moved backwards."
        if timestamp == self.last_timestamp:
            self.sequence = (self.sequence + 1) & self.sequence_mask
            if self.sequence == 0:
                timestamp = self.get_next_timestamp()
        else:
            self.sequence = 0

        self.last_timestamp = timestamp

        return ((timestamp - self.epoch) << self.timestamp_shift) | (
                self.data_center_id << self.worker_id_shift) | (
                        self.worker_id << self.worker_id_shift) | self.sequence

    def get_next_timestamp(self):
        """
        获取一个大于 last_timestamp 的时间戳
        Returns:
            一个大于当前 last_timestamp 的时间戳
        """
        timestamp = self.get_now()
        while timestamp <= self.last_timestamp:
            timestamp = self.get_now()
        return timestamp

    def get_now(self):
        """
        获取当前时间戳
        Returns:
            当前时间戳
        """
        return int(time.time()*1000)
