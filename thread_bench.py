#!/usr/bin/env python3


import concurrent.futures
import random

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import numpy as np


def worker(max_len=150):
    array = [[0] * max_len] * max_len
    # array = np.zeros((max_len, max_len), dtype=int)
    for i in range(max_len):
        for j in range(max_len):
            array[i][j] = random.randint(0, 98)

    for i in range(max_len):
        for j in range(max_len):
            if array[i][j] == 99:
                pass


def gen_threads(max_workers):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker) for i in range(max_workers)]
        for future in concurrent.futures.as_completed(futures):
            pass


def gen_processes(max_workers):
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker) for i in range(max_workers)]
        for future in concurrent.futures.as_completed(futures):
            pass


if __name__ == '__main__':
    import timeit
    print('just worker')
    for i in range(3):
        print(timeit.timeit('worker()', setup='from __main__ import worker', number=1))

    for num in [1, 2, 4, 8, 16]:
        print('worker in %s process' % num)
        for i in range(3):
            print(timeit.timeit('gen_processes(%s)' % num,
                                setup='from __main__ import gen_processes',
                                number=1))

    for num in [1, 2, 4, 8, 16]:
        print('worker in %s thread' % num)
        for i in range(3):
            print(timeit.timeit('gen_threads(%s)' % num,
                                setup='from __main__ import gen_threads',
                                number=1))
