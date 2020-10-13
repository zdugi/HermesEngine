import requests as bot
import urllib.parse
import queue
import threading
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import random
import yaml
import os


class HistorySet:
    def __init__(self):
        self._visited = set()
        self._lock = threading.Lock()

    def add(self, location):
        with self._lock:
            if location not in self._visited:
                self._visited.add(location)

    def visited(self, location):
        with self._lock:
            return location in self._visited

    def __len__(self):
        return len(self._visited)


class Page:
    def __init__(self, location, parent=None):
        self._location = location
        self._response = None
        self._code = 0
        self._time = 0
        self._parent = parent

    def get(self, timeout):
        self._time = time.time()
        self._response = bot.get(self._location, timeout=timeout)
        self._time = time.time() - self._time
        self._code = self._response.status_code

    def code(self):
        return self._code

    def time(self):
        return self._time

    def response(self):
        return self._response

    def parent(self):
        return self._parent

    def location(self):
        return self._location


# page workers lock
processLock = threading.Lock()

# running info counters
process = 0
exceptions_count = 0


def generate_url(root, target):
    if target.find('http') == 0:
        return target
    return urljoin(root, target)


def load_config(config_path):
    path = os.path.abspath(config_path)

    if not os.path.exists(path):
        print(f'[Error] Config file does not exist: {path}. Create one or download it from repository.')
        exit(1)

    with open(path) as config_input:
        config_dict = yaml.safe_load(config_input)

    if 'params' not in config_dict or 'startPool' not in config_dict or 'workers' not in config_dict:
        print('f[Error] Config file is not valid.')
        exit(1)

    params = config_dict['params']
    start_pool = config_dict['params']
    workers = config_dict['workers']

    # params validation
    if 'linksQueueMax' not in params or not isinstance(params['linksQueueMax'], int) or params['linksQueueMax'] <= 0:
        print('[Error] Param linksQueueMax is not valid.')
        exit(1)

    if 'requestMaxTime' not in params or not isinstance(params['requestMaxTime'], int) or params['requestMaxTime'] <= 0:
        print('[Error] Param requestMaxTime is not valid.')
        exit(1)

    if 'requestsPause' not in params or not isinstance(params['requestsPause'], float) or params['requestsPause'] <= 0:
        print('[Error] Param requestsPause is not valid.')
        exit(1)

    if 'threshHold' not in params or not isinstance(params['threshHold'], float) or params['requestsPause'] <= 0:
        print('[Error] Param threshHold is not valid.')
        exit(1)

    if 'sitesLogFile' not in params or not isinstance(params['sitesLogFile'], str) or not params['sitesLogFile']:
        print('[Error] Param sitesLogFile is not valid.')
        exit(1)

    # start pool validation
    if len(start_pool) < 1:
        print('[Error] Start pool must contain at least one url.')
        exit(1)

    # workers validation
    if 'page' not in workers or not isinstance(workers['page'], int) or workers['page'] <= 0:
        print('[Error] Number of page workers is not valid.')
        exit(1)

    if 'collector' not in workers or not isinstance(workers['collector'], int) or workers['collector'] <= 0:
        print('[Error] Number of collector workers is not valid.')
        exit(1)

    return config_dict


def worker_collector(config, visited, requests_queue, raw_queue):
    while True:
        location, parent = requests_queue.get()
        try:

            if visited.visited(location):
                continue

            quark = Page(location, parent)
            quark.get(config['params']['requestMaxTime'])

            if quark.code() != 200:
                visited.add(location)
                continue

            raw_queue.put(quark)
        except bot.exceptions.RequestException as e:
            print(f'\r[Exception] {e}', end='')
            visited.add(location)
        time.sleep(config['params']['requestsPause'])


def worker_page(config, winners, visited, requests_queue, raw_queue):
    global process
    global exceptions_count

    while True:
        try:
            while True:
                quark = raw_queue.get()
                location, response = quark.location(), quark.response()

                if response.text is None or response.text.lower().find('body') == -1:
                    visited.add(location)
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')

                # calculate loss
                links = soup.find_all('link')
                styles = soup.find_all('style')
                scripts = soup.find_all('script')
                a = soup.find_all('a')

                styles = len(links) + len(styles)
                scripts = len(scripts)
                links = len(a)
                words = len(soup.body.text.split()) + 1 if soup.body else 0.001

                score = (styles + 1) * (scripts + 1) / (links + words)

                rnd = random.random()
                interval = max(0.7 - 10*score, 0.05)
                if rnd < interval:
                    for a in soup.find_all('a'):
                        if requests_queue.full():
                            break
                        if a.get('href') is None:
                            continue
                        loc = generate_url(location, a.get('href'))

                        requests_queue.put((loc, quark.location()))

                hostname = urllib.parse.urlparse(location).hostname

                with processLock:
                    if not winners.visited(hostname) and score <= config['params']['threshHold']:
                        winners.add(hostname)
                        with open(config['params']['sitesLogFile'], 'a+') as f:
                            f.write(location + ';' + quark.parent() + '\n')
                        visited.add(location)
                    process += 1
        except Exception as e:
            print(f'\n[Exception] {e}', end='')
            with processLock:
                exceptions_count += 1


def main():
    program_start = time.time()
    config = load_config('../config.yml')

    # create structures
    visited = HistorySet()
    winners = HistorySet()
    requests_queue = queue.Queue(config['params']['linksQueueMax'])
    raw_queue = queue.Queue(config['params']['linksQueueMax'])

    # create empty file for winner sites
    with open(config['params']['sitesLogFile'], 'w') as f:
        f.write('')

    # create start pool
    for s in config['startPool']:
        requests_queue.put((s, 'origin'))

    # create page workers
    for i in range(config['workers']['page']):
        t = threading.Thread(target=worker_page, args=(config, winners, visited, requests_queue, raw_queue,))
        t.start()

    # create collector workers
    for i in range(config['workers']['collector']):
        t = threading.Thread(target=worker_collector, args=(config, visited, requests_queue, raw_queue,))
        t.start()

    # make some report on screen
    while True:
        time.sleep(0.1)
        t = (time.time() - program_start) / 60
        print('\rNew_links={:10d} Fetched_pages={:10d} Proceeded={:10d} Exceptions={:10d} Suitable={:10d} Time: {}'
              .format(requests_queue.qsize(), raw_queue.qsize(), process, exceptions_count, len(winners), t), end='')


if __name__ == '__main__':
    main()
