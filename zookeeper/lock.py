import random
import threading
import time

from kazoo.client import KazooClient


def require_lock(zk: KazooClient, lock_name: str, require_timeout: float) -> str | bool:
    """
    try to require lock in blocking.
    :param zk: zookeeper client
    :param lock_name: name of lock
    :param require_timeout: if not require lock before timeout, cancel trying.
    :return: the identifier of lock if success, else None.
    """
    wait = True

    def try_lock(event):
        nonlocal wait
        wait = False

    path = f'/locks/{lock_name}'
    if not zk.exists(path):
        zk.create(path, makepath=True)

    cur_node = zk.create(f'{path}/{lock_name}', ephemeral=True, sequence=True)
    identifier = cur_node.split("/")[-1]

    end = time.time() + require_timeout
    failure = False
    while time.time() < end:
        children = zk.get_children(path)
        if len(children) == 0:
            failure = False

        children = sorted(children)

        if children[0] == identifier:
            return identifier

        for (i, child) in enumerate(children):
            if child == identifier:
                wait = True
                zk.get(path + "/" + children[i - 1], watch=try_lock)
                break

        if not wait:
            failure = False

        while wait and time.time() < end:
            time.sleep(0.1)

    if time.time() < end or failure:
        zk.delete(cur_node)


def release_lock(zk: KazooClient, lock_name: str, identifier: str):
    """
    release lock
    :param zk: zookeeper client
    :param lock_name: name of lock
    :param identifier: the identifier of lock
    :return:
    """
    path = '/'.join(['', 'locks', lock_name, identifier])
    zk.delete(path)


def test_lock(zk: KazooClient, number: int):
    """
    test the function of lock.
    :param zk: zookeeper client
    :param number: the number of thread
    :return:
    """
    lock_name = "apple"
    lock_id = require_lock(zk, lock_name, 20)
    try:
        if not lock_id:
            print("lock fail")
            return
        print(f'{number}: require lock')
        time.sleep(0.1)

        if random.randint(0, 100) > 50:
            return
    finally:
        release_lock(zk, lock_name, lock_id)
        print(f'{number}: release lock')


def main():
    zk = KazooClient(hosts="192.168.153.131:2181")
    zk.start()

    threads = []
    for i in range(10):
        t = threading.Thread(target=test_lock, args=(zk, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()


if __name__ == '__main__':
    main()
