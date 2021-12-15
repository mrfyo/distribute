import time

from kazoo.client import KazooClient, KazooState

zk = KazooClient(hosts='192.168.153.131:2181')


def state_listener(state: str):
    if state == KazooState.LOST:
        print("lost")
    elif state == KazooState.SUSPENDED:
        print("suspended")
    else:
        print(state)


@zk.ChildrenWatch("")
def get_listener(event):
    print(event)


zk.start()

zk.get("/test_watch", watch=get_listener)

data, stat = zk.get("/test_watch")
print(data, stat)

print(zk.get_children("/"))

print(zk.create("/test_app", ephemeral=True, sequence=True))

print(zk.delete("/test_app", recursive=True))

print(zk.exists("/test_app"))

time.sleep(10)

# zk.set("/test_watch", b'5555')
