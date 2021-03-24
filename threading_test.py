import threading
from queue import Queue
import time
import asyncio


def testThread(num):
    time.sleep(2);
    print (threading.active_count())

if __name__ == '__main__':
    for i in range(100):
        t = threading.Thread(target=testThread, args=(i,))
        t.start()



'''def blocking_io():
    print(f"start blocking_io at {time.strftime('%X')}")
    # Note that time.sleep() can be replaced with any blocking
    # IO-bound operation, such as file operations.
    time.sleep(1)
    print(f"blocking_io complete at {time.strftime('%X')}")

async def main():
    print(f"started main at {time.strftime('%X')}")

    await asyncio.gather(
        asyncio.to_thread(blocking_io),
        asyncio.sleep(1))

    print(f"finished main at {time.strftime('%X')}")


asyncio.run(main())'''