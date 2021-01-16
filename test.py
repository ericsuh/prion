import asyncio


async def testing(evt):
    await asyncio.sleep(1)
    evt.set()


async def main():
    evt = asyncio.Event()
    asyncio.create_task(testing(evt))
    print(len(asyncio.all_tasks()))
    await evt.wait()
    print(len(asyncio.all_tasks()))


asyncio.run(main())
