import unittest
import asyncio
import ycdsjtools.aio.supervisor as aiosupervisor


class TestRunSupervised(unittest.TestCase):

    def test_basic(self):
        async def fun():
            t = asyncio.create_task(asyncio.sleep(2))

            await asyncio.sleep(0)

            await aiosupervisor.cancel_task(t)
            self.assertTrue(t.done())

        asyncio.run(fun())

    def test_nocancel(self):
        async def nocancel():
            while True:
                try:
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    break

        async def fun():
            t = asyncio.create_task(nocancel())

            await asyncio.sleep(0)

            with self.assertRaises(aiosupervisor.CancelTaskWarning):
                await aiosupervisor.cancel_task(t)

        asyncio.run(fun())

    def test_cancel_supervised(self):
        started = False

        async def noop():
            nonlocal started
            started = True
            await asyncio.sleep(2)

        async def fun():
            t = asyncio.create_task(aiosupervisor.run_supervised(noop, name='test_cancel_supervised'))

            await asyncio.sleep(0)

            await aiosupervisor.cancel_task(t)
            self.assertTrue(t.done())

        asyncio.run(fun())
        self.assertTrue(started, "The task didn't start, test is invalid")

    def test_nocancel_supervised(self):
        started = False

        async def nocancel():
            try:
                nonlocal started
                started = True
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass  # this will avoid passing the cancel to the supervisor

        async def fun():
            cevent = asyncio.Event()
            t = asyncio.create_task(aiosupervisor.run_supervised(nocancel, name='test_nocancel_supervised', cancel_event=cevent))

            await asyncio.sleep(0)

            cevent.set()
            await aiosupervisor.cancel_task(t)
            self.assertTrue(t.done())

        asyncio.run(fun())
        self.assertTrue(started)

    # this cannot be cancelled, the only way to close it is if it raise some other exception
    # this test is not run, kept here for reference
    def bad_test_wontcancel_supervised(self):
        async def wontcancel():
            while True:
                try:
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    pass  # this will avoid passing the cancel to the supervisor

        async def fun():
            t = asyncio.create_task(aiosupervisor.run_supervised(wontcancel, name="bad_test_wontcancel_supervised"))

            await asyncio.sleep(0.1)

            await aiosupervisor.cancel_task(t)
            self.assertTrue(t.done())

        asyncio.run(fun())

    # the only way to cancel `wontcancel` is by using external events
    def test_wontcancel_supervised_event(self):
        started = False

        async def wontcancel(cancel_event: asyncio.Event):
            nonlocal started
            while True:
                if cancel_event.is_set():
                    raise asyncio.CancelledError()

                try:
                    started = True
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    pass  # this will avoid passing the cancel to the supervisor

        async def fun():
            cevent = asyncio.Event()
            t = asyncio.create_task(
                aiosupervisor.run_supervised(
                    wontcancel, args=(cevent,),
                    name="test_wontcancel_supervised_event",
                    cancel_event=cevent
                )
            )

            await asyncio.sleep(0.1)

            cevent.set()
            await aiosupervisor.cancel_task(t)
            self.assertTrue(t.done())
            self.assertTrue(started, "The task didn't start, test is invalid")

        asyncio.run(fun())


if __name__ == '__main__':
    unittest.main()
