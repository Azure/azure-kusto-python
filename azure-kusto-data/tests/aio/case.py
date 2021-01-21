import unittest
from asyncio import new_event_loop, set_event_loop


class TestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.loop = new_event_loop()
        set_event_loop(self.loop)

    def tearDown(self) -> None:
        self.loop.close()
