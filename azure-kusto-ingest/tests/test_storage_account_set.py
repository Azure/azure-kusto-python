import unittest
import time

from azure.kusto.ingest._storage_account_set import _RankedStorageAccountSet


class RankedStorageAccountSetTests(unittest.TestCase):
    ACCOUNT_1: str = "ACCOUNT_1"
    ACCOUNT_2: str = "ACCOUNT_2"
    ACCOUNT_3: str = "ACCOUNT_3"
    ACCOUNT_4: str = "ACCOUNT_4"
    ACCOUNT_5: str = "ACCOUNT_5"

    def create_storage_account_set(self, time_provider=time.time) -> _RankedStorageAccountSet:
        storage_account_set = _RankedStorageAccountSet(time_provider=time_provider)
        storage_account_set.add_storage_account(self.ACCOUNT_1)
        storage_account_set.add_storage_account(self.ACCOUNT_2)
        storage_account_set.add_storage_account(self.ACCOUNT_3)
        storage_account_set.add_storage_account(self.ACCOUNT_4)
        storage_account_set.add_storage_account(self.ACCOUNT_5)
        return storage_account_set

    def test_check_rank_when_no_data(self):
        # When there's no success/failure data, all accounts should have the same rank
        storage_account_set = self.create_storage_account_set()
        self.assertEqual(storage_account_set.get_storage_account(self.ACCOUNT_1).get_rank(), 1)
        self.assertEqual(storage_account_set.get_storage_account(self.ACCOUNT_2).get_rank(), 1)
        self.assertEqual(storage_account_set.get_storage_account(self.ACCOUNT_3).get_rank(), 1)
        self.assertEqual(storage_account_set.get_storage_account(self.ACCOUNT_4).get_rank(), 1)
        self.assertEqual(storage_account_set.get_storage_account(self.ACCOUNT_5).get_rank(), 1)
        # Same test but with shuffled accounts
        ranked_accounts = storage_account_set.get_ranked_shuffled_accounts()
        for account in ranked_accounts:
            self.assertEqual(account.get_rank(), 1)
        pass

    def test_accounts_are_shuffled(self):
        # Create long list of accounts and verify that they are shuffled
        storage_account_set = _RankedStorageAccountSet()
        for i in range(100):
            storage_account_set.add_storage_account(f"ACCOUNT_{i}")
        ranked_accounts_1 = storage_account_set.get_ranked_shuffled_accounts()
        ranked_accounts_2 = storage_account_set.get_ranked_shuffled_accounts()
        # Use set to verify same accounts are in both lists
        a = set(ranked_accounts_1)
        b = set(ranked_accounts_2)
        self.assertEqual(a, b)
        # Use != to verify that the lists are not identical in order
        self.assertNotEqual(ranked_accounts_1, ranked_accounts_2)

    def test_check_rank_when_all_failure(self):
        current_time = 0

        def time_provider():
            return current_time

        storage_account_set = self.create_storage_account_set(time_provider)

        # Simulate data in 10 seconds passing
        for current_time in range(10):
            storage_account_set.add_account_result(self.ACCOUNT_1, False)
            storage_account_set.add_account_result(self.ACCOUNT_2, False)
            storage_account_set.add_account_result(self.ACCOUNT_3, False)
            storage_account_set.add_account_result(self.ACCOUNT_4, False)
            storage_account_set.add_account_result(self.ACCOUNT_5, False)

        # All accounts should have the same rank (0)
        self.assertEqual(storage_account_set.get_storage_account(self.ACCOUNT_1).get_rank(), 0)
        self.assertEqual(storage_account_set.get_storage_account(self.ACCOUNT_2).get_rank(), 0)
        self.assertEqual(storage_account_set.get_storage_account(self.ACCOUNT_3).get_rank(), 0)
        self.assertEqual(storage_account_set.get_storage_account(self.ACCOUNT_4).get_rank(), 0)
        self.assertEqual(storage_account_set.get_storage_account(self.ACCOUNT_5).get_rank(), 0)

    def test_check_rank_when_success_rate_is_different(self):
        current_time = 0

        def time_provider():
            return current_time

        storage_account_set = self.create_storage_account_set(time_provider)

        # Simulate data in 10 seconds passing
        for current_time in range(10):
            storage_account_set.add_account_result(self.ACCOUNT_1, True)  # 100% success
            storage_account_set.add_account_result(self.ACCOUNT_2, current_time % 2 == 0)  # 50% success
            storage_account_set.add_account_result(self.ACCOUNT_3, current_time % 3 == 0)  # 33% success
            storage_account_set.add_account_result(self.ACCOUNT_4, current_time % 4 == 0)  # 25% success
            storage_account_set.add_account_result(self.ACCOUNT_5, False)  # 0% success

        # Get shuffled accounts
        ranked_accounts = storage_account_set.get_ranked_shuffled_accounts()
        # Verify that the accounts are ranked in the correct order
        self.assertEqual(ranked_accounts[0].get_account_name(), self.ACCOUNT_1)
        self.assertIn(ranked_accounts[1].get_account_name(), [self.ACCOUNT_2, self.ACCOUNT_3])
        self.assertIn(ranked_accounts[2].get_account_name(), [self.ACCOUNT_2, self.ACCOUNT_3])
        self.assertEqual(ranked_accounts[3].get_account_name(), self.ACCOUNT_4)
        self.assertEqual(ranked_accounts[4].get_account_name(), self.ACCOUNT_5)
