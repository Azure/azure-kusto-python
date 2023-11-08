import time

from azure.kusto.ingest._storage_account_set import _RankedStorageAccountSet

ACCOUNT_1: str = "ACCOUNT_1"
ACCOUNT_2: str = "ACCOUNT_2"
ACCOUNT_3: str = "ACCOUNT_3"
ACCOUNT_4: str = "ACCOUNT_4"
ACCOUNT_5: str = "ACCOUNT_5"


def create_storage_account_set(time_provider=time.time) -> _RankedStorageAccountSet:
    storage_account_set = _RankedStorageAccountSet(time_provider=time_provider)
    storage_account_set.add_storage_account(ACCOUNT_1)
    storage_account_set.add_storage_account(ACCOUNT_2)
    storage_account_set.add_storage_account(ACCOUNT_3)
    storage_account_set.add_storage_account(ACCOUNT_4)
    storage_account_set.add_storage_account(ACCOUNT_5)
    return storage_account_set


def test_check_rank_when_no_data():
    # When there's no success/failure data, all accounts should have the same rank
    storage_account_set = create_storage_account_set()
    assert storage_account_set.get_storage_account(ACCOUNT_1).get_rank() == 1
    assert storage_account_set.get_storage_account(ACCOUNT_2).get_rank() == 1
    assert storage_account_set.get_storage_account(ACCOUNT_3).get_rank() == 1
    assert storage_account_set.get_storage_account(ACCOUNT_4).get_rank() == 1
    assert storage_account_set.get_storage_account(ACCOUNT_5).get_rank() == 1
    # Same test but with shuffled accounts
    ranked_accounts = storage_account_set.get_ranked_shuffled_accounts()
    for account in ranked_accounts:
        assert account.get_rank() == 1
    pass


def test_accounts_are_shuffled():
    # Create long list of accounts and verify that they are shuffled
    storage_account_set = _RankedStorageAccountSet()
    for i in range(100):
        storage_account_set.add_storage_account(f"ACCOUNT_{i}")
    ranked_accounts_1 = storage_account_set.get_ranked_shuffled_accounts()
    ranked_accounts_2 = storage_account_set.get_ranked_shuffled_accounts()
    # Use set to verify same accounts are in both lists
    a = set(ranked_accounts_1)
    b = set(ranked_accounts_2)
    assert a == b
    # Use != to verify that the lists are not identical in order
    assert ranked_accounts_1 != ranked_accounts_2


def test_check_rank_when_all_failure():
    current_time = 0

    def time_provider():
        return current_time

    storage_account_set = create_storage_account_set(time_provider)

    # Simulate data in 10 seconds passing
    for current_time in range(10):
        storage_account_set.add_account_result(ACCOUNT_1, False)
        storage_account_set.add_account_result(ACCOUNT_2, False)
        storage_account_set.add_account_result(ACCOUNT_3, False)
        storage_account_set.add_account_result(ACCOUNT_4, False)
        storage_account_set.add_account_result(ACCOUNT_5, False)

    # All accounts should have the same rank (0)
    assert storage_account_set.get_storage_account(ACCOUNT_1).get_rank() == 0
    assert storage_account_set.get_storage_account(ACCOUNT_2).get_rank() == 0
    assert storage_account_set.get_storage_account(ACCOUNT_3).get_rank() == 0
    assert storage_account_set.get_storage_account(ACCOUNT_4).get_rank() == 0
    assert storage_account_set.get_storage_account(ACCOUNT_5).get_rank() == 0


def test_check_rank_when_success_rate_is_different():
    current_time = 0

    def time_provider():
        return current_time

    storage_account_set = create_storage_account_set(time_provider)

    # Simulate data in 30 seconds passing
    for current_time in range(60):
        storage_account_set.add_account_result(ACCOUNT_1, True)  # 100% success
        storage_account_set.add_account_result(ACCOUNT_2, current_time % 10 != 0)  # ~90% success
        storage_account_set.add_account_result(ACCOUNT_3, current_time % 2 == 0)  # ~50% success
        storage_account_set.add_account_result(ACCOUNT_4, current_time % 3 == 0)  # ~33% success
        storage_account_set.add_account_result(ACCOUNT_5, False)  # 0% success

    # Get shuffled accounts
    ranked_accounts = storage_account_set.get_ranked_shuffled_accounts()
    # Verify that the accounts are ranked in the correct order
    assert ranked_accounts[0].get_account_name() == ACCOUNT_1
    assert ranked_accounts[1].get_account_name() == ACCOUNT_2
    assert ranked_accounts[2].get_account_name() in [ACCOUNT_3, ACCOUNT_4]
    assert ranked_accounts[3].get_account_name() in [ACCOUNT_3, ACCOUNT_4]
    assert ranked_accounts[4].get_account_name() == ACCOUNT_5

    # Verify the rank itself
    assert ranked_accounts[0].get_rank() == 1
    assert ranked_accounts[1].get_rank() > 0.88
    assert storage_account_set.accounts[ACCOUNT_3].get_rank() >= 0.5
    assert storage_account_set.accounts[ACCOUNT_4].get_rank() > 0.3
    assert ranked_accounts[4].get_rank() == 0


def test_old_results_count_for_less():
    current_time = 0

    def time_provider():
        return current_time

    storage_account_set = create_storage_account_set(time_provider)

    storage_account_set.add_account_result(ACCOUNT_1, True)
    current_time += 11
    storage_account_set.add_account_result(ACCOUNT_1, True)
    current_time += 11
    storage_account_set.add_account_result(ACCOUNT_1, True)
    current_time += 11
    storage_account_set.add_account_result(ACCOUNT_1, False)
    current_time += 11
    storage_account_set.add_account_result(ACCOUNT_1, False)
    current_time += 11
    storage_account_set.add_account_result(ACCOUNT_1, False)

    # rank should be smaller than 0.5 as new samples are more important
    assert storage_account_set.accounts[ACCOUNT_1].get_rank() < 0.5
