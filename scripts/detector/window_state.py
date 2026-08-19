""" /scripts/detector/window_state.py
# ============================================================
#   Script: Window State
# ============================================================
#   Purpose: 
#       Keeps a sliding window of recent transactions per account to analyze suspicious accounts.
#
#   Logic:
#    - Each rule family has its own deque per account.
#    - New transactions are added to the end of the deque, ensuring it will not be duplicated in deque.
#    - Transactions older than the window length are removed from the front of the deque.
#
#   Usage:
#    - Called by txn_consumer.py, not useable directly.
"""

from collections import deque
from datetime import datetime, timedelta

class WindowState:
    def __init__(self, window_config: dict) -> None:
        self.window_config = window_config  # Keeps rule family name and window length in minutes
        self.windows = {rule_family: {} for rule_family in window_config}

    def check_if_transaction_exists(self, rule_family, transaction) -> bool:
        """Checks if transaction is already in account's deque."""
        account_windows = self.windows[rule_family]

        if transaction.account_id in account_windows:
            for i in account_windows[transaction.account_id]:
                if transaction.transaction_id == i.transaction_id:
                    return True
            return False
        else:
            return False

    def clear_old_transactions(self, rule_family, account_id, current_time) -> None:
        """Clears old transactions according to window length limits."""
        window_length = timedelta(minutes=self.window_config[rule_family])
        account_windows = self.windows[rule_family]

        if account_id not in account_windows:
            return
        account_deque = account_windows[account_id]

        while account_deque and (current_time - account_deque[0].event_time) > window_length:
            account_deque.popleft()
        
    def add_transaction(self, rule_family, transaction) -> None:
        """Add new transaction to user's deque."""

        if rule_family not in self.windows:
            raise ValueError(f"Unknown rule family: {rule_family}")

        account_windows = self.windows[rule_family]

        if transaction.account_id not in account_windows:
            account_windows[transaction.account_id] = deque()

        self.clear_old_transactions(rule_family=rule_family, account_id=transaction.account_id, current_time=transaction.event_time)
        transaction_existance = self.check_if_transaction_exists(rule_family, transaction)

        if not transaction_existance:
            account_windows[transaction.account_id].append(transaction)