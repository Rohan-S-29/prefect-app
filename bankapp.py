from prefect import flow, task, get_run_logger
accounts = {}

@task
def create_account(account_id: str, initial_balance: float = 0.0):
    logger = get_run_logger()
    if account_id in accounts:
        logger.warning(f"Account {account_id} already exists.")
    else:
        accounts[account_id] = initial_balance
        logger.info(f"Created account {account_id} with balance {initial_balance}.")

@task
def deposit(account_id: str, amount: float):
    logger = get_run_logger()
    if account_id not in accounts:
        logger.error(f"Account {account_id} not found.")
        return
    accounts[account_id] += amount
    logger.info(f"Deposited {amount} to {account_id}. New balance: {accounts[account_id]}.")

@task
def withdraw(account_id: str, amount: float):
    logger = get_run_logger()
    if account_id not in accounts:
        logger.error(f"Account {account_id} not found.")
        return
    if accounts[account_id] < amount:
        logger.error(f"Insufficient funds for {account_id}. Available: {accounts[account_id]}.")
        return
    accounts[account_id] -= amount
    logger.info(f"Withdrew {amount} from {account_id}. New balance: {accounts[account_id]}.")

@task
def check_balance(account_id: str):
    logger = get_run_logger()
    balance = accounts.get(account_id, None)
    if balance is None:
        logger.error(f"Account {account_id} not found.")
    else:
        logger.info(f"Balance for {account_id}: {balance}.")

@flow(retries=3, retry_delay_seconds=20, timeout_seconds=40)
def bankflow():
    create_account("12345", 100.0)
    deposit("12345", 50.0)
    withdraw("12345", 30.0)
    check_balance("12345")

if __name__ == "__main__":
    bankflow()