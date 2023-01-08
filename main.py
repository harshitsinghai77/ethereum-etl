import subprocess
import os

import pandas as pd
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.exporters.blocks_and_transactions_item_exporter import (
    blocks_and_transactions_item_exporter,
)
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy

BLOCKS_FILE_NAME = "blocks.csv"
TRANSACTIONS_FILE_NAME = "transactions.csv"
TOKEN_TRANSFERS_FILE_NAME = "token_transfers.csv"
CONTRACT_ADDRESSES_FILE_NAME = "contract_addresses.txt"
INFURA_API_KEY = "a0c9aea3296549c0abdab757236a7f46"


def check_if_files_exist(file_name: str) -> bool:
    return os.path.exists(file_name)


def export_blocks_and_transactions() -> bool:
    """Exports blocks and transactions from start_block to end_block"""
    # command = f"""
    #     ethereumetl export_blocks_and_transactions --start-block 480000 --end-block 500000 \
    #     --blocks-output {BLOCKS_FILE_NAME} --transactions-output {BLOCKS_FILE_NAME} --max-workers 50 \
    #     --provider-uri https://mainnet.infura.io/v3/{INFURA_API_KEY}
    # """
    # result = subprocess.run(command.split())
    # if result.returncode == 0:
    #     return True
    print("Exporting blocks_and_transactions")

    start_block = 480000
    end_block = 500000
    batch_size = 100
    provider_uri = f"https://mainnet.infura.io/v3/{INFURA_API_KEY}"
    max_workers = 50
    blocks_output = BLOCKS_FILE_NAME
    transactions_output = TRANSACTIONS_FILE_NAME

    try:
        job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=batch_size,
            batch_web3_provider=ThreadLocalProxy(
                lambda: get_provider_from_uri(provider_uri, batch=True)
            ),
            max_workers=max_workers,
            item_exporter=blocks_and_transactions_item_exporter(
                blocks_output, transactions_output
            ),
            export_blocks=blocks_output is not None,
            export_transactions=transactions_output is not None,
        )
        job.run()
        return True
    except Exception as e:
        print("Failed to run export_blocks_and_transactions")
        print(e)
        return False


def export_token_transfers() -> bool:
    """Exports token transfers from start_block to end_block"""
    print("Exporting token_transfers")
    command = f"""
        ethereumetl export_token_transfers --start-block 480000 --end-block 500000 \
        --output {TOKEN_TRANSFERS_FILE_NAME} \
        --provider-uri https://mainnet.infura.io/v3/{INFURA_API_KEY}
    """
    result = subprocess.run(command.split())
    return result.returncode == 0  # result.returncode == 0 means it ran successfully


def create_contract_addresses_file() -> bool:
    """Creates a txt file with all the contract addresses"""
    print(
        "Extract token_address column from token_transfers.csv and writing it to contract_addresses.txt"
    )
    command = f"""
        ethereumetl extract_csv_column --input {TOKEN_TRANSFERS_FILE_NAME} \
        --column token_address \
        --output {CONTRACT_ADDRESSES_FILE_NAME}
    """
    result = subprocess.run(command.split())
    return result.returncode == 0  # result.returncode == 0 means it ran successfully


def export_contracts() -> bool:
    """Using the contract addresses file, export all the contracts
    contracts.csv will be used to determine the contract type i.e is_erc20 or is_erc721
    """
    print("Exporting contracts using contract_addresses.txt")
    command = f"""
        ethereumetl export_contracts --contract-addresses {CONTRACT_ADDRESSES_FILE_NAME} \
        --output contracts.csv \
        --provider-uri https://mainnet.infura.io/v3/{INFURA_API_KEY}
    """

    result = subprocess.run(command.split())
    return result.returncode == 0  # result.returncode == 0 means it ran successfully


# convert csv to parquet
def convert_csv_to_parquet(file_name: str):
    """Converts csv to parquet file"""
    if not check_if_files_exist(file_name):
        raise FileExistsError(f"File {file_name} does not exist")

    df = pd.read_csv(file_name)
    file_name = file_name.replace(".csv", ".parquet")
    df.to_parquet(file_name)
    print(f"Converted {file_name} to parquet")


def main():
    try:
        block_transaction_file = export_blocks_and_transactions()
        if block_transaction_file:
            convert_csv_to_parquet(BLOCKS_FILE_NAME)
            convert_csv_to_parquet(TRANSACTIONS_FILE_NAME)

        token_transfer_file = export_token_transfers()
        if token_transfer_file:
            convert_csv_to_parquet(TOKEN_TRANSFERS_FILE_NAME)

        if check_if_files_exist(TOKEN_TRANSFERS_FILE_NAME):
            create_contract_addresses_file()
            export_contracts()

    except Exception as e:
        print("Failed to run ethereumetl command")
        print(e)


if __name__ == "__main__":
    main()
