import os
import requests
import time
import random
from solana.keypair import Keypair
from solana.rpc.api import Client
from solana.publickey import PublicKey

# 定义多个 Solana 节点的 RPC 地址
RPC_URLS = [
    "https://api.mainnet-beta.solana.com/",
    "https://api.testnet.solana.com/",
    "https://api.devnet.solana.com",
    "https://lb.drpc.org/ogrpc?network=solana&dkey=AiT0vYE6BUu-uwVrSYUejkhQ3q3fl2QR76NqFhW5UfFk",
]

def get_random_rpc_client():
    """从 RPC_URLS 中随机选择一个 RPC 地址，返回相应的 Client。"""
    return Client(random.choice(RPC_URLS))

def generate_account():
    """生成一个新的 Solana 账户。"""
    account = Keypair.generate()
    return account

def get_balance(pub_key):
    """查询账户余额。"""
    client = get_random_rpc_client()  # 使用随机的 RPC 客户端
    try:
        response = client.get_balance(PublicKey(pub_key))
        print(f"Response from get_balance for {pub_key}: {response}")  # 调试信息

        # 获取余额
        return response['result']['value'] / 1e9 if response['result'] is not None else 0  # 返回 SOL 余额
    except Exception as e:
        print(f"Error fetching balance for {pub_key}: {e}")
        return 0

def get_recent_transactions(pub_key):
    """查询账户近期交易。"""
    client = get_random_rpc_client()  # 使用随机的 RPC 客户端
    url = client._provider.endpoint_uri  # 获取当前使用的 RPC 地址
    headers = {
        "Content-Type": "application/json"
    }
    
    # 使用 RPC API 查询交易
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getConfirmedSignaturesForAddress2",
        "params": [pub_key, {"limit": 1}]
    }
    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            transactions = response.json()
            return len(transactions['result']) > 0  # 如果有交易则返回 True
        else:
            print(f"Error fetching transactions: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Exception occurred while fetching transactions: {e}")
    return False

def save_to_file(account, balance):
    """保存有余额的账户信息到 txt 文件。"""
    with open("solana_accounts_with_balance.txt", "a") as f:
        f.write(f"Public Key: {account.public_key}\n")
        f.write(f"Private Key: {account.secret_key.hex()}\n")  # 保存私钥
        f.write(f"Balance: {balance} SOL\n\n")

def scan_account():
    """生成账户并扫描其余额和交易。"""
    print("Generating new account...")  # 调试信息：正在生成新账户
    account = generate_account()
    pub_key = account.public_key  # 获取公钥

    print(f"Public Key: {pub_key}")  # 打印公共密钥
    print(f"Private Key: {account.secret_key.hex()}")  # 打印私钥

    balance = get_balance(pub_key)
    print(f"Balance for {pub_key}: {balance} SOL")  # 打印余额

    # 如果余额大于0，保存账户信息
    if balance > 0:
        save_to_file(account, balance)
        print(f"Found account with balance: {pub_key} - {balance} SOL")
    else:
        print(f"Account {pub_key} has no balance.")  # 打印余额为0的信息

    # 检查是否有近期交易
    recent_transactions = get_recent_transactions(pub_key)
    if recent_transactions:
        save_to_file(account, balance)
        print(f"Found account with recent transactions: {pub_key}")
    else:
        print(f"No recent transactions for: {pub_key}")  # 打印没有交易的信息

def main():
    """主程序入口。"""
    print("Starting the Solana account scanner...")
    while True:
        try:
            scan_account()
            time.sleep(1)  # 每秒生成一个账户
        except Exception as e:
            print(f"An error occurred: {e}. Restarting the scanner...")
            time.sleep(5)  # 等待一段时间后重启

if __name__ == "__main__":
    main()
