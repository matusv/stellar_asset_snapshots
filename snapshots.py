from stellar_sdk import Account, Asset, Keypair, Network, TransactionBuilder, Server
from datetime import datetime
from pprint import pprint
from decimal import Decimal
from copy import deepcopy
import sys

def timestring_to_timestamp(timestring):
    return datetime.strptime(timestring, "%Y-%m-%dT%H:%M:%SZ").timestamp() 

def create_balance_snapshots(asset_code, asset_issuer, timestamp, server):

    snapshots = {}
    balances = {}
    ops_to_check = []
    checked_ops = []
    
    ops_to_check = get_initial_ops_to_check(asset_code, asset_issuer, server)
                
    while len(ops_to_check) > 0:
        #optimalization nlog(n) to n: keeping a sorted list by sorted insert
        ops_to_check.sort(key=lambda x: x[1])
        ops_to_check.reverse()
        op_id, op_timestamp = ops_to_check.pop()
        op = server.operations().operation(op_id).order(desc=True).call()
        
        created_at_ts = timestring_to_timestamp(op["created_at"])
        if timestamp is not None and created_at_ts > timestamp:
            checked_ops.append(op["id"])
            continue

        if op["type"] == "payment":

            if op["id"] not in checked_ops:
                from_pk = op["from"]
                to_pk = op["to"]
                amount = op["amount"]
                
                op_timestamp = timestring_to_timestamp(op["created_at"])

                if from_pk not in balances:
                    balances[from_pk] = Decimal("0")

                if to_pk not in balances:
                    balances[to_pk] = Decimal("0")
                    
                balances[from_pk] -= Decimal(amount)
                balances[to_pk] += Decimal(amount)
                
                if op["created_at"] not in snapshots:
                    snapshots[op["created_at"]] = {"balances": {}, "transactions": []}
                    
                snapshots[op["created_at"]]["balances"] = deepcopy(balances)
                snapshots[op["created_at"]]["transactions"].append((from_pk, to_pk, amount))
                
                checked_ops.append(op["id"]) 
                call_builder = server.operations().for_account(to_pk).order(desc=False)
                records = ops_page = call_builder.call()["_embedded"]["records"]
                
                while records_page := call_builder.next()["_embedded"]["records"]: 
                    records += records_page
                
                for record in records:
                    if ("asset_code" in record and "asset_issuer" in record and
                        record["asset_code"] == asset_code and record["asset_issuer"] == asset_issuer):
                        if record["type"] == "payment":
                            
                            ids_to_check = [x[0] for x in ops_to_check]
                            if record["id"] not in checked_ops and record["id"] not in ids_to_check:
                                op_to_check = (
                                    record["id"],
                                    timestring_to_timestamp(record["created_at"])
                                )
                                ops_to_check.append(op_to_check)
                        else:
                            pass
    return snapshots


def get_initial_ops_to_check(asset_code, asset_issuer, server):

    ops_to_check = []

    call_builder = server.operations().for_account(asset_issuer).order(desc=False)
    records = call_builder.call()["_embedded"]["records"]

    while page_records := call_builder.next()["_embedded"]["records"]: 
        records += page_records
    
    for record in records:
        if ("asset_code" in record and record["asset_code"] == asset_code and
            "asset_issuer" in record and record["asset_issuer"] == asset_issuer):

            if record["type"] == "payment":
                op_to_check = (
                    record["id"],
                    timestring_to_timestamp(record["created_at"])
                )
                ops_to_check.append(op_to_check)

            else:
                print(f"Unknown type: {record['type']}")
                    
    return ops_to_check


def main() -> int:
    if len(sys.argv) == 4:
        _, asset, issuer, timestamp = sys.argv
    
    elif len(sys.argv) == 3:
        _, asset, issuer = sys.argv
        timestamp = None

    server = Server(horizon_url="https://horizon-testnet.stellar.org")

    snapshots = create_balance_snapshots(
        asset_code="A6", 
        asset_issuer="GCFRQ3CQG5XY4CH5E4ZSTACZ27YYFEWZG636J4GIBWY3FKE3KSJ5ZBCK", 
        timestamp=None,
        server=server
    )

    for timestamp, snapshot in snapshots.items():
        print(timestamp)
        pprint(snapshot)
    return 0


if __name__ == '__main__':
    sys.exit(main()) 