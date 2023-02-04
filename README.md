# stellar_asset_snapshots

`python snapshots.py *asset_code* *issuer* *timestamp*`

Omit timestamp to create snapshots until now.

Output:
```
{
    *timestamp stellar format*: {
      "balances": {
          *public key*: *amount*,
          ...
      },
      "transactions": [
          (*from pub key*, *to pub key*, *amount*)
      ]
    }
    ...
}
```

