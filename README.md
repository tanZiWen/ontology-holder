# Ontology-holder

Ontology-holder show the holder of ONT and ONG.

## How to use?

Ontology-holder need an ontoloyg node to sync block data, and need a mysql scheme (ontolog-holder) to save data.

rename config-simpe.json tp config.json,
and set Mysql address to "MysqlDataSourceName" field, and set Ontology node rpc address to "OntologyRpcAddress" field.

Note that you need create db scheme "ontology-holder" with utf-8 charset befer setup Ontology-holder.

## API

contract must be ont contract address: 0100000000000000000000000000000000000000 or ong contract address 0200000000000000000000000000000000000000

1. Get holder list of asset

```
http://localhost:8080/getAssetHolder?qid=1&contract=0100000000000000000000000000000000000000&from=0&count=100
```

from and count must larger 0, and count must smaller than 100.

2. Get asset base info

```
http://localhost:8080/GetAssetInfo?qid=1&contract=0100000000000000000000000000000000000000
```

3. Get total holder count

```
http://localhost:8080/getAssetHolderCount?qid=1&contract=0100000000000000000000000000000000000000
```

4. Get balance of address

```
http://localhost:8090/getBalance?address=AMX6ZebrPDFELCYRMpSMbZWrhWkKbKg4y8&contract=0200000000000000000000000000000000000000
```

contract param is option.

