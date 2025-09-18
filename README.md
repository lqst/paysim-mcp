# MCP Server for supply chain
Workshop: Let's create some fraud/aml investigation tools exposed through MCP



## Setup
Create .env file
```param
CLIENT_SECRET=...
CLIENT_ID=...
DB_URI=neo4j://localhost:7687
DB_USER=neo4j
DB_PASSWORD=....
DB_NAME=....
SESSION_DB_NAME=... (can be the same as DB_NAME)
```

## Run it

```sh
uv run server.py
```


## Some questions to try
- Who holds the account ACC_MULE_129
- Is that the only account for this customer?
- Where is the money coming from?
- Have this customer appeared in the news lately?

