# Clone dolt earnings db
dolt clone post-no-preference/earnings

# Start dolt server
dolt sql-server --config=config.yaml

# Update dolt earnings db from remote
dolt fetch origin
