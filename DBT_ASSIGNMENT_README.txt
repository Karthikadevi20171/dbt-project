### Pre-requisities:

- Do the DBT core setup through VS code and python installation

- Create Snowflake trial account 

- Create Github repository to store and maintain a DBT code 

### Created and loaded the stage tables with raw data in snowflake:

DBT_DB.STAGE.STAGE_CUSTOMER
DBT_DB.STAGE.STAGE_ORDER

### DBT Models creation:

- Created the below DBT models, yml and md files.
  HUB_CUSTOMER.sql
  Link_Customer_Order.sql
  SAT_CUSTOMER_DETAILS.sql
  HUB_CUSTOMER.yml
  HUB_CUSTOMER.md
  
### DBT log:

All three DBT models has been created and executed successfully and the logs are stored in dbt.logs

### DBT tests:

The test (unique, not-null and referential integrity) has been performed on the primary_key columns and it has been tested successfully.

### DBT Document generation:

The below commands were executed to generate the documents automatically from DBT model runs.

dbt docs generate
dbt docs serve

### Push the dbt changes to github repository