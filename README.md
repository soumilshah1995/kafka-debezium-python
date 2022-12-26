![rob drawio](https://user-images.githubusercontent.com/39345855/209569115-cba1534f-58bb-4a5d-89e3-ec5800cbeb7a.png)


# Steps 

### Step 1 :
```
docker-compose up --build up
```


### Step 2 :
* Login into PG Admin 
* Username: postgres
* password : postgres
* database name : postgres
```

CREATE TABLE sales (
	InvoiceID int NOT NULL ,
	ItemID int NOT NULL,
	Category varchar(255),
	Price decimal ,
	Quantity int not NULL,
	OrderDate timestamp,
	DestinationState varchar(2),
	ShippingType varchar(255),
	Referral varchar(255),
	PRIMARY KEY (InvoiceID)

)

ALTER TABLE public.sales REPLICA IDENTITY  FULL
```

### Step 3: 
* Create Debezium connector 
```
curl --location --request POST 'http://localhost:8083/connectors/' \
--header 'Content-Type: application/json' \
--data-raw '{
  "name": "sales-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "postgres",
    "database.server.name": "postgres",
    "table.include.list": "public.sales"
  }
}'
```
* Verify 
```
http://localhost:8083/connectors/sales-connector
```

### Step 4:
```
cd kafka-code
python python-producer-posgres.py

python 

```
### Step 5: 
* in case if you want to see logs into docker exec in container 

```
docker networks ls 
COPY THE NAME 

docker run --tty --network <COPY NAME>  confluentinc/cp-kafkacat kafkacat -b kafka:29092 -C -s key=s -s value=avro -r http://schema-registry:8081 -t postgres.public.sales

```
# Happy Learning  

# Note
* i am working on streaming part currently with hudi stuck on deserialization part in spark issue can be found here 
* https://stackoverflow.com/questions/74921510/kafka-pyspark-deserialization-for-avro-messages

