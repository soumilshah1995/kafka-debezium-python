try:
    import kafka
    import json
    import requests
    import os
    import sys
    from json import dumps
    from kafka import KafkaProducer

    from kafka import KafkaConsumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    import io
    from confluent_kafka import Consumer, KafkaError
    from avro.io import DatumReader, BinaryDecoder
    import avro.schema

    from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
    from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
    from confluent_kafka.avro.serializer import (SerializerError,  # noqa
                                                 KeySerializerError,
                                                 ValueSerializerError)

    print("ALL ok")
except Exception as e:
    print("Error : {} ".format(e))

SCHEME_REGISTERY = "http://schema-registry:8081"
TOPIC = "postgres.public.sales"
BROKER = "localhost:9092"

schema = """
{
   "type":"record",
   "name":"Envelope",
   "namespace":"postgres.public.sales",
   "fields":[
      {
         "name":"before",
         "type":[
            "null",
            {
               "type":"record",
               "name":"Value",
               "fields":[
                  {
                     "name":"invoiceid",
                     "type":"int"
                  },
                  {
                     "name":"itemid",
                     "type":"int"
                  },
                  {
                     "name":"category",
                     "type":[
                        "null",
                        "string"
                     ],
                     "default":null
                  },
                  {
                     "name":"price",
                     "type":[
                        "null",
                        {
                           "type":"record",
                           "name":"VariableScaleDecimal",
                           "namespace":"io.debezium.data",
                           "fields":[
                              {
                                 "name":"scale",
                                 "type":"int"
                              },
                              {
                                 "name":"value",
                                 "type":"bytes"
                              }
                           ],
                           "connect.doc":"Variable scaled decimal",
                           "connect.version":1,
                           "connect.name":"io.debezium.data.VariableScaleDecimal"
                        }
                     ],
                     "doc":"Variable scaled decimal",
                     "default":null
                  },
                  {
                     "name":"quantity",
                     "type":"int"
                  },
                  {
                     "name":"orderdate",
                     "type":[
                        "null",
                        {
                           "type":"long",
                           "connect.version":1,
                           "connect.name":"io.debezium.time.MicroTimestamp"
                        }
                     ],
                     "default":null
                  },
                  {
                     "name":"destinationstate",
                     "type":[
                        "null",
                        "string"
                     ],
                     "default":null
                  },
                  {
                     "name":"shippingtype",
                     "type":[
                        "null",
                        "string"
                     ],
                     "default":null
                  },
                  {
                     "name":"referral",
                     "type":[
                        "null",
                        "string"
                     ],
                     "default":null
                  }
               ],
               "connect.name":"postgres.public.sales.Value"
            }
         ],
         "default":null
      },
      {
         "name":"after",
         "type":[
            "null",
            "Value"
         ],
         "default":null
      },
      {
         "name":"source",
         "type":{
            "type":"record",
            "name":"Source",
            "namespace":"io.debezium.connector.postgresql",
            "fields":[
               {
                  "name":"version",
                  "type":"string"
               },
               {
                  "name":"connector",
                  "type":"string"
               },
               {
                  "name":"name",
                  "type":"string"
               },
               {
                  "name":"ts_ms",
                  "type":"long"
               },
               {
                  "name":"snapshot",
                  "type":[
                     {
                        "type":"string",
                        "connect.version":1,
                        "connect.parameters":{
                           "allowed":"true,last,false"
                        },
                        "connect.default":"false",
                        "connect.name":"io.debezium.data.Enum"
                     },
                     "null"
                  ],
                  "default":"false"
               },
               {
                  "name":"db",
                  "type":"string"
               },
               {
                  "name":"schema",
                  "type":"string"
               },
               {
                  "name":"table",
                  "type":"string"
               },
               {
                  "name":"txId",
                  "type":[
                     "null",
                     "long"
                  ],
                  "default":null
               },
               {
                  "name":"lsn",
                  "type":[
                     "null",
                     "long"
                  ],
                  "default":null
               },
               {
                  "name":"xmin",
                  "type":[
                     "null",
                     "long"
                  ],
                  "default":null
               }
            ],
            "connect.name":"io.debezium.connector.postgresql.Source"
         }
      },
      {
         "name":"op",
         "type":"string"
      },
      {
         "name":"ts_ms",
         "type":[
            "null",
            "long"
         ],
         "default":null
      },
      {
         "name":"transaction",
         "type":[
            "null",
            {
               "type":"record",
               "name":"ConnectDefault",
               "namespace":"io.confluent.connect.avro",
               "fields":[
                  {
                     "name":"id",
                     "type":"string"
                  },
                  {
                     "name":"total_order",
                     "type":"long"
                  },
                  {
                     "name":"data_collection_order",
                     "type":"long"
                  }
               ]
            }
         ],
         "default":null
      }
   ],
   "connect.name":"postgres.public.sales.Envelope"
}
"""


schema = avro.schema.Parse(schema)
reader = DatumReader(schema)


def decode_method_2(msg_value):
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(5)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict


def fetch_schema():
    from confluent_kafka.schema_registry import SchemaRegistryClient
    sr = SchemaRegistryClient({"url": 'http://localhost:8081'})
    subjects = sr.get_subjects()
    for subject in subjects:
        schema = sr.get_latest_version(subject)
        print(schema.version)
        print(schema.schema_id)
        print(schema.schema.schema_str)


def main():
    print("Listening *****************")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=False,
        group_id="some group"
    )

    for msg in consumer:
        msg_value = msg.value
        print("\n")
        print("decode_method_2", decode_method_2(msg_value))
        print("\n")

main()