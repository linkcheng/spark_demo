from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([
        StructField("id", StringType(), True),
        StructField("mobile", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_time", TimestampType(), True),
        StructField("created_ip", StringType(), True),
    ])

data = {
    "id": "11111",
    "mobile": "18212341234",
    "email": None,
    "created_time": '2019-01-03 15:40:27',
    "created_ip": "11.122.68.106",
}

data_list = [(1, str(data))]
df = spark.createDataFrame(data_list, ("key", "value"))
df.selectExpr("value AS json").collect()
df.select(from_json("value", schema=schema).alias("json")).collect()

df.selectExpr("CAST(value AS STRING) as json").select(from_json(col("json"),schema=schema).as("data")).collect()




schema = ArrayType(
    StructType([
        StructField("id", StringType(), True),
        StructField("mobile", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_time", TimestampType(), True),
        StructField("created_ip", StringType(), True),
    ])
)

data = {
    "id": "11111",
    "mobile": "18212341234",
    "email": None,
    "created_time": '2019-01-03 15:40:27',
    "created_ip": "11.122.68.106",
}

data_list = [(1, str(data))]
df = spark.createDataFrame(data_list, ("key", "value"))
df.select(explode(from_json("value", schema).alias("json"))).collect()




schema = StructType([
    StructField("id", LongType()),
    StructField("es", LongType()),
    StructField("database", StringType()),
    StructField("table", StringType()),
    StructField("type", StringType()),
    StructField("isDdl", BooleanType()),
    StructField("data", ArrayType(StructType([
        StructField("id", LongType()),
        StructField("mobile", StringType()),
        StructField("created_time", TimestampType()),
        StructField("gender", StringType()),
        StructField("person_id", LongType()),
        StructField("utm_source", StringType()),
        StructField("updated_time", TimestampType()),
        StructField("app_source", StringType()),
    ]))),
])

d = {
    "data": [
        {
            "id": "1111111401",
            "name": "XYF20806056",
            "password_digest": "994ead90845bd930afdf48b3b9ff334e",
            "mobile": "18212341234",
            "email": null,
            "created_time": '2019-01-03 15:40:27',
            "created_ip": "11.122.68.106",
            "last_login_time": "2019-01-02 15:39:52",
            "last_login_ip": "11.122.68.106",
            "old_id": "0",
            "weixin_open_id": null,
            "gender": "3",
            "person_id": "1943564",
            "utm_source": "信用飞APP",
            "order_cnt": "0",
            "bill_status": "1",
            "console_remark": "",
            "updated_time": '2019-01-03 15:40:27',
            "app_source": "autoupdate",
            "is_get_authorize": "0",
            "user_address_list": null,
            "bankNumber": null,
            "biz_event_status": null,
            "biz_event_time": null,
            "biz_event_data": null,
            "invitation_code": null,
            "used_invitation_code": null
        },
        {
            "id": "1111111402",
            "name": null,
            "password_digest": "",
            "mobile": "15112341234",
            "email": null,
            "created_time": '2019-01-03 15:40:27',
            "created_ip": "22.104.147.244",
            "last_login_time": "2019-01-02 15:39:52",
            "last_login_ip": "22.104.147.244",
            "old_id": "0",
            "weixin_open_id": null,
            "gender": "3",
            "person_id": "0",
            "utm_source": "小白信用分",
            "order_cnt": "0",
            "bill_status": "1",
            "console_remark": "",
            "updated_time": '2019-01-03 15:40:27',
            "app_source": "",
            "is_get_authorize": "0",
            "user_address_list": null,
            "bankNumber": null,
            "biz_event_status": null,
            "biz_event_time": null,
            "biz_event_data": null,
            "invitation_code": null,
            "used_invitation_code": null
        }
    ],
    "database": "shoufuyou_v2",
    "es": 15412341234,
    "id": 119,
    "isDdl": false,
    "mysqlType": {
        "id": "bigint(20) unsigned",
        "name": "varchar(63)",
        "password_digest": "varchar(63)",
        "mobile": "varchar(63)",
        "email": "varchar(127)",
        "created_time": "datetime",
        "created_ip": "varchar(63)",
        "last_login_time": "datetime",
        "last_login_ip": "varchar(63)",
        "old_id": "bigint(20) unsigned",
        "weixin_open_id": "varchar(63)",
        "gender": "enum('male','female','other')",
        "person_id": "bigint(20) unsigned",
        "utm_source": "varchar(127)",
        "order_cnt": "smallint(5) unsigned",
        "bill_status": "enum('none','has_bill','has_overdue','overdue')",
        "console_remark": "varchar(512)",
        "updated_time": "timestamp",
        "app_source": "varchar(63)",
        "is_get_authorize": "tinyint(1)",
        "user_address_list": "varchar(500)",
        "bankNumber": "varchar(36)",
        "biz_event_status": "varchar(32)",
        "biz_event_time": "datetime",
        "biz_event_data": "varchar(500)",
        "invitation_code": "varchar(16)",
        "used_invitation_code": "varchar(16)"
    },
    "old": null,
    "sql": "",
    "sqlType": {
        "id": -5,
        "name": 12,
        "password_digest": 12,
        "mobile": 12,
        "email": 12,
        "created_time": 93,
        "created_ip": 12,
        "last_login_time": 93,
        "last_login_ip": 12,
        "old_id": -5,
        "weixin_open_id": 12,
        "gender": 4,
        "person_id": -5,
        "utm_source": 12,
        "order_cnt": 5,
        "bill_status": 4,
        "console_remark": 12,
        "updated_time": 93,
        "app_source": 12,
        "is_get_authorize": -7,
        "user_address_list": 12,
        "bankNumber": 12,
        "biz_event_status": 12,
        "biz_event_time": 93,
        "biz_event_data": 12,
        "invitation_code": 12,
        "used_invitation_code": 12
    },
    "table": "User",
    "ts": 15412341234,
    "type": "INSERT"
}