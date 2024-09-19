article_schema_v1 = """

{
    "name": "article_schema_v1",
    "namespace": "com.article.schema.v1",
    "doc": "The key is TICKET. The first version of my schema for the extracted articles.",
    "type": "record",
    "fields": [
        {
            "name": "ticket",
            "type": "string"
        },
        {
            "name": "url",
            "type": [
                "null",
                "string"
            ],
            "default": null
        },
        {
            "name": "title",
            "type": [
                "null",
                "string"
            ],
            "default": null
        },
        {
            "name": "article_body",
            "type": [
                "null",
                "string"
            ],
            "default": null
        },
        {
            "name": "timestp",
            "type": [
                "null",
                "long"
            ],
            "default": null
        }
    ]
}

"""



prices_schema_v1 = """

{
    "name": "prices_schema_v1",
    "namespace": "com.prices.schema.v1",
    "doc": "Schema for the Prices record containing ticket information, timestamp, price and volume data, and technical analysis indicators. The key is ticket.",
    "type": "record",
    "fields": [
        {
            "name": "ticket",
            "type": "string"
        },
        {
            "name": "timestp",
            "type": "long"
        },
        {
            "name": "price_n_volume",
            "type": {
                "type": "map",
                "values": "string"
            }
        },
        {
            "name": "technicals",
            "type": {
                "type": "map",
                "values": "string"
            }
        }
    ]
}

"""