article_schema_v1 = """

{
    "name": "article_schema_v1",
    "namespace": "com.article.schema.v1",
    "doc": "The key is TICKER. The first version of my schema for the extracted articles.",
    "type": "record",
    "fields": [
        {
            "name": "ticker",
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
    "doc": "Schema for the Prices record containing ticker information, timestamp, price and volume data, and technical analysis indicators. The key is ticker.",
    "type": "record",
    "fields": [
        {
            "name": "ticker",
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


balance_sheet_schema_v1 = """

{
    "name": "prices_schema_v1",
    "namespace": "com.prices.schema.v1",
    "doc": "Schema for the BalanceSheet model. The key is ticker.",
    "type": "record",
    "fields": [
        {
            "name": "ticker",
            "type": "string"
        },
        {
            "name": "timestp",
            "type": "long"
        },
        {
            "name": "earnings_ratios",
            "type": {
                "type": "map",
                "values": "string"
            }
        },
        {
            "name": "balance_sheet",
            "type": {
                "type": "map",
                "values": "string"
            }
        }
    ]
}

"""