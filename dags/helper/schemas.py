article_schema_v2 = """

{
	"type": "record",
	"name": "article_schema_v2",
	"namespace": "com.article.schema.v2",
	"doc": "The key is TICKET. The first version of my schema for the extracted articles.",
	"fields": [
		{
			"name": "ticket",
			"type": "string"
		},
		{
			"name": "url",
			"type": "string"
		},
		{
			"name": "title",
			"type": "string"
		},
		{
			"name": "article_body",
			"type": "string"
		},
		{
			"name": "timestp",
			"type": "long"
		},
		{
			"name": "source",
			"type": "string",
			"default": "seeking_alpha"
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


balance_sheet_schema_v1 = """

{
    "name": "prices_schema_v1",
    "namespace": "com.prices.schema.v1",
    "doc": "Schema for the BalanceSheet model. The key is ticket.",
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


stock_sentiment_schema_v1 = """

{
	"type": "record",
	"name": "StockSentiment",
	"namespace": "com.sentiment.schema.v1",
	"doc": "Schema for the StockSentiment model. The key is the stock ticker (ticket).",
	"fields": [
		{
			"name": "next_month_prediction",
			"type": "float",
			"doc": "Prediction for the stock's performance next month."
		},
		{
			"name": "next_year_prediction",
			"type": "float",
			"doc": "Prediction for the stock's performance next year."
		},
		{
			"name": "reasoning",
			"type": "string",
			"doc": "Detailed reasoning behind the stock sentiment and predictions."
		},
		{
			"name": "ticket",
			"type": "string",
			"doc": "Stock ticker symbol."
		},
		{
			"name": "timestp",
			"type": "long",
			"doc": "Unix timestamp for when the sentiment was generated",
			"default": 0
		}
	]
}

"""
