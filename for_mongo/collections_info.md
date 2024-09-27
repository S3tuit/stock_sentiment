Run *mongo_setup.sh* to create all the indexes needed.
---

# DB: stock_test

## collection: articles_test
This database stores article info scraped and/or retrived via API. Usually, this is the structure of the data:
```json
{
    "ticket": "test",
    "url": "https://test",
    "title": "The Biggest Companies...",
    "article_body": "leg",
    "timestp": Long(1726688788)
  }
```

**Indexes:**

- Unique index (ticket, url). Usefull to ensure Mongo won't store the same article twice.

```bash
use stock_test;

db.articles_test.createIndex(
   { ticket: 1, url: 1 },
   { unique: true, name: "articles_test_uniq_ticket_url" }
);
```

- Index on timestp in descending order. Usefull for speeding up queries.

```bash
db.articles_test.createIndex(
    { timestp: -1 },
    { name: "articles_test_timestp_index"}
);
```

## collection: balance_sheet
This database stores balance sheet and earning data retrived via API. Usually, this is the structure of the data:
```json
{
    "ticket": "test",
    "timestp": Long(1719705600),
    "earnings_ratios": {
      "a_ratio": "77.77",
      "another_ratio": "33.33"
    },
    "balance_sheet": {
      "balance": "55",
      "sheet": "11"
    }
  }
```

**Indexes:**

- Unique index (ticket, timestp). Usefull to ensure Mongo won't store the same balance_sheet twice. Usefull also for queries.

```bash
use stock_test;

db.balance_sheet.createIndex(
   { ticket: 1, timestp: -1 },
   { unique: true, name: "balance_sheet_test_ticket_tmstp" }
);
```

## collection: price_info
This database stores balance sheet and earning data retrived via API. Usually, this is the structure of the data:
```json
{
    "ticket": "test",
    "timestp": Long(1719705600),
    "price_n_volume": {
      "high": "7.3400",
      "volume": "10"
    },
    "technicals": {
      "indicator1": "1",
      "indicator2": "2"
    }
  }
```

**Indexes:**

- Unique index (ticket, timestp). Usefull to ensure Mongo won't store the same price_info twice. Usefull also for queries.

```bash
use stock_test;

db.price_info.createIndex(
   { ticket: 1, timestp: -1 },
   { unique: true, name: "price_test_ticket_tmstp" }
);
```

## collection: stock_sentiment
This database stores the stock sentiment/prediction got using openai API. Usually, this is the structure of the data:
```json
{
    "ticket": "test",
    "timestp": Long(1719705600),
    "next_month_prediction": 5,
    "next_year_prediction": 5,
    "reasoning": "This is why..."
    }
  }
```

**Indexes:**

- Unique index (ticket, timestp). Usefull to ensure Mongo won't store the same prediction twice. Usefull also for queries.

```bash
use stock_test;

db.stock_sentiment.createIndex(
   { ticket: 1, timestp: -1 },
   { unique: true, name: "sentiment_test_ticket_tmstp" }
);
```