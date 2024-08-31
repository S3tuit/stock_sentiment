recommendation_schema_v1 = """

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