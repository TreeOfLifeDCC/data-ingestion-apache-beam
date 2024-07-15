annotations_schema = {
    "fields": [
        {
            "name": "tax_id",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "kingdom",
            "type": "string",
            "mode": "NULLABLE"
        },
        {
            "name": "phylum",
            "type": "string",
            "mode": "NULLABLE"
        },
        {
            "name": "class",
            "type": "string",
            "mode": "NULLABLE"
        },
        {
            "name": "order",
            "type": "string",
            "mode": "NULLABLE"
        },
        {
            "name": "family",
            "type": "string",
            "mode": "NULLABLE"
        },
        {
            "name": "genus",
            "type": "string",
            "mode": "NULLABLE"
        },
        {
            "name": "species",
            "type": "string",
            "mode": "NULLABLE"
        },
        {
            "name": "annotations",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "type",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "gene_id",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "gene_version",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "gene_source",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "gene_biotype",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "transcript_id",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "transcript_version",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "transcript_source",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "transcript_biotype",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "tag",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "exon_number",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "exon_id",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "exon_version",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "protein_id",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "protein_version",
                    "type": "string",
                    "mode": "NULLABLE"
                },
                {
                    "name": "gene_name",
                    "type": "string",
                    "mode": "NULLABLE"
                }
            ]
        }
    ]
}
