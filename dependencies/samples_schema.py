samples_schema = {
    "fields": [
        {"name": "biosample_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "tracking_status", "type": "STRING", "mode": "REQUIRED"},
        {
            "name": "characteristics",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "field_name", "type": "STRING", "mode": "REQUIRED"},
                {"name": "field_value", "type": "STRING", "mode": "REQUIRED"},
                {"name": "unit", "type": "STRING", "mode": "NULLABLE"},
                {"name": "ontology_term", "type": "STRING", "mode": "NULLABLE"},
            ],
        },
        {
            "name": "experiments",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "study_accession", "type": "STRING", "mode": "NULLABLE"},
                {
                    "name": "secondary_study_accession",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
                {"name": "sample_accession", "type": "STRING", "mode": "NULLABLE"},
                {
                    "name": "secondary_sample_accession",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
                {"name": "experiment_accession", "type": "STRING", "mode": "NULLABLE"},
                {"name": "run_accession", "type": "STRING", "mode": "NULLABLE"},
                {"name": "submission_accession", "type": "STRING", "mode": "NULLABLE"},
                {"name": "tax_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "scientific_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "instrument_platform", "type": "STRING", "mode": "NULLABLE"},
                {"name": "instrument_model", "type": "STRING", "mode": "NULLABLE"},
                {"name": "library_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "nominal_length", "type": "STRING", "mode": "NULLABLE"},
                {"name": "library_layout", "type": "STRING", "mode": "NULLABLE"},
                {"name": "library_strategy", "type": "STRING", "mode": "NULLABLE"},
                {"name": "library_source", "type": "STRING", "mode": "NULLABLE"},
                {"name": "library_selection", "type": "STRING", "mode": "NULLABLE"},
                {"name": "read_count", "type": "STRING", "mode": "NULLABLE"},
                {"name": "base_count", "type": "STRING", "mode": "NULLABLE"},
                {"name": "center_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "first_public", "type": "STRING", "mode": "NULLABLE"},
                {"name": "last_updated", "type": "STRING", "mode": "NULLABLE"},
                {"name": "experiment_title", "type": "STRING", "mode": "NULLABLE"},
                {"name": "study_title", "type": "STRING", "mode": "NULLABLE"},
                {"name": "study_alias", "type": "STRING", "mode": "NULLABLE"},
                {"name": "experiment_alias", "type": "STRING", "mode": "NULLABLE"},
                {"name": "run_alias", "type": "STRING", "mode": "NULLABLE"},
                {"name": "fastq_bytes", "type": "STRING", "mode": "NULLABLE"},
                {"name": "fastq_md5", "type": "STRING", "mode": "NULLABLE"},
                {"name": "fastq_ftp", "type": "STRING", "mode": "NULLABLE"},
                {"name": "fastq_aspera", "type": "STRING", "mode": "NULLABLE"},
                {"name": "fastq_galaxy", "type": "STRING", "mode": "NULLABLE"},
                {"name": "submitted_bytes", "type": "STRING", "mode": "NULLABLE"},
                {"name": "submitted_md5", "type": "STRING", "mode": "NULLABLE"},
                {"name": "submitted_ftp", "type": "STRING", "mode": "NULLABLE"},
                {"name": "submitted_aspera", "type": "STRING", "mode": "NULLABLE"},
                {"name": "submitted_galaxy", "type": "STRING", "mode": "NULLABLE"},
                {"name": "submitted_format", "type": "STRING", "mode": "NULLABLE"},
                {
                    "name": "library_construction_protocol",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
                {"name": "sra_bytes", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sra_md5", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sra_ftp", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sra_aspera", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sra_galaxy", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sample_alias", "type": "STRING", "mode": "NULLABLE"},
                {"name": "broker_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sample_title", "type": "STRING", "mode": "NULLABLE"},
                {"name": "nominal_sdev", "type": "STRING", "mode": "NULLABLE"},
                {"name": "first_created", "type": "STRING", "mode": "NULLABLE"},
            ],
        },
        {
            "name": "assemblies",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "accession", "type": "STRING", "mode": "NULLABLE"},
                {"name": "assembly_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "description", "type": "STRING", "mode": "NULLABLE"},
                {"name": "study_accession", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sample_accession", "type": "STRING", "mode": "NULLABLE"},
                {"name": "last_updated", "type": "STRING", "mode": "NULLABLE"},
                {"name": "version", "type": "STRING", "mode": "NULLABLE"},
            ],
        },
        {
            "name": "analyses",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "study_accession", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sample_accession", "type": "STRING", "mode": "NULLABLE"},
                {"name": "analysis_accession", "type": "STRING", "mode": "NULLABLE"},
                {"name": "analysis_type", "type": "STRING", "mode": "NULLABLE"},
                {"name": "submitted_ftp", "type": "STRING", "mode": "NULLABLE"},
            ],
        },
        {
            "name": "relationships",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "source", "type": "STRING", "mode": "NULLABLE"},
                {"name": "type", "type": "STRING", "mode": "NULLABLE"},
                {"name": "target", "type": "STRING", "mode": "NULLABLE"},
            ],
        },
    ]
}
