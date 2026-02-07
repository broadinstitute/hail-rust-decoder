CREATE TABLE IF NOT EXISTS phenotype_plots (
    phenotype            String,
    ancestry             LowCardinality(String),
    plot_type            LowCardinality(String),
    gcs_uri              String
)
ENGINE = ReplacingMergeTree()
ORDER BY (phenotype, ancestry, plot_type)
SETTINGS index_granularity = 8192
