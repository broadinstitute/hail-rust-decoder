CREATE TABLE IF NOT EXISTS significant_variants (
    -- Identity
    phenotype            LowCardinality(String),
    ancestry             LowCardinality(String),
    sequencing_type      LowCardinality(String),

    -- Variant position
    xpos                 Int64,
    contig               LowCardinality(String),
    position             UInt32,
    ref                  String,
    alt                  String,

    -- Association stats
    pvalue               Float64,
    beta                 Float64,
    se                   Float64,
    af                   Float64,

    -- Case/control breakdown fields (nullable)
    ac_cases             Nullable(Float64),
    ac_controls          Nullable(Float64),
    af_cases             Nullable(Float64),
    af_controls          Nullable(Float64),
    association_ac       Nullable(Float64)
)
ENGINE = MergeTree()
PARTITION BY phenotype
ORDER BY (sequencing_type, xpos, ref, alt)
SETTINGS index_granularity = 8192
