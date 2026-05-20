
-- Scraper staging table — per-listing rows ingested from external portals
-- (muaban.net, nhatot.com). Used for analytics (broker vs individual posting rate,
-- volume per source per time window). Not part of the user-facing listings table.
CREATE TABLE data_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    source VARCHAR(20) NOT NULL CHECK (source IN ('nhatot', 'muaban')),
    source_id VARCHAR(100) NOT NULL,
    source_url TEXT,

    title VARCHAR(500),
    description TEXT,
    raw_data JSONB,

    property_type VARCHAR(50),
    transaction_type VARCHAR(20) DEFAULT 'ban'
        CHECK (transaction_type IN ('ban','cho-thue')),

    poster_type VARCHAR(20)
        CHECK (poster_type IN ('moi_gioi','chu_nha','khong_xac_dinh')),
    poster_confidence_score SMALLINT
        CHECK (poster_confidence_score IS NULL OR poster_confidence_score BETWEEN 0 AND 100),
    poster_classification_reason TEXT,

    price BIGINT CHECK (price IS NULL OR price >= 0),
    area DECIMAL(10,2),

    province VARCHAR(100),
    ward VARCHAR(100),

    phone_masked VARCHAR(20),
    phone_full VARCHAR(20),
    contact_name VARCHAR(255),

    posted_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_data_sources_source_id UNIQUE (source, source_id)
);

CREATE INDEX idx_data_sources_posted_at ON data_sources (posted_at DESC);
CREATE INDEX idx_data_sources_source_posted ON data_sources (source, posted_at DESC);
CREATE INDEX idx_data_sources_province_type ON data_sources (province, property_type);
CREATE INDEX idx_data_sources_poster_type ON data_sources (poster_type);
CREATE INDEX idx_data_sources_phone_full ON data_sources (phone_full);
CREATE INDEX idx_data_sources_last_seen_at ON data_sources (last_seen_at DESC);