CREATE TYPE institution_type AS ENUM ('hospital', 'clinic', 'medical_school', 'veterinary_school', 'academic_medical_center', 'other');

CREATE TABLE institutions (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    type institution_type NOT NULL,
    country TEXT NOT NULL,
    state TEXT,
    city TEXT,
    address TEXT,
    website TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    additional_attributes JSONB,  -- For country-specific fields, e.g., {"accreditation": "AVMA", "bed_count": 500}
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    search_vector TSVECTOR  -- For full-text search
);

-- Indexes for efficiency
CREATE INDEX idx_country ON institutions(country);
CREATE INDEX idx_name ON institutions(name);
CREATE INDEX idx_search_vector ON institutions USING GIN(search_vector);

-- Trigger to update search_vector
CREATE FUNCTION update_search_vector() RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('english', COALESCE(NEW.name, '') || ' ' || COALESCE(NEW.country, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_search_vector
BEFORE INSERT OR UPDATE ON institutions
FOR EACH ROW EXECUTE FUNCTION update_search_vector();