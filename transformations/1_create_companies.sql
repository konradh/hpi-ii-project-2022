DROP TABLE IF EXISTS companies;
CREATE TABLE companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state TEXT,
    reference_id TEXT,
    registration_authority TEXT,
    is_active INTEGER DEFAULT 1,
    name TEXT,
    type TEXT,
    address TEXT,
    purpose TEXT,
    capital REAL,
    currency TEXT
);

INSERT INTO companies (state, reference_id, registration_authority)
    SELECT DISTINCT
        state,
        reference_id,
        registration_authority
    FROM
        "corporate-events";
