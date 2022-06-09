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
SELECT DISTINCT state, reference_id, registration_authority
FROM "corporate-events";

DROP TABLE IF EXISTS events;
CREATE TABLE events (
    company_id INTEGER,
    event_date TEXT,
    event_type TEXT,
    status TEXT,
    information TEXT,
    FOREIGN KEY (company_id) REFERENCES companies(id)
);

INSERT INTO events
SELECT
    c.id,
    date(substr(e.event_date, -4) || '-' || substr(e.event_date, 4, 2)  || '-' || substr(e.event_date, 1, 2)) as ed,
    e.event_type, e.status, e.information
FROM "corporate-events" e, companies c
WHERE c.state = e.state AND c.reference_id = e.reference_id and c.registration_authority = e.registration_authority
ORDER BY c.id, ed;

DROP TABLE IF EXISTS persons;
CREATE TABLE persons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT,
    last_name TEXT,
    birth_date TEXT,
    birth_location TEXT
);

DROP TABLE IF EXISTS corporate_roles;
CREATE TABLE corporate_roles (
    company_id INTEGER,
    person_id INTEGER,
    role TEXT,
    active INTEGER DEFAULT 1,
    start_date TEXT,
    end_date TEXT,
    FOREIGN KEY (company_id) REFERENCES companies(id),
    FOREIGN KEY (person_id) REFERENCES persons(id)
);

DROP TABLE IF EXISTS typed_events;
CREATE TABLE typed_events (
    company_id INTEGER,
    event_date Text,
    type TEXT,
    data TEXT,
    FOREIGN KEY (company_id) REFERENCES companies(id)
);
