DROP TABLE IF EXISTS events;
CREATE TABLE events (
    company_id INTEGER,
    event_date TEXT,
    event_type TEXT,
    status TEXT,
    information TEXT,
    FOREIGN KEY (company_id) REFERENCES companies(id)
);

DROP TABLE IF EXISTS typed_events;
CREATE TABLE typed_events (
    company_id INTEGER,
    event_date Text,
    type TEXT,
    data TEXT,
    FOREIGN KEY (company_id) REFERENCES companies(id)
);

INSERT INTO events
    SELECT
        c.id,
        date(substr(e.event_date, -4) || '-' || substr(e.event_date, 4, 2)  || '-' || substr(e.event_date, 1, 2)) as ed,
        e.event_type, e.status, e.information
    FROM 
        "corporate-events" e,
        companies c
    WHERE
            c.state = e.state
        AND c.reference_id = e.reference_id
        AND c.registration_authority = e.registration_authority
    ORDER BY c.id, ed;
