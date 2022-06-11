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
