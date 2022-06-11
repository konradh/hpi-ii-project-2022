DROP TABLE IF EXISTS "parents";
CREATE TABLE "parents" (
    parent INTEGER,
    child INTEGER,
    FOREIGN KEY(parent) REFERENCES companies(id),
    FOREIGN KEY(child) REFERENCES companies(id)
    -- Maybe model more of the relationship and add dates and so on...
);

DROP TABLE IF EXISTS "companies-with-lei";
CREATE TEMPORARY TABLE "companies-with-lei" (
    id INTEGER,
    lei TEXT,
    FOREIGN KEY(id) REFERENCES companies(id),
    FOREIGN KEY(lei) REFERENCES "lei-data"(LEI)
);

INSERT INTO "companies-with-lei"
    SELECT DISTINCT
        companies.id,
        lei.LEI
    FROM
        "companies",
        "lei-data" lei
    WHERE -- Matching of HR and LEI data needs to be improved
            lei.Entity_LegalJurisdiction = 'DE'
        AND companies.name = lei.Entity_LegalName;


INSERT INTO parents (parent, child)
    SELECT DISTINCT parents.id, children.id
    FROM
        "companies-with-lei" parents,
        "companies-with-lei" children,
        "lei-relationship-data" relationships
    WHERE
            relationships.Relationship_StartNode_NodeID = children.lei
        AND relationships.Relationship_EndNode_NodeID = parents.lei
        AND (
                relationships.Relationship_RelationshipType = 'IS_ULTIMATELY_CONSOLIDATED_BY'
             OR relationships.Relationship_RelationshipType = 'IS_DIRECTLY_CONSOLIDATED_BY'
        );
