from flask import Flask, render_template, redirect, request
import sqlite3
from dataclasses import dataclass
from Levenshtein import distance


@dataclass
class Company:
    id: int
    name: str
    similarity: float = 0


app = Flask(__name__)
db_conn = sqlite3.connect("../data/corporate-task3.sqlite")
app.companies = [
    Company(*row) for row in db_conn.execute("SELECT id, name FROM companies WHERE name IS NOT NULL")
]
db_conn.close()


@app.route("/")
def hello_world():
    return redirect("/search")


@app.route("/search")
def search():
    results = []
    if (name := request.args.get("companyName")) is not None:
        for company in app.companies:
            company.similarity = distance(company.name, name)
        results = sorted(app.companies, key=lambda x: x.similarity)[:10]

    return render_template("search.html", results=results, searchedCompanyName=name or "SAP SE")


@app.route("/companies/<id>")
def show_company(id: int):
    db_conn = sqlite3.connect("../data/corporate-task3.sqlite")
    db_conn.row_factory = sqlite3.Row
    company = db_conn.execute("SELECT * FROM companies WHERE id = ?", (id,)).fetchone()

    persons = db_conn.execute(
        "SELECT p.last_name, p.first_name, cr.role, cr.start_date "
        "FROM corporate_roles cr JOIN persons p on cr.person_id = p.id "
        "WHERE cr.active = 1 "
        "AND cr.company_id = ?"
        "ORDER BY p.last_name, p.first_name",
        (id,)
    ).fetchall()

    parents = list(db_conn.execute(
        "SELECT id, name "
        "FROM companies JOIN parents p on companies.id = p.parent "
        "WHERE p.child = ? "
        "ORDER BY name",
        (id,)
    ).fetchall())

    children = list(db_conn.execute(
        "SELECT id, name "
        "FROM companies JOIN parents p on companies.id = p.child "
        "WHERE p.parent = ? "
        "ORDER BY name",
        (id,)
    ).fetchall())

    related_companies = db_conn.execute(
        "SELECT c.id, c.name, GROUP_CONCAT(p.last_name || ', ' || p.first_name || ' (' || r.role || ')', ', ') as persons_in_common "
        "FROM companies c JOIN corporate_roles r on c.id = r.company_id JOIN persons p on r.person_id = p.id "
        "WHERE p.id IN ("
        "   SELECT persons.id "
        "   FROM persons JOIN corporate_roles cr on persons.id = cr.person_id "
        "   WHERE cr.active = 1 "
        "   AND cr.company_id = ?"
        ") "
        "AND r.active = 1 "
        "AND c.id != ? "
        "GROUP BY c.id, c.name "
        "ORDER BY c.name",
        (id, id)
    ).fetchall()

    db_conn.close()

    return render_template(
        "company.html",
        company=company,
        persons=persons,
        parents=parents,
        children=children,
        related_companies=related_companies
    )
