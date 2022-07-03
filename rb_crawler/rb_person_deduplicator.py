from __future__ import annotations

import sqlite3

import click
import dataclasses
import logging
from tqdm import tqdm
from typing import List
from Levenshtein import distance, jaro_winkler


@dataclasses.dataclass
class Person:
    id: int = None
    first_name: str = None
    last_name: str = None
    birth_date: str = None
    birth_place: str = None
    deleted: int = 0
    duplicates: List[Person] = dataclasses.field(default_factory=list)
    visited: bool = False

    def first_name_similiary(self, other_person: Person):
        return jaro_winkler(self.first_name, other_person.first_name)

    def last_name_similiary(self, other_person: Person):
        return jaro_winkler(self.last_name, other_person.last_name)

    def place_similarity(self, other_person: Person):
        return jaro_winkler(self.birth_place, other_person.birth_place)

    def is_similar(self, other_person: Person):
        if distance(self.birth_date, other_person.birth_date) > 1:
            return False

        first_name_dist = jaro_winkler(self.first_name, other_person.first_name)
        if first_name_dist < 0.94 \
            and self.first_name not in other_person.first_name and other_person.first_name not in self.first_name:
            return False

        if jaro_winkler(self.last_name, other_person.last_name) < 0.94:
            return False

        if jaro_winkler(self.birth_place, other_person.birth_place) < 0.90:
            return False

        return True

    def add_duplicate(self, other_person: Person):
        if other_person not in self.duplicates:
            self.duplicates.append(other_person)

    def transitive_hull(self):
        hull = []
        for duplicate in self.duplicates:
            h = duplicate.transitive_hull()
            for d in h:
                if d not in hull:
                    hull.append(d)
        self.duplicates = hull
        self.visited = True
        return hull + [self]


class SQLExecutor:
    def __init__(self, database):
        self.db_conn: sqlite3.Connection = sqlite3.connect(database)
        # self.db_conn.set_trace_callback(print)
        self.db_conn.row_factory = sqlite3.Row

    def run(self):
        try:
            self.execute_queries()
        finally:
            self.db_conn.commit()
            self.db_conn.close()

    def execute_queries(self):
        pass

class InvalidDatesRemover(SQLExecutor):
    def execute_queries(self):
        # There are only a few hundred of those, so don't worry about trying to correct them
        self.db_conn.execute("UPDATE persons SET birth_date = null WHERE birth_date < '1800'")

class PersonEqualityDeduplicator(SQLExecutor):
    DUPLICATE_PERSONS_QUERY = (
        "SELECT min(p1.id) as main_entity_id, p2.id as duplicate_id, p2.first_name, p2.last_name, p2.birth_date, p2.birth_location "
        "FROM persons p1, persons p2 "
        "WHERE "
        "    p1.deleted = 0 AND p2.deleted = 0"
        "  AND p1.birth_date is not null "
        "  AND p1.id < p2.id "
        "  AND p1.first_name = p2.first_name "
        "  AND p1.last_name = p2.last_name "
        "  AND p1.birth_date = p2.birth_date "
        "  AND p1.birth_location = p2.birth_location "
        "GROUP BY p2.id, p2.first_name, p2.last_name, p2.birth_date, p2.birth_location"
    )

    def execute_queries(self):
        # Create index on person_id so that each update query does not have to scan the table to find the correct id
        self.db_conn.execute(
            "CREATE INDEX IF NOT EXISTS corporate_roles_id "
            "ON corporate_roles(person_id)"
        )
        total_lines = self.db_conn.execute(f"SELECT count(*) FROM ({self.DUPLICATE_PERSONS_QUERY})").fetchone()[0]
        query_cursor = self.db_conn.execute(self.DUPLICATE_PERSONS_QUERY)
        update_cursor = self.db_conn.cursor()
        ids_to_delete = []
        for row in tqdm(query_cursor, total=total_lines):
            # Map all corporate roles of duplicate person to the same person
            update_cursor.execute(
                "UPDATE corporate_roles "
                "SET person_id = ? "
                "WHERE person_id = ?",
                (row["main_entity_id"], row["duplicate_id"])
            )
            ids_to_delete.append(str(row["duplicate_id"]))

        # Delete duplicate persons
        self.db_conn.set_trace_callback(print)
        self.db_conn.execute(f"UPDATE persons SET deleted = 1 WHERE id IN ({','.join(ids_to_delete)})")

class PersonFuzzyDeduplicator(SQLExecutor):
    PERSONS_QUERY = (
            "SELECT * FROM persons "
            "WHERE deleted = 0 AND birth_location IS NOT NULL AND birth_date IS NOT NULL"
        )

    # Benötigte Partitionen:
    # p = 2000000 Personen
    # Für 100 Partitionen pro Partition: 20000 ** 2 * 10 ** -9 / 60 Minuten = 0.007 => insgesamt 0.7 Minuten
    # Für 10 Partitionen: 10x 0.67 Minuten => 6.7 Minuten
    # Für 1 Partition: 67 Minuten
    # Unter der Annahme, dass ein Vergleich höchstens 3CPU-Takte braucht, ist also vermutlich Faktor 100 höher...

    def group_by1(self, person: Person):
        return person.first_name[:3] + person.last_name[-3:] + person.last_name[3:]

    def group_by2(self, person: Person):
        return person.last_name[:4] + person.birth_date[2:7]

    def group_by3(self, person: Person):
        return person.birth_date[-5:] + person.first_name[-3:]

    def group_by4(self, person: Person):
        return person.birth_date[2:7] + person.first_name[3:]

    def execute_queries(self):
        # As there are quite a lot of cases where persons have the name but different birth dates/places, we cannot
        # reliably match the persons where one of those is missing
        query_cursor = self.db_conn.execute(self.PERSONS_QUERY)
        total_lines = self.db_conn.execute(f"SELECT count(*) FROM ({self.PERSONS_QUERY})").fetchone()[0]
        groupings = [self.group_by1, self.group_by2, self.group_by3, self.group_by4]
        persons = []
        person_groups = [{} for _ in groupings]

        for row in tqdm(query_cursor, total=total_lines):
            person = Person(*row)
            persons.append(person)
            for i, grouping in enumerate(groupings):
                if grouping(person) not in person_groups[i]:
                    person_groups[i][grouping(person)] = [person]
                else:
                    person_groups[i][grouping(person)].append(person)

        for person_grouping in person_groups:
            for person_group in tqdm(sorted(person_grouping.values(), key=len, reverse=True)):
                for i, person1 in enumerate(person_group):
                    for j in range(i + 1, len(person_group)):
                        person2 = person_group[j]
                        if person1.is_similar(person2):
                            person1.add_duplicate(person2)

        update_cursor = self.db_conn.cursor()
        ids_to_delete = []
        for person in persons:
            if not person.visited:
                person.transitive_hull()
                if len(person.duplicates) == 0:
                    continue
                # Map all corporate roles of duplicate person to the same person
                duplicate_ids = list(map(lambda p: str(p.id), person.duplicates))
                update_cursor.execute(
                    "UPDATE corporate_roles "
                    f"SET person_id = {person.id} "
                    f"WHERE person_id IN ({','.join(duplicate_ids)})",
                )
                ids_to_delete += duplicate_ids

        # Delete duplicate persons
        self.db_conn.set_trace_callback(print)
        self.db_conn.execute(f"UPDATE persons SET deleted = 1 WHERE id IN ({','.join(ids_to_delete)})")


@click.command()
@click.option("-d", "--database", default="../data/corporate-new.sqlite", help="The sqlite database file to connect to")
def run(database):
    InvalidDatesRemover(database).run()
    PersonEqualityDeduplicator(database).run()
    PersonFuzzyDeduplicator(database).run()


if __name__ == '__main__':
    logging.disable(logging.WARNING)
    run()
