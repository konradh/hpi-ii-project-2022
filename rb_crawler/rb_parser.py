from __future__ import annotations

import re
import sqlite3

import click
import enum
import dataclasses
from typing import List, Optional, Any
import logging
import locale
import json
from tqdm import tqdm

no_match = 0


@dataclasses.dataclass
class TypedEvent:
    date: str
    type: EventType
    data: Any


class EventType(enum.Enum):
    COMPANY_DEACTIVATED = "COMPANY_DEACTIVATED"
    NEW_NAME = "NEW_NAME"
    NEW_TYPE = "NEW_TYPE"
    NEW_ADDRESS = "NEW_ADDRESS"
    NEW_PURPOSE = "NEW_PURPOSE"
    NEW_CAPITAL = "NEW_CAPITAL"
    NEW_CORPORATE_ROLE = "NEW_CORPORATE_ROLE"
    CORPORATE_ROLE_DEACTIVATED = "CORPORATE_ROLE_DEACTIVATED"
    CORPORATE_ROLE_REACTIVATED = "CORPORATE_ROLE_REACTIVATED"
    ROLE_REVOKED = "ROLE_REVOKED"


@dataclasses.dataclass
class Company:
    id: int
    name: str = None
    address: str = None
    type: str = None
    purpose: str = None
    capital: int = None
    currency: str = "EUR"
    is_active: bool = True
    corporate_roles: List[CorporateRole] = dataclasses.field(default_factory=list)
    persons: List[Person] = dataclasses.field(default_factory=list)
    typed_events: List[TypedEvent] = dataclasses.field(default_factory=list)

    def add_typed_event(self, date, event_type: EventType, data):
        if isinstance(data, str):
            data = {
                "value": data
            }
        self.typed_events.append(TypedEvent(date, event_type, data))

    def set_name(self, name, date):
        if name != self.name:
            self.add_typed_event(date, EventType.NEW_NAME, name)
            self.name = name

    def set_address(self, address, date):
        if address != self.address:
            self.add_typed_event(date, EventType.NEW_ADDRESS, address)
            self.address = address

    def set_type(self, company_type, date):
        if company_type != self.type:
            self.add_typed_event(date, EventType.NEW_TYPE, company_type)
            self.type = company_type

    def set_purpose(self, purpose, date):
        if purpose != self.purpose:
            self.add_typed_event(date, EventType.NEW_PURPOSE, purpose)
            self.purpose = purpose

    def set_capital(self, capital, currency, date):
        if capital != self.capital or currency != self.currency:
            self.add_typed_event(date, EventType.NEW_CAPITAL, {
                "capital": capital,
                "currency": currency
            })
            self.capital = capital
            self.currency = currency

    def set_active(self, is_active, date):
        if not is_active and self.is_active:
            self.add_typed_event(date, EventType.COMPANY_DEACTIVATED, '')
            self.is_active = False
        if is_active and not self.is_active:
            raise ValueError("Shutdown Company cannot be reactivated")

    def find_or_insert_person(self, new_person):
        for person in self.persons:
            if person.same_person(new_person):
                return person.merge(new_person)
        self.persons.append(new_person)
        return new_person

    def find_or_insert_corporate_role(self, role: CompanyRole, person):
        for corporate_role in self.corporate_roles:
            if corporate_role.role == role and corporate_role.person == person:
                return corporate_role
        new_corporate_role = CorporateRole(self)
        new_corporate_role.role = role
        new_corporate_role.person = person
        self.corporate_roles.append(new_corporate_role)
        return new_corporate_role


@dataclasses.dataclass(init=False)
class CorporateRole:
    company: Company = None
    person: Person = None
    role: CompanyRole = None
    start_date: str = None
    end_date: Optional[str] = None
    active: Optional[bool] = None

    def __init__(self, company):
        self.company = company

    def assign_date(self, is_active, date):
        if is_active:
            self.start_date = date
            if self.active is None:
                self.company.add_typed_event(date, EventType.NEW_CORPORATE_ROLE, {
                    "role": self.role.name,
                    "person": self.person.to_dict()
                })
            else:
                # Reactivation
                self.end_date = None
                self.company.add_typed_event(date, EventType.CORPORATE_ROLE_REACTIVATED, {
                    "role": self.role.name,
                    "person": self.person.to_dict()
                })
        else:
            self.end_date = date
            self.company.add_typed_event(date, EventType.CORPORATE_ROLE_DEACTIVATED, {
                "role": self.role.name,
                "person": self.person.to_dict()
            })
        self.active = is_active


class CompanyRole(enum.Enum):
    MANAGER = "Geschäftsführer"
    OWNER = "Inhaber"
    BOARD_MEMBER = "Vorstand"
    CHAIRMAN = "Vorstandsvorsitzender"
    LIQUIDATOR = "Liquidator"
    SOLE_PROXY = "Einzelprokura"
    PROXY = "Prokura"
    SOLE_PROXY_ = "Einzelprokurist"
    PROXY_ = "Prokurist"

    @classmethod
    def roles_regex(cls):
        return "|".join(map(lambda c: c.value, cls))

    def normalize(self):
        if self == CompanyRole.SOLE_PROXY_:
            return CompanyRole.SOLE_PROXY
        if self == CompanyRole.PROXY_:
            return CompanyRole.PROXY
        return self


@dataclasses.dataclass(init=False)
class Person:
    id: int = None
    first_name: str = None
    last_name: str = None
    birth_date: str = None
    birth_place: str = None

    def to_dict(self):
        return {
            "first_name": self.first_name,
            "last_name": self.last_name,
            "birth_date": self.birth_date,
            "birth_place": self.birth_place
        }

    def same_person(self, other_person: Person) -> bool:
        if self.first_name != other_person.first_name:
            return False
        if self.last_name != other_person.last_name:
            return False
        if self.birth_place is not None and other_person.birth_place is not None and self.birth_place != other_person.birth_place:
            return False
        if self.birth_date is not None and other_person.birth_date is not None and self.birth_date != other_person.birth_date:
            return False
        return True

    def merge(self, other_person: Person):
        self.birth_date = self.birth_date or other_person.birth_date
        self.birth_place = self.birth_place or other_person.birth_place
        return self


class RbParser:
    def __init__(self, database):
        self.db_conn: sqlite3.Connection = sqlite3.connect(database)
        self.db_conn.row_factory = sqlite3.Row
        self.db_cursor: sqlite3.Cursor = self.db_conn.cursor()

    def run(self):
        total_lines = self.db_conn.execute("select count(*) from events").fetchone()[0]
        self.db_cursor.execute("select * from events")
        current_company_id = -1
        current_company_events = []
        try:
            for i, row in enumerate(tqdm(self.db_cursor, total=total_lines)):
                if row["company_id"] == current_company_id:
                    current_company_events.append(row)
                else:
                    if len(current_company_events) != 0:
                        CompanyParser(self.db_conn, current_company_id, current_company_events).run()
                    current_company_id = int(row["company_id"])
                    current_company_events = [row]
            # Ensure last company is processed as well
            CompanyParser(self.db_conn, current_company_id, current_company_events).run()
        finally:
            self.tear_down()

    def tear_down(self):
        self.db_cursor.close()
        self.db_conn.commit()
        self.db_conn.close()


class CompanyParser:
    def __init__(self, db_conn, company_id, company_raw_events):
        self.db_conn: sqlite3.Connection = db_conn
        self.db_cursor: Optional[sqlite3.Cursor] = None
        self.company = Company(company_id)
        self.company_raw_events = company_raw_events

    def run(self):
        for i, event in enumerate(self.company_raw_events):
            self.parse_raw_event(event)
        self.save()

    def parse_raw_event(self, event: sqlite3.Row):
        if event["event_type"] == "delete":
            self.company.is_active = False
        else:
            match = self.parse_preamble(event)
            if match and len(match) > 150:
                logging.warning(f"Long match: {match}")
            self.parse_capital(event)
            self.parse_purpose(event)
            self.parse_persons(event)

    def parse_preamble(self, event: sqlite3.Row):
        information = event["information"]
        if (match := re.search(r'^(?:[A-Z]+ \d+(?: .)?: )?(.+?), ([^,.]+?) ?\(((.+?), )?(\d{5}) *([^,]+?)(, (.+?)|Gegenstand: (.+?))?\)',
                                    information)) is not None:
            self.company.set_name(match.group(1), event["event_date"])
            self.company.set_address(f"{match.group(4)}, {match.group(5)} {match.group(6)}", event["event_date"])
            if event["event_type"] == "create" and (rf := re.search(r'Rechtsform: (.+?)[.;]', information)) is not None:
                self.company.set_type(rf.group(1), event["event_date"])
            return match.group(0)
        if (match := re.search(r'^(?:[A-Z]+ \d+(?: .)?: )?(.+?), ([^,.(]+?[a-z])([A-Z][^,]+?), (\d{5}) ([^,.]+?)\.', information)) is not None:
            self.company.set_name(match.group(1), event["event_date"])
            self.company.set_address(f"{match.group(3)}, {match.group(4)} {match.group(2)}", event["event_date"])
            if event["event_type"] == "create" and (rf := re.search(r'Rechtsform: (.+?)[.;]', information)) is not None:
                self.company.set_type(rf.group(1), event["event_date"])
            return match.group(0)
        if (match := re.search(r'^(?:[A-Z]+ \d+(?: .)?: )?(.+?), ([^,.(]+?), ([^,]+?), (\d{5}) ([^,.]+?)\.', information)) is not None:
            self.company.set_name(match.group(1), event["event_date"])
            self.company.set_address(f"{match.group(3)}, {match.group(4)} {match.group(5)}", event["event_date"])
            if event["event_type"] == "create" and (rf := re.search(r'Rechtsform: (.+?)[.;]', information)) is not None:
                self.company.set_type(rf.group(1), event["event_date"])
            return match.group(0)
        # Fallback incomplete match
        if (match := re.search(r'^(.+?),', information)) is not None:
            self.company.set_name(match.group(1), event["event_date"])
            if (match := re.search(r'(\d{5}) ([^,.]+)', information)) is not None:
                self.company.set_address(f"???, {match.group(1)} {match.group(2)}", event["event_date"])
            return ''

        logging.warning(f"Preamble not matched: {information[:150]}")
        global no_match
        no_match += 1

    def parse_purpose(self, event: sqlite3.Row):
        match = re.search(r'[Gg]egenstand: (.+?)\.', event["information"])
        if match is not None:
            self.company.set_purpose(match.group(1), event["event_date"])

    def parse_capital(self, event: sqlite3.Row):
        match = re.search(r'[Kk]apital: (\d{1,3}(?:\.\d{3})*(?:,\d{2})?) ([A-Z]+)', event["information"])
        if match is not None:
            self.company.set_capital(locale.atof(match.group(1)), match.group(2), event["event_date"])

    def parse_persons(self, event: sqlite3.Row):
        information = re.sub(rf' (Rechtsform|Rechtsverhaeltnis|Rechtsverhältnis|{CompanyRole.roles_regex()})', r'$\1', event["information"])

        person_regex_only_name = r'(?:\d+\.[ \n]?)?(?:([^,;$]+?), ([^,;$ .]+(?: [^,;$ .]+)?)(?:, geb\. (?:[^,;$ .]+(?: [^,;$ .]+)?))?)'
        person_regex = rf'{person_regex_only_name}, (?!geb\.)([^,;$]+?), \*(\d{{2}}\.\d{{2}}\.\d{{4}})'
        person_multi_regex = rf'(({person_regex}; )*({person_regex}[.,;$ ]))'
        if len(matches := re.findall(rf'(?<![Nn]icht mehr)\$({CompanyRole.roles_regex()}):;? {person_multi_regex}', information)) != 0:
            for match in matches:
                self.create_persons(True, event["event_date"], match[0], match[1], person_regex, 4, 3)
        if len(matches := re.findall(rf'[Nn]icht mehr\$({CompanyRole.roles_regex()}):;? {person_multi_regex}', information)) != 0:
            for match in matches:
                self.create_persons(False, event["event_date"], match[0], match[1], person_regex, 4, 3)

        alternative_person_regex = rf'{person_regex_only_name}, \*(\d{{2}}\.\d{{2}}\.\d{{4}}), ([^,;$ ]+( [^,;$ ]+)*?)'
        alternative_person_multi_regex = rf'(({alternative_person_regex}; )*({alternative_person_regex}[.,;$ ]))'
        if len(matches := re.findall(rf'(?<![Nn]icht mehr)\$({CompanyRole.roles_regex()}):;? {alternative_person_multi_regex}', information)) != 0:
            for match in matches:
                self.create_persons(True, event["event_date"], match[0], match[1], alternative_person_regex, 3, 4)
        if len(matches := re.findall(rf'[Nn]icht mehr\$({CompanyRole.roles_regex()}):;? {alternative_person_multi_regex}', information)) != 0:
            for match in matches:
                self.create_persons(False, event["event_date"], match[0], match[1], alternative_person_regex, 3, 4)

        only_name_multi_regex = rf'(({person_regex_only_name}; )*({person_regex_only_name}[.,;$ ]))'
        if len(matches := re.findall(rf'[Nn]icht mehr\$({CompanyRole.roles_regex()}):;? {only_name_multi_regex}', information)) != 0:
            for match in matches:
                self.create_persons(False, event["event_date"], match[0], match[1], person_regex_only_name)

    def create_persons(self, active: bool, date: str, role: str, person_string: str, person_regex, birth_date_group=-1, birth_place_group=-1):
        persons = person_string.split('; ')
        for person in persons:
            match = re.search(person_regex, person)

            new_person = Person()
            new_person.last_name = match.group(1)
            new_person.first_name = match.group(2)
            if birth_place_group > 0:
                birth_place = match.group(birth_place_group)
                # Trim places with trailing punctuation
                birth_place = re.sub(r'[.,/]+$', '', birth_place)
                new_person.birth_place = birth_place
            if birth_date_group > 0:
                birth_date = match.group(birth_date_group)
                # note: using time library function fails if date is invalid (e.g. 26.00.1976...)
                new_person.birth_date = f"{birth_date[6:]}-{birth_date[3:5]}-{birth_date[:2]}"
            person = self.company.find_or_insert_person(new_person)

            corporate_role = self.company.find_or_insert_corporate_role(CompanyRole(role).normalize(), person)
            corporate_role.assign_date(active, date)

    def save(self):
        self.db_cursor = self.db_conn.cursor()
        self.save_company()
        self.save_persons()
        self.save_corporate_roles()
        self.save_typed_events()
        self.db_cursor.close()

    def save_company(self):
        self.db_cursor.execute(
            "update companies "
            "set name = ?, type = ?, address = ?, purpose = ?, capital = ?, currency = ?, is_active = ? "
            "where id = ?",
            (
                self.company.name,
                self.company.type,
                self.company.address,
                self.company.purpose,
                self.company.capital,
                self.company.currency,
                self.company.is_active,
                self.company.id
            )
        )

    def save_persons(self):
        for person in self.company.persons:
            self.db_cursor.execute(
                "insert into persons (first_name, last_name, birth_date, birth_location)"
                "values (?, ?, ?, ?) ",
                (person.first_name, person.last_name, person.birth_date, person.birth_place)
            )
            person.id = self.db_cursor.lastrowid

    def save_corporate_roles(self):
        self.db_cursor.executemany(
            "insert into corporate_roles (company_id, person_id, role, active, start_date, end_date) "
            "values (?, ?, ?, ?, ?, ?)",
            [(self.company.id, r.person.id, r.role.name, r.active, r.start_date, r.end_date) for r in self.company.corporate_roles]
        )

    def save_typed_events(self):
        self.db_cursor.executemany(
            "insert into typed_events (company_id, event_date, type, data) "
            "values (?, ?, ?, ?)",
            [(self.company.id, event.date, event.type.name, json.dumps(event.data)) for event in self.company.typed_events]
        )


@click.command()
@click.option("-d", "--database", default="../data/corporate-new.sqlite", help="The sqlite database file to connect to")
def run(database):
    RbParser(database).run()


if __name__ == '__main__':
    locale.setlocale(locale.LC_NUMERIC, 'de_DE.UTF-8')
    logging.disable(logging.WARNING)
    run()
