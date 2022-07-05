# Assumption: RB and LEI data have no company duplicates
# 1. Create temporary table id,postal code,name from companies
# 2. Create temporary table (lei,name,postal code) from german lei companies with relationships
# 3. Fuzzy match names for all combinations in postal code

from tqdm import tqdm
import string
import sqlite3
import argparse
import sys
import re
import Levenshtein as matching

SELECT_COUNT = \
'''SELECT COUNT(*) FROM ({})'''

CREATE_LEI_TEMP = \
'''CREATE TEMPORARY TABLE `lei-temp`
       (lei TEXT,
        name TEXT,
        postalcode TEXT)'''

SELECT_LEI = \
'''SELECT LEI, Entity_LegalName, Entity_LegalAddress_PostalCode
   FROM `lei-data`
   WHERE Entity_LegalAddress_Country="DE"
         AND Entity_EntityStatus="ACTIVE"'''

INSERT_LEI_TEMP = \
'''INSERT INTO `lei-temp` VALUES (?, ?, ?)'''

CREATE_RB_TEMP = \
'''CREATE TEMPORARY TABLE `rb-temp`
       (id INTEGER,
        name TEXT,
        postalcode TEXT)'''

SELECT_RB = \
'''SELECT id, name, address
   FROM `companies`'''

INSERT_RB_TEMP = \
'''INSERT INTO `rb-temp` VALUES (?, ?, ?)'''

SELECT_JOIN_TEMP = \
'''SELECT rb.id as id, lei.lei as lei, rb.name as rb_name, lei.name as lei_name
   FROM `rb-temp` as rb, `lei-temp` as lei
   WHERE rb.postalcode = lei.postalcode'''

DROP_RB_LEI = \
'''DROP TABLE IF EXISTS `rb-lei`'''

CREATE_RB_LEI = \
'''CREATE TABLE `rb-lei`
       (id INTEGER,
        lei TEXT)'''

INSERT_RB_LEI = \
'''INSERT INTO `rb-lei` VALUES (?, ?)'''

def temporary_LEI(connection):
    no_postal = 0
    postal_regex = re.compile('[0-9]{5}')

    connection.execute(CREATE_LEI_TEMP)
    rows = connection.execute(SELECT_LEI)
    num_rows = connection.execute(SELECT_COUNT.format(SELECT_LEI)).fetchone()[0]
    insert_cursor = connection.cursor()
    for row in tqdm(rows, total=num_rows, desc='Creating temporary LEI'):
        postal = postal_regex.search(row[2])
        if postal is None:
            no_postal += 1
            continue
        postal = postal.group()
        insert_cursor.execute(INSERT_LEI_TEMP, (row[0], row[1], postal))
    print(f'{no_postal}/{num_rows} of German LEI companies without recognizable postal code', file=sys.stderr)

def temporary_RB(connection):
    error = 0
    postal_regex = re.compile('[0-9]{5}')

    connection.execute(CREATE_RB_TEMP)
    rows = connection.execute(SELECT_RB)
    num_rows = connection.execute(SELECT_COUNT.format(SELECT_RB)).fetchone()[0]
    insert_cursor = connection.cursor()
    for row in tqdm(rows, total=num_rows, desc='Creating temporary RB'):
        if row[1] is None or row[2] is None:
            error += 1
            continue
        postal = postal_regex.search(row[2])
        if postal is None:
            error += 1
            continue
        postal = postal.group()
        insert_cursor.execute(INSERT_RB_TEMP, (row[0], row[1], postal))
    print(f'{error}/{num_rows} of German RB companies without recognizable postal code or name', file=sys.stderr)

BAD_CHARACTERS = set(string.ascii_letters + string.digits)
EVIL_CHARACTERS = set(string.digits)

def better_edit_distance_one_direction(a, b):
    diff = 0
    ops = matching.editops(a, b)
    for operation, spos, dpos in ops:
        char = ''
        if operation == 'delete':
            char = a[spos]
        else:
            char = b[dpos]
        if char in BAD_CHARACTERS:
            diff += 1
        if char in EVIL_CHARACTERS:
            diff += 5
    return diff

def better_edit_distance(a, b):
    return min(better_edit_distance_one_direction(a, b), better_edit_distance_one_direction(a, b))

def match_join(connection):
    matches = 0

    connection.execute(DROP_RB_LEI)
    connection.execute(CREATE_RB_LEI)

    num_rows = connection.execute(SELECT_COUNT.format(SELECT_JOIN_TEMP)).fetchone()[0]
    rows = connection.execute(SELECT_JOIN_TEMP)
    insert_cursor = connection.cursor()
    for row in tqdm(rows, total=num_rows, desc='Fuzzy matching company names'):
        dist = better_edit_distance(row[2], row[3])
        if dist == 0:
            insert_cursor.execute(INSERT_RB_LEI, (row[0], row[1]))
            matches += 1
    print(f'{matches} matches found between RB and LEI company names', file=sys.stderr)

def parse_args():
    parser = argparse.ArgumentParser(description='Fuzzy match LEI relationship and RB companies')
    parser.add_argument('database', help='sqlite database to operate on')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    conn = sqlite3.connect(args.database)
    temporary_LEI(conn)
    temporary_RB(conn)
    match_join(conn)
    conn.commit()

