#!/usr/bin/env python3
import re
import sys
import xlrd
from neo4j.v1 import GraphDatabase, basic_auth
from neo4j.exceptions import ConstraintError

wb = xlrd.open_workbook(sys.argv[1])
sheets = wb.sheets() if len(sys.argv) == 2 else [wb.sheet_by_name(sys.argv[2])]


class MatrixParser:
    MONTHS = [None, 'styczeń', 'luty', 'marzec', 'kwiecień', 'maj', 'czerwiec', 'lipiec', 'sierpień', 'wrzesień',
              'październik', 'listopad', 'grudzień']
    SHEET_NAME_FIXES = {
        'GRUPA ostatnia MARZEC': 'GRUPA 12 MARZEC',
        'Grupa ostatnia LUTY': 'GRUPA 12 LUTY',
        'Grupa ostatnia STYCZEN': 'GRUPA 13 STYCZEŃ',
    }
    PLAYER_FIXES = {
        'Kowalski Piotr': 'Piotr Kowalski',
        'Katarzyna Błach': 'Katarzyna Mazur',
        'Nazarov Konstiantyn': 'Konstiantyn Nazarov',
        'Wycisło Wojtek': 'Wojtek Wycisło',
        'Lipiński Marek': 'Marek Lipiński',
        'Ronkiewicz Łukasz': 'Łukasz Ronkiewicz',
    }

    def handles(self, name):
        if 'wolna' in name:
            return False

        return 'grupa' in name or ('masters' in name and 0 == name.index('masters'))

    def parse(self, sheet, session):
        count = 0
        sheet_name = sheet.name if sheet.name not in self.SHEET_NAME_FIXES else self.SHEET_NAME_FIXES[sheet.name]
        group_month = sheet_name.lower().replace('grupa', '').strip()
        print(group_month)
        if len(group_month.split()) == 1:
            group_month += ' luty' if sheet_name.lower() == 'masters' else ' styczeń'

        group, month = group_month.split()
        month_no = self.MONTHS.index(month)
        print('>> ' + sheet.name)
        players = []
        for row in range(2, sheet.nrows):
            if sheet.cell(row, 0).value == '':
                break
            player = sheet.cell(row, 0).value
            player = re.sub(r'[0-9\.\(\)\-]', ' ', player).strip()
            player = ' '.join(player.split()[0:2])  # only 2 first words
            player = player if player not in self.PLAYER_FIXES else self.PLAYER_FIXES[player]
            if ' ' in player:
                player = player.split()[1]

            player = player[0].upper() + player[1:]
            players.append((row, player))

            try:
                session.run("CREATE({}:Player {{name: '{}'}})".format(player.replace(' ', ''), player))
            except ConstraintError:
                pass

        for row, player in players:
            for col in range(1, len(players) + 1):
                try:
                    score = str(sheet.cell(row, col).value).lower()
                    if len(score) < 3:
                        continue

                    opponent = players[col - 1][1]
                    print('>> Group: {}, month: {} | {} ({}) {}'.format(group, month, player, score, opponent))
                    try:
                        player_sets, opponent_sets = score.split('x')
                        relationship = 'win' if player_sets > opponent_sets else 'lose'
                    except ValueError:
                        if score in ['walk+', 'walk-']:
                            relationship = 'win' if score == 'walk+' else 'lose'
                        else:
                            raise Exception('Unknown score {} for players {} {}', score, player, opponent)
                    query = """
                            MATCH(n:Player {{name: '{}'}}), (m:Player {{name: '{}'}})
                            CREATE UNIQUE(n)-[:{} {{score: '{}', group: '{}', month: '{}', month_no: {}}}]->(m)
                        """.format(player, opponent, relationship, score, group, month, month_no)
                    try:
                        session.run(query)
                        count += 1
                    except ConstraintError as e:
                        print('Create relationship: ' + str(e))
                except Exception as e:
                    print("Unexpected error: {}, msg: {}".format(sys.exc_info()[0], str(e)))
                    raise e

        return count


class Runner:
    parsers = [MatrixParser()]
    sheets = []
    driver = None

    def __init__(self, path, custom_sheet=None, clean=False):
        workbook = xlrd.open_workbook(path)
        self.sheets = workbook.sheets() if not custom_sheet else [workbook.sheet_by_name(custom_sheet)]
        self.driver = GraphDatabase.driver('bolt://localhost:7687', auth=basic_auth('neo4j', 'Welcome01'))
        session = self.driver.session()
        if clean:
            session.run('MATCH (n) DETACH DELETE n')

        session.run('CREATE CONSTRAINT ON (player:Player) ASSERT player.name IS UNIQUE')

    def run(self):
        count = 0
        for sheet in sheets:
            for parser in self.parsers:
                if parser.handles(sheet.name.lower()):
                    session = self.driver.session()
                    count += parser.parse(sheet, session)
                    try:
                        session.close()
                    except ConstraintError as e:
                        print('Session close: ' + str(e))
        return count


runner = Runner(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None, True)
count = runner.run()
print('>> created {} relationships'.format(count))
