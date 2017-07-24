#!/usr/bin/env python3
import re
import sys
import xlrd
from neo4j.v1 import GraphDatabase, basic_auth
from neo4j.exceptions import ConstraintError


class PlayersFetcher:
    PLAYER_FIXES = {
        'Kowalski Piotr': 'Piotr Kowalski',
        'Katarzyna Błach': 'Katarzyna Mazur',
        'Nazarov Konstiantyn': 'Konstiantyn Nazarov',
        'Wycisło Wojtek': 'Wojtek Wycisło',
        'Lipiński Marek': 'Marek Lipiński',
        'Ronkiewicz Łukasz': 'Łukasz Ronkiewicz',
        'Damian Pierikarz': 'Damian Piernikarz',
        'Jacek Godyn': 'Jacek Godyń',
    }

    @staticmethod
    def get_surname(player):
        player = re.sub(r'[0-9\.\(\)\-]', ' ', player).strip()
        player = ' '.join(player.split()[0:2])  # only 2 first words
        player = player if player not in PlayersFetcher.PLAYER_FIXES else PlayersFetcher.PLAYER_FIXES[player]
        if ' ' in player:
            player = player.split()[1]

        return player[0].upper() + player[1:]

    @staticmethod
    def fetch_players(session, sheet, start=2, offset=0):
        players = []
        for row in range(start, sheet.nrows):
            if sheet.cell(row, offset).value == '':
                break
            if 'JUMP' in sheet.cell(row, offset).value:
                continue

            player = PlayersFetcher.get_surname(sheet.cell(row, offset).value)
            players.append((row, player))

            try:
                session.run("CREATE({}:Player {{name: '{}'}})".format(player.replace(' ', ''), player))
            except ConstraintError:
                pass
        return players


class FlatParser:
    MAX_MATCHES = 17

    def handles(self, name):
        return 'dywizja b' in name

    def parse(self, year, sheet, session):
        count = 0
        round = sheet.name.replace('.', '').split()[1]

        print('>> ' + sheet.name)
        players = PlayersFetcher.fetch_players(session, sheet, offset=1, start=4)

        for row, player in players:
            for col in range(3, self.MAX_MATCHES * 3, 3):
                try:
                    if len(sheet.cell(row, col).value.strip()) == 0:
                        break

                    opponent = PlayersFetcher.get_surname(sheet.cell(row, col).value)
                    score = str(sheet.cell(row, col + 1).value).lower()
                    if len(score) < 3:
                        continue

                    print('>> Group: B, round: {} | {} ({}) {}'.format(round, player, score, opponent))
                    try:
                        player_sets, opponent_sets = score.split('x')
                        relationship = 'win' if player_sets > opponent_sets else 'lose'
                    except ValueError:
                        score = score.replace('over', '')
                        score = score.replace('ower', '')
                        if score in ['walk+', 'walk-']:
                            relationship = 'win' if score == 'walk+' else 'lose'
                        else:
                            print('Unknown score {} for players {} {}', score, player, opponent)
                            continue
                    query = """
                            MATCH(n:Player {{name: '{}'}}), (m:Player {{name: '{}'}})
                            CREATE UNIQUE(n)-[:{} {{score: '{}', group: 'B', round: '{}', year: {}}}]->(m)
                        """.format(player, opponent, relationship, score, round, year)
                    try:
                        session.run(query)
                        count += 1
                    except ConstraintError as e:
                        print('Create relationship: ' + str(e))
                except Exception as e:
                    print("Unexpected error: {}, msg: {}".format(sys.exc_info()[0], str(e)))
                    raise e
        return count


class MatrixParser:
    MONTHS = [None, 'styczeń', 'luty', 'marzec', 'kwiecień', 'maj', 'czerwiec', 'lipiec', 'sierpień', 'wrzesień',
              'październik', 'listopad', 'grudzień']
    SHEET_NAME_FIXES = {
        'GRUPA ostatnia MARZEC': 'GRUPA 12 MARZEC',
        'Grupa ostatnia LUTY': 'GRUPA 12 LUTY',
        'Grupa ostatnia STYCZEN': 'GRUPA 13 STYCZEŃ',
    }

    def handles(self, name):
        if 'wolna' in name:
            return False
        return 'grupa' in name or ('masters' in name and 0 == name.index('masters')) or 'dywizja a' in name

    def parse(self, year, sheet, session):
        count = 0
        if 'dywizja' in sheet.name.lower():
            round = sheet.name.split()[1]
            group = 'A'
        else:
            sheet_name = sheet.name if sheet.name not in self.SHEET_NAME_FIXES else self.SHEET_NAME_FIXES[sheet.name]
            group_month = sheet_name.lower().replace('grupa', '').strip()
            print(group_month)
            if len(group_month.split()) == 1:
                group_month += ' luty' if sheet_name.lower() == 'masters' else ' styczeń'

            group, month = group_month.split()
            round = self.MONTHS.index(month)

        print('>> ' + sheet.name)
        players = PlayersFetcher.fetch_players(session, sheet)

        for row, player in players:
            for col in range(1, len(players) + 1):
                try:
                    score = str(sheet.cell(row, col).value).lower()
                    if len(score) < 3:
                        continue

                    opponent = players[col - 1][1]
                    print('>> Group: {}, round: {} | {} ({}) {}'.format(group, round, player, score, opponent))
                    try:
                        player_sets, opponent_sets = score.split('x')
                        relationship = 'win' if player_sets > opponent_sets else 'lose'
                    except ValueError:
                        if score in ['walk+', 'walk-']:
                            relationship = 'win' if score == 'walk+' else 'lose'
                        else:
                            print('Unknown score {} for players {} {}', score, player, opponent)
                            continue
                    query = """
                            MATCH(n:Player {{name: '{}'}}), (m:Player {{name: '{}'}})
                            CREATE UNIQUE(n)-[:{} {{score: '{}', group: '{}', round: '{}', year: {}}}]->(m)
                        """.format(player, opponent, relationship, score, group, round, year)
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
    parsers = [MatrixParser(), FlatParser()]
    sheets = []
    driver = None

    def __init__(self, path, custom_sheet=None, clean=False):
        workbook = xlrd.open_workbook(path)
        self.sheets = workbook.sheets() if (not custom_sheet or len(custom_sheet) == 0) \
            else [workbook.sheet_by_name(custom_sheet)]
        self.driver = GraphDatabase.driver('bolt://localhost:7687', auth=basic_auth('neo4j', 'Welcome01'))
        session = self.driver.session()
        if clean:
            session.run('MATCH (n) DETACH DELETE n')

        session.run('CREATE CONSTRAINT ON (player:Player) ASSERT player.name IS UNIQUE')

    def run(self, year):
        count = 0
        for sheet in self.sheets:
            for parser in self.parsers:
                if parser.handles(sheet.name.lower()):
                    session = self.driver.session()
                    count += parser.parse(year, sheet, session)
                    try:
                        session.close()
                    except ConstraintError as e:
                        print('Session close: ' + str(e))
        return count


runner = Runner(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None, sys.argv[3] if len(sys.argv) > 3 else False)
total = runner.run(2017 if '2017' in sys.argv[1] else 2016)
print('>> created {} relationships'.format(total))
