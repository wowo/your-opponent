#!/usr/bin/env python3
import re
import sys
import xlrd
from neo4j.v1 import GraphDatabase, basic_auth
from neo4j.exceptions import ConstraintError

driver = GraphDatabase.driver('bolt://localhost:7687', auth=basic_auth('neo4j', 'Welcome01'))
session = driver.session()
session.run('MATCH (n) DETACH DELETE n')
session.close()

wb = xlrd.open_workbook(sys.argv[1])
sheets = wb.sheets() if len(sys.argv) == 2 else [wb.sheet_by_name(sys.argv[2])]
months = [None, 'styczeń', 'luty', 'marzec', 'kwiecień', 'maj', 'czerwiec', 'lipiec', 'sierpień', 'wrzesień',
          'październik', 'listopad', 'grudzień']
sheet_name_fixes = {
    'GRUPA ostatnia MARZEC': 'GRUPA 12 MARZEC',
    'Grupa ostatnia LUTY': 'GRUPA 12 LUTY',
    'Grupa ostatnia STYCZEN': 'GRUPA 13 STYCZEŃ',
}
player_fixes = {
    'Kowalski Piotr': 'Piotr Kowalski',
    'Katarzyna Błach': 'Katarzyna Mazur',
    'Nazarov Konstiantyn': 'Konstiantyn Nazarov',
    'Wycisło Wojtek': 'Wojtek Wycisło',
    'Lipiński Marek': 'Marek Lipiński',
    'Ronkiewicz Łukasz': 'Łukasz Ronkiewicz',
}
count = 0
for sheet in sheets:
    session = driver.session()

    session.run('CREATE CONSTRAINT ON (player:Player) ASSERT player.name IS UNIQUE')
    if 'grupa' in sheet.name.lower() and 'wolna' not in sheet.name.lower():
        sheet_name = sheet.name if sheet.name not in sheet_name_fixes else sheet_name_fixes[sheet.name]
        group_month = sheet_name.lower().replace('grupa', '').strip()
        if len(group_month.split()) == 1:
            group_month += ' styczeń'

        group, month = group_month.split()
        month_no = months.index(month)
        print('>> ' + sheet.name)
        players = []
        for row in range(2, sheet.nrows):
            if sheet.cell(row, 0).value == '':
                break
            player = sheet.cell(row, 0).value
            player = re.sub(r'[0-9\.\(\)\-]', ' ', player).strip()
            player = ' '.join(player.split()[0:2])  # only 2 first words
            player = player if player not in player_fixes else player_fixes[player]
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
                            if score == 'walk+':
                                relationship = 'win'
                                player_sets = 5
                                opponent_sets = 0
                            else:
                                relationship = 'lose'
                                player_sets = 0
                                opponent_sets = 5
                        else:
                            raise Exception('Unknown score {} for players {} {}', score, player, opponent)
                    query = """
                        MATCH(n:Player {{name: '{}'}}), (m:Player {{name: '{}'}})
                        CREATE (n)-[:{} {{score: '{}', group: {}, month: '{}', month_no: {}}}]->(m)
                    """.format(player, opponent, relationship, score, group, month, month_no)
                    try:
                        session.run(query)
                        count += 1
                    except ConstraintError as e:
                        print('Create relationship: ' + str(e))
                except Exception as e:
                    print("Unexpected error: {}, msg: {}".format(sys.exc_info()[0], str(e)))
                    raise e
    try:
        session.close()
    except ConstraintError as e:
        print('Session close: ' + str(e))

print('>> created {} relationships'.format(count))
