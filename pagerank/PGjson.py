import sqlite3
conn = sqlite3.connect('PageRankdb.sqlite')
curr = conn.cursor()

print('Creating JSON output on to-be created js file')
number = int(input('enter the no. of nodes'))

curr.execute('''
SELECT count(from_id) as inbound, old_rank, new_rank, id, url
FROM Pages join Links on pages.id = Links.to_id
WHERE html is NOT NULL and error is NULL
Group By id
ORDER BY id, inbound
''')

file_handle = open('pg.js','w')
nodes = list()
max_rank = None
min_rank = None
for row in curr:
    nodes.append(row)
    rank = row[2]
    if max_rank is None or max_rank < rank: max_rank = rank
    if min_rank is None or min_rank> rank: min_rank = rank
    if len(nodes) > number: break

if max_rank == min_rank or max_rank is None or min_rank is None:
    print('There is problem with pageRank.py, check it')
    quit()

file_handle.write('pageRankJson = {"nodes":[\n')
count = 0
map = dict()
ranks = dict()
for row in nodes:
    if count > 0 : file_handle.write(',\n')
    rank = row[2]
    rank = 19*((rank - min_rank)/(max_rank - min_rank))
    file_handle.write('{'+'"weight":'+str(row[0])+',"rank":'+str(rank)+',')
    file_handle.write('"id":'+str(row[3])+', "url":"'+row[4]+'"}')
    map[row[3]] = count
    ranks[row[3]] = rank
    count = count + 1
file_handle.write('],\n')

curr.execute('SELECT DISTINCT from_id, to_id from Links')
file_handle.write('"links":[\n')

count = 0
for row in curr:
    if row[0] not in map or row[1] not in map: continue
    if count > 0 : file_handle.write(',\n')
    rank = ranks[row[0]]
    srank = 19 * ((rank - min_rank)/(max_rank - min_rank))
    file_handle.write('{"source":'+str(map[row[0]])+',"target":'+str(map[row[1]])+',"value":3}')
    count = count + 1
file_handle.write(']};')
file_handle.close()
curr.close()
print('open html file to checkout the fun!!!!!!!!!!!!!!!!!!!!')
