import sqlite3
conn = sqlite3.connect('PageRankdb.sqlite')
curr = conn.cursor()

from_ids = list()
curr.execute('SELECT DISTINCT from_id FROM Links')
for row in curr:
    from_ids.append(row[0])

links = list()
to_ids = list()
curr.execute('SELECT DISTINCT from_id, to_id FROM LINKS')
for row in curr:
    from_id = row[0]
    to_id = row[1]
    if from_id == to_id:continue
    if from_id not in from_ids:continue
    if to_id not in from_ids:continue
    links.append(row)
    #print(links)
    if to_id not in to_ids:
        to_ids.append(to_id)
print(links,'links')
print(to_ids,'to_ids')
old_rank = dict()
for row in from_ids:
    curr.execute('SELECT new_rank from Pages where id = ?',(row,))
    old_rank[row] = curr.fetchone()[0]
print(from_ids,'from_ids')
print(old_rank,'old_rank')
# itni baar iterate
sval = input('how many iterations: ')
many = 1
if(len(sval)>0) : many = int(sval)

#sanity Check
if len(old_rank)<1 :
    print('nothing in old_rank, check data')
    quit()

#doing PAGE RANK IN MEMORY AND THEN WILL PUT IN DB
for i in range(many):
    new_ranks = dict()
    total = 0.0
    for (from_id, rank) in list(old_rank.items()):
        total = total + rank
        new_ranks[from_id] = 0.0
    print(new_ranks,'new_ranks')
    print(total,'total')

    for (node , rank) in list(old_rank.items()):
        give_ids = list()
        for (from_id, to_id) in links:
            if from_id != node:continue
            if to_id not in to_ids:continue
            give_ids.append(to_id)
        if(len(give_ids)<1):continue
        amount = rank/len(give_ids)

        for ids in give_ids:
            new_ranks[ids] = new_ranks[ids] + amount
    print(new_ranks,'new ranks actually')

    newtot = 0
    for (node, rank) in list(new_ranks.items()):
        newtot = newtot + rank
    evap = (total - newtot)/len(new_ranks)
    print(newtot,'newtot\n', evap,'evap')

    for node in new_ranks:
        new_ranks[node] = new_ranks[node] + evap
    print(new_ranks,'revised')

    newtot = 0
    for (node, rank) in list(new_ranks.items()):
        newtot = newtot + rank
    print('newtotal',newtot)

    total_diff = 0
    for (node , rank) in list(old_rank.items()):
        f_rank = new_ranks[node]
        diff = abs(rank - f_rank)
        total_diff = total_diff + diff
    print(total_diff,'total_difference')
    avg_diff = total_diff/len(old_rank)
    print(i+1, avg_diff)
    old_rank = new_ranks


curr.execute('UPDATE Pages SET old_rank = new_rank')
for (id, new_rank) in list(new_ranks.items()):
    curr.execute('UPDATE Pages SET new_rank = ? where id = ?',(new_rank,id))
conn.commit()
curr.close()
