import sqlite3
conn = sqlite3.connect('PageRankdb.sqlite')
curr = conn.cursor()
curr.execute('''
SELECT Count(from_id) as inbound, old_rank, new_rank, id, url
from Pages join Links on Pages.id = Links.to_id
WHERE html is NOT NULL
GROUP BY id
ORDER BY inbound DESC
''')
count = 0
for row in curr:
    if count < 50 : print(row)
    count = count+1
print(count, 'rows')
curr.close()
