import sqlite3
conn = sqlite3.connect('PageRankdb.sqlite')
curr = conn.cursor()
curr.execute('Select count(*) from Pages where html is NOT NULL')
print(curr.fetchone()[0])
curr.close()
