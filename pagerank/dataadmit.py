import ssl
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen
import urllib.error
import sqlite3

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('PageRankdb.sqlite')
curr = conn.cursor()

curr.execute('''
CREATE TABLE IF NOT EXISTS Pages
(id integer primary key, url text unique, html text, error INTEGER
, old_rank real, new_rank real)
''')

curr.execute('''
Create table if not exists Links(from_id integer, to_id integer,
unique(from_id, to_id))
''')

curr.execute('''
create table if not exists Webs(url text unique)
''')

curr.execute('''SELECT id, url from Pages where html is NULL or error is NULL
order by Random() limit 1''')
row = curr.fetchone()
print(row)
if row is not None:
    print('restart the crawler, there is some error, remove the sqlite file as well')
else :
    starturl = input('enter the url you wish to crawl for: ')
    if (len(starturl)<1): starturl = 'http://www.dr-chuck.com/'
    if (starturl.endswith('/')): starturl = starturl[:-1]
    web = starturl
    print(web,starturl,'if there is backslash removed')
    if (starturl.endswith('htm') or starturl.endswith('html')):
        pos = starturl.rfind('/')
        web = starturl[:pos]
        print(web,starturl,'if ends with html taking only first part in web insert web in webs and starturl in pages')
    if (len(web)>1):
        curr.execute('Insert or ignore into Webs(url) values(?)', (web,))
        curr.execute('Insert or ignore into Pages(url,html,new_rank) values(?, NULL, 1.0)',(starturl,))
        conn.commit()

curr.execute('SELECT url FROM Webs')
webs = list()
for row in curr:
    webs.append(str(row[0]))
print('the url are taken from web database:')
print(webs)

many = 0
while True:
    if(many<1) :
        retrieve = input('enter no. of pages to retrieve: ')
        if (len(retrieve)<1):break
        many = int(retrieve)
    many = many - 1
    curr.execute('SELECT id,url from Pages where html is NULL and error is NULL order by RANDOM() LIMIT 1')
    try:
         row = curr.fetchone()
         fromid = row[0]
         url = row[1]
    except:
        print('No unretrieved pages were found')
        many = 0
        break
    print(fromid,url,end=' ')
#this page is being retrieved so no links from this page
    curr.execute('DELETE FROM Links WHERE from_id = ?',(fromid,))
    try:
        document = urlopen(url, context = ctx)
        html = document.read()
        if document.getcode()!=200:
            print('Error on page: ' , document.getcode())
            curr.execute('Update Pages set error = ? where url = ?',(document.getcode(),url))
        if 'text/html' != document.info().get_content_type() :
            print('ignoring non text pages ')
            curr.execute('Delete from Pages where url = ? ',(url,))
            conn.commit()
            continue

        print('('+str(len(html))+')' , end=' ')

        soup = BeautifulSoup(html, 'html.parser')
    except KeyboardInterrupt:
        print('program interuppted by user')
        break
    except:
        print('unable to retrieve or parse pages')
        curr.execute('update Pages set error = -1 where url = ?',(url,))
        conn.commit()
        continue

    curr.execute('insert or ignore into Pages(url,html,new_rank) values(?,NULL,1.0)' , (url,))
    curr.execute('update Pages set html = ? where url = ?', (memoryview(html) , url))
    #print(memoryview(html))
    conn.commit()


#retrieving the anchor Pages
    tags = soup('a')
    count = 0
    for tag in tags:
        href = tag.get('href', None)
        if(href is None) : continue
        uh = urlparse(href)
        if(len(uh.scheme) < 1):
            href = urljoin(url,href)
        ipos = href.find('#')
        if(ipos>1) : href = href[:ipos]
        if (href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif')) : continue
        if (href.endswith('/')) : href = href[:-1]
        if (len(href)<1): continue

    # checking if the url is any of the Webs
        found = False
        for web in webs:
            if(href.startswith(web)):
                found = True
                break
        if not found: continue
        curr.execute('Insert or ignore into Pages(url, html , new_rank) values(?,NULL,1.0)', (href,))
        count = count + 1
        conn.commit()

        curr.execute('SELECT id from Pages where url = ? LIMIT 1',(href,))
        try :
            row = curr.fetchone()
            toid = row[0]
            print(toid)
        except :
            print('id could not be retrieved')
            continue
        curr.execute('INSERT OR IGNORE into Links(from_id,to_id) values(?,?)' , (fromid,toid))
        print('inserted')
    print(count)
curr.close()
