#!/usr/bin/env python
import pika
import sqlite3
import csv
import json
from xml.etree import ElementTree

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')




def CreateCSV(query, FileName):
    db = sqlite3.connect(DBlocation)
    cur = db.cursor()
    sql = query
    cur.execute(sql)
    AllRows = cur.fetchall()
    FileOpen = open(f'{FileName}.csv', 'w', newline='')
    file = csv.writer(FileOpen)
    file.writerows(AllRows)
    FileOpen.close()
    print(f'{FileName}.csv has been created')
    db.close()


def CreateJSON(query, FileName):
    db = sqlite3.connect(DBlocation)
    cur = db.cursor()
    sql = query
    cur.execute(sql)
    output = cur.fetchall()
    JSON = json.dumps(output)
    with open(f"{FileName}.json", "w") as text_file:
        text_file.write(JSON)
    print(f'{FileName}.json has been created')
    db.close()


def CreateCSVtable(filename, tablename):
    db = sqlite3.connect(DBlocation)
    cur = db.cursor()
    cur.execute(f'DROP TABLE IF EXISTS {tablename}')
    print(f'CREATE TABLE {tablename}')
    cur.execute(f'CREATE TABLE {tablename} ( Country TEXT PRIMARY KEY, Number INTEGER)')
    f = open(f'{filename}.csv', newline='')
    reader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_NONE)
    for row in reader:
        a = row[0]
        b = row[1]
        cur.execute(f'insert into {tablename} (Country,Number) values (\'{a}\',{b});')
        c = (f'insert into {tablename} (Country,Number) values (\'{a}\',{b});')
    db.commit()
    db.close()


def CreateJSONtable(FileName, TableName):
    db = sqlite3.connect(DBlocation)
    cur = db.cursor()
    cur.execute(f'DROP TABLE IF EXISTS {TableName}')
    cur.execute(f'CREATE TABLE {TableName} ( Country TEXT PRIMARY KEY, Number INTEGER)')
    f = open(f'{FileName}.csv', newline='')
    reader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_NONE)
    for row in reader:
        a = row[0]
        b = row[1]
        cur.execute(f'insert into {TableName} (Country,Number) values (\'{a}\',{b});')
        c = (f'insert into {TableName} (Country,Number) values (\'{a}\',{b});')
        print(a)
        print(b)
        print(c)
    db.commit()
    db.close()

def CreateTableXML(FileName,TableName):
    dom = ElementTree.parse(f'{FileName}.xml')
    args_list = ([t.text for t in dom.iter(tag)] for tag in ['Country','Album_Name','Year','NumberOfSellsAlbums'])
    db = sqlite3.connect('C:\sqlite\chinook.db')
    cur = db.cursor()
    sqltuples = list(zip(*args_list))
    a=sqltuples[0]
    a=str(a)
    a=a[2:]
    b=a.split(',')
    Country=b[0].replace("'","")
    Album_Name=b[1].replace("'","")
    Year=b[2].replace("'","")
    Sells=b[3].replace("'","").replace(")","")
    cur.execute(f'DROP TABLE IF EXISTS {TableName}')
    cur.execute(f'CREATE TABLE {TableName} ( Country TEXT PRIMARY KEY, Album_Name TEXT,Year INTEGER,NumberOfSellsAlbums INTEGER)')
    query = f"insert into {TableName} (Country,Album_Name,Year,NumberOfSellsAlbums) VALUES (\'{Country}\', \'{Album_Name}\', {Year}, {Sells});"
    print(f'CREATE TABLE {TableName}')
    cur.execute(query)
    db.commit()

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    a=str(body)
    a=a[2:-1]
    split=a.split('@@@')
    Country=(split[0])
    Year=(split[1])
    global DBlocation
    DBlocation=(split[2])
    CreateCSV("select BillingCountry, count(InvoiceID) as Counter from invoices group by BillingCountry order by 2 desc", 21)
    CreateCSV("select A.country , sum(counter) Total from (select inv_itms.*,inv.BillingCountry as country from invoices inv inner join (select invoiceid,count(*) counter from invoice_items group by invoiceid) inv_itms on inv.InvoiceId = inv_itms.InvoiceId) A group by A.Country order by sum(counter) desc",22)
    CreateJSON("select distinct inv.BillingCountry as Country , alb.Title as Album_Name from invoice_items inv_itms  inner join invoices inv on inv.InvoiceId = inv_itms.InvoiceId inner join  tracks trks on inv_itms.TrackId = trks.TrackId inner join albums alb on trks.AlbumId = alb.AlbumId",23)
    CreateCSVtable(21, 'TableOne')
    CreateCSVtable(22, 'TableTwo')
    CreateTableXML('XML', 'XMLTable')
channel.basic_consume(
    queue='hello', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

if __name__ == '__main__': callback()