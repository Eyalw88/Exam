#!/usr/bin/env python
import pika

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='hello')
    x=input("Write a country: ")
    y = input("Pick an Year: ")
    c ="C:\sqlite\chinook.db"
    channel.basic_publish(exchange='',
                          routing_key='hello',
                          body=f'{x}@@@{y}@@@{c}')

    print(" The Message Has Been sent")
    connection.close()

if __name__ == '__main__': main()