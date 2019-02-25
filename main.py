#!/usr/bin/env python3
import sys
from twisted.protocols import basic
from twisted.internet import reactor
from twisted.internet.protocol import Factory
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy_utils.functions import database_exists
import sqlite3


class Memcached(basic.LineReceiver):

    _words = []

    def rawDataReceived(self, data):
        # we are setting, and the key is in _words
        self.setLineMode()
        words = self._words
        if len(words) != 5:
            self.sendLine(b'ERROR')
            print("Error: on a set, command must contain 5 words.")

        key = words[1]
        metadata = int(words[2])
        length_in_bytes = int(words[4])

        if data[len(data) - 2] == 13 and data[len(data) - 1] == 10:
            data = data[:len(data) - 2]

        if len(data) != length_in_bytes:
            self.sendLine(b'CLIENT_ERROR bad data chunk')
            self.sendLine(b'ERROR')
            print('Error: The length of data needs to match the length argument')
            return

        session = Session()
        try:
            args = {
                'key': "'{}'".format(key),
                'metadata': metadata,
                'length_in_bytes': length_in_bytes,
                'value': sqlite3.Binary(data)
            }
            session.execute("""
                INSERT OR REPLACE INTO memcached (KEY, METADATA, LENGTH_IN_BYTES, VALUE) 
                VALUES (:key, :metadata, :length_in_bytes, :value);
                """, args)
            session.commit()
            self.sendLine(b'STORED')
        except:
            session.rollback()
            self.sendLine(b'ERROR')
            print('Error: database connection failed')
            raise

    def connectionMade(self):
        pass

    def connectionLost(self, reason):
        pass

    def lineReceived(self, line):
        words = self._words = line.decode('cp437').split()
        command = words[0].lower()
        commands = {
            'get': self.get,
            'set': self.set,
            'delete': self.delete
        }
        if command in commands.keys():
            commands[command]()
        else:
            self.sendLine(b'ERROR')
            print("Error: command must be one of: get, set, delete")

    def get(self):
        words = self._words
        session = Session()
        try:
            result = session.execute("SELECT VALUE FROM memcached WHERE KEY = :key", {'key': "'{}'".format(words[1])}).fetchone()
            if result:
                self.sendLine(result[0])
            self.sendLine(b'END')
            session.commit()
        except:
            self.sendLine(b'ERROR')
            print('Error: database connection failed')
            raise

    def set(self):
        self.setRawMode()

    def delete(self):
        words = self._words
        session = Session()
        try:
            session.execute("DELETE from memcached where KEY = :key", {'key': "'{}'".format(words[1])})
            session.commit()
            self.sendLine(b"DELETED")

        except:
            session.rollback()
            self.sendLine(b'ERROR')
            print('Error: database connection failed')
            raise


class MemcachedFactory(Factory):
    def buildProtocol(self, addr):
        self.protocol = Memcached
        return Memcached()


def create_db(engine):
    # I would use ORM instead of raw sql if this were for realsies.  And alembic for migrations.
    with engine.begin() as conn:
        conn.execute("""
        CREATE TABLE memcached(
          KEY             varchar(250) primary key,
          METADATA        int not null,
          LENGTH_IN_BYTES int not null,
          VALUE           blob not null
        );
        """)


def serve():
    reactor.listenTCP(11211, MemcachedFactory())
    reactor.run()


def show():
    session = Session()
    try:
        results = session.execute("SELECT * FROM memcached").fetchall()
        for row in results:
            print('{0}, {1}, {2}, {3}'.format(*row))

    except:
        print('Error: database connection failed')
        raise


Session = scoped_session(sessionmaker())


def main(operation):
    connection_string = 'sqlite:///memcached.db'
    db_engine = create_engine(connection_string)
    Session.configure(bind=db_engine)
    # in case of multiple processes this could have race condition
    if not database_exists(connection_string):
        create_db(db_engine)
    operation()


def print_help():
    print("""usage: main.py [serve|show] database.sqlite

arguments:
  serve                 serves this application
  show                  dumps the contents of the database
  database.sqlite       required argument""")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print_help()
    elif sys.argv[1] not in ['serve', 'show']:
        print_help()
    elif sys.argv[2] != 'database.sqlite':
        print_help()
    else:
        main(serve if sys.argv[1] == 'serve' else show)

