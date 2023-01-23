import psycopg2
import threading
import logging

class DataBasePool:
    #Classic Singleton __new__ function
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DataBasePool, cls).__new__(cls)
        return cls.instance


    #Creating an instance
    def __init__(self, min_num: int, max_num: int, **kwargs):
        defaultKwargs = { 'host': '127.0.0.1', 'port': '5432', 'password': '1234', 'database': 'test_database', 'user': 'postgres' }
        self.kwargs = { **defaultKwargs, **kwargs }

        self.min_num = min_num
        self.max_num = max_num

        self.pool = []
        self.in_use = []

        for i in range(self.min_num):
            connection = psycopg2.connect(**self.kwargs)
            self.pool.append(connection)


    # Decorator function for thread locking
    def locking(fn):
        lock = threading.Lock()
        def wrapper(*args, **kwargs):
            lock.acquire()
            try:
                return fn(*args, **kwargs)
            finally:
                lock.release()
        return wrapper


    @locking
    def get_connection(self):
        if (len(self.in_use) + len(self.pool)) < self.max_num:
            if self.pool != []:
                connection = self.pool.pop()
            else:
                connection = psycopg2.connect(**self.kwargs)
            self.in_use.append(connection)
            return connection
        logging.info("All connections are occupied")


    @locking
    def return_connection(self, connection):
        self.pool.append(connection)
        self.in_use.remove(connection)


    @locking
    def close_all(self):
        for connection in self.pool:
                connection.close()
        for connection in self.in_use:
            connection.close()
        self.pool = []
        self.used = []


    def __exit__(self):
        self.close_all()
