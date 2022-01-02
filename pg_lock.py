import zlib


class AlreadyLockedError(Exception):
    def __init__(self, lock_id, ):
        self.lock_id = lock_id
        self.message = f"Lock {self.lock_id} acquired by another job"
        super().__init__(self.message)


class PgAdivisoryLock:
    def __init__(self, connection, lock_id, wait_for_lock=False):
        self.conn = connection
        self.lock_id = lock_id
        self.lock_cs = zlib.crc32(lock_id)
        self.wait_for_lock = wait_for_lock
        self.is_acquired_lock = None

    def __enter__(self):
        try:
            if self.wait_for_lock:
                self.__wait_for_lock()
            else:
                rs = self.__acquire_lock()
                if not rs[0]:
                    raise AlreadyLockedError(self.lock_id)
            self.is_acquired_lock = True
        except Exception as e:
            raise e

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.is_acquired_lock:
            self.__release_lock()

    def __acquire_lock(self):
        self.conn.execute("SELECT pg_try_advisory_lock(%s, %s)", (self.lock_cs, self.lock_cs,))
        return self.conn.fetchone()

    def __wait_for_lock(self):
        self.conn.execute("SELECT pg_advisory_lock(%s, %s)", (self.lock_cs, self.lock_cs,))
        return self.conn.fetchone()

    def __release_lock(self):
        self.conn.execute("SELECT pg_advisory_unlock(%s, %s)", (self.lock_cs, self.lock_cs,))
        return self.conn.fetchone()
