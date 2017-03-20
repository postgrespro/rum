# coding: utf-8
"""
	Test RUM index with big base 'pglist'

	Copyright (c) 2015-2016, Postgres Professional
"""
import unittest
import os
import sys
import gzip
import testgres as tg

if sys.version_info[0] < 3:
    import urllib as request
else:
    import urllib.request as request

from os.path import expanduser


class PglistTests(unittest.TestCase):

    def setUp(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))

        self.node = tg.get_new_node("pglist",
                                    os.path.join(current_dir, "tmp_install"))
        try:
            self.node.init()
            self.node.append_conf("postgresql.conf",
                                  "shared_buffers='4GB'\n"
                                  "maintenance_work_mem='2GB'\n"
                                  "max_wal_size='2GB'\n"
                                  "work_mem='50MB'")
            self.node.start()
        except Exception as e:
            self.printlog(os.path.join(self.node.logs_dir, "postgresql.log"))
            raise e

    def tearDown(self):
        tg.stop_all()

    def init_pglist_data(self, node):
        # Check if 'pglist' base exists
        bases = node.execute("postgres",
                             "SELECT count(*) FROM pg_database "
                             "WHERE datistemplate = false AND "
                             "  datname = 'pglist'")
        if bases[0][0] != 0:
            return

        # Check if 'pglist' dump exists
        home = expanduser("~")
        pglist_dump = os.path.join(home, "pglist-28-04-16.dump")
        if not os.path.isfile(pglist_dump):
            pglist_dumpgz = pglist_dump + ".gz"
            if not os.path.isfile(pglist_dumpgz):
                print("Downloading: {0}".format(pglist_dumpgz))
                request.urlretrieve(
                    "http://www.sai.msu.su/~megera/postgres/files/pglist-28-04-16.dump.gz",
                    pglist_dumpgz)

            print("Decompressing: {0}".format(pglist_dumpgz))
            gz = gzip.open(pglist_dumpgz, 'rb')
            with open(pglist_dump, 'wb') as f:
                f.write(gz.read())

            os.remove(pglist_dumpgz)

        # Restore dump file
        print("Restoring 'pglist'")
        node.safe_psql("postgres", "CREATE DATABASE pglist")
        node.psql("pglist", filename=pglist_dump)

        node.safe_psql("pglist", "CREATE EXTENSION rum")

    def printlog(self, logfile):
        with open(logfile, 'r') as log:
            for line in log.readlines():
                print(line)

    def test_order_by(self):
        """Tests SELECT constructions to 'pglist' base"""
        try:
            self.init_pglist_data(self.node)

            print("Creating index 'rumidx_orderby_sent'")

            self.node.safe_psql(
                "pglist",
                "CREATE INDEX rumidx_orderby_sent ON pglist USING rum ("
                "  fts rum_tsvector_timestamp_ops, sent) "
                "  WITH (attach=sent, to=fts, order_by_attach=t)")

            print("Running tests")

            self.assertEqual(
                self.node.safe_psql(
                    "pglist",
                    "SELECT sent, subject "
                    "  FROM pglist "
                    "  WHERE fts @@ "
                    "    to_tsquery('english', 'backend <-> crushed') "
                    "  ORDER BY sent <=| '2016-01-01 00:01' LIMIT 5"
                ),
                b'1999-06-02 11:52:46|Re: [HACKERS] PID of backend\n'
            )

            self.assertEqual(
                self.node.safe_psql(
                    "pglist",
                    "SELECT count(*) FROM pglist "
                    "WHERE fts @@ to_tsquery('english', 'tom & lane')"
                ),
                b'222813\n'
            )

            self.node.safe_psql("pglist", "DROP INDEX rumidx_orderby_sent");

            print("Creating index 'pglist_rum_idx'")

            self.node.safe_psql(
                "pglist",
                "CREATE INDEX pglist_rum_idx ON pglist USING rum ("
                "  fts rum_tsvector_ops)")

            print("Running tests")

            self.assertEqual(
                self.node.execute(
                    "pglist",
                    "SELECT id FROM pglist "
                    "WHERE fts @@ to_tsquery('english', 'postgres:*') "
                    "ORDER BY fts <=> to_tsquery('english', 'postgres:*') "
                    "LIMIT 9"
                )[0][0],
                816114
            )

            self.node.safe_psql("pglist", "DROP INDEX pglist_rum_idx");

        except Exception as e:
            self.printlog(os.path.join(self.node.logs_dir, "postgresql.log"))
            raise e

if __name__ == "__main__":
    unittest.main()
