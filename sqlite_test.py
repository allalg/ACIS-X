import sqlite3
c = sqlite3.connect(':memory:')
c.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)')
c.execute('INSERT INTO t (id, name) VALUES (1, "foo")')
c.execute('INSERT OR IGNORE INTO t (id) VALUES (1)')
c.execute('INSERT OR IGNORE INTO t (id) VALUES (2)')
print(c.execute('SELECT * FROM t').fetchall())
