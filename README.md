DbDump
=========

DbDump is a python that will be dump database from remote to your local. This is especially for *very big database* that you will have a lock database, and some timeout process problem. 

Features:
  - Dump with each table not the whole database at a time.
  - Blacklist table will dump only structure only
  - Dump only one specific table.

Version
--------------

0.1 Beta

Required
--------------
  - mysql client binary(mysql, mysqldump)
  - zcat*

* in MacOS we have some with original zcat package. see fix here http://od-eon.com/blogs/calvin/zcat-bug-mac-osx/

Config
--------------
  - check config.template.py and make yourself config.py

Usage
--------------

  - dump every table

```run.py```

  - dump one table

```run.py --table [tablename]```