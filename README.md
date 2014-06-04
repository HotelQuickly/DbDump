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

  - dump every table ```run.py```
  - dump one table ```run.py --table [tablename]```

## The MIT License (MIT)

Copyright (c) 2014 Hotel Quickly Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
