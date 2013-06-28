#!/usr/bin/env python

'''
Created on Jun 28, 2013

@author: zinuzoid
'''

import subprocess
import os
# import shlex
import time
import config


def run_cmd(cmd):
#     args = shlex.split(cmd)
#     print(args)
#     print cmd
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, \
                         shell=True)
    (stdout, stderr) = p.communicate() 
    return {'stdout' : stdout, 'stderr' :stderr}

def run_sql(cmd, use_db=True):
    return run_cmd("echo \"%s;\" | mysql -h %s -u %s -p%s %s" % \
                  (cmd, config.DB_LOCAL_HOST, config.DB_LOCAL_USER, \
                   config.DB_LOCAL_PASSWORD, config.DB_LOCAL_NAME))
    
def log(message, newline=True):
    if(newline):
        print message
    else:
        print message,
        
def dump(line):
    run_cmd("cd %s && mysqldump -h %s -u %s -p%s --compress --skip-comments --quick  %s %s > table.sql" \
                  % (config.TMP_DIRECTORY, config.DB_REMOTE_HOST, \
                     config.DB_REMOTE_USER, config.DB_REMOTE_PASSWORD, \
                     config.DB_REMOTE_NAME, line))
    log(str(os.path.getsize(config.TMP_DIRECTORY.rstrip('/') + '/table.sql') / 1000000.0) + ' MB,', False)
    run_cmd("cd %s && cat table.sql | mysql -h %s -u %s -p%s %s" % \
            (config.TMP_DIRECTORY, config.DB_REMOTE_HOST, config.DB_REMOTE_USER, \
                     config.DB_REMOTE_PASSWORD, config.DB_NAME_TEMPORARY ))
    
def dump_with_gz(line):
    run_cmd("cd %s && mysqldump -h %s -u %s -p%s --compress --skip-comments --quick  %s %s | gzip > table.gz" \
                  % (config.TMP_DIRECTORY, config.DB_REMOTE_HOST, \
                     config.DB_REMOTE_USER, config.DB_REMOTE_PASSWORD, \
                     config.DB_REMOTE_NAME, line))
    log(str(os.path.getsize(config.TMP_DIRECTORY.rstrip('/') + '/table.gz') / 1000000.0) + ' MB,', False)
    run_cmd("cd %s && zcat table.gz | mysql -h %s -u %s -p%s %s" % \
            (config.TMP_DIRECTORY, config.DB_REMOTE_HOST, config.DB_REMOTE_USER, \
                     config.DB_REMOTE_PASSWORD, config.DB_NAME_TEMPORARY ))

if __name__ == '__main__':
    print "HQLiveDump..."
    all_time_start = time.time()
    log("drop db %s" % config.DB_NAME_TEMPORARY)
    run_sql("DROP DATABASE IF EXITS %s" % config.DB_NAME_TEMPORARY);
    log("create db %s" % config.DB_NAME_TEMPORARY)
    run_sql("CREATE DATABASE %s" % config.DB_NAME_TEMPORARY);
    str_list_table = run_sql("SHOW TABLES")['stdout']
    list_table = str.splitlines(str_list_table)
    log("listing table... got %s tables" % len(list_table))
    list_table.pop(0) # remove table header
    for idx, line in enumerate(list_table):
        if(line not in config.TABLE_BLACKLIST):
            log(line + ',', False)
            table_time_start = time.time()
            run_cmd("cd %s && rm table.gz" % config.TMP_DIRECTORY)
            dump_with_gz(line);
            table_time_end = time.time()
            table_time_used = table_time_end - table_time_start
            log('done in ' + str(table_time_used) + ' s')
    
    
    all_time_end = time.time()
    all_time_used = all_time_end - all_time_start
    log('all done in ' + str(all_time_used) + ' s')
    log("bye")
    
    
    