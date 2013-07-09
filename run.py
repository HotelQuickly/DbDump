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
import sys


def run_cmd(cmd):
#     args = shlex.split(cmd)
#     print(args)
#     print '#',cmd
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, \
                         shell=True)
    (stdout, stderr) = p.communicate()
    if(len(stderr) != 0):
        print '#', cmd
        print stderr
        print 'Please check the error!!!'
        exit() 
    return {'stdout' : stdout, 'stderr' :stderr}

def run_sql_local(cmd, use_db=True):
    dblocalpassword = ''
    if(config.DB_LOCAL_PASSWORD != ''):
        dblocalpassword = '-p' + config.DB_LOCAL_PASSWORD
        
    if(use_db):
        return run_cmd("echo \"%s;\" | mysql -h %s -u %s %s %s" % \
                      (cmd, config.DB_LOCAL_HOST, config.DB_LOCAL_USER, \
                       dblocalpassword, config.DB_LOCAL_NAME))
    else:
        return run_cmd("echo \"%s;\" | mysql -h %s -u %s %s" % \
                      (cmd, config.DB_LOCAL_HOST, config.DB_LOCAL_USER, \
                       dblocalpassword))
    
def run_sql_local_temporary(cmd, use_db=True):
    dblocalpassword = ''
    if(config.DB_LOCAL_PASSWORD != ''):
        dblocalpassword = '-p' + config.DB_LOCAL_PASSWORD
        
    if(use_db):
        return run_cmd("echo \"%s;\" | mysql -h %s -u %s %s %s" % \
                      (cmd, config.DB_LOCAL_HOST, config.DB_LOCAL_USER, \
                       dblocalpassword, config.DB_LOCAL_NAME_TEMPORARY))
    else:
        return run_cmd("echo \"%s;\" | mysql -h %s -u %s %s" % \
                      (cmd, config.DB_LOCAL_HOST, config.DB_LOCAL_USER, \
                       dblocalpassword))
    
def run_sql_remote(cmd, use_db=True):
    if(use_db):
        return run_cmd("echo \"%s;\" | mysql -h %s -u %s -p%s %s" % \
                      (cmd, config.DB_REMOTE_HOST, config.DB_REMOTE_USER, \
                       config.DB_REMOTE_PASSWORD, config.DB_REMOTE_NAME))
    else:
        return run_cmd("echo \"%s;\" | mysql -h %s -u %s -p%s" % \
                      (cmd, config.DB_REMOTE_HOST, config.DB_REMOTE_USER, \
                       config.DB_REMOTE_PASSWORD))
    
def log(message, newline=True):
    if(newline):
        print message
    else:
        print message,
        
def dump(line):
    dblocalpassword = ''
    if(config.DB_LOCAL_PASSWORD != ''):
        dblocalpassword = '-p' + config.DB_LOCAL_PASSWORD
        
    run_cmd("cd %s && mysqldump -h %s -u %s -p%s --compress --skip-comments --quick  %s %s > table.sql" \
                  % (config.TMP_DIRECTORY, config.DB_REMOTE_HOST, \
                     config.DB_REMOTE_USER, config.DB_REMOTE_PASSWORD, \
                     config.DB_REMOTE_NAME, line))
    log(str(os.path.getsize(config.TMP_DIRECTORY.rstrip('/') + '/table.sql') / 1000000.0) + ' MB,', False)
    run_cmd("cd %s && cat table.sql | mysql -h %s -u %s %s %s" % \
            (config.TMP_DIRECTORY, config.DB_LOCAL_HOST, config.DB_LOCAL_USER, \
                     dblocalpassword, config.DB_LOCAL_NAME_TEMPORARY))
    
def dump_with_gz(line):
    dblocalpassword = ''
    if(config.DB_LOCAL_PASSWORD != ''):
        dblocalpassword = '-p' + config.DB_LOCAL_PASSWORD
        
    run_cmd("cd %s && mysqldump -h %s -u %s -p%s --compress --skip-comments --quick  %s %s | gzip > table.gz" \
                  % (config.TMP_DIRECTORY, config.DB_REMOTE_HOST, \
                     config.DB_REMOTE_USER, config.DB_REMOTE_PASSWORD, \
                     config.DB_REMOTE_NAME, line))
    log(str(os.path.getsize(config.TMP_DIRECTORY.rstrip('/') + '/table.gz') / 1000000.0) + ' MB,', False)
    run_cmd("cd %s && zcat table.gz | mysql -h %s -u %s %s %s" % \
            (config.TMP_DIRECTORY, config.DB_LOCAL_HOST, config.DB_LOCAL_USER, \
                     dblocalpassword, config.DB_LOCAL_NAME_TEMPORARY))
    
def dump_no_data_with_gz(line):
    dblocalpassword = ''
    if(config.DB_LOCAL_PASSWORD != ''):
        dblocalpassword = '-p' + config.DB_LOCAL_PASSWORD
        
    run_cmd("cd %s && mysqldump -h %s -u %s -p%s --compress --skip-comments --quick --no-data  %s %s | gzip > table.gz" \
                  % (config.TMP_DIRECTORY, config.DB_REMOTE_HOST, \
                     config.DB_REMOTE_USER, config.DB_REMOTE_PASSWORD, \
                     config.DB_REMOTE_NAME, line))
    log(str(os.path.getsize(config.TMP_DIRECTORY.rstrip('/') + '/table.gz') / 1000000.0) + ' MB,', False)
    run_cmd("cd %s && zcat table.gz | mysql -h %s -u %s %s %s" % \
            (config.TMP_DIRECTORY, config.DB_LOCAL_HOST, config.DB_LOCAL_USER, \
                     dblocalpassword, config.DB_LOCAL_NAME_TEMPORARY))
    
def dump_one_table(table):    
    log("Dump one table: " + table)
    str_list_table = run_sql_remote("SHOW TABLES")['stdout']
    list_table = str.splitlines(str_list_table)
    log(list_table)
    if(table in list_table):
        log('Got table in remote')
    else:
        raise RuntimeError('Not found table in remote! please check table name!')
    log("create db if not exits %s" % config.DB_LOCAL_NAME_TEMPORARY)
    run_sql_local_temporary("CREATE DATABASE IF NOT EXISTS %s" % config.DB_LOCAL_NAME_TEMPORARY, use_db=False);
    log("drop table if exits %s" % table)
    run_sql_local_temporary("DROP TABLE IF EXISTS %s" % table);
    log('start dump table: ' + table)
    table_time_start = time.time()
    run_cmd("cd %s && rm table.gz" % config.TMP_DIRECTORY)
    dump_with_gz(table);
    table_time_end = time.time()
    table_time_used = table_time_end - table_time_start
    log('done in ' + str(table_time_used) + ' s')
    log("drop table if exits %s" % table)
    run_sql_local("DROP TABLE IF EXISTS %s" % table);
    log("rename table %s " % table, False)
    table_time_start = time.time()
    run_sql_local("RENAME TABLE " + config.DB_LOCAL_NAME_TEMPORARY + "." + table + " TO " + config.DB_LOCAL_NAME + "." + table, use_db=False)
    table_time_end = time.time()
    table_time_used = table_time_end - table_time_start
    log('done in ' + str(table_time_used) + ' s')
    log('done dump table: ' + table)
    

if __name__ == '__main__':
    
    argv = sys.argv
    table = ''
    if(len(argv) > 1):
        if(argv[1] == '--table'):
            if(len(argv) == 3):
                table = argv[2]
                dump_one_table(table)
                exit()
            else:
                print 'dump only a table ex: ./run.py --table log_visit'
                exit()
        else:
            print 'dump only a table ex: ./run.py --table log_visit'    
            exit()
    
    print "HQLiveDump..."
    all_time_start = time.time()
    log("drop db %s" % config.DB_LOCAL_NAME_TEMPORARY)
    run_sql_local_temporary("DROP DATABASE IF EXISTS %s" % config.DB_LOCAL_NAME_TEMPORARY, use_db=False);
    log("create db %s" % config.DB_LOCAL_NAME_TEMPORARY)
    run_sql_local_temporary("CREATE DATABASE %s" % config.DB_LOCAL_NAME_TEMPORARY, use_db=False);
    str_list_table = run_sql_remote("SHOW TABLES")['stdout']
    list_table = str.splitlines(str_list_table)
    log("listing table... got %s tables" % len(list_table))
    list_table.pop(0)  # remove table header
    for idx, line in enumerate(list_table):
        if(line not in config.TABLE_BLACKLIST):
            log(line + ',', False)
            table_time_start = time.time()
            run_cmd("cd %s && rm table.gz" % config.TMP_DIRECTORY)
            dump_with_gz(line);
            table_time_end = time.time()
            table_time_used = table_time_end - table_time_start
            log('done in ' + str(table_time_used) + ' s')
        else:
            log(line + '(structure only),', False)
            table_time_start = time.time()
            run_cmd("cd %s && rm table.gz" % config.TMP_DIRECTORY)
            dump_no_data_with_gz(line);
            table_time_end = time.time()
            table_time_used = table_time_end - table_time_start
            log('done in ' + str(table_time_used) + ' s')
    
    log('move database ' + config.DB_LOCAL_NAME_TEMPORARY + ' to ' + config.DB_LOCAL_NAME + ' ')
    log("drop db %s" % config.DB_LOCAL_NAME)
    run_sql_local_temporary("DROP DATABASE IF EXISTS %s" % config.DB_LOCAL_NAME, use_db=False);
    log("create db %s" % config.DB_LOCAL_NAME)
    run_sql_local_temporary("CREATE DATABASE %s" % config.DB_LOCAL_NAME, use_db=False);
    for idx, line in enumerate(list_table):
        log(line + ' ', False)
        table_time_start = time.time()
        run_sql_local("RENAME TABLE " + config.DB_LOCAL_NAME_TEMPORARY + "." + line + " TO " + config.DB_LOCAL_NAME + "." + line, use_db=False)
        table_time_end = time.time()
        table_time_used = table_time_end - table_time_start
        log('done in ' + str(table_time_used) + ' s')
    
    log("drop db %s" % config.DB_LOCAL_NAME_TEMPORARY)
    run_sql_local_temporary("DROP DATABASE IF EXISTS %s" % config.DB_LOCAL_NAME_TEMPORARY, use_db=False);
    
    # ## run post dump script here
    
    
    # ##
    
    all_time_end = time.time()
    all_time_used = all_time_end - all_time_start
    log('all done in ' + str(all_time_used) + ' s')
    log("bye")
    
    
    
