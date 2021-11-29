#-------------------------------------------------------------------------------
# Name:        ts
# Purpose:     Task Scheduler - to be run at startup
#              requires Python 2.x
# Author:      Amir
#
# Created:     30/01/2011
# Copyright:   (c) Amir 2011
# Licence:     <your licence>
#-------------------------------------------------------------------------------
#!/usr/bin/env python

import sqlite3
import argparse
import multiprocessing
import subprocess
import datetime
import time
import shlex
import os
import sys


def main():
    parser = argparse.ArgumentParser(description='Task scheduler.')
    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands',
                                       help='additional help',
                                       dest='subparser_name')

    parser_Display = subparsers.add_parser('display', help='Display scheduled tasks.')
    parser_Display.add_argument('fromtaskid', type=int, help='Starting task id to display.')
    parser_Display.add_argument('totaskid', type=int, help='Ending task id to display.')
    parser_Display.set_defaults(func=Display)

    parser_Add = subparsers.add_parser('add', help='Add a new scheduled task.')
    parser_Add.add_argument('name', type=str, help='Task name.')
    parser_Add.add_argument('program', type=str, help='Path of program to run.')
    parser_Add.add_argument('message', type=str, help='Message to display when task runs.')
    parser_Add.add_argument('frequency', type=int, help='Frequency in days to run the task, 0 for every logon.')
    parser_Add.add_argument('--start_offset', type=int,
      help='Set start date offset to 0 for today or a +/- number of days from today.')
    parser_Add.add_argument('--end_offset', type=int,
      help='Set end date offset to 0 for today or a +/- number of days from today.')
    parser_Add.add_argument('--output', type=int, choices=[0,1], default=0,
      help='Enter 0 if program does not communicate with standard output and \
      error streams, otherwise enter 1 to detect and capture exceptions and \
      error codes (useful for scripts).')
    parser_Add.add_argument('--run_once', type=int, choices=[0,1],
      help='Enter 1 to run this task one time only.')
    parser_Add.set_defaults(func=Add)

    parser_Update = subparsers.add_parser('update', help='Update task properties.')
    parser_Update.add_argument('taskid', type=int, help='ID for task to be updated')
    parser_Update.add_argument('--program', type=str, help='Path of program to run.')
    parser_Update.add_argument('--message', type=str, help='Message to display when task runs.')
    parser_Update.add_argument('--frequency', type=int, help='Frequency in days to run the task, 0 for every logon.')
    parser_Update.add_argument('--start_offset', type=int,
      help='Set start date offset to 0 for today or a +/- number of days from today.')
    parser_Update.add_argument('--end_offset', type=int,
      help='Set end date offset to 0 for today or a +/- number of days from today.')
    parser_Update.add_argument('--output', choices=[0,1], type=int, help='Enter 0 \
        if program does not communicate with standard output and error streams, \
        otherwise enter 1 to detect and capture exceptions and error codes \
        (useful for scripts).')
    parser_Update.add_argument('--run_once', type=int, choices=[0,1],
      help='Enter 1 to run this task one time only.')
    parser_Update.set_defaults(func=Update)

    parser_Delete = subparsers.add_parser('delete', help='Delete a scheduled task.')
    parser_Delete.add_argument('taskid', type=int, help='ID for task to be deleted.')
    parser_Delete.set_defaults(func=Delete)

    parser_Run = subparsers.add_parser('run', help='Run all scheduled tasks according to saved properties.')
    parser_Run.add_argument('delay', type=int, help='Delay in seconds before running any program.')
    parser_Run.add_argument('daemon', type=int, choices=[0,1], help='Enter 1 to run in daemon mode or 0 to run \
                                one time only and exit - in the later case do not use daemon features in tasks.')
    parser_Run.set_defaults(func=Run)

    parser_vacuum = subparsers.add_parser('vacuum', help='Shrink database file.')
    parser_vacuum.set_defaults(func=ShrinkDb)

    args = parser.parse_args()
    args.func(args)


# To do: Add new attributes
class Args():
    def __init__(self, taskid=None, fromtaskid=None, totaskid=None,
                 program=None, message=None, frequency=None,
                 offset=None, output=None):
        self.taskid = taskid
        self.fromtaskid = fromtaskid
        self.totaskid = totaskid
        self.program = program
        self.message = message
        self.frequency = frequency
        self.offset = offset
        self.output = output

def async_msgbox(txtError='', txtMsg='', title='', parent=None, height=6):
    multiprocessing.Process(target=MsgBox, kwargs={'txtError':txtError, 'txtMsg':txtMsg, \
                                        'title':title, 'parent':parent, 'height':height}).start()

def MsgBox(txtError='', txtMsg='', title='', parent=None, height=6):
    import Tkinter
    if not parent:
        root = Tkinter.Tk()
        root.withdraw()

    window = Tkinter.Toplevel()
    window.transient(parent)
    window.wm_attributes("-topmost", 1)
    window.title(title)

    scrollbar = Tkinter.Scrollbar(window)
    scrollbar.pack(side=Tkinter.RIGHT, fill=Tkinter.Y)

    textbox = Tkinter.Text(window, wrap=Tkinter.WORD, padx=5, pady=3, width=50, height=height, \
                    font=("verdana", 10), background='beige', yscrollcommand=scrollbar.set)


    textbox.tag_config('errors', foreground='red')
    textbox.tag_config('messages', foreground='black')

    textbox.insert(Tkinter.CURRENT, txtError, 'errors')

    if txtError and txtMsg:
        textbox.insert(Tkinter.CURRENT, '\n\n', 'errors')

    textbox.insert(Tkinter.CURRENT, txtMsg, 'messages')
    textbox.pack(fill=Tkinter.BOTH, expand=1)

    scrollbar.config(command=textbox.yview)
    textbox.config(state=Tkinter.DISABLED)
    window.grab_set()
    window.focus_set()

    window.wait_window()
    if parent:
        parent.grab_set()
        parent.focus_set()


def GetAbsScriptPath():
    AbsScriptPath = os.path.abspath( __file__ )
    AbsScriptPath_no_extension = AbsScriptPath.rpartition('.')[0]
    if not AbsScriptPath_no_extension:
        AbsScriptPath_no_extension = AbsScriptPath
    return (AbsScriptPath, AbsScriptPath_no_extension)

def ShrinkDb(args):
    conn = GetConnection()
    conn.execute('VACUUM')
    conn.close()
    print '--------------------------'
    print 'Done'
    print

def get_data(sql_script, params=None):
    conn = GetConnection()
    if params:
        cur = conn.execute(sql_script, params)
    else:
        cur = conn.execute(sql_script)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

def save_data(sql_script, params=None):
    conn = GetConnection()
    if params:
        cur = conn.execute(sql_script, params)
    else:
        cur = conn.execute(sql_script)
    conn.commit()
    lastrowid = cur.lastrowid
    cur.close()
    conn.close()
    return lastrowid # available when inserting otherwise None

# To do: Add new attributes
def Display(args = None):
    sql_script = """SELECT ID, program, frequency, message, DATETIME(lastrun, 'unixepoch', 'localtime'), output
                    FROM   tblTask
                    WHERE ID >= ? AND ID <= ?
                    ORDER BY ID"""

    tasks = get_data(sql_script, (args.fromtaskid, args.totaskid))

    print '--------------------------'
    for (ID, program, frequency, message, lastrun, output) in tasks:
        print 'ID        : %s' % ID
        print 'Program   : %s' % program
        print 'Frequency : %s' % frequency
        print 'Message   : %s' % message
        print 'Last run  : %s' % lastrun
        print 'Output    : %s' % output
        print

# To do: Add new attributes
def Add(args):
    conn = GetConnection()

    i_today = datetime.date.today().toordinal()
    i_start = None
    i_end = None

    if args.start_offset:
        i_start = i_today + args.start_offset

    if args.end_offset:
        i_end = i_today + args.end_offset

    sql_script = """INSERT INTO tblTask
                    (name, program, frequency, message, [start], [end], output) values (?,?,?,?,?,?,?)"""

    lastrowid = save_data(sql_script, (args.name, args.program, args.frequency, \
                                        args.message, i_start, i_end, args.output))

    args = Args(fromtaskid=lastrowid, totaskid=lastrowid)
    Display(args)

# To do: Add new attributes
def Update(args):
    field_list = []
    value_list = []

    if not args.program == None:
        field_list.append('program = ?')
        value_list.append(args.program)

    if not args.message == None:
        field_list.append('message = ?')
        value_list.append(args.message)

    if not args.frequency == None:
        field_list.append('frequency = ?')
        value_list.append(args.frequency)

    if not args.output == None:
        field_list.append('output = ?')
        value_list.append(args.output)

    if len(field_list) == 0:
        return

    str_sql = 'UPDATE tblTask SET {0} WHERE ID = ?'
    str_sql = str_sql.format(', '.join(field_list))
    value_list.append(args.taskid)

    save_data(str_sql, tuple(value_list))

    args = Args(fromtaskid=args.taskid, totaskid=args.taskid)
    Display(args)


def Delete(args):
    save_data('DELETE FROM tblTask WHERE ID = ?', (args.taskid,))
    print '--------------------------'
    print 'Task DELETED : %s' % args.taskid
    print


def GetConnection():
    strDatabasePath = GetAbsScriptPath()[1] + '.db3'
    Existed = False

    if os.path.exists(strDatabasePath):
        Existed = True

    conn = sqlite3.connect(strDatabasePath, timeout=30)

    if not Existed:
        conn.execute("""CREATE TABLE [tblTask] (
                                        [ID]            INTEGER  PRIMARY KEY AUTOINCREMENT NOT NULL,
                                        [name]          TEXT     NOT NULL,
                                        [program]       TEXT     NULL,
                                        [frequency]     INTEGER  NULL,
                                        [message]       TEXT     NULL,
                                        [output]        INTEGER  NULL,
                                        [start]         INTEGER  NULL,
                                        [end]           INTEGER  NULL,
                                        [run_once]      INTEGER  NULL,
                                        [startup_only]  INTEGER  NULL,
                                        [lastrun]       INTEGER  NULL,
                                        [running]       INTEGER  NULL,
                                        [disabled]      INTEGER  NULL
                                                )""")

    return conn


def run_program(ID, program, output=0):
    txtError = RunProcess(ID, program, output)

    if not txtError:
        try:
            save_data("UPDATE tblTask SET lastrun = strftime('%s', 'now') + 0 WHERE ID = ?", (ID,))
        except Exception as ex:
            txtError += 'Program ran but time stamp failed:' + '\n'
            txtError += program + '\n'
            txtError += str(ex).rstrip('\n') + '\n'
            txtError += 'Task ID: %s' % ID

    return txtError



# To do: test function
def RunProcess(ID, program, output=0):
    txtError = ''
    input_list = shlex.split(str(program))
    if not output:
        try:
            subprocess.Popen(input_list)
        except Exception as ex:
            txtError += 'Error executing program:' + '\n' + str(program) + '\n'
            txtError += str(ex).rstrip('\n') + '\n'
            txtError += 'Task ID: %s' % ID
    else:
        try:
            p = subprocess.Popen(input_list, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (str_out, str_error) = p.communicate()
            if str_error:
                raise Exception(str_error)
        except Exception as ex:
            txtError += 'Error executing program:' + '\n' + str(program) + '\n'
            txtError += str(ex).rstrip('\n') + '\n'
            txtError += 'Task ID: %s' % ID

    return txtError


def lock_tasks(task_id_list, flag):
    str_sql = "UPDATE tblTask SET running = {0} WHERE  ID IN ({1})"
    str_sql = str_sql.format(flag, ','.join('?' * len(task_id_list)))
    try:
        save_data(str_sql, tuple(task_id_list))
    except Exception as ex:
        return str(ex)
    else:
        return ''

# To do: test function - create ring in python - manual page 56
# create checkphone in python
# create front ends wxpython, jtk, qt
# check mysql
def Run(args):
    time.sleep(args.delay)
    startup = True

    # set all tasks as not running yet
    try:
        save_data('UPDATE tblTask SET running = NULL')
    except Exception as ex:
        async_msgbox(txtError=str(ex), title='Task Scheduler', height=12)


    if not args.daemon:
        run2(args, startup)
        return

    while True:
        multiprocessing.Process(target=run2, args=(args, startup)).start()
        time.sleep(30)
        startup = False


def run2(args, startup):
    sql_script = """FROM   tblTask
                    WHERE  IFNULL(lastrun, 0) + IFNULL(frequency, 0) <= strftime('%s', 'now') + 0
                    {0}
                    AND    strftime('%s', 'now') + 0 BETWEEN IFNULL([start], 0) AND IFNULL([end], 32503672800)
                    AND    NOT (IFNULL(lastrun, 0) > 0 AND IFNULL(run_once, 0) = 1)
                    AND    NOT IFNULL(running, 0) = 1
                    AND    NOT IFNULL(disabled, 0) = 1"""

    if startup:
        sql_script = sql_script.format('')
    else:
        sql_script = sql_script.format('AND NOT IFNULL(startup_only, 0) = 1')


    sql_query = "SELECT ID, program, message, output, DATETIME(lastrun, 'unixepoch', 'localtime') " + sql_script
    tasks = get_data(sql_query)

    if len(tasks) == 0:
        return

    task_id_list = [ID for (ID, program, message, output, lastrun) in tasks ]

    # set tasks as running
    lock_error = lock_tasks(task_id_list, 1)
    if lock_error:
        async_msgbox(txtError=lock_error, title='Task Scheduler', height=12)

    program_list = [ (ID, program, output)
                        for (ID, program, message, output, lastrun) in tasks if program ]

    error_list = []

    for (ID, program, output) in program_list:
        txtError = run_program(ID, program, output)
        if txtError:
            error_list.append(txtError)


    txtError = ''
    txtMessage = ''

    msg_list = [ message + '\n' + 'Last run: %s' %lastrun + '\n' + 'Task ID: %s' %ID \
                 for (ID, program, message, output, lastrun) in tasks if message ]
    txtMessage = '\n\n'.join(msg_list)

    txtError = '\n\n'.join(error_list)


    if txtMessage:
        try:
            async_msgbox(txtError=txtError, txtMsg=txtMessage, title='Task Scheduler', height=12)

            id_list = [ID for (ID, program, message, output, lastrun) in tasks if message and not program]
            str_sql = """UPDATE tblTask
                         SET    lastrun = strftime('%s', 'now') + 0
                         WHERE  ID IN ({0})"""

            str_sql = str_sql.format(','.join('?' * len(id_list)))

            save_data(str_sql, tuple(id_list))
        except Exception as ex:
            async_msgbox(txtError=str(ex), title='Task Scheduler', height=12)

    elif txtError:
        async_msgbox(txtError=txtError, title='Task Scheduler', height=12)

    # set tasks as not running
    lock_error = lock_tasks(task_id_list, 0)
    if lock_error:
        async_msgbox(txtError=lock_error, title='Task Scheduler', height=12)



if __name__ == '__main__':
    main()
