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

import sys
import os
import datetime
import threading
import shutil
import argparse
import sqlite3

database_file_name = 'ts.db3'

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
    parser_Add.add_argument('program', type=str, help='Path of program to run.')
    parser_Add.add_argument('message', type=str, help='Message to display when task runs.')
    parser_Add.add_argument('frequency', type=int, help='Frequency in days to run the task, 0 for every logon.')
    parser_Add.add_argument('offset', type=int,
      help='Set last run date offset to 0 for today or a +/- number of days from today.')
    #parser_Add.add_argument('--baz', choices='XYZ', help='baz help')
    parser_Add.set_defaults(func=Add)

    parser_Update = subparsers.add_parser('update', help='Update task properties.')
    parser_Update.add_argument('taskid', type=int, help='ID for task to be updated')
    parser_Update.add_argument('--program', type=str, help='Path of program to run.')
    parser_Update.add_argument('--message', type=str, help='Message to display when task runs.')
    parser_Update.add_argument('--frequency', type=int, help='Frequency in days to run the task, 0 for every logon.')
    parser_Update.add_argument('--offset', type=int,
      help='Set last run date offset to 0 for today or a +/- number of days from today.')
    parser_Update.set_defaults(func=Update)

    parser_Delete = subparsers.add_parser('delete', help='Delete a scheduled task.')
    parser_Delete.add_argument('taskid', type=int, help='ID for task to be deleted.')
    parser_Delete.set_defaults(func=Delete)

    parser_Run = subparsers.add_parser('run', help='Run all scheduled tasks according to saved properties.')
    parser_Run.add_argument('seconds', type=int, help='Delay in seconds before run.')
    parser_Run.set_defaults(func=Run)

    parser_vacuum = subparsers.add_parser('vacuum', help='Shrink database file.')
    parser_vacuum.set_defaults(func=ShrinkDb)

    args = parser.parse_args()
    args.func(args)

class Args():
    def __init__(self, taskid=None, fromtaskid=None, totaskid=None,
                 program=None, message=None, frequency=None, offset=None):
        self.taskid = taskid
        self.fromtaskid = fromtaskid
        self.totaskid = totaskid
        self.program = program
        self.message = message
        self.frequency = frequency
        self.offset = offset
	
def MsgBox(txtError=None, txtMsg=None, title=None):
    import Tkinter
    window = Tkinter.Tk()
    window.wm_attributes("-topmost", 1)
    window.title(title)
    if txtError:
        labelerror = Tkinter.Label(window, padx=10, pady=10, wraplength=500,
                            justify='left', text=txtError, fg='red')
	labelerror.pack()
    if txtMsg:
        label = Tkinter.Label(window, padx=10, pady=10, wraplength=500,
                            justify='left', text=txtMsg)
        label.pack()
    window.mainloop()

def GetScriptLocation():
    arg0 = sys.argv[0]
    scriptName = shutil._basename(arg0)
    return shutil.abspath(arg0).rstrip(scriptName)

def ShrinkDb(args):
    conn = GetConnection()
    conn.execute('VACUUM')
    conn.close()
    print '--------------------------'
    print 'Done'
    print


def Display(args):
    conn = GetConnection()
    cur = conn.execute("""SELECT * from tblTask WHERE ID >= ? AND ID <= ? ORDER BY ID""",
                            (args.fromtaskid, args.totaskid))
    tasks = cur.fetchall()
    cur.close()
    conn.close()

    print '--------------------------'
    for (ID, program, frequency, message, lastrun) in tasks:
        print 'ID        : %s' % ID
        print 'Program   : %s' % program
        print 'Frequency : %s' % frequency
        print 'Message   : %s' % message
        print 'Last run  : %s' % datetime.date.fromordinal(lastrun).strftime("%A - %d %b %Y")
        print


def Add(args):
    conn = GetConnection()
    intLastrun = datetime.date.today().toordinal() + args.offset
    cur = conn.execute("""INSERT INTO tblTask
                        (program, frequency, message, lastrun) values (?,?,?,?)""",
                        (args.program, args.frequency, args.message, intLastrun)
                      )
    conn.commit()
    lastrowid = cur.lastrowid
    cur.close()
    conn.close()

    args = Args(fromtaskid=lastrowid, totaskid=lastrowid)
    Display(args)


def Update(args):
    conn = GetConnection()

    if not args.program == None:
        conn.execute("""UPDATE tblTask SET program = ? WHERE ID = ?""",
                        (args.program, args.taskid))
        conn.commit()

    if not args.message == None:
        conn.execute("""UPDATE tblTask SET message = ? WHERE ID = ?""",
                        (args.message, args.taskid))
        conn.commit()

    if not args.frequency == None:
        conn.execute("""UPDATE tblTask SET frequency = ? WHERE ID = ?""",
                        (args.frequency, args.taskid))
        conn.commit()

    if not args.offset == None:
        intNewdate = datetime.date.today().toordinal() + args.offset
        conn.execute("""UPDATE tblTask SET lastrun = ? WHERE ID = ?""",
                        (intNewdate, args.taskid))
        conn.commit()

    conn.close()
    args = Args(fromtaskid=args.taskid, totaskid=args.taskid)
    Display(args)

def Delete(args):
    conn = GetConnection()
    conn.execute("""DELETE FROM tblTask WHERE ID = ?""", (args.taskid,))
    conn.commit()
    conn.close()
    print '--------------------------'
    print 'Task DELETED : %s' % args.taskid
    print

def GetConnection():
    strDatabasePath = os.path.join(GetScriptLocation() + database_file_name)
    Existed = False
    if os.path.exists(strDatabasePath):
        Existed = True

    conn = sqlite3.connect(strDatabasePath)

    if not Existed:
        conn.execute("""CREATE TABLE tblTask
                     (ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                      program TEXT,
                      frequency INTEGER,
                      message TEXT,
                      lastrun INTEGER)""")

    return conn

def RunProcess(program):
    import subprocess
    import shlex
    subprocess.Popen(shlex.split(str(program)))


def Run(args):
    rlock = threading.Condition()
    rlock.acquire()
    rlock.wait(args.seconds)

    intCurrentDateOrdinal = datetime.date.today().toordinal()
    conn = GetConnection()
    cur = conn.execute("""SELECT ID, program, message, lastrun
                            FROM tblTask WHERE (lastrun + frequency) <= ? """,
                            (intCurrentDateOrdinal,))
    tasks = cur.fetchall()
    cur.close()
    conn.close()

    txtMessage = ''
    txtError = ''

    for (ID, program, message, lastrun) in tasks:
        if message.strip():
            txtMessage += message + '\n' + 'Last run: '
            txtMessage += datetime.date.fromordinal(lastrun).strftime("%A - %d %b %Y") + '\n'
            txtMessage += 'Task ID: %s' % ID + '\n\n'

        if program.strip():
            try:
                RunProcess(program)
            except:
                txtError += 'Error executing program:' + '\n' + str(program) + '\n'
                txtError += 'Task ID: %s' % ID + '\n\n'
            else:
                args = Args(taskid=ID, offset=0)
                Update(args)


    if txtMessage:
        try:
            MsgBox(txtError=txtError.rstrip('\n\n'),
                   txtMsg=txtMessage.rstrip('\n\n'), title='Task Scheduler')
        except:
            import tkMessageBox
            tkMessageBox.showerror('Task Scheduler error', 'Unable to display messages.')
        else:
            for (ID, program, message, lastrun) in tasks:
                if message.strip() and not program.strip():
                    args = Args(taskid=ID, offset=0)
                    Update(args)
    elif txtError:
        MsgBox(txtError=txtError.rstrip('\n\n'), title='Task Scheduler')



if __name__ == '__main__':
    main()
