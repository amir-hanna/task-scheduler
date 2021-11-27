# task-scheduler
A command line Task Scheduler to display a message or run a program at a certain date and time. to be run at startup.

Requires Python 2.7

usage: 
ts.py [-h] {display,add,update,delete,run,vacuum}

optional arguments:
  -h, --help            show this help message and exit

subcommands:
    display             Display scheduled tasks.
    add                 Add a new scheduled task.
    update              Update task properties.
    delete              Delete a scheduled task.
    run                 Run all scheduled tasks according to saved properties.
    vacuum              Shrink database file.
