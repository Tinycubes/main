# Tinecubes Interface

## Requirements:

- Operating System: Windows
- [Python 3.6](https://www.python.org/downloads/release/python-364/)

## Running the tinycubes

The tinycubes interface can be started with the simple command:
````commandline
python tinycubes.py
````

You can also pass arguments:

- -b *value* or --bin *value*:
    
    Replace *value* with an integer that represents the interval 
    in seconds of each event. 
    
    When not set, by default the bin 
    is set to 3600.
    
````commandline
python tinycubes.py -b  86400
````

- -w or --wait:

    Using *-w* or *--wait* the `nc.exe` will not run. That makes it\`s 
    initialization mandatory.
    
    The interface server will go into standby mode until `nc.exe` 
    is run by the user.
    
````commandline
python tinycubes.py -w
````

- -h or --help:

    Displays available commands.
    
````commandline
python tinycubes -h
````


