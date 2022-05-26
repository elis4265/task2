from argparse import ArgumentParser
import os
import time
import logging
import sys
import hashlib
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler, LoggingEventHandler, FileSystemEventHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path
import shutil
from dirsync import sync


class LoggerInFolder(Exception):
    pass

class NotValidRange(Exception):
    pass

class NestedFolders(Exception):
    pass

class NotFile(Exception):
    pass

class ArgParser(): 

    def __init__(self) -> None:
        self.exceptions_list = []


        self.parser = ArgumentParser(description='Program that synchronizes two folders.')
        self.parser.add_argument("-i", type=self.path_arg,
                            help="path to source folder",  metavar="<string>")
        self.parser.add_argument("-o", type=self.path_arg,
                            help="path to replica folder", metavar="<string>")
        self.parser.add_argument("-t", type=int,
                            help="sync interval", metavar="<int>")
        self.parser.add_argument("-l", 
                            help="path to log file path", metavar="<string>")

    def parse_args(self) -> object:
        args = self.parser.parse_args()
        self.nested_folders(args.i, args.o)
        self.log_in_folders(args.i, args.o, args.l)
        self.file_arg(args.l)
        self.valid_num(args.t)
        self.handle_parse()
        return args

    def path_arg(self, arg: str) -> str | None:
        if os.path.isdir(arg):
            return arg
        else:
            self.exceptions_list.append(NotADirectoryError(arg))
            return None

    def file_arg(self, arg: str) -> str | None:
        if os.path.isfile(arg):
            return arg
        else:
            self.exceptions_list.append(NotFile("Is not a file"))
            return None

    def is_common_path(self, potential_parent: str, potential_child: str) -> bool:
        self.potential_parent = os.path.abspath(potential_parent)
        self.potential_child = os.path.abspath(potential_child)
        return os.path.commonpath([self.potential_parent]) == os.path.commonpath([self.potential_parent, self.potential_child])

    def nested_folders(self, arg1: str, arg2: str) -> None:  
        val1 = self.is_common_path(arg1,arg2)
        val2 = self.is_common_path(arg2,arg1) 
        if (val1 or val2):
            self.exceptions_list.append(NestedFolders("The folder can't contain each other"))
        else:
            return None

    def log_in_folders(self, src_pth: str, rep_pth: str, log_pth: str) -> str | None:
        val1 = self.is_common_path(src_pth, log_pth)
        val2 = self.is_common_path(rep_pth, log_pth)
        if (val1 or val2):
            self.exceptions_list.append(LoggerInFolder("Logger can't be in same folder as source or replica"))
        else:
            return None

    def valid_num(self, arg: int) -> int | None:
        if arg > 0:
            return arg
        else:
            exceptions_list.append(NotValidRange("Synchronization time should be positive integer"))
            return None

    def handle_parse(self) -> None:
        for i in self.exceptions_list:
            print(i)
        

class ScanFiles():
    """ Scan files in source folder, get mh5 sum and store them into dict """

    def __init__(self, root_dir: str):
        self.files_and_checksums = {}
        self.file_list = []
        self.root_dir = root_dir

    def get_file_list(self) -> list:
        for root, dirs, files in os.walk(self.root_dir):
	        for file in files:
		        self.file_list.append(os.path.join(root,file))
        return self.file_list
   
    def hash_bytestr_iter(self, bytesiter, hasher, ashexstr=False):
        for block in bytesiter:
            hasher.update(block)
        return hasher.hexdigest() if ashexstr else hasher.digest()

    def file_as_blockiter(self, afile, blocksize=65536) -> None:
        with afile:
            block = afile.read(blocksize)
            while len(block) > 0:
                yield block
                block = afile.read(blocksize)

    def fill_dict(self) -> None:
        for fname in self.file_list:
            hashed = hash_bytestr_iter(file_as_blockiter(open(fname, 'rb')),hashlib.sha256())
            self.files_and_checksums[fname] = hashed
    
    def add_file(self, fname: str) -> None:
        hashed = self.hash_bytestr_iter(self.file_as_blockiter(open(fname, 'rb')),hashlib.sha256())
        self.files_and_checksums[fname] = hashed
    
    def remove_file(self, fname: str) -> None:
        self.files_and_checksums.pop(fname)

    def is_new(self, fname: str) -> bool:
        if fname in self.files_and_checksums:
            return False
        return True

    def is_copy(self, fname: str) -> bool:
        hashed = self.hash_bytestr_iter(self.file_as_blockiter(open(fname, 'rb')),hashlib.sha256())
        if hashed in self.files_and_checksums.values():
            return True
        return False
        
class CustomEventHandler(FileSystemEventHandler):
    """ Logs and stdout creation, copying and deletion of files in watched directory."""

    def __init__(self, log_pth: str, root_dir: str):
        self.f = open(log_pth, "w");    
        self.scanner = ScanFiles(root_dir)

   
    def on_created(self, event) -> None:
        super().on_created(event)
        if (not event.is_directory):
            if (self.scanner.is_copy(event.src_path)):
                what = 'directory' if event.is_directory else 'file'
                self.scanner.add_file(event.src_path)
                self.f.write("Copied file: %s %s" %(what, event.src_path))
                self.f.write("\n")
                print("Copied", what, event.src_path)
            elif(not self.scanner.is_copy((event.src_path))):
                what = 'directory' if event.is_directory else 'file'
                self.scanner.add_file(event.src_path)
                self.f.write("Created: %s %s" %(what, event.src_path))
                self.f.write("\n")
                print("Created", what, event.src_path)

    def on_modified(self, event) -> None:
        super().on_modified(event)
        if (not event.is_directory):
            what = 'directory' if event.is_directory else 'file'
            self.scanner.add_file(event.src_path)
    
    def on_deleted(self, event) -> None:
        super().on_deleted(event)
        what = 'directory' if event.is_directory else 'file'
        self.scanner.remove_file(event.src_path)
        self.f.write("Deleted: %s %s" %(what,event.src_path))
        self.f.write("\n")
        print("Deleted", what, event.src_path)
 
        
class Watcher():
    """ Initialize and start observer, sync folders """

    def __init__(self, log_pth: str, src_pth: str, dest_pth: str, time: int):
        self.observer = Observer()
        self.log_pth = log_pth
        self.src_pth = os.path.abspath(src_pth)
        self.dest_pth = os.path.abspath(dest_pth)
        self.time = time
        self.empty_logger = logging.getLogger('empty_logger')

    def start_observer(self) -> None:
        shutil.copytree(self.src_pth, self.dest_pth, dirs_exist_ok=True)
        event_handler = CustomEventHandler(self.log_pth, self.src_pth)
        self.observer.schedule(event_handler, path=self.src_pth, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(self.time)
                sync(self.src_pth, self.dest_pth, 'sync',logger = self.empty_logger, purge = True)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()

def main() -> None:
    args = ArgParser().parse_args()
    watch = Watcher(args.l, args.i,args.o, args.t).start_observer()
 
   

if __name__ == "__main__":
    main()