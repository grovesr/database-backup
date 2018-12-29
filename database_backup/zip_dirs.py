#!/usr/bin/python3
# encoding: utf-8
'''
database_backup.zip_dirs -- is a CLI program used to automate zipping of directories

@author:     Rob Groves

@copyright:  2018. All rights reserved.

@license:    license

@contact:    robgroves0@gmail.com
@deffield    updated: Updated
'''

import sys
import os
import json
import logging
import glob
import time
from logging.handlers import SMTPHandler
logger = logging.getLogger(__name__)

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from datetime import datetime
from subprocess import run, PIPE, CalledProcessError

__all__ = []
__version__ = 0.1
__date__ = '2018-12-26'
__updated__ = '2018-12-26'

DEBUG = 0
TESTRUN = 0
PROFILE = 0

class ImproperlyConfigured(Exception):
    '''Generic exception to raise and log configuration errors.'''
    def __init__(self, msg):
        super(ImproperlyConfigured).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created on %s.
  Copyright 2018. All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-b", "--backupdir", dest="backupdir", help="place tar'd and zipped directories in BACKUPDIR [default: %(default)s]", default = ".")
        parser.add_argument("-k", "--keepdays", dest="keepdays", help="if other backup files exist, keep the last KEEPDAYS worth [default: %(default)s which means keep all]", default="-1", type=int)
        parser.add_argument("-s", "--secretfile", dest="secretfile", help="use this secrets file to set database users and passwords [default: %(default)s]", default="./.database_secret.json")
        parser.add_argument("-e", "--email", dest="adminemail", help="email address for log error updates [default: None]", default="")
        parser.add_argument("-t", "--testlog", dest="testlog", action="store_true", help="test log end email capabilities (do nothing else) [default: %(default)s]", default=False)
        parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="run in verbose mode [default: %(default)s]", default=False)
        parser.add_argument(dest="directories", help="space separated list of directories to zip", nargs='+')

        # Process arguments
        args = parser.parse_args()

        directories = args.directories
        backupdir = args.backupdir
        keepdays = args.keepdays
        secretfile = args.secretfile
        verbose = args.verbose
        testlog = args.testlog
        adminemail = args.adminemail
        if not os.path.isdir(backupdir):
            os.makedirs(backupdir)
        logging.basicConfig(filename=backupdir+"/backuplog.log", 
                            format='%(levelname)s:%(asctime)s %(message)s', 
                            level=logging.DEBUG)
        if DEBUG:
            for directory in directories:
                sys.stdout.write("directory to backup: %s" % directory)
            sys.stdout.write("backup dir = %s" % backupdir)

        return zip_dirs(directories=directories, backupdir=backupdir, keepdays=keepdays,
                            secretfile=secretfile, verbose=verbose, testlog=testlog, adminemail=adminemail)
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2
    

    
def zip_dirs(directories=None, backupdir="", keepdays=-1, secretfile='', verbose=False,
             testlog=False, adminemail=""):
    try:
        with open(secretfile) as f:
            SECRETS=json.loads(f.read())
    except FileNotFoundError as e:
        logger.error("Secretfile %s not found" % secretfile)
        sys.stderr.write(e.strerror + ":\n")
        sys.stderr.write(secretfile + "\n")
        return -1
    def get_secret(setting, secrets=SECRETS):
        """
        get the secret setting or return explicit exception
        """
        try:
            return secrets[setting]
        except KeyError:
            error_msg = "Set the {0} environment variable in the secret file".format(setting)
            raise ImproperlyConfigured(error_msg)
    emailSubject = "Problem with database backup!!!"
    try:
        emailHost = get_secret("EMAIL_HOST")
        emailUser = get_secret("EMAIL_USER")
        emailPort = get_secret("EMAIL_PORT")
        emailUseTLS = get_secret("EMAIL_USE_TLS")
        emailPassword = get_secret("EMAIL_PASS")
        emailFromUser = get_secret("EMAIL_FROM_USER")
        if adminemail == "":
            logger.info("No admin email specified using --email argument, no email logging enabled.")
        else:
            isSecure = None
            if emailUseTLS == "True":
                isSecure = ()
            smtpHandler = SMTPHandler((emailHost, emailPort), 
                                      emailFromUser, 
                                      adminemail, 
                                      emailSubject, 
                                      credentials=(emailUser, emailPassword,), 
                                      secure=isSecure)
            smtpHandler.setLevel(logging.ERROR)
            logger.addHandler(smtpHandler)
    except ImproperlyConfigured:
        pass
    if testlog:
        logger.info("Test of logging capabilities for info messages")
        logger.error("Test of logging capabilities for error messages")
    else:
        for directory in directories:
            if os.path.exists(directory):
                backuproot =  directory.replace(os.path.sep,'_')
                backupfile = "%s%s%s.%s.gz" %(backupdir,os.path.sep, backuproot, datetime.now().isoformat())
                if verbose:
                    tarcommand = "tar -czf %s %s" % (backupfile, directory)
                    sys.stdout.write("Deleting files with this root name %s" % backuproot)
                    sys.stdout.write("using command: rm %s*" % backuproot)
                    sys.stdout.write("Taring and gzipping %s to %s" % (directory, backupfile))
                    sys.stdout.write("using command: %s" % tarcommand)
                try:
                    run(["tar", "-czf",backupfile, directory], stderr=PIPE, check=True)
                except CalledProcessError as e:
                    if verbose:
                        sys.stdout.write("directory=%s Error='%s'" % (directory, e.stderr.decode()))
                    logger.error("directory=%s Error='%s'" % (directory, e.stderr.decode()))
                    continue
                if keepdays >= 0:
                    now = time.time() - 1 # 1 second fudge factor so we don't delete a just created file if keepdays = 0
                    for file in glob.glob(os.path.join(backupdir, backuproot + '*')):
                        fullPath = os.path.join(backupdir, file)
                        if os.path.isfile(fullPath):
                            mtime = os.path.getmtime(fullPath)
                            if now - mtime >= keepdays * 86400:
                                # remove old files
                                if DEBUG:
                                    sys.stdout.write("Deleting %s" % fullPath)
                                try:
                                    os.remove(fullPath)
                                except OSError as e:
                                    if verbose:
                                        sys.stdout.write("directory=%, Unable to remove file %s" % (directory, backupfile))
                                    logger.error("directorye=%, Unable to remove file %s" % (directory, backupfile))
                                    continue
                try:
                    os.chmod(backupfile, 0o600, follow_symlinks=True)
                except OSError as e:
                    if verbose:
                        sys.stdout.write("directory=%, Unable to chmod 700 on file %s" % (directory, backupfile))
                    logger.error("directory=%, Unable to chmod 700 on file %s" % (directory, backupfile))  
                logger.info("deleted old files and tar'd and gzipped %s file size=%s to %s" % (directory, os.path.getsize(backupfile), backupfile))
                # remove files older than keepdays days old
            else:
                logger.info("directory %s doesn't exist. Ignoring" % directory)
                if verbose:
                    sys.stdout.write("directory %s doesn't exist. Ignoring" % directory)
    return 0

if __name__ == "__main__":
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'database_backup.zip_dirs_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())