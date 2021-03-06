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

Requires a json-formatted 'secretfile' containing various database and email information.
Below is a generic example of the contents of '.database_secret.json':

{
    "MYSQLDB1NAME_DB_USER":"db1_username",
    "MYSQLDB1NAME_DB_PASS":"db1_user_password",
    "MYSQLDB2NAME_DB_USER":"db2_username",
    "MYSQLDB2NAME_DB_PASS":"db2_user_password",
    "EMAIL_HOST":"smtp.gmail.com",
    "EMAIL_PORT":"587",
    "EMAIL_USER":"dummy@gmail.com",
    "EMAIL_USE_TLS":"True",
    "EMAIL_PASS":"emailpassword",
    "EMAIL_FROM_USER":"dummy@gmail.com"
    }

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
        if not os.path.isdir(args.backupdir):
            os.makedirs(args.backupdir)
        logging.basicConfig(filename=args.backupdir+"/backuplog.log", 
                            format='%(levelname)s:%(asctime)s %(message)s', 
                            level=logging.DEBUG)
        if DEBUG:
            for directory in args.directories:
                sys.stdout.write("directory to backup: %s\n" % directory)
            sys.stdout.write("backup dir = %s\n" % args.backupdir)

        return zip_dirs(args)
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help\n")
        return 2
    

    
def zip_dirs(args):
    try:
        with open(args.secretfile) as f:
            SECRETS=json.loads(f.read())
    except FileNotFoundError as e:
        if args.verbose:
            sys.stdout.write("Secretfile %s not found\n" % args.secretfile)
        else:
            logger.error("Secretfile %s not found" % args.secretfile)
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
        if args.adminemail == "":
            if args.verbose:
                sys.stdout.write("No admin email specified using --email argument, no email logging enabled.\n")
            else:
                logger.info("No admin email specified using --email argument, no email logging enabled.")
        else:
            isSecure = None
            if emailUseTLS == "True":
                isSecure = ()
            smtpHandler = SMTPHandler((emailHost, emailPort), 
                                      emailFromUser, 
                                      args.adminemail, 
                                      emailSubject, 
                                      credentials=(emailUser, emailPassword,), 
                                      secure=isSecure)
            smtpHandler.setLevel(logging.ERROR)
            logger.addHandler(smtpHandler)
    except ImproperlyConfigured:
        pass
    if args.testlog:
        logger.info("Test of logging capabilities for info messages")
        logger.error("Test of logging capabilities for error messages")
    else:
        for directory in args.directories:
            if os.path.exists(directory):
                backuproot =  directory.replace(os.path.sep,'_')
                backupfile = "%s%s%s.%s.tgz" %(args.backupdir,os.path.sep, backuproot, datetime.now().isoformat().replace(':', '.'))
                if args.verbose:
                    tarcommand = "tar -czf %s %s" % (backupfile, directory)
                    sys.stdout.write("Deleting files with this root name %s\n" % backuproot)
                    sys.stdout.write("using command: rm %s*\n" % backuproot)
                    sys.stdout.write("Taring and gzipping %s to %s\n" % (directory, backupfile))
                    sys.stdout.write("using command: %s\n" % tarcommand)
                try:
                    run(["tar", "-czf",backupfile, directory], stderr=PIPE, check=True)
                except CalledProcessError as e:
                    # try again after a 5 second delay
                    time.sleep(5)
                    try:
                        run(["tar", "-czf", backupfile, directory], stderr=PIPE, check=True)
                    except CalledProcessError as e:
                        if args.verbose:
                            sys.stdout.write("unable to tar directory=%s Error='%s'\n" % (directory, e.stderr.decode()))
                        else:
                            logger.error("unable to tar directory=%s Error='%s'" % (directory, e.stderr.decode()))
                        continue
                if args.keepdays >= 0:
                    # remove files older than keepdays days old
                    now = time.time() - 1 # 1 second fudge factor so we don't delete a just created file if keepdays = 0
                    for file in glob.glob(os.path.join(args.backupdir, backuproot + '*')):
                        if os.path.isfile(file):
                            mtime = os.path.getmtime(file)
                            if now - mtime >= args.keepdays * 86400:
                                # remove old files
                                if DEBUG:
                                    sys.stdout.write("Deleting %s\n" % file)
                                try:
                                    os.remove(file)
                                except OSError as e:
                                    if args.verbose:
                                        sys.stdout.write("directory=%, Unable to remove file %s\n" % (directory, file))
                                    else:
                                        logger.error("directorye=%, Unable to remove file %s" % (directory, file))
                                    continue
                try:
                    os.chmod(backupfile, 0o600, follow_symlinks=True)
                except OSError as e:
                    if args.verbose:
                        sys.stdout.write("directory=%, Unable to chmod 700 on file %s\n" % (directory, backupfile))
                    else:
                        logger.error("directory=%, Unable to chmod 700 on file %s" % (directory, backupfile)) 
                if args.verbose:
                    sys.stdout.write("deleted old files and tar'd and gzipped %s file size=%s to %s\n" % (directory, os.path.getsize(backupfile), backupfile))
                else:
                    logger.info("deleted old files and tar'd and gzipped %s file size=%s to %s" % (directory, os.path.getsize(backupfile), backupfile))
                # now copy the currently created file "backupfile" into the "Current" directory 
                # under the backupdir directory, creating the "Current" directory if necessary 
                # and deleting old files with the same root name
                pathToCurrent = os.path.join(args.backupdir, "Current")
                if not os.path.exists(pathToCurrent):
                    # create Current dir
                    try:
                        os.mkdir(pathToCurrent)
                    except OSError as e:
                        if args.verbose:
                            sys.stdout.write("unable to create %s directory\n" % pathToCurrent)
                        else:
                            logger.error("unable to create %s directory\n" % pathToCurrent)
                        return -1
                else:
                    # delete links with same root name and make a link to the just created backupfile
                    if not os.path.isdir(pathToCurrent):
                        if args.verbose:
                            sys.stdout.write("unable to create %s directory it already exists as a file\n" % pathToCurrent)
                        else:
                            logger.error("unable to create %s directory it already exists as a file\n" % pathToCurrent)
                        return -1
                    for file in glob.glob(os.path.join(pathToCurrent, "%s*" % backuproot)):
                        fullPath = os.path.join(pathToCurrent, file)
                        if os.path.isfile(fullPath):
                            try:
                                os.remove(fullPath)
                            except OSError as e:
                                if args.verbose:
                                    sys.stdout.write("directory=%, Unable to remove current backup file %s\n" % (directory, file))
                                else:
                                    logger.error("directorye=%, Unable to remove current backup file %s" % (directory, file))
                                continue
                    # create link to current backupfile in "Current"
                    try:
                        os.link(backupfile, os.path.join(pathToCurrent, os.path.basename(backupfile)))
                    except OSError as e:
                        if args.verbose:
                            sys.stdout.write("directory=%, Unable to create link to current backup file %s\n" % (directory, os.path.join(pathToCurrent, os.path.basename(backupfile))))
                        else:
                            logger.error("directory=%, Unable to create link to current backup file %s" % (directory, os.path.join(pathToCurrent, os.path.basename(backupfile))))
                        continue
            else:
                if args.verbose:
                    sys.stdout.write("directory %s doesn't exist. Ignoring\n" % directory)
                else:
                    logger.info("directory %s doesn't exist. Ignoring" % directory)
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