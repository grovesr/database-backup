#!/usr/bin/python3
# encoding: utf-8
'''
database_backup.mysql_backup -- is a CLI program that is used to backup MYSQL databases

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
import time
import glob
from logging.handlers import SMTPHandler
logger = logging.getLogger(__name__)

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from datetime import datetime
from subprocess import run, PIPE, CalledProcessError

__all__ = []
__version__ = 0.1
__date__ = '2018-11-06'
__updated__ = '2018-11-06'

DEBUG = 0
TESTRUN = 0
PROFILE = 0
ADMIN = "robgroves0@gmail.com"

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
        parser.add_argument("-b", "--backupdir", dest="backupdir", help="place backup files in BACKUPDIR [default: %(default)s]", default = ".")
        parser.add_argument("-k", "--keepdays", dest="keepdays", help="if other backup files exist, keep the last KEEPDAYS worth [default: %(default)s which means keep all]", default="-1", type=int)
        parser.add_argument("-s", "--secretfile", dest="secretfile", help="use this secrets file to set database users and passwords [default: %(default)s]", default="./.database_secret.json")
        parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="run in verbose mode [default: %(default)s]", default=False)
        parser.add_argument("-e", "--email", dest="adminemail", help="email address for log error updates [default: None]", default="")
        parser.add_argument("-t", "--testlog", dest="testlog", action="store_true", help="test log end email capabilities (do nothing else) [default: %(default)s]", default=False)
        parser.add_argument(dest="databases", help="space separated list of databases to backup", nargs='+')

        # Process arguments
        args = parser.parse_args()
        if not os.path.isdir(args.backupdir):
            os.makedirs(args.backupdir)
        logging.basicConfig(filename=args.backupdir+"/backuplog.log", 
                            format='%(levelname)s:%(asctime)s %(message)s', 
                            level=logging.DEBUG)
        if DEBUG:
            for database in args.databases:
                sys.stdout.write("database to backup: %s" % database)
            sys.stdout.write("backup dir = %s" % args.backupdir)
            if args.keepdays >=0:
                sys.stdout.write("number of days worth of backups to keep = %d" % args.keepdays)
            else:
                sys.stdout.write("number of days worth of backups to keep = ALL")
            sys.stdout.write("secret file = %s" % args.secretfile)

        return mysql_backup(args)
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
    

    
def mysql_backup(args):
    try:
        with open(args.secretfile) as f:
            SECRETS=json.loads(f.read())
    except FileNotFoundError as e:
        if args.verbose:
            sys.stderr.write(e.strerror + ":\n")
            sys.stderr.write(args.secretfile + "\n")
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
        for database in args.databases:
            backupfile = "%s/%s.%s.sql" %(args.backupdir, database, datetime.now().isoformat())
            user = get_secret(database.upper() + "_DB_USER")
            password = get_secret(database.upper() + "_DB_PASS")
            if args.verbose:
                dumpcommand = "mysqldump -u %s -p %s" % (user, database)
                sys.stdout.write("Backing up and gzipping %s database to %s.gz" % (database, backupfile))
                sys.stdout.write("using command: %s" % dumpcommand)
            with open(backupfile, "wb", 0) as out:
                try:
                    run(["mysqldump", "-u", user, "-p"+password, database], stderr=PIPE, stdout=out, check=True)
                except CalledProcessError as e:
                    if args.verbose:
                        sys.stdout.write("database=%s Error='%s'\n" % (database, e.stderr.decode()))
                    else:
                        logger.error("database=%s Error='%s'" % (database, e.stderr.decode()))
                    continue
            try:
                run(["gzip", backupfile], stderr=PIPE, check=True)
            except CalledProcessError as e:
                if args.verbose:
                    sys.stdout.write("unable to gzip file=%s Error='%s'\n" % (backupfile, e.stderr.decode()))
                else:
                    logger.error("unable to gzip file=%s Error='%s'" % (backupfile, e.stderr.decode()))
                continue  
            try:
                os.chmod(backupfile+".gz", 0o600, follow_symlinks=True)
            except OSError as e:
                if args.verbose:
                    sys.stdout.write("database=%, Unable to chmod 700 on file %s" % (database, backupfile))
                else:
                    logger.error("database=%, Unable to chmod 700 on file %s" % (database, backupfile))
            if args.verbose:
                sys.stdout.write("backed up and gzipped %s file size=%s\n" % (database, os.path.getsize(backupfile+".gz")))
            else:
                logger.info("backed up and gzipped %s file size=%s" % (database, os.path.getsize(backupfile+".gz")))
            # remove files older than keepdays days old
            if args.keepdays >= 0:
                now = time.time() - 1 # 1 second fudge factor so we don't delete a just created file if keepdays = 0
                for file in glob.glob(os.path.join(args.backupdir, database + '*')):
                    fullPath = os.path.join(args.backupdir, file)
                    if os.path.isfile(fullPath):
                        mtime = os.path.getmtime(fullPath)
                        if now - mtime >= args.keepdays * 86400:
                            # remove old files
                            if DEBUG:
                                sys.stdout.write("Deleting %s" % fullPath)
                            os.remove(fullPath)
    return 0

if __name__ == "__main__":
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'database_backup.mysql_backup_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())