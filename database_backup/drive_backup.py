#!/usr/bin/python3
# encoding: utf-8
'''
database_backup.drive_backup -- is a CLI program used to automate backing up gzipped directories to Gogle Drive

@author:     Rob Groves

@copyright:  2018. All rights reserved.

@license:    license

@contact:    robgroves0@gmail.com
@deffield    updated: Updated
'''

from __future__ import print_function
from google_drive import GoogleDrive

import sys
import os
import json
import logging
import glob
from logging.handlers import SMTPHandler
logger = logging.getLogger(__name__)
from time import time
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from datetime import datetime
from subprocess import run, PIPE, CalledProcessError

__all__ = []
__version__ = 0.1
__date__ = '2018-12-30'
__updated__ = '2018-12-30'

DEBUG = 0
TESTRUN = 0
PROFILE = 0
SCOPES = 'https://www.googleapis.com/auth/drive'

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


requires a json formatted 'keyfile' also containing the Google Drive server-server credentials.
see https://developers.google.com/identity/protocols/OAuth2ServiceAccount for more information

USAGE
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-l", "--logfile", dest="logfile", help="file to use for logging purposes [default: %(default)s]", default="./backup.log")
        parser.add_argument("-s", "--secretfile", dest="secretfile", help="use this secrets file to set database users and passwords [default: %(default)s]", default="./.database_secret.json")
        parser.add_argument("-k", "--keyfile", dest="keyfile", help="this file contains the Google Drive server-server encryption keys [default: %(default)s]", default="./.backup-key.json")
        parser.add_argument("-e", "--email", dest="adminemail", help="email address for log error updates [default: None]", default="")
        parser.add_argument("-t", "--testlog", dest="testlog", action="store_true", help="test log end email capabilities (do nothing else) [default: %(default)s]", default=False)
        parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="run in verbose mode [default: %(default)s]", default=False)
        parser.add_argument(dest="directories", help="space separated list of directories to zip", nargs='+')

        # Process arguments
        args = parser.parse_args()
        logging.basicConfig(filename=args.logfile, 
                            format='%(levelname)s:%(asctime)s %(message)s', 
                            level=logging.INFO)
        logging.getLogger('googleapiclient').setLevel(logging.ERROR)
        logging.getLogger('oauth2client').setLevel(logging.ERROR)
        return drive_backup(args)
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

def drive_backup(args):
    settings = {}
    settings["logfile"] = args.logfile
    settings["secretfile"] = args.secretfile
    settings["keyfile"] = args.keyfile
    settings["email"] = args.adminemail
    settings["testlog"] = args.testlog
    settings["verbose"] = args.verbose
    settings["scopes"] = SCOPES
    directories = args.directories
    try:
        with open(settings.get("secretfile")) as f:
            SECRETS=json.loads(f.read())
    except FileNotFoundError as e:
        logger.error("Secretfile %s not found" % args.secretfile)
        sys.stderr.write(e.strerror + ":\n")
        sys.stderr.write(settings.get("secretfile") + "\n")
        return -1
    def get_secret(setting, secrets=SECRETS):
        """
        get the secret setting or return explicit exception
        """
        try:
            return secrets[setting]
        except KeyError:
            error_msg = "Set the {0} environment variable in the secret file".format(setting)
            logger.error("Set the {0} environment variable in the secret file".format(setting))
            raise ImproperlyConfigured(error_msg)
    emailSubject = "Database backup to Google Drive Information!!!"
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
        gdrive = GoogleDrive(settings=settings)
        backupFolder = gdrive.create_new_folder('Backup')
        successful = []
        for directory in directories:
            if os.path.exists(directory):
                backuproot =  directory.replace(os.path.sep,'_')
                utcnow = datetime.utcnow().isoformat()
                backupfile = "%s%s%s.%s.tgz" %('/tmp',os.path.sep, backuproot, datetime.now().isoformat())
                if gdrive.verbose:
                    tarcommand = "tar -czf %s %s" % (backupfile, directory)
                    sys.stdout.write("Taring and gzipping %s to %s\n" % (directory, backupfile))
                    sys.stdout.write("using command: %s\n" % tarcommand)
                try:
                    run(["tar", "-czf", backupfile, directory], stderr=PIPE, check=True)
                except CalledProcessError as e:
                    # try again after a 5 second delay
                    time.sleep(5)
                    try:
                        run(["tar", "-czf", backupfile, directory], stderr=PIPE, check=True)
                    except CalledProcessError as e:
                        if gdrive.verbose:
                            sys.stdout.write("unable to tar directory=%s Error='%s'\n" % (directory, e.stderr.decode()))
                        else:
                            logger.error("unable to tar directory=%s Error='%s'" % (directory, e.stderr.decode()))
                        continue
                if gdrive.upload_file_to_folder(backupFolder.get('id'), backupfile) is not None:
                    successful.append(directory)
                for rmfile in glob.glob("%s*" % os.path.join('/tmp', backuproot)):
                    fileToRemove = os.path.join('/tmp', rmfile)
                    try:
                        run(["rm", fileToRemove], stderr=PIPE, check=True)
                        if gdrive.verbose:
                            sys.stdout.write("removing %s from filesystem\n" % fileToRemove)
                    except CalledProcessError as e:
                        if gdrive.verbose:
                            sys.stdout.write("unable to remove %s, Error='%s'\n" % (fileToRemove, e.stderr.decode()))
                        else:
                            logger.error("unable to remove %s, Error='%s'" % (fileToRemove, e.stderr.decode()))
                        continue
                oldFiles = gdrive.list_files_in_drive(query="modifiedTime < '%sZ' and name contains '%s'" % (utcnow, backuproot))
                for file in oldFiles:
                    gdrive.delete_file(fileid=file.get('id'))
            else:
                if gdrive.verbose:
                    sys.stdout.write("directory %s doesn't exist. Ignoring\n" % directory)
                else:
                    logger.info("directory %s doesn't exist. Ignoring" % directory)
        if args.verbose:
            sys.stdout.write("Uploaded the following directories to Google Drive: %s\n" % str(successful))
        else:
            smtpHandler.setLevel(logging.INFO)
            logger.info("Uploaded the following directories to Google Drive: %s" % str(successful))
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