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
from googleapiclient import discovery
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials

import sys
import os
import json
import logging
import glob
from logging.handlers import SMTPHandler
logger = logging.getLogger(__name__)

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
        logfile = args.logfile
        directories = args.directories
        secretfile = args.secretfile
        keyfile = args.keyfile
        verbose = args.verbose
        testlog = args.testlog
        adminemail = args.adminemail
        logging.basicConfig(filename=logfile, 
                            format='%(levelname)s:%(asctime)s %(message)s', 
                            level=logging.INFO)
        logging.getLogger('googleapiclient').setLevel(logging.ERROR)
        logging.getLogger('oauth2client').setLevel(logging.ERROR)
        return drive_backup(directories=directories, secretfile=secretfile, keyfile=keyfile, verbose=verbose, 
                            testlog=testlog, adminemail=adminemail)
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

def create_new_folder(service,  name, verbose=False):
    """Will create a new folder in the root of the supplied GDrive, 
    Retruns:
        The folder resource
    """
    backupdirs = list_files_in_drive(service, namequery="= '%s'" % name, 
                                     mimetypequery="= 'application/vnd.google-apps.folder'")
    if len(backupdirs) == 0:
        folder_metadata = {
        'name' : name,
        'mimeType' : 'application/vnd.google-apps.folder'
        }
        try:
            service.files().create(body=folder_metadata, fields='id, name').execute()
        except HttpError as e:
            if verbose:
                sys.stdout.write("unable to create folder %s: %s\n" % (name, str(e)))
            return None
        backupdirs = list_files_in_drive(service, namequery="= '%s'" % name, 
                                         mimetypequery="= 'application/vnd.google-apps.folder'")
        folder = backupdirs[0]
        if verbose:
            sys.stdout.write("Folder Creation Complete\n")
            sys.stdout.write("Folder Name: %s\n" % folder.get('name'))
            sys.stdout.write("Folder ID: %s \n" % folder.get('id'))
        logger.info("Folder %s creation complete, ID=%s" % (folder.get('name'), folder.get('id')))
    else:
        folder = backupdirs[0]
        if verbose:
            sys.stdout.write("Folder already exists\n")
            sys.stdout.write("Folder Name: %s\n" % folder.get('name'))
            sys.stdout.write("Folder ID: %s \n" % folder.get('id'))
    return folder

def delete_file(service,  fileid, verbose=False):
    """Will delete the given fileid on the supplied GDrive, 
    Retruns:
        True if sucessful
    """
    try:
        file = service.files().get(fileId=fileid, fields='name').execute()
        service.files().delete(fileId=fileid).execute()
        if verbose:
            sys.stdout.write("deleted file %s fileid=%s\n" % (file.get('name'), fileid))
        logger.info("deleted file %s fileid=%s" % (file.get('name'), fileid))
        result = True
    except HttpError as e:
        if verbose:
            sys.stdout.write("unable to delete file %s fileid=%s: %s\n" % (file.get('name'), fileid, str(e)))
            logger.error("unable to delete file %s fileid=%s: %s" % (file.get('name'), fileid, str(e)))
        result = False
    return result

def get_service(keyfile, scopes, verbose=False):
    """Get a service that communicates to a Google API.
    Returns:
      A service that is connected to the specified API.
    """
    if verbose:
        sys.stdout.write("Acquiring credentials...\n")
    credentials = ServiceAccountCredentials.from_json_keyfile_name(filename=keyfile, scopes=scopes)

    # Build the service object for use with any API
    if verbose:
        sys.stdout.write("Acquiring service...\n")
    service = discovery.build(serviceName="drive", version="v3", credentials=credentials,
                              cache_discovery=False)
    
    if verbose:
        sys.stdout.write("Service acquired!\n")
    return service

def upload_file_to_folder(service, folderID, fileName, verbose=False):
    """Uploads the file to the specified folder id on the said Google Drive
    Returns:
            file resource
    """
    file_metadata = None
    if folderID is None:
        file_metadata = {
            'name' : fileName
        }
    else:
        file_metadata = {
              'name' : fileName,
              'parents': [ folderID ]
        }

    media = MediaFileUpload(fileName, resumable=True)
    try:
        folder = service.files().get(fileId=folderID).execute()
        file = service.files().create(body=file_metadata, media_body=media, fields='name,id,size,parents').execute()
        logger.info("Uploaded file %s ID=%s to: %s" % (file.get('name'), file.get('id'), folder.get('name')))
        if verbose:
            sys.stdout.write("Uploaded file %s ID=%s to: %s\n" % (file.get('name'), file.get('id'), folder.get('name')))
            sys.stdout.write("File Size: %s \n" % file.get('size'))
            sys.stdout.write("parents ID: %s\n" % str(file.get('parents')))
    except HttpError as e:
        if verbose:
            sys.stdout.write("unable to upload file  %s: %s\n" % (fileName, str(e)))
        logger.error ("Unable to upload file %s to: %s\n" % (fileName, folderID))
        return None

    return file

def list_files_in_drive(service, datequery='', namequery="", mimetypequery="", parentid='', verbose=False):
    """Queries Google Drive for all files satisfying name contains string
    Returns:
            list of file resources
    """
    q=''
    if len(namequery) > 0:
        q += "name %s" % namequery
    if len(mimetypequery) > 0:
        if q:
            q += " and "
        q += "mimeType %s" % mimetypequery
    if len(parentid) > 0:
        if q:
            q += " and "
        q += "'%s' in parents" % parentid
    if len(datequery) > 0:
        if q:
            q += " and "
        q += "modifiedTime %s" % datequery
    try:
        if len(q) > 0:
            files= service.files().list(q=q).execute()
        else:
            files = service.files().list().execute()
    except HttpError as e:
        if verbose:
            sys.stdout.write("unable to list files  %s: %s\n" % (q, str(e)))
        logger.error("unable to list files  %s: %s\n" % (q, str(e)))
        return []
    if verbose:
        for file in files.get('files'):
            thisFile = service.files().get(fileId=file.get('id'), fields='id,parents,name,size,modifiedTime').execute()
            sys.stdout.write("File ID: %s \n" % thisFile.get('id'))
            sys.stdout.write("File Name: %s \n" % thisFile.get('name'))
            sys.stdout.write("File size: %s\n" % thisFile.get('size'))
            sys.stdout.write("Modified: %s\n" % thisFile.get('modifiedTime'))
            sys.stdout.write("parents ID: %s\n" % str(thisFile.get('parents')))
    return files.get('files')

def upload_file_to_root(service, fileName, verbose=False):
    """Uploads the file to the root directory on the said Google Drive
    Returns:
            fileID, A string of the ID from the uploaded file
    """
    return upload_file_to_folder(service=service, folderID=None, fileName=fileName, verbose=verbose)

def drive_backup(directories=None, secretfile='', keyfile='', verbose=False,
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
        service = get_service(keyfile, SCOPES, verbose=verbose)
        backupFolder = create_new_folder(service, 'Backup', verbose=verbose)
        for directory in directories:
            if os.path.exists(directory):
                backuproot =  directory.replace(os.path.sep,'_')
                utcnow = datetime.utcnow().isoformat()
                backupfile = "%s%s%s.%s.gz" %('/tmp',os.path.sep, backuproot, datetime.now().isoformat())
                if verbose:
                    tarcommand = "tar -czf %s %s" % (backupfile, directory)
                    sys.stdout.write("Taring and gzipping %s to %s\n" % (directory, backupfile))
                    sys.stdout.write("using command: %s\n" % tarcommand)
                try:
                    run(["tar", "-czf", backupfile, directory], stderr=PIPE, check=True)
                except CalledProcessError as e:
                    if verbose:
                        sys.stdout.write("unable to tar directory=%s Error='%s'\n" % (directory, e.stderr.decode()))
                    logger.error("unable to tar directory=%s Error='%s'" % (directory, e.stderr.decode()))
                    continue
                upload_file_to_folder(service, backupFolder.get('id'), backupfile, verbose)
                for rmfile in glob.glob("%s*" % os.path.join('/tmp', backuproot)):
                    fileToRemove = os.path.join('/tmp', rmfile)
                    try:
                        run(["rm", fileToRemove], stderr=PIPE, check=True)
                        if verbose:
                            sys.stdout.write("removing %s from filesystem\n" % fileToRemove)
                    except CalledProcessError as e:
                        if verbose:
                            sys.stdout.write("unable to remove %s, Error='%s'\n" % (fileToRemove, e.stderr.decode()))
                            logger.error("unable to remove %s, Error='%s'" % (fileToRemove, e.stderr.decode()))
                        continue
                oldFiles = list_files_in_drive(service, verbose=False, datequery="< '%sZ'" % utcnow, namequery="contains '%s%s%s'" % ('/tmp', os.path.sep, backuproot))
                for file in oldFiles:
                    delete_file(service, fileid=file.get('id'), verbose=verbose)
            else:
                logger.info("directory %s doesn't exist. Ignoring" % directory)
                if verbose:
                    sys.stdout.write("directory %s doesn't exist. Ignoring\n" % directory)
        smtpHandler.setLevel(logging.INFO)
        logger.info("Uploaded the following directories to Google Drive: %s" % str(directories))
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