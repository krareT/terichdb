#!/usr/bin/python
# coding=utf-8

"""
 install terarkdb online.

 @author royguo@terark.com
 @date 2016-07-21
"""

import platform
import sys
import urllib2
import json
import subprocess
import tarfile
import os
import shutil

""" Default Install PATH """
INSTALL_PATH = "/usr/local"

# TerarkDB Release Information
TERARKDB_INFO = None
# Current OS Platform
PLATFORM = None
# Selected TerarkDB Version
VERSION = None
# BMI Support Flag
BMI = False
# Selected Package Name
PACKAGE_NAME = None


def select_terarkdb_version():
    global TERARKDB_INFO
    global VERSION
    ver = raw_input('Please select a release version[e.g. 0.13.8], leave empty for latest version ...\n')
    while True:
        if ver is None or not ver.strip():
            print 'use default version : ', TERARKDB_INFO[0]
            VERSION = TERARKDB_INFO[0]
            break
        elif ver in TERARKDB_INFO:
            print 'use version : ', ver
            VERSION = ver
            break
        else:
            ver = raw_input('Wrong input, please retry: \n')


def check_platform():
    global PLATFORM
    print 'Checking OS platform ...\t',
    s = platform.system()
    if s.lower().find('linux') > -1:
        print 'Linux \t OK'
        PLATFORM = 'linux'
    elif s.lower().find('darwin') > -1:
        print 'MacOS \t OK'
        PLATFORM = 'darwin'
    else:
        print 'Windows online install is not supported yet'
        exit(0)


# TODO
def check_compiler():
    pass


def check_BMI_support():
    global PLATFORM
    global BMI
    print 'Checking BMI support ... \t',
    if PLATFORM.lower().find('linux') > -1:
        cmd = "cat /proc/cpuinfo | sed -n '/^flags\s*:\s*/s/^[^:]*:\s*//p'"
        output, error = subprocess.Popen(cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE).communicate()
        if output.find('bmi') > -1:
            BMI = True
            print 'Supported'
        else:
            print 'Not Supported'
    elif PLATFORM.lower().find('darwin') > -1:
        cmd = "sysctl -n 'machdep.cpu.features' | tr 'A-Z' 'a-z' | sed -E 's/[[:space:]]+/'$'\\\n/g'"
        output, error = subprocess.Popen(cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE).communicate()
        if output.find('bmi') > -1:
            BMI = True
            print 'Supported'
        else:
            print 'Not Supported'
    else:
        print 'Windows online install is not supported yet'
        exit(0)


def check_package_info():
    global TERARKDB_INFO
    print 'Checking TerarkDB releases ...'
    try:
        resp = urllib2.urlopen("http://terark.com/download/terarkdb/releases")
        jsonstr = resp.read()
        resp.close()
        TERARKDB_INFO = json.loads(jsonstr)
        TERARKDB_INFO = [str(i) for i in sorted(TERARKDB_INFO, reverse=True)]
        print 'Available versions : \t', TERARKDB_INFO
    except RuntimeError, e:
        print e


def download_package():
    global TERARKDB_INFO
    global VERSION
    global PLATFORM
    global BMI
    global PACKAGE_NAME

    selection = []
    resp = urllib2.urlopen("http://terark.com/download/terarkdb/release/" + VERSION)
    jsonstr = resp.read()
    resp.close()
    pkgs = json.loads(jsonstr)
    for pkg in [p for p in pkgs if p['name'].lower().find(PLATFORM) > -1]:
        if (BMI and pkg['name'].find('bmi2-1') > -1) or (not BMI and pkg['name'].find('bmi2-0') > -1):
            selection.append({"name": pkg['name'].split('/')[1], "url": pkg['url']})

    print "Please select TerarkDB package fit for your system :"

    for i in range(len(selection)):
        print i + 1, '\t', selection[i]['name']

    while True:
        index = input('#')
        if 0 < index <= len(selection):
            break

    PACKAGE_NAME = selection[index - 1]['name']
    print 'Downloading ... '

    tmp = open(PACKAGE_NAME, 'wb')
    downloadfile = urllib2.urlopen(selection[index - 1]['url'])
    chunk = 512
    downloaded_bytes = 0
    while True:
        data = downloadfile.read(chunk)
        if not data:
            print 'Download finished!'
            break
        tmp.write(data)
        downloaded_bytes += chunk
        sys.stdout.write('Downloading ... \t %s bytes\r' % downloaded_bytes)
        sys.stdout.flush()
    tmp.close()


def install_package():
    global INSTALL_PATH
    print 'Decompressing, may take a few seconds... '

    if os.path.exists("terarkdb_tmp"):
        shutil.rmtree("terarkdb_tmp")
    if PACKAGE_NAME.endswith("tgz"):
        tar = tarfile.open(PACKAGE_NAME, "r:gz")
        tar.extractall("terarkdb_tmp")
        tar.close()
    else:
        print 'Unexpected package name, exit!'
        exit(0)

    # allow user to change install directory.
    path = raw_input("Please enter a install path or leave it empty(Default `%s`)" % INSTALL_PATH)
    if path is not None and path.strip() is not '':
        INSTALL_PATH = path

    # create dir if not exist
    if not os.path.exists(INSTALL_PATH):
        try:
            os.makedirs(INSTALL_PATH)
        except OSError, e:
            print e
            print "Wrong installation directory, please change a directory and retry."
            exit(0)

    basepath = INSTALL_PATH + "/" + PACKAGE_NAME[0:-4]

    # delete previous installation
    if os.path.exists(basepath):
        shutil.rmtree(basepath)

    shutil.move("terarkdb_tmp/pkg/" + PACKAGE_NAME[0:-4], basepath)

    if os.path.exists(basepath):
        print """
            **************************************************************************************************

            TerarkDB is installed successfully:

            \t %s/
                                \_api/          Third-party APIs, e.g. LevelDB API.
                                \_include/      TerarkDB native API.
                                \_lib/          Libraries
                                \_bin/          Useful tools, e.g. Schema to C++ Structure converter.

            \t\033[91m Don't forget to setup your environment variables : \033[00m
        """ % basepath
        if PLATFORM == 'darwin':
            print """
                \t\t 1. `export PATH=$PATH:%s/bin`
                \t\t 2. `export DYLD_LIBRARY_PATH=%s/lib`
            """ % (basepath, basepath)
        elif PLATFORM == 'linux':
            print """
                \t\t 1. `export PATH=$PATH:%s/bin`
                \t\t 2. `export LIBRARY_PATH=%s/lib`
            """ % (basepath, basepath)
        print """
            **************************************************************************************************
        """
    else:
        print 'Something goes wrong, please retry !'
        exit(0)


def cleanup():
    os.remove(PACKAGE_NAME)
    shutil.rmtree("terarkdb_tmp")
    print 'Finish installation\n\n'


if __name__ == '__main__':
    check_package_info()
    select_terarkdb_version()
    check_platform()
    check_compiler()
    check_BMI_support()
    download_package()
    install_package()
    cleanup()
