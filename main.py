# -*- coding: utf-8 -*-

''' db import '''
from ftplib import FTP
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import *

import sys
import os
from zipfile import ZipFile
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import date

#from progress.bar import Bar

DEPTH = 3
UPDATE_DB = False
#DB_ENGINE = 'mysql://gb_labs:c7295da1uio@mysql48.1gb.ru/gb_labs'
DB_ENGINE = 'sqlite:///sample.db'



TAG_COLLECTION = [
    ['id', 'purchasenumber', 'docnumber', 'href', 'purchaseobjectinfo', 
     'maxprice', 'enddate'],
    ['ns2:registrationnumber', 'ns2:registrationnumber',
      'ns2:registrationnumber', 'ns2:urloos', 'ns2:name', 'initialsum', 'deliveryenddatetime'],
    ]


def progress(count, total, suffix=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()  # As suggested by Rom Ruben

def base_fz():
    fz = [
        {'url': 'ftp.zakupki.gov.ru',
     'name': 'fz223free',
     'pass': 'fz223free',
     'dir': '/out/published/',
     'prefix': 'purchaseNotice',
     },
    {'url': 'ftp.zakupki.gov.ru',
     'name': 'free',
     'pass': 'free',
     'dir': '/94fz/',
     'prefix': 'notification',
    },
    {'url': 'ftp.zakupki.gov.ru',
     'name': 'free',
     'pass': 'free',
     'dir': '/fcs_regions/',
     'prefix': 'notifications',
    },   
    ]
    return fz

def base_types():
    noticeTypes = {
                'AE': u'Электронный аукцион', 
                'AE94': u'Электронный аукцион ФЗ94',
                'EP': u'',
                'IS': u'',
                'OA': u'Открытый аукцион',
                'OK': u'Открытый конкурс',
                'ZK': u'Запрос котировок',
               }
    return noticeTypes

def base_regions():
    regions = [
            {
                'id': 1,
                'title': 'Moskva',
                'name': u'Москва',
        },
         {
                'id': 2,
                'title': 'Moskovskaja_obl',
                'name': u'Московская область',
        },
         {
                'id': 3,
                'title': 'Leningradskaja_obl',
                'name': u'Ленинградская область',
        },
         {
                'id': 3,
                'title': 'Leningradskaya_obl',
                'name': u'Ленинградская область',
        },
         {
                'id': 4,
                'title': 'Sankt-Peterburg',
                'name' : u'СПБ',
        },     
        
        
       
    ]
    return regions
'''
def base_regions():
    regions = [
            {
                'id': 1,
                'title': 'Moskva',
                'name': u'Москва',
        },
         {
                'id': 2,
                'title': 'Moskovskaja_obl',
                'name': u'Московская область',
        },
         {
                'id': 3,
                'title': 'Leningradskaja_obl',
                'name': u'Ленинградская область',
        },
         {
                'id': 4,
                'title': 'Sankt-Peterburg',
                'name' : u'СПБ',
        },     
        
        
       
    ]
    return regions
 '''   

def add_to_db(db_session, log_file, region, path, update=True, *args):
    bardb = tqdm(total=len(args))
    for file in args:
        try:
            db_file = db_session.query(File).filter_by(path=path, name=file).one_or_none()
        except:
            log_file.write(' DB ERR REGION - %s, FILE - %s\n' % (region, file))
        else:
            if db_file != None:
                if update:
                    try:
                        db_file.name=file
                        db_file.path=path 
                        db_file.indexed=0
                        db_file.region_id=region
                        db_session.merge(db_file)
                        db_session.commit()
                    except:
                        log_file.write('UPDATE ERR REGION - %s, FILE - %s\n' % (region, file))
                    else:
                        print ('FILE %s UPDATED' % file)
            else:
                try:
                    new_folder = File(name=file, path=path, 
                                      indexed=0, region_id=region, added=date.today())
                    db_session.add(new_folder)
                    db_session.commit()
                except:
                    log_file.write('UPDATE ERR REGION - %s, FILE - %s\n' % (region, file))
                else:
                    print ('FILE %s ADDED' % file)
        bardb.update(1)

def connect_to_ftp(region, **kwargs):
    ftp = FTP(kwargs['url'])
    ftp.login(user=kwargs['name'], passwd=kwargs['pass'])
    print ftp
    folder = kwargs['dir'] + region
    ftp.cwd(folder)
    return ftp

def scan_ftp(ftp_session, db_session, log_file, region, rang):
    index = ftp_session.nlst()
    files = [file for file in index if (file.endswith('zip')) and (file not in rang)]
    p =  ftp_session.pwd()
    add_to_db(db_session, log_file,\
               region, p, UPDATE_DB, *files)
    dirs = [dir for dir in index if dir.find('.') == (-1)]
    return dirs

def connect_to_db(dburl):
    engine = create_engine(dburl, encoding='utf-8', convert_unicode=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def db_from_zero():
    engine = create_engine(DB_ENGINE, encoding='utf-8', convert_unicode=True)
    create_db(engine=engine)
    session = connect_to_db(DB_ENGINE)
    regions=base_regions()
    for region in regions:
        new_region = Region(name=region['name'], folder=region['title'])
        session.add(new_region)
        session.commit()
    
    

def add_files():
    log = open('log.txt', 'a')
    fz = base_fz()
    regions = base_regions()
    session = connect_to_db(DB_ENGINE)
    files = session.query(File).all()
    f_name = [file.name for file in files]
    for f in fz:
        for reg in regions:
            i = reg['id']
            region = reg['title']
            folders = []
            try:
                ftp = connect_to_ftp(region=region, **f)
                folders.append(ftp.pwd())
            except:
                log.write('Connection error: FZ - %s, REGION - %s\n' % (f, region))
            else:
                if ftp != None:
                    data = ftp.nlst()
                    barftp = tqdm(total=len(data))
                    for d in data:
                        sys.stdout.write ('Folder: %s \n' % d)
                        sys.stdout.flush() 
                        if d.startswith(f['prefix']):
                            sys.stdout.write ('Folder scan: %s \n' % d)
                            sys.stdout.flush() 
                            folders.append('/' + d)
                            try:    
                                ftp.cwd(folders[0] + folders[1])
                            except:
                                log.write('Folder error: Folder - %s, REGION - %s\n' % (folders[1], region))
                            else:
                                dirs = scan_ftp(ftp_session=ftp, db_session=session, 
                                                log_file=log, region=i, rang=f_name)
                                c_loop = 0
                                for dir in dirs:
                                    try:
                                        folder = ftp.pwd() + '/' + dir 
                                        ftp.cwd(folder)
                                    except:
                                        log.write('Folder error: Folder - %s, REGION - %s\n' % (folder, region))
                                    else: 
                                        scan_ftp(ftp_session=ftp, db_session=session, 
                                                 log_file=log, region=i, rang=f_name)
                                        #del dirs[0]
                                    c_loop += 1    
                                print ('folder %s - passed!' % d)
                                barftp.update(1)
    log.close()
    add_notices()
    
def add_notices():
    regions = base_fz()
    session = connect_to_db(DB_ENGINE)
    files = session.query(File).filter_by(indexed=0).all()
    #data = session.query(Notice).all()
    data = session.query().values(Notice.docNum)
    notices = [d[0] for d in data]
    #print data
    total = len(files)
    fz223 = [file for file in files if file.path.find(regions[0]['dir']) != (-1)]
    fz44 = [file for file in files if file.path.find(regions[1]['dir']) != (-1)]
    fz94 = [file for file in files if file.path.find(regions[2]['dir']) != (-1)]
    sys.stdout.write( 'FZ223: %i files;\nFZ44: %i files;\nFZ94: %i files;\n' % (len(fz223), len(fz44), len(fz94)))
    sys.stdout.flush() 
    fz_files = [fz223, fz94, fz44]
    filenum = 0
    folder = ''
    bar = tqdm(total=total)
    for num, fz in enumerate(fz_files):
        #progress(filenum, total=total, suffix=regions[num]['dir'])
        counter = len(fz)
        #num +=1
        #bar_small = Bar('Current progress', max=counter)
        for i, file in enumerate(fz):
            if (file.name.find('2017') != (-1) or
                    file.name.find('2018') != (-1)):
                ftp = FTP(regions[num]['url'])
                ftp.login(user=regions[num]['name'], passwd=regions[num]['pass'])
                sys.stdout.write ('Connected to %s \n' % regions[num]['name'])
                sys.stdout.flush() 
                if file.path != folder:
                    ftp.cwd(file.path)
                try:
                    with open(file.name, 'wb') as f:
                        ftp.retrbinary('RETR ' + file.name, f.write)
                        f.close()
                except:
                    with open('log.txt', 'a') as log_file:
                        log_file.write('Error downloading %s \n' % file.name)
                        #log_file.close()
                    sys.stdout.write('fail!\n')
                    sys.stdout.flush() 
                else:
                    #sys.stdout.write('LOADED %s \n' % file.name)
                    #sys.stdout.flush() 
                    success = True
                    with ZipFile(file.name, 'r') as myzip:
                        xmls = myzip.namelist()
                        bar2 = tqdm(total=len(xmls))
                        for xml in xmls:
                            #xmled = False
                            #sys.stdout.write(xml)
                            toparse = myzip.read(xml)
                            soup = BeautifulSoup(toparse, 'lxml')
                            
                            if soup.find(TAG_COLLECTION[0][0]):
                                #sys.stdout.write('TAG COLLECTION 0 ACTIVATED\n')
                                #sys.stdout.flush()
                                                                
                                ind = 0
                                
                            elif soup.find(TAG_COLLECTION[1][0]):
                                #sys.stdout.write('TAG COLLECTION 1 ACTIVATED\n')
                                #sys.stdout.flush()
                                
                                ind = 1
                        
                            else:
                                success = False
                                ind = 2
                            if ind != 2:
                                vars = []
                                for tag in TAG_COLLECTION[ind]:
                                    if hasattr(soup.find(tag), 'get_text'):
                                        vars.append(soup.find(tag).get_text())
                                    else:
                                        #sys.stdout.write('NO ATTR\n')
                                        #sys.stdout.flush()
                                        vars.append("NO")
                                #fulltext = toparse.decode('utf-8')
                                if vars[2] not in notices:
                                        new_notice = Notice(file_id = file.id, 
                                                                            notice_id = vars[0],
                                                                            purchNum = vars[1],
                                                                            docNum = vars[2],
                                                                            href = vars[3],
                                                                            purchobjinfo = vars[4],
                                                                            maxprice = vars[5],
                                                                            enddate = vars[6],
                                                                            fulltext = u" "
                                                                            )
                                        try:
                                            session.add(new_notice)
                                            session.commit()
                                            #sys.stdout.write('data in file %s parsed!\n' % xml)
                                        except:
                                            with open('log.txt', 'a') as log_file:
                                                log_file.write('Error parsing %s \n' % xml)
                                                success = False
                                                #log_file.close()
                                        else:
                                            pass
                                            #sys.stdout.write('SUCCESS %s !\n' % xml)
                                            #sys.stdout.flush()
                            bar2.update(1)
                        bar2.close()         
                    if success == False:
                        file.indexed = 0
                        session.merge(file)
                        session.commit()
                        sys.stdout.write('PARSED WITHOUT SUCCESS %s !\n' % file.name)
                        sys.stdout.flush() 
                    else:
                        file.indexed = 1
                        session.merge(file)
                        session.commit()
                        #sys.stdout.write('SUCCESS %s !\n' % file.name)
                        #sys.stdout.flush() 
                    os.remove(file.name)
                    ftp.close()
                                  
            else:
                #print file.name
                try:
                    file.indexed = 1
                    session.merge(file)
                    session.commit()
                except:
                    with open('log.txt', 'a') as log_file:
                        log_file.write('Error deindexing %s' % file.name)
                        #log_file.close()
                else:
                    pass
                    #bar_small.next()
            #ftp.close()
            #bar_small.finish()   
            bar.update(1)
    bar.close()
        