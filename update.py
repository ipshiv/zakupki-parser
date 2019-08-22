# -*- coding: utf-8 -*-

from main import connect_to_db, tqdm, FTP, sys, ZipFile, BeautifulSoup, TAG_COLLECTION, os, create_engine, sessionmaker,\
    base_fz
from models import File, NoticeNew
from datetime import date, datetime
from paths import folders_to_parse
from sqlalchemy.engine import ResultProxy

def update_db():
    year0 = str(date.today().year)
    year1 = str(date.today().year - 1)
    folders = folders_to_parse
    #session = connect_to_db('mysql://pythonist:Awsedr56@192.168.1.123/goszakup')
    engine = create_engine('mysql://pythonist:Awsedr56@192.168.1.123/goszakup', encoding='utf-8', convert_unicode=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    data = session.query().values(File.name)
    data =[d[0] for d in data]
    bar_big = tqdm(total=len(folders))
    for folder in folders:
        if folder['dir'].find(u'Moskva') != (-1):
            region_id = 1
        elif folder['dir'].find(u'Moskovskaja_obl'):
            region_id = 2
        elif (folder['dir'].find(u'Leningradskaja_obl') != (-1) or folder['dir'].find(u'Leningradskaya_obl') != (-1)):
            region_id = 3
        elif folder['dir'].find(u'Sankt-Peterburg') != (-1):
            region_id = 4
        else:
            region_id = 1
        ftp = FTP(folder['url'])
        ftp.login(user=folder['name'], passwd=folder['pass'])
        #print ftp
        try:
            ftp.cwd(folder['dir'])
        except:
            bar_big.update(1)
            pass
        else:
            index = ftp.nlst()
            fles = [file for file in index if (file.endswith('zip')) and (file not in data)]
            bar_files = tqdm(total=len(fles))
            for fle in fles:
                new_file = File(name=fle, path=folder['dir'], 
                                          indexed=1, region_id=region_id, added=date.today())
                session.add(new_file)
                session.commit()
                session.refresh(new_file)
                #print (new_file.id)
                if  (fle.find(year0) != (-1) or
                        fle.find(year1) != (-1)):
                    try:
                        with open(fle, 'wb') as f:
                            ftp.retrbinary('RETR ' + fle, f.write)
                            f.close()
                    except:
                        with open('log.txt', 'a') as log_file:
                            log_file.write('%s ===== Error downloading %s \n' % str(date.today()), fle )
                            log_file.close()
                        sys.stdout.write('fail!\n')
                        sys.stdout.flush() 
                    else:
                        success = True
                        with ZipFile(fle, 'r') as myzip:
                            xmls = myzip.namelist()
                            bar2 = tqdm(total=len(xmls))
                            for xml in xmls:
                                toparse = myzip.read(xml)
                                soup = BeautifulSoup(toparse, 'lxml')
                                
                                if soup.find(TAG_COLLECTION[0][0]):
                                                                    
                                    ind = 0
                                    
                                elif soup.find(TAG_COLLECTION[1][0]):
                                    ind = 1
                            
                                else:
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
                                    try:
                                        vars[5] = int(float(vars[5]))
                                    except:
                                        vars[5] = 0
                                    else:
                                        pass
        
                                    try:
                                        vars[6] = datetime.strptime(vars[6].split(u'T')[0], '%Y-%m-%d')
                                    except:
                                        vars[6] = 0
                                    else:
                                        pass
                                    
                                    new_notice = NoticeNew(file_id = new_file.id, 
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
                            sys.stdout.write('PARSED WITHOUT SUCCESS %s !\n' % fle)
                            sys.stdout.flush() 
                    os.remove(fle)
                    ftp.close()
                    session.commit()
                    #bar_model.update(1)
                bar_files.update(1)
            session.commit()
            bar_files.close()
            bar_big.update(1)
    bar_big.close()

def reparse():
    year0 = str(date.today().year)
    year1 = str(date.today().year - 1)
    START = input("Enter start number...\n")
    xml_counter = 0
    notices = 0
    regions = base_fz()
    reg_num = {'/out':0, '/94f':1, '/fcs':2}
    engine = create_engine('mysql://pythonist:Awsedr56@192.168.1.123/goszakup', encoding='utf-8', convert_unicode=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    raw_ids = session.query().values(NoticeNew.file_id)
    raw_ids = [id[0] for id in raw_ids]
    file_ids = list(set(raw_ids))
    data = session.query(File).all()
    filtered_data = [d for d in data if 
                     ((d.name.find(year0) != (-1) or d.name.find(year1) != (-1)) and 
                     (d.id not in file_ids))]
    big_bar = tqdm(total=len(data))
    big_bar.update(START)
    for row in filtered_data[START:]:
        sys.stdout.write ('Parsing %s \n' % row.name)
        sys.stdout.flush() 
        num = reg_num[row.path[0:4]]
        ftp = FTP(regions[num]['url'])
        ftp.login(user=regions[num]['name'], passwd=regions[num]['pass'])
        ftp.cwd(row.path)
        sys.stdout.write ('Resdy to load %s !!!\n' % row.name)
        sys.stdout.flush()
        try:
            with open(row.name, 'wb') as f:
                ftp.retrbinary('RETR ' + row.name, f.write)
                f.close()
        except:
            sys.stdout.write ('Got error loading file %s/%s !!!\nPress C_ontinue or E_xit\n' % row.path, row.name)
            sys.stdout.flush()
            key = raw_input("Waiting for key input...\n")
            """
            NEED A KEY HOLDER
            """
        else:
            success = True
            with ZipFile(row.name, 'r') as myzip:
                xmls = myzip.namelist()
                bar2 = tqdm(total=len(xmls))
                xml_counter += len(xmls)
                for xml in xmls:
                    toparse = myzip.read(xml)
                    soup = BeautifulSoup(toparse, 'lxml')
                    
                    if soup.find(TAG_COLLECTION[0][0]):
                                                        
                        ind = 0
                        
                    elif soup.find(TAG_COLLECTION[1][0]):
                        ind = 1
                
                    else:
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
                        try:
                            vars[5] = int(float(vars[5]))
                        except:
                            vars[5] = 0
                        else:
                            pass

                        try:
                            vars[6] = datetime.strptime(vars[6].split(u'T')[0], '%Y-%m-%d')
                        except:
                            vars[6] = 0
                        else:
                            pass
                        
                        new_notice = NoticeNew(file_id = row.id, 
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
                        except:
                            with open('log.txt', 'a') as log_file:
                                log_file.write('Error parsing %s \n' % xml)
                                success = False
                                #log_file.close()
                        else:
                            #sys.stdout.write ('Added Notice\n')
                            #sys.stdout.flush()
                            notices+=1
                                #sys.stdout.write('SUCCESS %s !\n' % xml)
                                #sys.stdout.flush()
                    bar2.update(1)
                bar2.close()         
            if success == False:
                sys.stdout.write('PARSED WITHOUT SUCCESS %s !\n' % row.name)
                sys.stdout.flush() 
        os.remove(row.name)
        ftp.close()
        session.commit()
        #bar_model.update(1)
        big_bar.update(1)
    session.commit()
    big_bar.close()
    sys.stdout.write ('Total files parsed:   %i\n \
                       Total notices parsed: %i\n \
                       Total notices added:  %i', len(data), xml_counter, notices)
    sys.stdout.flush()
    
    
    