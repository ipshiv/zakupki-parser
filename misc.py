# -*- coding: utf-8 -*-

from main import *
from models import *
from datetime import datetime
def sql_work():
    f = open('toload.sql', 'r')
    text = f.read().decode('utf-8')
    data = text.split('INSERT INTO `notices` VALUES ')
    session = connect_to_db(DB_ENGINE)
    for d in data[1:]:
        vars = d.replace("'","").split(',')
        new_notice = Notice(file_id = vars[1], 
                                        notice_id = vars[2],
                                        purchNum = vars[3],
                                        docNum = vars[4],
                                        href = vars[5],
                                        purchobjinfo = vars[6],
                                        maxprice = vars[7],
                                        enddate = vars[8],
                                        fulltext = u" "
                                        )
        session.add(new_notice)
    session.commit()

def copy_db():
    session0 = connect_to_db(DB_ENGINE)
    session1 = connect_to_db('mysql://pythonist:Awsedr56@192.168.1.123/goszakup')
    bar_common = tqdm(total=4)
    for model in [File, Notice]:
        if model == Notice:
            data = session0.query(model).all()
            bar_model = tqdm(total = len(data))
            for i, row in enumerate(data):
                if row.purchobjinfo != "":
                    try:
                        maxprice = int(float(row.maxprice))
                    except:
                        maxprice = 0
                        #sys.stdout.write('MAXPRICE WAS %s !\n' % row.maxprice)
                        #sys.stdout.flush()
                    else:
                        pass 
                    
                    try:
                        new_date = datetime.strptime(row.enddate.split(u'T')[0], '%Y-%m-%d')
                    except:
                        new_date = 0
                        #sys.stdout.write('DATE WAS %s !\n' % row.enddate)
                        #sys.stdout.flush() 
                    else:
                        pass
                    
                    new_data = NoticeNew(file_id = row.file_id,
                                      notice_id = row.notice_id,
                                      purchNum = row.purchNum,
                                      docNum = row.docNum,
                                      href = row.href,
                                      purchobjinfo = row.purchobjinfo,
                                      maxprice = maxprice,
                                      enddate = new_date,
                                      fulltext = ''
                                      )
                    session1.add(new_data)
                    bar_model.update(1)
                else:
                    bar_model.update(1)
                if (i % 5000) == 0:
                    session1.commit()
            session1.commit()
            bar_model.close()   
                     
        elif model == File:
            regions = base_fz()
            reg_num = {'/out':0, '/94f':1, '/fcs':2}
            data = session0.query(model).all()
            data1 = session1.query().values(File.name)
            data1 =[d[0] for d in data1]
            data = [d for d in data if d.name not in data1]
            bar_model = tqdm(total=len(data))
            for row in data:
                folder = ''
                row.indexed = 1
                session1.merge(row)
                if ((row.name.find('2017') != (-1) or
                    row.name.find('2018') != (-1))) and (row.indexed == 0):
                    #region = [reg for reg in regions if reg['id'] == row.region_id]
                    num = reg_num[row.path[0:4]]
                    ftp = FTP(regions[num]['url'])
                    ftp.login(user=regions[num]['name'], passwd=regions[num]['pass'])
                    sys.stdout.write ('Connected to %s \n' % regions[num]['name'])
                    sys.stdout.flush() 
                    if row.path != folder:
                        ftp.cwd(row.path)
                    try:
                        with open(row.name, 'wb') as f:
                            ftp.retrbinary('RETR ' + row.name, f.write)
                            f.close()
                    except:
                        with open('log.txt', 'a') as log_file:
                            log_file.write('Error downloading %s \n' % row.name)
                            #log_file.close()
                        sys.stdout.write('fail!\n')
                        sys.stdout.flush() 
                    else:
                        success = True
                        with ZipFile(row.name, 'r') as myzip:
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
                                    #success = False
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
                                        session1.add(new_notice)
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
                            sys.stdout.write('PARSED WITHOUT SUCCESS %s !\n' % row.name)
                            sys.stdout.flush() 
                        os.remove(row.name)
                        ftp.close()
                        session1.commit()
                    #bar_model.update(1)
                bar_model.update(1)
            session1.commit()
            bar_model.close()
        else:
            data = session0.query(model).all()
            bar_model = tqdm(total = len(data))
            for row in data:
                session1.merge(row)
                bar_model.update(1)
            session1.commit()
            bar_model.close()
        bar_common.update(1)
    bar_common.close()