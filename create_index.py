# -*- coding: utf-8 -*-
#####################
##IMPORT WORK STUFF##
#####################

#--Common Libraries
import os, os.path
import sys
import string
#--Whoosh index
from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED, NGRAMWORDS, NUMERIC, DATETIME
from whoosh.analysis import SimpleAnalyzer, StandardAnalyzer, StemmingAnalyzer, LanguageAnalyzer, NgramAnalyzer, SpaceSeparatedTokenizer, NgramFilter
from whoosh import index
#--Whoosh search
from whoosh.searching import Results, Searcher
from whoosh.query import Term, Prefix
from whoosh.qparser import QueryParser, PrefixPlugin, MultifieldPlugin
from whoosh.lang.snowball.russian import *
#--db Connection
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import *
from main import connect_to_db, DB_ENGINE
#--MISC
from datetime import datetime, date
from tqdm import tqdm

################
##Create Index##
################

#--Создаем схему
schema = Schema(name_to_user=TEXT(stored=True), name=TEXT(analyzer=LanguageAnalyzer('ru'), spelling=True), price=NUMERIC, enddate=DATETIME, region=NUMERIC, index_date=DATETIME, href=STORED)

if not os.path.exists("indexdir"):
    os.mkdir("indexdir")
    #умный_машин создает индекс
    ix = index.create_in("indexdir", schema)
    write = ix.writer()
    #умный_машин парсит документы дл¤ индекса
    session = connect_to_db('mysql://pythonist:Awsedr56@192.168.1.123/goszakup')
    data = session.query(NoticeNew).all()
    #dest = ['mat_deshevostroi.txt', 'mat_gipostroy.txt', 'mat_intellect.txt', 'mat_isolux.txt', 'mat_mc.txt', 'mat_petrovich.txt', 'mat_rian.txt', 'mat_vsesmesi.txt']
    #print (data)
    bar = tqdm(total=len(data))
    for d in data:
        if d.maxprice == None:
            maxprice = 0
        else:
            maxprice = d.maxprice
        if d.enddate == None:
            enddate = date(2015, 1, 1)
        else:
            enddate = d.enddate 
        try:
            source_file = session.query(File).filter_by(id=d.file_id).first()
            write.add_document(name_to_user = d.purchobjinfo,
                               name=d.purchobjinfo, price=maxprice,
                               enddate=datetime.combine(enddate, datetime.min.time()), 
                               region=source_file.region_id, 
                               index_date=datetime.combine(source_file.added, datetime.min.time()), 
                               href=d.href)
        except IndexError:
            pass
        bar.update(1)
    write.commit()
    bar.close()
    print'Exit without errors'