# -*- coding: utf-8 -*-

''' db import '''
from sqlalchemy import Column, Integer, String, Boolean, Sequence, ForeignKey, Text, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Region(Base):
    
    __tablename__ = 'regions'
    
    id = Column(Integer, Sequence('region_id_seq'), primary_key=True)
    name = Column(String(120), unique=True)
    folder = Column(String(120), unique=True)
    
class NoticeType(Base):
    
    __tablename__ = 'noticetypes'
    
    id = Column(Integer, Sequence('noticetype_id_seq'), primary_key=True)
    name = Column(String(120), unique=True)
    shild = Column(String(120), unique=True)
 
        
class File(Base):
    
    __tablename__ = 'files'
    
    id = Column(Integer,Sequence('file_id_seq'), primary_key=True)
    name = Column(String(240))
    path = Column(String(240))
    indexed = Column(Boolean, default=0)
    added = Column(Date)
    region_id = Column(Integer, ForeignKey('regions.id'), nullable=False)
    #n_type_id = Column(Integer, ForeignKey('noticetypes.id'), nullable=False)
    
    _inregion = relationship('Region', backref='hasfiles', lazy='joined')
    #n_type = relationship('NoticeType', back_populates='files')
    
class Notice(Base):
    
    __tablename__ = 'notices'
    
    id = Column(Integer,Sequence('notice_id_seq'), primary_key=True)    
    file_id = Column(Integer, ForeignKey('files.id'), unique=False, nullable=False)
    
    notice_id = Column(String(240), unique=False)
    purchNum = Column(String(240), unique=False)
    docNum = Column(String(240), unique=False)
    href = Column(String(240), unique=False, nullable=False)
    purchobjinfo = Column(Text, unique=False, nullable=False)
    maxprice = Column(String(240), unique=False, nullable=False)
    enddate = Column(String(240), unique=False, nullable=False)
    
    fulltext = Column(Text, unique=False, nullable=False)
    
    #_infile = relationship('File', backref='hasnotices', lazy='joined')
    
class NoticeNew(Base):
    
    __tablename__ = 'purches'
    
    id = Column(Integer,Sequence('notice_id_seq'), primary_key=True)    
    file_id = Column(Integer, ForeignKey('files.id'), unique=False, nullable=False)
    
    notice_id = Column(String(240), unique=False)
    purchNum = Column(String(240), unique=False)
    docNum = Column(String(240), unique=False)
    href = Column(String(240), unique=False, nullable=False)
    purchobjinfo = Column(Text, unique=False, nullable=False)
    maxprice = Column(Integer, unique=False, nullable=False)
    enddate = Column(Date, unique=False, nullable=False)
    fulltext = Column(Text, unique=False, nullable=False)
    
    #_infile = relationship('File', backref='hasnotices', lazy='joined')
    
def create_db(engine):
    Base.metadata.create_all(engine)
    
    
    
    
    
