from alembic.config import Config
from alembic import command
from sqlalchemy import Column, String, Integer, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from . import settings


engine = create_engine(settings.DB_URI)

session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

Base = declarative_base()

def create_all(engine=engine):
    # create the tables if they don't yet exist
    Base.metadata.create_all(engine)
    # then load the Alembic configuration and generate the
    # version table, "stamping" it with the most recent rev
    alembic_cfg = Config(settings.ALEMBIC_INI_PATH)
    alembic_cfg.set_main_option('script_location',
                                settings.ALEMBIC_SCRIPT_LOCATION)
    command.stamp(alembic_cfg, "head")

def get_session():
    return(Session())

class ContratoXls(Base):
    __tablename__ = 'contratos_xls'

    GOBIERNO = Column(String)
    SIGLAS = Column(String)
    DEPENDENCIA = Column(String)
    CLAVEUC = Column(String)
    NOMBRE_DE_LA_UC = Column(String)
    RESPONSABLE = Column(String)
    CODIGO_EXPEDIENTE = Column(Integer)
    TITULO_EXPEDIENTE = Column(String)
    PLANTILLA_EXPEDIENTE = Column(String)
    NUMERO_PROCEDIMIENTO = Column(String)
    EXP_F_FALLO = Column(String) # DateTime, actually date in format %Y-%m-%d 00:00:00 GMT
    PROC_F_PUBLICACION = Column(String) # DateTime, in format %Y-%m-%d %H:%M
    FECHA_APERTURA_PROPOSICIONES = Column(String) # DateTime, in format %Y-%m-%d %H:%M
    CARACTER = Column(String)
    TIPO_CONTRATACION = Column(String)
    TIPO_PROCEDIMIENTO = Column(String)
    FORMA_PROCEDIMIENTO = Column(String)
    CODIGO_CONTRATO = Column(Integer, primary_key=True)
    TITULO_CONTRATO = Column(String)
    FECHA_INICIO = Column(String) # Date, in format %Y-%m-%d
    FECHA_FIN = Column(String) # Date, in format %Y-%m-%d
    IMPORTE_CONTRATO = Column(Float)
    MONEDA = Column(String)
    ESTATUS_CONTRATO = Column(String)
    ARCHIVADO = Column(String)
    CONVENIO_MODIFICATORIO = Column(Boolean)
    RAMO = Column(String)
    CLAVE_PROGRAMA = Column(String)
    APORTACION_FEDERAL = Column(Float)
    FECHA_CELEBRACION = Column(String) # DateTime, actually date in format %Y-%m-%d 00:00:00 GMT
    CONTRATO_MARCO = Column(Boolean)
    COMPRA_CONSOLIDADA = Column(Boolean)
    PLURIANUAL = Column(Boolean)
    CLAVE_CARTERA_SHCP = Column(String)
    ESTRATIFICACION_MUC = Column(String)
    FOLIO_RUPC = Column(Integer)
    PROVEEDOR_CONTRATISTA = Column(String)
    ESTRATIFICACION_MPC = Column(String)
    SIGLAS_PAIS = Column(String)
    ESTATUS_EMPRESA = Column(String)
    CUENTA_ADMINISTRADA_POR = Column(String)
    C_EXTERNO = Column(Boolean)
    ORGANISMO = Column(String)
    ANUNCIO = Column(String)
    _SOURCE = Column(String, primary_key=True)
    _UPDATED = Column(DateTime)

class ContratoXlsHistorial(Base):
    __tablename__ = 'contratos_xls_hist'

    GOBIERNO = Column(String)
    SIGLAS = Column(String)
    DEPENDENCIA = Column(String)
    CLAVEUC = Column(String)
    NOMBRE_DE_LA_UC = Column(String)
    RESPONSABLE = Column(String)
    CODIGO_EXPEDIENTE = Column(Integer)
    TITULO_EXPEDIENTE = Column(String)
    PLANTILLA_EXPEDIENTE = Column(String)
    NUMERO_PROCEDIMIENTO = Column(String)
    EXP_F_FALLO = Column(String) # DateTime, actually date in format %Y-%m-%d 00:00:00 GMT
    PROC_F_PUBLICACION = Column(String) # DateTime, in format %Y-%m-%d %H:%M
    FECHA_APERTURA_PROPOSICIONES = Column(String) # DateTime, in format %Y-%m-%d %H:%M
    CARACTER = Column(String)
    TIPO_CONTRATACION = Column(String)
    TIPO_PROCEDIMIENTO = Column(String)
    FORMA_PROCEDIMIENTO = Column(String)
    CODIGO_CONTRATO = Column(Integer)
    TITULO_CONTRATO = Column(String)
    FECHA_INICIO = Column(String) # Date, in format %Y-%m-%d
    FECHA_FIN = Column(String) # Date, in format %Y-%m-%d
    IMPORTE_CONTRATO = Column(Float)
    MONEDA = Column(String)
    ESTATUS_CONTRATO = Column(String)
    ARCHIVADO = Column(String)
    CONVENIO_MODIFICATORIO = Column(Boolean)
    RAMO = Column(String)
    CLAVE_PROGRAMA = Column(String)
    APORTACION_FEDERAL = Column(Float)
    FECHA_CELEBRACION = Column(String) # DateTime, actually date in format %Y-%m-%d 00:00:00 GMT
    CONTRATO_MARCO = Column(Boolean)
    COMPRA_CONSOLIDADA = Column(Boolean)
    PLURIANUAL = Column(Boolean)
    CLAVE_CARTERA_SHCP = Column(String)
    ESTRATIFICACION_MUC = Column(String)
    FOLIO_RUPC = Column(Integer)
    PROVEEDOR_CONTRATISTA = Column(String)
    ESTRATIFICACION_MPC = Column(String)
    SIGLAS_PAIS = Column(String)
    ESTATUS_EMPRESA = Column(String)
    CUENTA_ADMINISTRADA_POR = Column(String)
    C_EXTERNO = Column(Boolean)
    ORGANISMO = Column(String)
    ANUNCIO = Column(String)
    _SOURCE = Column(String)
    _UPDATED = Column(DateTime)
    _REMOVED = Column(Boolean)
    _ID = Column(Integer, primary_key=True, autoincrement=True)

class SourceXls(Base):
    __tablename__ = 'sources_xls'

    _SOURCE = Column(String, primary_key=True)
    _UPDATED = Column(DateTime, primary_key=True)
    SHA256 = Column(String)
