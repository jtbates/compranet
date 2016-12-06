import json

from alembic.config import Config
from alembic import command
from sqlalchemy import Column, String, Integer, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.types import TypeDecorator, VARCHAR

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

class JSONEncodedObject(TypeDecorator):
    """Represents an immutable structure as a json-encoded string.

    Usage::
        JSONEncodedObject(255)
    """

    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value

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
    IDENTIFICADOR_CM = Column(String)
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
    IDENTIFICADOR_CM = Column(String)
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

class ContratoWeb(Base):
    __tablename__ = 'contratos_web'

    ANUNCIO = Column(String, primary_key=True)
    CARACTER = Column(String)
    CODIGO_EXPEDIENTE = Column(String)
    FECHA_APERTURA_PROPOSICIONES = Column(String)
    FORMA_PROCEDIMIENTO = Column(String)
    NOMBRE_DE_LA_UC = Column(String)
    NUMERO_PROCEDIMIENTO = Column(String)
    PROC_F_PUBLICACION = Column(String)
    TIPO_CONTRATACION = Column(String)
    TITULO_EXPEDIENTE = Column(String)
    _ANEXOS_CP = Column(JSONEncodedObject) # list
    _ANEXOS_OD = Column(JSONEncodedObject) # list
    #_CONDITIONAL_PREFIX_FOR = Column(String) # 155 if _ANEOXS_CP not null
    _CORREO_DEL_OPERADOR = Column(String)
    _CREDITO_EXTERNO = Column(String)
    _DESC_ANUNCIO = Column(String)
    _ENTIDAD_FEDERATIVA = Column(String)
    _EXCLUSIVO_MIPYMES = Column(String)
    _NOMBRE_DEL_OPERADOR = Column(String)
    _NOTAS = Column(String)
    _PROCEDIMIENTOS = Column(JSONEncodedObject) # list
    _UPDATED = Column(DateTime)

class ContratoWebHistorial(Base):
    __tablename__ = 'contratos_web_hist'

    ANUNCIO = Column(String)
    CARACTER = Column(String)
    CODIGO_EXPEDIENTE = Column(String)
    FECHA_APERTURA_PROPOSICIONES = Column(String)
    FORMA_PROCEDIMIENTO = Column(String)
    NOMBRE_DE_LA_UC = Column(String)
    NUMERO_PROCEDIMIENTO = Column(String)
    PROC_F_PUBLICACION = Column(String)
    TIPO_CONTRATACION = Column(String)
    TITULO_EXPEDIENTE = Column(String)
    _ANEXOS_CP = Column(JSONEncodedObject) # list
    _ANEXOS_OD = Column(JSONEncodedObject) # list
    #_CONDITIONAL_PREFIX_FOR = Column(String) # 155 if _ANEOXS_CP not null
    _CORREO_DEL_OPERADOR = Column(String)
    _CREDITO_EXTERNO = Column(String)
    _DESC_ANUNCIO = Column(String)
    _ENTIDAD_FEDERATIVA = Column(String)
    _EXCLUSIVO_MIPYMES = Column(String)
    _NOMBRE_DEL_OPERADOR = Column(String)
    _NOTAS = Column(String)
    _PROCEDIMIENTOS = Column(JSONEncodedObject) # list
    _UPDATED = Column(DateTime)
    _REMOVED = Column(Boolean)
    _ID = Column(Integer, primary_key=True, autoincrement=True)
