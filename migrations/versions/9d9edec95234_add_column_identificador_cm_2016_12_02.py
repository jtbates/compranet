"""Add column IDENTIFICADOR_CM 2016-12-02

Revision ID: 9d9edec95234
Revises: 660818444ba0
Create Date: 2016-12-03 17:29:00.722059

"""
import logging
import os

from alembic import op
import sqlalchemy as sa

from compranet import database, settings, xlsx
from compranet.database import ContratoXls

# revision identifiers, used by Alembic.
revision = '9d9edec95234'
down_revision = '660818444ba0'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('contratos_xls', sa.Column('IDENTIFICADOR_CM', sa.String(), nullable=True))
    op.add_column('contratos_xls_hist', sa.Column('IDENTIFICADOR_CM', sa.String(), nullable=True))

    files_161202 = {'2013': 'Contratos2013_161202210240.xlsx',
                    '2014': 'Contratos2014_161202205922.xlsx',
                    '2015': 'Contratos2015_161202205520.xlsx',
                    '2016': 'Contratos2016_161202203527.xlsx'}

    logger = logging.getLogger('alembic')
    session = database.get_session()
    source_col = '_SOURCE'
    cc_col = 'CODIGO_CONTRATO'
    new_col = 'IDENTIFICADOR_CM'
    for source, fn in files_161202.items():
        pathname = os.path.join(settings.RAW_DIR, fn)
        if os.path.exists(pathname):
            logger.info("Loading {}...".format(pathname))
            df = xlsx.xlsx2df(pathname)
            logger.info("Updating column {}...".format(new_col))
            # query all current CODIGO_CONTRATO values for this source
            cc_q = (session.query(ContratoXls.CODIGO_CONTRATO)
                               .filter(ContratoXls._SOURCE==source))
            cc_l = [row[0] for row in cc_q.all()]
            # function to get values for new column from source dataframe
            def get_new_val(cc, new_col=new_col, df=df):
                val_series = df[new_col][df[cc_col] == cc]
                assert len(val_series) <= 1
                if len(val_series) == 1:
                    new_val = val_series.iloc[0]
                else:
                    new_val = None
                return(new_val)
                
            mappings = [{source_col: source,
                         cc_col: cc,
                         new_col: get_new_val(cc)} for cc in cc_l]
            session.bulk_update_mappings(ContratoXls, mappings)
            session.commit()


def downgrade():
    op.drop_column('contratos_xls_hist', 'IDENTIFICADOR_CM')
    op.drop_column('contratos_xls', 'IDENTIFICADOR_CM')
