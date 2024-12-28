"""Change Census locality geometry from Point to Multipolygon

Revision ID: 90077a8f7b48
Revises: 8a25cb2a00ce
Create Date: 2024-12-23 09:50:50.748940

"""
from alembic import op
from geoalchemy2 import Geometry

# revision identifiers, used by Alembic.
revision = '90077a8f7b48'
down_revision = '7bf4811afc85'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        table_name='georef_localidades_censales',
        column_name='geometria',
        type_=Geometry('MULTIPOLYGON', srid=4326),
        existing_type=Geometry('POINT', srid=4326),
    )


def downgrade():
    op.alter_column(
        table_name='georef_localidades_censales',
        column_name='geometria',
        type_=Geometry('POINT', srid=4326),
        existing_type=Geometry('MULTIPOLYGON', srid=4326),
    )
