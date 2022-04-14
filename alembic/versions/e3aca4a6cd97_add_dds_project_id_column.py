"""add dds_project_id column

Revision ID: e3aca4a6cd97
Revises: ea812cd3ab7b
Create Date: 2022-04-26 10:04:45.348954

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e3aca4a6cd97'
down_revision = 'ea812cd3ab7b'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('delivery_orders', sa.Column('dds_project_id', sa.String(), nullable=True))


def downgrade():
    op.drop_column('delivery_orders', 'dds_project_id')
