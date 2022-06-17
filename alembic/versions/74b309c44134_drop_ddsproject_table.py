"""Drop DDSProject table

Revision ID: 74b309c44134
Revises: 8a4cc1553379
Create Date: 2022-06-16 15:37:24.124003

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '74b309c44134'
down_revision = '8a4cc1553379'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table('dds_projects')
    op.add_column(
            'delivery_orders',
            sa.Column('ngi_project_name', sa.String(), nullable=True))


def downgrade():
    op.drop_column('delivery_orders', 'ngi_project_name')
    op.create_table(
        'dds_projects',
        sa.Column('dds_project_id', sa.String(), nullable=False),
        sa.Column('project_name', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('dds_project_id')
    )
