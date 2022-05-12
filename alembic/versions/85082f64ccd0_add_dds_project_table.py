"""add dds_project_table

Revision ID: 85082f64ccd0
Revises: ea812cd3ab7b
Create Date: 2022-05-11 13:15:55.389832

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '85082f64ccd0'
down_revision = 'ea812cd3ab7b'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table('dds_projects',
            sa.Column('dds_project_id', sa.String, nullable=False, primary_key=True),
            sa.Column('project_name', sa.String),
            )


def downgrade():
    op.drop_table('dds_projects')
