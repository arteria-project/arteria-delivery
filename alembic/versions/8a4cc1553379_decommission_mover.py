"""decommission mover

Revision ID: 8a4cc1553379
Revises: 85082f64ccd0
Create Date: 2022-06-02 10:35:30.789457

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8a4cc1553379'
down_revision = '85082f64ccd0'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('delivery_orders') as batch_op:
        batch_op.drop_column('mover_delivery_id')
        batch_op.drop_column('md5sum_file')
        batch_op.alter_column('mover_pid', new_column_name='dds_pid')


def downgrade():
    with op.batch_alter_table('delivery_orders') as batch_op:
        batch_op.alter_column('dds_pid', new_column_name='mover_pid')
    op.add_column('delivery_orders', sa.Column('md5sum_file', sa.String(), nullable=True))
    op.add_column('delivery_orders', sa.Column('mover_delivery_id', sa.String(), nullable=True))
