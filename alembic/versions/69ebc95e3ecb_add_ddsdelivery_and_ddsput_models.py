"""Add DDSDelivery and DDSPut Models

Revision ID: 69ebc95e3ecb
Revises: 74b309c44134
Create Date: 2023-03-10 16:10:37.844346

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '69ebc95e3ecb'
down_revision = '74b309c44134'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'dds_deliveries',
        sa.Column('dds_project_id', sa.String, primary_key=True),
        sa.Column('ngi_project_name', sa.String),
        sa.Column('date_started', sa.DateTime, nullable=False),
        sa.Column('date_completed', sa.DateTime),
        sa.Column('status', sa.Enum(
            'pending',
            'delivery_in_progress',
            'delivery_successful',
            'delivery_failed',
            'delivery_skipped',
            name='deliverystatus'
        )),
    )

    op.create_table(
        'dds_puts',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            'dds_project_id',
            sa.String,
            sa.ForeignKey("dds_deliveries.dds_project_id"),
            nullable=False),
        sa.Column('dds_pid', sa.Integer, nullable=False),
        sa.Column('delivery_source', sa.String, nullable=False),
        sa.Column('delivery_path', sa.String, nullable=False),
        sa.Column('destination', sa.String),
        sa.Column('date_started', sa.DateTime, nullable=False),
        sa.Column('date_completed', sa.DateTime),
        sa.Column('status', sa.Enum(
            'pending',
            'delivery_in_progress',
            'delivery_successful',
            'delivery_failed',
            'delivery_skipped',
            name='deliverystatus'
        )),
    )

def downgrade():
    op.drop_table('dds_puts')
    op.drop_table('dds_deliveries')
