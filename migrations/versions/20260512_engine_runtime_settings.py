"""Engine runtime_settings persistence (singleton JSON row).

Revision ID: 20260512_engine_runtime_settings
Revises: 20260511_signal_pipeline_observability
Create Date: 2026-05-12
"""

from alembic import op
import sqlalchemy as sa

revision = "20260512_engine_runtime_settings"
down_revision = "20260511_signal_pipeline_observability"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "runtime_engine_settings",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("runtime_engine_settings")
