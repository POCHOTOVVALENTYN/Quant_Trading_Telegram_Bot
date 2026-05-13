"""Signal pipeline: failure_reason on signals; f_weekly_filter, f_cvd on decision logs.

Revision ID: 20260511_signal_pipeline_observability
Revises: 20260420_execution_audit_trail
Create Date: 2026-05-11
"""

from alembic import op
import sqlalchemy as sa


revision = "20260511_signal_pipeline_observability"
down_revision = "20260420_execution_audit_trail"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("signals", sa.Column("failure_reason", sa.String(length=512), nullable=True))
    op.add_column(
        "signal_decision_logs",
        sa.Column("f_weekly_filter", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "signal_decision_logs",
        sa.Column("f_cvd", sa.Boolean(), nullable=True),
    )


def downgrade():
    op.drop_column("signal_decision_logs", "f_cvd")
    op.drop_column("signal_decision_logs", "f_weekly_filter")
    op.drop_column("signals", "failure_reason")
