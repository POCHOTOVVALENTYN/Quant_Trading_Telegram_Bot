"""Add execution audit trail table.

Revision ID: 20260420_execution_audit_trail
Revises: 20260318_ideal_accounting_links
Create Date: 2026-04-20
"""

from alembic import op
import sqlalchemy as sa


revision = "20260420_execution_audit_trail"
down_revision = "20260318_ideal_accounting_links"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "execution_audit_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("signal_id", sa.Integer(), nullable=True),
        sa.Column("position_id", sa.Integer(), nullable=True),
        sa.Column("order_id", sa.Integer(), nullable=True),
        sa.Column("symbol", sa.String(), nullable=True),
        sa.Column("strategy", sa.String(), nullable=True),
        sa.Column("event_type", sa.String(), nullable=False),
        sa.Column("severity", sa.String(), nullable=False, server_default="INFO"),
        sa.Column("message", sa.String(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["signal_id"], ["signals.id"]),
        sa.ForeignKeyConstraint(["position_id"], ["positions.id"]),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"]),
    )
    op.create_index("ix_execution_audit_logs_signal_id", "execution_audit_logs", ["signal_id"])
    op.create_index("ix_execution_audit_logs_position_id", "execution_audit_logs", ["position_id"])
    op.create_index("ix_execution_audit_logs_order_id", "execution_audit_logs", ["order_id"])
    op.create_index("ix_execution_audit_logs_event_type", "execution_audit_logs", ["event_type"])
    op.create_index("ix_execution_audit_logs_created_at", "execution_audit_logs", ["created_at"])
    op.create_index("ix_execution_audit_symbol_ts", "execution_audit_logs", ["symbol", "created_at"])
    op.create_index("ix_execution_audit_event_ts", "execution_audit_logs", ["event_type", "created_at"])


def downgrade():
    op.drop_index("ix_execution_audit_event_ts", table_name="execution_audit_logs")
    op.drop_index("ix_execution_audit_symbol_ts", table_name="execution_audit_logs")
    op.drop_index("ix_execution_audit_logs_created_at", table_name="execution_audit_logs")
    op.drop_index("ix_execution_audit_logs_event_type", table_name="execution_audit_logs")
    op.drop_index("ix_execution_audit_logs_order_id", table_name="execution_audit_logs")
    op.drop_index("ix_execution_audit_logs_position_id", table_name="execution_audit_logs")
    op.drop_index("ix_execution_audit_logs_signal_id", table_name="execution_audit_logs")
    op.drop_table("execution_audit_logs")
