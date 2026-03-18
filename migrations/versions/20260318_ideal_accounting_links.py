"""Add position side and strict order linkage.

Revision ID: 20260318_ideal_accounting_links
Revises: 
Create Date: 2026-03-18
"""

from alembic import op
import sqlalchemy as sa


revision = "20260318_ideal_accounting_links"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # positions.side (nullable for legacy rows)
    op.add_column("positions", sa.Column("side", sa.Enum("LONG", "SHORT", name="signaltype"), nullable=True))

    # orders linkage columns
    op.add_column("orders", sa.Column("client_order_id", sa.String(), nullable=True))
    op.add_column("orders", sa.Column("position_id", sa.Integer(), nullable=True))
    op.create_index("ix_orders_client_order_id", "orders", ["client_order_id"])
    op.create_index("ix_orders_position_id", "orders", ["position_id"])
    op.create_index("ix_orders_exchange_order_id", "orders", ["exchange_order_id"])
    op.create_unique_constraint("uq_orders_exchange_order_id", "orders", ["exchange_order_id"])
    op.create_unique_constraint("uq_orders_client_order_id", "orders", ["client_order_id"])
    op.create_foreign_key("fk_orders_position_id_positions", "orders", "positions", ["position_id"], ["id"])


def downgrade():
    op.drop_constraint("fk_orders_position_id_positions", "orders", type_="foreignkey")
    op.drop_constraint("uq_orders_client_order_id", "orders", type_="unique")
    op.drop_constraint("uq_orders_exchange_order_id", "orders", type_="unique")
    op.drop_index("ix_orders_exchange_order_id", table_name="orders")
    op.drop_index("ix_orders_position_id", table_name="orders")
    op.drop_index("ix_orders_client_order_id", table_name="orders")
    op.drop_column("orders", "position_id")
    op.drop_column("orders", "client_order_id")

    op.drop_column("positions", "side")

