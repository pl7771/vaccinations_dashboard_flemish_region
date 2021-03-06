"""etl_metadata

Revision ID: c317ec5b7286
Revises:
Create Date: 2021-11-14 23:11:07.183061

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c317ec5b7286"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "etl_metadata",
        sa.Column("table", sa.String(length=255), nullable=False),
        sa.Column("last_date_processed", sa.Date(), nullable=False),
        sa.Column("last_run_date_time", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("table"),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("etl_metadata")
    # ### end Alembic commands ###
