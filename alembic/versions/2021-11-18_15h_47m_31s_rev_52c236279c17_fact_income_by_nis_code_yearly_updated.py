"""fact_income_by_nis_code_yearly_updated

Revision ID: 52c236279c17
Revises: b89feedf3563
Create Date: 2021-11-18 15:47:31.267515

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
from app.utils import get_db_type

revision = "52c236279c17"
down_revision = "b89feedf3563"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "fact_income_by_nis_code_yearly_updated",
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("municipality_niscode", sa.String(5), nullable=False),
        sa.Column("nr_of_declarations", sa.Integer(), nullable=False),
        sa.Column("nr_zero_incomes", sa.Integer(), nullable=False),
        sa.Column("total_taxable_income", sa.Float(), nullable=False),
        sa.Column("total_net_income", sa.Float(), nullable=False),
        sa.Column("nr_total_income", sa.Integer(), nullable=False),
        sa.Column("total_net_professional_income", sa.Float(), nullable=False),
        sa.Column("nr_net_professional_income", sa.Integer(), nullable=False),
        sa.Column("total_taxes", sa.Float(), nullable=False),
        sa.Column("nr_taxes", sa.Integer(), nullable=False),
        sa.Column("nr_population", sa.Integer(), nullable=False),
        (
            sa.CheckConstraint("LEN(municipality_niscode)=5")
                ) if (get_db_type() == "mssql") else (
            sa.CheckConstraint("length(municipality_niscode)==5")
        ),
        sa.PrimaryKeyConstraint("year", "municipality_niscode"),
    )

def downgrade():
    op.drop_table("fact_income_by_nis_code_yearly_updated")
