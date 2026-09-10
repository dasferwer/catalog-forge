from pathlib import Path

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    source = Path(__file__).resolve().parents[1] / "001_initial.sql"
    for statement in source.read_text().split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade():
    raise RuntimeError(
        "Initial schema downgrade is intentionally unsupported; restore a backup instead"
    )
