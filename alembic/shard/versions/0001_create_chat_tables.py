import sqlalchemy as sa

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "chats",
        sa.Column(
            "id",
            sa.Uuid(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "chat_members",
        sa.Column("chat_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(
            ["chat_id"], ["chats.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("chat_id", "user_id"),
    )

    op.create_table(
        "messages",
        sa.Column(
            "id",
            sa.Uuid(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("chat_id", sa.Uuid(), nullable=False),
        sa.Column("sender_id", sa.Uuid(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["chat_id"], ["chats.id"], ondelete="CASCADE"
        ),
    )

    op.create_index("idx_messages_chat_id", "messages", ["chat_id"])
    op.create_index("idx_messages_sent_at", "messages", ["sent_at"])


def downgrade() -> None:
    op.drop_index("idx_messages_sent_at", table_name="messages")
    op.drop_index("idx_messages_chat_id", table_name="messages")
    op.drop_table("messages")
    op.drop_table("chat_members")
    op.drop_table("chats")
