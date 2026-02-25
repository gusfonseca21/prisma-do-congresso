import sqlalchemy as sa


class BaseMixin:
    id_lote = sa.Column(sa.Integer, sa.ForeignKey("lote.id"), nullable=False)
    id = sa.Column(sa.Integer, sa.Identity(start=1, cycle=False), primary_key=True)
