import sqlalchemy as sa


class LoteMixin:
    id_lote = sa.Column(sa.Integer, sa.ForeignKey("lote.id"), nullable=False)
