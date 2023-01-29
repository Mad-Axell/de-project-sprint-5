from lib.pg_connect import PgConnect
from cdm.models.repositories.restaurant_settlement_report_repositories import RestaurantSettlementReportRepository


class RestaurantSettlementReportLoader:

    def __init__(self, pg: PgConnect) -> None:
        self.repository = RestaurantSettlementReportRepository(pg)

    def load_report_by_days(self):
        self.repository.load_settlement_by_days()