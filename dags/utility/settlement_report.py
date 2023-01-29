

                
class restaurant_settlement_report_:

    def __init__(self, pg: PgConnect) -> None:
        self.repository = SettlementRepository(pg)

    def load_report_by_days(self):
        self.repository.load_settlement_by_days()