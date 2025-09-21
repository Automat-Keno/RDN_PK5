"""
Procesor danych: przetwarzanie JSON z API PSE (pk5l-wp) → „ładne” pola → Mongo upsert.
"""

import datetime
import pytz
from typing import List, Dict, Any, Optional


class OptimizedDataProcessor:
    """Procesor danych do transformacji JSON → dokumenty i zapis do Mongo."""

    FIELD_MAPPING = {
        "grid_demand_fcst": "Prognozowane zapotrzebowanie sieci",
        "req_pow_res": "Wymagana rezerwa mocy OSP",
        "surplus_cap_avail_tso": "Nadwyżka mocy dostępna dla OSP (7) + (9) - [(3) - (12)] - (13)",
        "gen_surplus_avail_tso_above": "Nadwyżka mocy dostępna dla OSP ponad wymaganą rezerwę moc (5) - (4)",
        "avail_cap_gen_units_stor_prov": "Moc dyspozycyjna JW i magazynów energii świadczących usługi bilansujące w ramach RB",
        "avail_cap_gen_units_stor_prov_tso": "Moc dyspozycyjna JW i magazynów energii świadczących usługi bilansujące w ramach RB dostępna dla OSP",
        "fcst_gen_unit_stor_prov": "Przewidywana generacja JW i magazynów energii świadczących usługi bilansujące w ramach RB (3) - (9)",
        "fcst_gen_unit_stor_non_prov": "Prognozowana generacja JW i magazynów energii nie świadczących usług bilansujących w ramach RB",
        "fcst_wi_tot_gen": "Prognozowana sumaryczna generacja źródeł wiatrowych",
        "fcst_pv_tot_gen": "Prognozowana sumaryczna generacja źródeł fotowoltaicznych",
        "planned_exchange": "Planowane saldo wymiany międzysystemowej",
        "fcst_unav_energy": "Prognozowana wielkość niedyspozycyjności wynikająca z ograniczeń sieciowych występujących w sieci przesyłowej oraz sieci dystrybucyjnej w zakresie dostarczania energii elektrycznej",
        "sum_unav_oper_cond": "Prognozowana wielkość niedyspozycyjności wynikających z warunków eksploatacyjnych JW świadczących usługi bilansujące w ramach RB",
        "pred_gen_res_not_cov": "Przewidywana generacja zasobów wytwórczych nieobjętych obowiązkami mocowymi",
        "cap_market_obligation": "Obowiązki mocowe wszystkich jednostek rynku mocy"
    }

    def __init__(
        self,
        url_template: str,
        data_start: str,
        int_cols: List[str],
        float_cols: List[str],
        date_cols: List[str],
        fields_to_utc: Optional[List[str]] = None,
        fields_to_add_hour: Optional[Dict[str, str]] = None,
        date_format: Optional[str] = None,
        mongo_connector=None,
        kolekcja_mongo: Optional[str] = None
    ):
        self.url_template = url_template
        self.data_start = data_start
        self.int_cols = int_cols or []
        self.float_cols = float_cols or []
        self.date_cols = date_cols or []
        self.fields_to_utc = fields_to_utc or []
        self.fields_to_add_hour = fields_to_add_hour or {}
        self.date_format = date_format
        self.mongo_connector = mongo_connector
        self.kolekcja_mongo = kolekcja_mongo
        self.data_start_dt = datetime.datetime.strptime(data_start, '%Y-%m-%d')

    # --- CSV placeholder (pozostawiamy dla kompatybilności) ---
    def process_csv_content(self, csv_content: bytes) -> List[Dict[str, Any]]:
        return []

    # --- JSON flow poniżej ---
    def _to_int(self, v):
        if v is None:
            return None
        try:
            return int(round(float(v)))
        except Exception:
            return None

    def _iso_utc_start_of_day_for_business_date(self, business_date_str: str) -> str:
        """Doba = północ lokalna (Europe/Warsaw) dla business_date, zrzut do UTC ISO."""
        warsaw = pytz.timezone('Europe/Warsaw')
        dt_local_date = datetime.datetime.strptime(business_date_str, '%Y-%m-%d').date()
        dt_local = warsaw.localize(datetime.datetime.combine(dt_local_date, datetime.time(0, 0)))
        return dt_local.astimezone(pytz.UTC).isoformat()

    def process_json_value(self, json_obj: dict) -> List[Dict[str, Any]]:
        """
        Przekształca JSON z API PSE (pk5l-wp) do 'ładnych' pól i upsertuje do Mongo po (Doba, Godzina).
        """
        items = []
        if isinstance(json_obj, dict):
            items = json_obj.get('value', [])
        elif isinstance(json_obj, list):
            items = json_obj
        else:
            items = []

        result: List[Dict[str, Any]] = []

        for row in items:
            # Godzina z 'plan_dtime' (lokalny czas 'YYYY-MM-DD HH:MM:SS')
            try:
                godz = int(datetime.datetime.strptime(row.get('plan_dtime'), '%Y-%m-%d %H:%M:%S').hour)
            except Exception:
                godz = None

            # Doba: północ lokalna z 'business_date' → UTC ISO
            doba_iso_utc = None
            if row.get('business_date'):
                doba_iso_utc = self._iso_utc_start_of_day_for_business_date(row['business_date'])

            doc: Dict[str, Any] = {
                'Doba': doba_iso_utc,
                'Godzina': godz,
            }

            # Mapowanie API → „ładne” nazwy
            for api_key, pretty_key in self.FIELD_MAPPING.items():
                doc[pretty_key] = row.get(api_key)

            # Wymuszenie typów dla kolumn całkowitych wg configu
            for col in self.int_cols:
                if col in doc:
                    doc[col] = self._to_int(doc[col])

            result.append(doc)

        # Upsert do Mongo po (Doba, Godzina)
        if self.mongo_connector and self.kolekcja_mongo and len(result) > 0:
            if self.mongo_connector.connect():
                col = self.mongo_connector.db[self.kolekcja_mongo]
                for doc in result:
                    key = {'Doba': doc.get('Doba'), 'Godzina': doc.get('Godzina')}
                    col.update_one(key, {'$set': doc}, upsert=True)

        return result
