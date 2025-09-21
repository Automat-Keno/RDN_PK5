"""
Procesor danych: JSON z API PSE (pk5l-wp) → techniczne pola (ASCII, z podkreśleniami)
i zapis w Mongo w układzie: jeden dokument na dzień z polami `first` (pierwszy snapshot)
i `newest` (najnowszy snapshot). `data_cet` = początek doby lokalnej (Warszawa) w UTC.
"""

import datetime
import pytz
from typing import List, Dict, Any, Optional


class OptimizedDataProcessor:
    """Procesor danych do transformacji JSON → dokumenty i zapis do Mongo."""

    # Mapowanie: klucz z API → techniczna nazwa (underscored, ASCII)
    FIELD_MAPPING = {
        "grid_demand_fcst": "Prognozowane_zapotrzebowanie_sieci",
        "req_pow_res": "Wymagana_rezerwa_mocy_OSP",
        "surplus_cap_avail_tso": "Nadwyzka_mocy_dostepna_dla_OSP_(7)_+_(9)_-_[(3)_-_(12)]_-_(13)",
        "gen_surplus_avail_tso_above": "Nadwyzka_mocy_dostepna_dla_OSP_ponad_wymagana_rezerwe_moc_(5)_-_(4)",
        "avail_cap_gen_units_stor_prov": "Moc_dyspozycyjna_JW_i_magazynow_energii_swiadczacych_uslugi_bilansujace_w_ramach_RB",
        "avail_cap_gen_units_stor_prov_tso": "Moc_dyspozycyjna_JW_i_magazynow_energii_swiadczacych_uslugi_bilansujace_w_ramach_RB_dostepna_dla_OSP",
        "fcst_gen_unit_stor_prov": "Przewidywana_generacja_JW_i_magazynow_energii_swiadczacych_uslugi_bilansujace_w_ramach_RB_(3)_-_(9)",
        "fcst_gen_unit_stor_non_prov": "Prognozowana_generacja_JW_i_magazynow_energii_nie_swiadczacych_uslug_bilansujacych_w_ramach_RB",
        "fcst_wi_tot_gen": "Prognozowana_sumaryczna_generacja_zrodel_wiatrowych",
        "fcst_pv_tot_gen": "Prognozowana_sumaryczna_generacja_zrodel_fotowoltaicznych",
        "planned_exchange": "Planowane_saldo_wymiany_miedzysystemowej",
        "fcst_unav_energy": "Prognozowana_wielkosc_niedyspozycyjnosci_wynikajaca_z_ograniczen_sieciowych_wystepujacych_w_sieci_przesylowej_oraz_sieci_dystrybucyjnej_w_zakresie_dostarczania_energii_elektrycznej",
        "sum_unav_oper_cond": "Prognozowana_wielkosc_niedyspozycyjnosci_wynikajacych_z_warunkow_eksploatacyjnych_JW_swiadczacych_uslugi_bilansujace_w_ramach_RB",
        "pred_gen_res_not_cov": "Przewidywana_generacja_zasobow_wytworczych_nieobjetych_obowiazkami_mocowymi",
        "cap_market_obligation": "Obowiazki_mocowe_wszystkich_jednostek_rynku_mocy"
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

    # --- CSV placeholder (dla kompatybilności; nieużywane w tym strumieniu) ---
    def process_csv_content(self, csv_content: bytes) -> List[Dict[str, Any]]:
        return []

    # --- JSON flow ---
    def _to_int(self, v):
        if v is None:
            return None
        try:
            return int(round(float(v)))
        except Exception:
            return None

    def _start_of_business_day_utc(self, business_date_str: str) -> datetime.datetime:
        """
        Zwraca datetime UTC odpowiadający północy lokalnej (Europe/Warsaw)
        dla podanego business_date (YYYY-MM-DD).
        """
        warsaw = pytz.timezone('Europe/Warsaw')
        dt_local_date = datetime.datetime.strptime(business_date_str, '%Y-%m-%d').date()
        dt_local_midnight = warsaw.localize(datetime.datetime.combine(dt_local_date, datetime.time(0, 0)))
        return dt_local_midnight.astimezone(pytz.UTC)

    def process_json_value(self, json_obj: dict) -> List[Dict[str, Any]]:
        """
        Przekształca JSON z API PSE (pk5l-wp) do technicznych pól.
        W Mongo zapisuje jeden dokument na dzień:
          - data_cet = początek doby lokalnej (Warszawa) w UTC (bez dodanej godziny)
          - first = pierwszy snapshot (pełna lista godzin)
          - newest = ostatni snapshot (pełna lista godzin)
        Każdy element w first/newest ma:
          - Doba = start_dnia_UTC + Godzina (czyli per-godzina timestamp)
          - Godzina = 0..23 (int)
        """
        items = []
        if isinstance(json_obj, dict):
            items = json_obj.get('value', [])
        elif isinstance(json_obj, list):
            items = json_obj
        else:
            items = []

        result: List[Dict[str, Any]] = []
        start_day_utc: Optional[datetime.datetime] = None

        for row in items:
            # Godzina z 'plan_dtime' (lokalny czas 'YYYY-MM-DD HH:MM:SS')
            try:
                godz = int(datetime.datetime.strptime(row.get('plan_dtime'), '%Y-%m-%d %H:%M:%S').hour)
            except Exception:
                godz = None

            # Ustal początek doby lokalnej w UTC tylko raz (z pierwszego rekordu z business_date)
            if start_day_utc is None and row.get('business_date'):
                start_day_utc = self._start_of_business_day_utc(row['business_date'])

            # Doba per rekord: początek_doby_UTC + godzina
            doba_utc_datetime = None
            if start_day_utc is not None and godz is not None:
                doba_utc_datetime = start_day_utc + datetime.timedelta(hours=godz)

            doc: Dict[str, Any] = {
                'Doba': doba_utc_datetime,
                'Godzina': godz,
            }

            # Mapowanie API → techniczne nazwy
            for api_key, tech_key in self.FIELD_MAPPING.items():
                doc[tech_key] = row.get(api_key)

            # Wymuszenie typów dla kolumn całkowitych wg configu
            for col in self.int_cols:
                if col in doc:
                    doc[col] = self._to_int(doc[col])

            result.append(doc)

        # Upsert do Mongo: jeden dokument na dzień (`first` niezmienny, `newest` nadpisywany)
        if self.mongo_connector and self.kolekcja_mongo and len(result) > 0 and start_day_utc is not None:
            if self.mongo_connector.connect():
                col = self.mongo_connector.db[self.kolekcja_mongo]

                data_cet = start_day_utc  # identyfikator dnia (początek doby lokalnej zapisany w UTC)
                now_utc = datetime.datetime.now(pytz.UTC)

                existing = col.find_one({'data_cet': data_cet})

                if existing:
                    # Tylko podmieniamy newest + aktualizujemy znacznik czasu
                    col.update_one(
                        {'_id': existing['_id']},
                        {'$set': {
                            'newest': result,
                            'last_update': now_utc
                        }}
                    )
                else:
                    # Pierwszy zapis → tworzymy first + newest + last_update
                    col.insert_one({
                        'data_cet': data_cet,
                        'first': result,
                        'newest': result,
                        'last_update': now_utc
                    })

        return result
