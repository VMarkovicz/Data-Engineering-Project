# app/queries.py
from datetime import date

class Services:
    @staticmethod
    def get_asset_price_daily(
        asset_key: str,
        start_date: date,
        end_date: date,
    ) -> str:
        return f"""
            SELECT 
                dt.date_key AS date,
                da.asset_key AS ticker,
                da.type AS asset_type,
                dcv.close AS close_price,
                dcv.high AS high_price,
                dcv.low AS low_price,
                dcv.volume AS volume
            FROM fact_value fv
            INNER JOIN dim_time dt 
                ON fv.date_key = dt.date_key
            INNER JOIN dim_asset da 
                ON fv.asset_key = da.asset_key
            INNER JOIN dim_cryptostock_value dcv 
                ON fv.cryptostock_value_key = dcv.cryptostock_value_key
            WHERE da.asset_key = '{asset_key}'
                AND dt.date_key BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY dt.date_key ASC;
        """
    @staticmethod
    def get_inflation_by_year(
        start_date: date,
        end_date: date,
    ) -> str:
        start_year = start_date.year
        end_year = end_date.year
        return f"""
            SELECT 
                dt.year,
                dv.value AS cpi_value,
                dv.unit
            FROM fact_value fv
            INNER JOIN dim_time dt 
                ON fv.date_key = dt.date_key
            INNER JOIN dim_socioeconomical_indicator dsi 
                ON fv.socioeconomical_indicator_key = dsi.socioeconomical_indicator_key
            INNER JOIN dim_country dc 
                ON fv.country_key = dc.country_key
            INNER JOIN dim_value dv 
                ON fv.value_key = dv.value_key
            WHERE dsi.socioeconomical_indicator_key = 'FP.CPI.TOTL'
                AND dt.year BETWEEN {start_year} AND {end_year}
                AND dc.country_code = 'USA'
            GROUP BY dt.year, dv.value, dv.unit
            ORDER BY dt.year ASC;
        """

