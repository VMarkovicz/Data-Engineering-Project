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
    @staticmethod
    def get_realestate_price_index_by_year_and_state(
        reference_year: int,
        state_code: str,
    ) -> str:
        return f"""
            SELECT 
                dri.realestate_indicator_key,
                dri.indicator_name,
                dri.indicator_description,
                dr.state_name,
                dr.state_code,
                dt.year,
                dt.month_name,
                AVG(dv.value) AS avg_value,
                MIN(dv.value) AS min_value,
                MAX(dv.value) AS max_value,
                STDDEV(dv.value) AS stddev_value,
                COUNT(*) AS data_points,
                dv.unit
            FROM fact_value fv
            JOIN dim_realestate_indicator dri 
                ON fv.realestate_indicator_key = dri.realestate_indicator_key
            JOIN dim_region dr 
                ON fv.region_key = dr.region_key
            JOIN dim_time dt 
                ON fv.date_key = dt.date_key
            JOIN dim_value dv 
                ON fv.value_key = dv.value_key
            WHERE fv.realestate_indicator_key IS NOT NULL
                AND dr.state_code IS NOT NULL
                AND dt.year = {reference_year}
                AND dr.state_code = '{state_code}'
            GROUP BY 
                dri.realestate_indicator_key, 
                dri.indicator_name,
                dri.indicator_description,
                dr.state_name,
                dr.state_code,
                dt.year,
                dt.month_name,
                month_number,
                dv.unit
            HAVING COUNT(*) >= 3
            ORDER BY dr.state_code, dt.year DESC, dt.month_number DESC, dri.realestate_indicator_key;

        """
