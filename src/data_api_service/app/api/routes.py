from fastapi import APIRouter, Depends, HTTPException
import duckdb
from data_api_service.app.db import get_duckdb_connection
from src.data_api_service.models.types import (
    TimeRange,
    AssetPriceDaily,
    InflationByYear,
    InflationAdjustedResponse,
    RealEstatePriceByStateResponse,
    RealEstatePriceMonthly,
    AssetFilter,
)

from src.data_api_service.models.services import Services

router = APIRouter()

@router.get(
    "/asset_vs_inflation",
    response_model=InflationAdjustedResponse,
    summary="Compare asset prices adjusted for inflation",
    description="Returns a time series of asset prices adjusted for inflation over a specified time range. Max year is 2023.",
)
async def get_asset_price_adjusted_to_inflation(
    time_range: TimeRange = Depends(),
    asset: AssetFilter = Depends(),
    db: duckdb.DuckDBPyConnection = Depends(get_duckdb_connection)
):
    if not asset.asset_key:
        raise HTTPException(status_code=400, detail="asset_key is required for this endpoint")
    map_crypto_assets = {"BTC": "BTC/USD", "ETH": "ETH/USD", "SOL": "SOL/USD"}
    try:
        sql = Services.get_asset_price_daily(
            asset_key=map_crypto_assets.get(asset.asset_key, asset.asset_key),
            start_date=time_range.start_date,
            end_date=time_range.end_date,
        )
        stock_daily_prices = db.sql(sql).fetchall()
        
        if not stock_daily_prices:
            raise HTTPException(status_code=404, detail=f"No data found for asset {asset.asset_key}")
        

        sql_inflation = Services.get_inflation_by_year(
            start_date=time_range.start_date,
            end_date=time_range.end_date,
        )
        inflation_during_period = db.sql(sql_inflation).fetchall()
        
        if not inflation_during_period:
            raise HTTPException(status_code=404, detail="No CPI data found for the specified period")
        
        # Create a dictionary for fast CPI lookup by year
        cpi_by_year = {row[0]: float(row[1]) for row in inflation_during_period}
        
        # Determine base year CPI
        base_cpi = 108.566932118964

        
        # Adjust prices for inflation
        stock_daily_prices_adjusted = []
        
        for row in stock_daily_prices:
            date, ticker, asset_type, close_price, high_price, low_price, volume = row
            year = date.year
            
            if year not in cpi_by_year:
                continue
            
            current_cpi = cpi_by_year[year]
            adjustment_factor = base_cpi / current_cpi
            
            adjusted_close = float(close_price) * adjustment_factor
            adjusted_high = float(high_price) * adjustment_factor
            adjusted_low = float(low_price) * adjustment_factor
            
            stock_daily_prices_adjusted.append((
                date, ticker, asset_type,
                adjusted_close, adjusted_high, adjusted_low, volume
            ))
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    # Build response with ADJUSTED prices
    series = [
        AssetPriceDaily(
            date=row[0],
            ticker=row[1],
            asset_type=row[2],
            close_price=float(row[3]),
            high_price=float(row[4]),
            low_price=float(row[5]),
            volume=float(row[6]),
        )
        for row in stock_daily_prices_adjusted 
    ]
    
    inflation_series = [
        InflationByYear(
            year=row[0],
            cpi_value=float(row[1]),
            unit=row[2],
        )
        for row in inflation_during_period
    ]
    
    return InflationAdjustedResponse(
        time_range=time_range.model_dump(),
        inflation_values=inflation_series,
        series=series,
    )
@router.get(
    "/realestate/monthly",
    response_model=RealEstatePriceByStateResponse,
    summary="Get real estate prices by state and month",
    description="Returns monthly aggregated real estate indicators grouped by state and year.",
)
async def get_realestate_monthly(
    reference_year: int,
    state_code: str,
    db: duckdb.DuckDBPyConnection = Depends(get_duckdb_connection),
):
    try:
        sql = Services.get_realestate_price_index_by_year_and_state(
            reference_year=reference_year,
            state_code=state_code,
        )

        result = db.sql(sql).fetchall()
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No real estate data found for state {state_code} in the given date range"
            )

        monthly_rows = [
            RealEstatePriceMonthly(
                    month_name=row[6],
                    avg_value=row[7],
                    min_value=row[8],
                    max_value=row[9],
                    stddev_value=row[10],
                    data_points=row[11],
                    unit=row[12],
                )
                for row in result
        ]
       
        (
            realestate_indicator_key,
            indicator_name,
            _,
            state_name,
            _,
            year,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
        ) = result[0]

        return RealEstatePriceByStateResponse(
                state=state_name,
                year=year,
                indicator_name=indicator_name,
                indicator_id=realestate_indicator_key,
                series=monthly_rows,
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error in get_realestate_monthly: {str(e)}"
        )