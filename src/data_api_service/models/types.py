from pydantic import BaseModel, Field
from datetime import date
from typing import Optional
from enum import Enum


class AssetType(str, Enum):
    """Enum for asset types"""
    STOCK = "STOCK"
    CRYPTO = "CRYPTO"

class AssetFilter(BaseModel):
    asset_type: Optional[AssetType] = None
    asset_key: str  # e.g. ticker, coin symbol

class TimeRange(BaseModel):
    start_date: date
    end_date: date

class AssetPriceDaily(BaseModel):
    """Output model for get_asset_price_daily query"""
    date: date
    ticker: str = Field(..., description="Asset ticker symbol")
    asset_type: AssetType = Field(..., description="Type of asset (stock or crypto)")
    close_price: float = Field(..., description="Closing price")
    high_price: float = Field(..., description="Highest price of the day")
    low_price: float = Field(..., description="Lowest price of the day")
    volume: float = Field(..., description="Trading volume")


class InflationByYear(BaseModel):
    """Output model for get_inflation_by_year query"""
    year: int = Field(..., description="Calendar year")
    cpi_value: float = Field(..., description="Consumer Price Index value (base year 2010=100)")
    unit: Optional[str] = Field(None, description="Unit of measurement")

class InflationAdjustedResponse(BaseModel):
    time_range: TimeRange
    inflation_values: list[InflationByYear]
    series: list[AssetPriceDaily]