"""
REST API server for getting gift data, statistics, and history.
"""
from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import typing
import asyncio
from pathlib import Path

from core import star_gifts_data
import features.history_manager as history_manager
import utils.utils as utils
import utils.constants as constants
import config


app = FastAPI(
    title="TG Gifts Notifier API",
    description="API for getting Telegram star gift data",
    version="1.0.0"
)


# Response models
class GiftResponse(BaseModel):
    """Response model for gift."""
    id: int
    number: int
    sticker_file_name: str
    price: int
    convert_price: int
    available_amount: int
    total_amount: int
    require_premium: bool
    user_limited: int | None
    is_limited: bool
    first_appearance_timestamp: int | None
    last_sale_timestamp: int | None
    is_upgradable: bool
    history_size: int


class HistoryEntryResponse(BaseModel):
    """Response model for history entry."""
    timestamp: int
    available_amount: int
    price: int
    convert_price: int
    is_upgradable: bool


class StatisticsResponse(BaseModel):
    """Response model for gift statistics."""
    gift_id: int
    total_sold: int
    current_available: int
    sale_rate_per_hour: float | None
    estimated_sold_out_seconds: int | None
    is_critical: bool
    history_entries_count: int


class GlobalStatisticsResponse(BaseModel):
    """Response model for global statistics."""
    total_gifts: int
    limited_gifts: int
    available_gifts: int
    premium_only_gifts: int
    upgradable_gifts: int
    critical_gifts: int
    total_unique_gifts: int
    average_price: float
    average_convert_price: float


class HealthResponse(BaseModel):
    """Response model for healthcheck."""
    status: str
    uptime_seconds: int
    gifts_count: int
    last_update: int | None


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "TG Gifts Notifier API",
        "version": "1.0.0",
        "endpoints": {
            "gifts": "/gifts",
            "gift_by_id": "/gifts/{id}",
            "history": "/gifts/{id}/history",
            "statistics": "/gifts/{id}/statistics",
            "global_statistics": "/statistics",
            "health": "/health"
        }
    }


@app.get("/health")
async def health():
    """
    Healthcheck endpoint.
    
    Returns:
        System health status
    """
    try:
        data = star_gifts_data.StarGiftsData.load(config.DATA_FILEPATH)
        
        # Calculate last update time
        last_update = None
        if data.star_gifts:
            timestamps = [
                g.history[-1].timestamp if g.history else g.first_appearance_timestamp
                for g in data.star_gifts
                if g.history or g.first_appearance_timestamp
            ]
            if timestamps:
                last_update = max(t for t in timestamps if t is not None)
        
        return HealthResponse(
            status="healthy",
            uptime_seconds=0,  # Can add startup time tracking
            gifts_count=len(data.star_gifts),
            last_update=last_update
        )
    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            uptime_seconds=0,
            gifts_count=0,
            last_update=None
        )


@app.get("/gifts", response_model=list[GiftResponse])
async def get_gifts(
    limited_only: bool = Query(False, description="Only limited gifts"),
    premium_only: bool = Query(False, description="Only Premium gifts"),
    available_only: bool = Query(False, description="Only available gifts"),
    min_price: int | None = Query(None, description="Minimum price"),
    max_price: int | None = Query(None, description="Maximum price")
):
    """
    Gets list of all gifts with filtering.
    
    Args:
        limited_only: Only limited gifts
        premium_only: Only Premium gifts
        available_only: Only available gifts
        min_price: Minimum price
        max_price: Maximum price
    
    Returns:
        List of gifts
    """
    try:
        data = star_gifts_data.StarGiftsData.load(config.DATA_FILEPATH)
        
        gifts = data.star_gifts.copy()
        
        # Apply filters
        if limited_only:
            gifts = [g for g in gifts if g.is_limited]
        
        if premium_only:
            gifts = [g for g in gifts if g.require_premium]
        
        if available_only:
            gifts = [g for g in gifts if g.is_limited and g.available_amount > 0]
        
        if min_price is not None:
            gifts = [g for g in gifts if g.price >= min_price]
        
        if max_price is not None:
            gifts = [g for g in gifts if g.price <= max_price]
        
        return [
            GiftResponse(
                **g.model_dump(exclude={"history", "sticker_file_id"}),
                history_size=len(g.history)
            )
            for g in gifts
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/gifts/{gift_id}", response_model=GiftResponse)
async def get_gift_by_id(gift_id: int):
    """
    Gets information about specific gift by ID.
    
    Args:
        gift_id: Gift ID
    
    Returns:
        Gift data
    """
    try:
        data = star_gifts_data.StarGiftsData.load(config.DATA_FILEPATH)
        
        gift = next((g for g in data.star_gifts if g.id == gift_id), None)
        
        if gift is None:
            raise HTTPException(status_code=404, detail=f"Gift with ID {gift_id} not found")
        
        return GiftResponse(
            **gift.model_dump(exclude={"history", "sticker_file_id"}),
            history_size=len(gift.history)
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/gifts/{gift_id}/history", response_model=list[HistoryEntryResponse])
async def get_gift_history(gift_id: int):
    """
    Gets gift change history.
    
    Args:
        gift_id: Gift ID
    
    Returns:
        Change history
    """
    try:
        data = star_gifts_data.StarGiftsData.load(config.DATA_FILEPATH)
        
        gift = next((g for g in data.star_gifts if g.id == gift_id), None)
        
        if gift is None:
            raise HTTPException(status_code=404, detail=f"Gift with ID {gift_id} not found")
        
        return [
            HistoryEntryResponse(**entry.model_dump())
            for entry in gift.history
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/gifts/{gift_id}/statistics", response_model=StatisticsResponse)
async def get_gift_statistics(gift_id: int):
    """
    Gets statistics for gift.
    
    Args:
        gift_id: Gift ID
    
    Returns:
        Gift statistics
    """
    try:
        data = star_gifts_data.StarGiftsData.load(config.DATA_FILEPATH)
        
        gift = next((g for g in data.star_gifts if g.id == gift_id), None)
        
        if gift is None:
            raise HTTPException(status_code=404, detail=f"Gift with ID {gift_id} not found")
        
        stats = history_manager.calculate_statistics(gift)
        
        return StatisticsResponse(**stats.model_dump())
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/statistics", response_model=GlobalStatisticsResponse)
async def get_global_statistics():
    """
    Gets global statistics for all gifts.
    
    Returns:
        Global statistics
    """
    try:
        data = star_gifts_data.StarGiftsData.load(config.DATA_FILEPATH)
        stats = history_manager.calculate_global_statistics(data.star_gifts)
        
        return GlobalStatisticsResponse(**stats.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def run_api_server(host: str = "127.0.0.1", port: int = 8000):
    """
    Starts API server.
    
    Args:
        host: Host to listen on
        port: Port to listen on
    """
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    run_api_server()
