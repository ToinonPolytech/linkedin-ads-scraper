from .database import Base
from sqlalchemy import Column, Integer, String, Date, JSON, Text, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from typing import Optional
from datetime import date
import logging

logger = logging.getLogger(__name__)

class LinkedInAd(Base):
    __tablename__ = "linkedin_ads"

    ad_id: Mapped[str] = mapped_column(primary_key=True)
    creative_type: Mapped[Optional[str]]
    advertiser_name: Mapped[Optional[str]]
    advertiser_logo: Mapped[Optional[str]]
    headline: Mapped[Optional[str]]
    description: Mapped[Optional[str]]
    promoted_text: Mapped[Optional[str]]
    image_url: Mapped[Optional[str]]
    view_details_link: Mapped[Optional[str]]
    campaign_start_date: Mapped[Optional[date]]
    campaign_end_date: Mapped[Optional[date]]
    campaign_impressions_range: Mapped[Optional[str]]
    campaign_impressions_by_country: Mapped[Optional[dict]] = mapped_column(JSON)
    company_id: Mapped[Optional[int]]
    ad_type: Mapped[Optional[str]]
    ad_redirect_url: Mapped[Optional[str]]
    utm_parameters: Mapped[Optional[str]]
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
