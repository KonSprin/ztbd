# models.py
from sqlalchemy import String, Float, Text, JSON, Integer, BigInteger, Date, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from typing import Optional
from .database import Base


# Games table model
class Game(Base):
    __tablename__ = 'games'
    
    appid: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(String(500))
    release_date: Mapped[Optional[Date]] = mapped_column(Date)
    required_age: Mapped[Optional[int]] = mapped_column(Integer)
    price: Mapped[Optional[float]] = mapped_column(Float)
    dlc_count: Mapped[Optional[int]] = mapped_column(Integer)
    detailed_description: Mapped[Optional[str]] = mapped_column(Text)
    about_the_game: Mapped[Optional[str]] = mapped_column(Text)
    short_description: Mapped[Optional[str]] = mapped_column(Text)
    reviews: Mapped[Optional[str]] = mapped_column(Text)
    header_image: Mapped[Optional[str]] = mapped_column(String(500))
    website: Mapped[Optional[str]] = mapped_column(String(500))
    support_url: Mapped[Optional[str]] = mapped_column(String(500))
    support_email: Mapped[Optional[str]] = mapped_column(String(255))
    windows: Mapped[Optional[bool]] = mapped_column(Boolean)
    mac: Mapped[Optional[bool]] = mapped_column(Boolean)
    linux: Mapped[Optional[bool]] = mapped_column(Boolean)
    metacritic_score: Mapped[Optional[int]] = mapped_column(Integer)
    metacritic_url: Mapped[Optional[str]] = mapped_column(String(500))
    achievements: Mapped[Optional[int]] = mapped_column(Integer)
    recommendations: Mapped[Optional[int]] = mapped_column(Integer)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    supported_languages: Mapped[Optional[dict]] = mapped_column(JSON)
    full_audio_languages: Mapped[Optional[dict]] = mapped_column(JSON)
    packages: Mapped[Optional[dict]] = mapped_column(JSON)
    developers: Mapped[Optional[dict]] = mapped_column(JSON)
    publishers: Mapped[Optional[dict]] = mapped_column(JSON)
    categories: Mapped[Optional[dict]] = mapped_column(JSON)
    genres: Mapped[Optional[dict]] = mapped_column(JSON)
    screenshots: Mapped[Optional[dict]] = mapped_column(JSON)
    movies: Mapped[Optional[dict]] = mapped_column(JSON)
    user_score: Mapped[Optional[int]] = mapped_column(Integer)
    score_rank: Mapped[Optional[str]] = mapped_column(String(100))
    positive: Mapped[Optional[int]] = mapped_column(Integer)
    negative: Mapped[Optional[int]] = mapped_column(Integer)
    estimated_owners: Mapped[Optional[str]] = mapped_column(String(100))
    average_playtime_forever: Mapped[Optional[int]] = mapped_column(Integer)
    average_playtime_2weeks: Mapped[Optional[int]] = mapped_column(Integer)
    median_playtime_forever: Mapped[Optional[int]] = mapped_column(Integer)
    median_playtime_2weeks: Mapped[Optional[int]] = mapped_column(Integer)
    discount: Mapped[Optional[int]] = mapped_column(Integer)
    peak_ccu: Mapped[Optional[int]] = mapped_column(Integer)
    tags: Mapped[Optional[dict]] = mapped_column(JSON)
    pct_pos_total: Mapped[Optional[int]] = mapped_column(Integer)
    num_reviews_total: Mapped[Optional[int]] = mapped_column(Integer)
    pct_pos_recent: Mapped[Optional[int]] = mapped_column(Integer)
    num_reviews_recent: Mapped[Optional[int]] = mapped_column(Integer)

# Reviews table model
class Review(Base):
    __tablename__ = 'reviews'
    
    # id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    app_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)
    app_name: Mapped[Optional[str]] = mapped_column(String(500))
    review_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, unique=True)
    language: Mapped[Optional[str]] = mapped_column(String(50))
    review: Mapped[Optional[str]] = mapped_column(Text)
    timestamp_created: Mapped[Optional[int]] = mapped_column(BigInteger)
    timestamp_updated: Mapped[Optional[int]] = mapped_column(BigInteger)
    recommended: Mapped[Optional[bool]] = mapped_column(Boolean)
    votes_helpful: Mapped[Optional[int]] = mapped_column(BigInteger)
    votes_funny: Mapped[Optional[int]] = mapped_column(BigInteger)
    weighted_vote_score: Mapped[Optional[float]] = mapped_column(Float)
    comment_count: Mapped[Optional[int]] = mapped_column(Integer)
    steam_purchase: Mapped[Optional[bool]] = mapped_column(Boolean)
    received_for_free: Mapped[Optional[bool]] = mapped_column(Boolean)
    written_during_early_access: Mapped[Optional[bool]] = mapped_column(Boolean)
    author_steamid: Mapped[Optional[int]] = mapped_column(BigInteger, index=True)
    author_num_games_owned: Mapped[Optional[int]] = mapped_column(BigInteger)
    author_num_reviews: Mapped[Optional[int]] = mapped_column(BigInteger)
    author_playtime_forever: Mapped[Optional[float]] = mapped_column(Float)
    author_playtime_last_two_weeks: Mapped[Optional[float]] = mapped_column(Float)
    author_playtime_at_review: Mapped[Optional[float]] = mapped_column(Float)
    author_last_played: Mapped[Optional[float]] = mapped_column(Float)

# class Product(Base):
#     __tablename__ = "products"

#     id: Mapped[int] = mapped_column(primary_key=True, index=True)
#     name: Mapped[str] = mapped_column(String(20), unique=True, index=True)
#     description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
#     price: Mapped[float] = mapped_column(Float)
#     quantity: Mapped[int] = mapped_column(default=0)
#     category: Mapped[str] = mapped_column(String)
#     created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
#     updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    
#     # Relacja do historii zmian
#     history: Mapped[List["ProductHistory"]] = relationship(back_populates="product", cascade="all, delete-orphan")


# class ProductHistory(Base):
#     __tablename__ = "product_history"

#     id: Mapped[int] = mapped_column(primary_key=True, index=True)
#     product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
#     field_name: Mapped[str] = mapped_column(String)
#     old_value: Mapped[Optional[str]] = mapped_column(String, nullable=True)
#     new_value: Mapped[str] = mapped_column(String)
#     changed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
#     change_type: Mapped[str] = mapped_column(String)
    
#     # Relacja do produktu
#     product: Mapped["Product"] = relationship(back_populates="history")
