from sqlalchemy import String, Float, Text, JSON, Integer, BigInteger, Date, Boolean
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import Mapped, mapped_column
from typing import Optional
from .database import Base


class Game(Base):
    __tablename__ = 'games'
    
    appid: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(String(500))
    release_date: Mapped[Optional[Date]] = mapped_column(Date)
    required_age: Mapped[Optional[int]] = mapped_column(Integer)
    price: Mapped[Optional[float]] = mapped_column(Float)
    dlc_count: Mapped[Optional[int]] = mapped_column(Integer)
    detailed_description: Mapped[Optional[str]] = mapped_column(MEDIUMTEXT)
    about_the_game: Mapped[Optional[str]] = mapped_column(MEDIUMTEXT)
    short_description: Mapped[Optional[str]] = mapped_column(MEDIUMTEXT)
    reviews: Mapped[Optional[str]] = mapped_column(MEDIUMTEXT)
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
    notes: Mapped[Optional[str]] = mapped_column(MEDIUMTEXT)
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


class Review(Base):
    __tablename__ = 'reviews'
    
    app_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)
    app_name: Mapped[Optional[str]] = mapped_column(String(500))
    review_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, unique=True)
    language: Mapped[Optional[str]] = mapped_column(String(50))
    review: Mapped[Optional[str]] = mapped_column(MEDIUMTEXT)
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


# DIMENSION TABLES - Normalize JSON arrays from games table

class Developer(Base):
    """Normalized developer dimension"""
    __tablename__ = 'developers'
    
    developer_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    game_count: Mapped[Optional[int]] = mapped_column(Integer, default=0)


class Publisher(Base):
    """Normalized publisher dimension"""
    __tablename__ = 'publishers'
    
    publisher_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    game_count: Mapped[Optional[int]] = mapped_column(Integer, default=0)


class Genre(Base):
    """Normalized genre dimension"""
    __tablename__ = 'genres'
    
    genre_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(500))


class Category(Base):
    """Normalized category dimension"""
    __tablename__ = 'categories'
    
    category_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(500))


class Tag(Base):
    """Normalized tag dimension"""
    __tablename__ = 'tags'
    
    tag_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    total_votes: Mapped[Optional[int]] = mapped_column(Integer, default=0)


# ASSOCIATION TABLES - Many-to-many relationships

class GameDeveloper(Base):
    """Many-to-many: Games <-> Developers"""
    __tablename__ = 'game_developers'
    
    game_appid: Mapped[int] = mapped_column(Integer, primary_key=True)
    developer_id: Mapped[int] = mapped_column(Integer, primary_key=True)


class GamePublisher(Base):
    """Many-to-many: Games <-> Publishers"""
    __tablename__ = 'game_publishers'
    
    game_appid: Mapped[int] = mapped_column(Integer, primary_key=True)
    publisher_id: Mapped[int] = mapped_column(Integer, primary_key=True)


class GameGenre(Base):
    """Many-to-many: Games <-> Genres"""
    __tablename__ = 'game_genres'
    
    game_appid: Mapped[int] = mapped_column(Integer, primary_key=True)
    genre_id: Mapped[int] = mapped_column(Integer, primary_key=True)


class GameCategory(Base):
    """Many-to-many: Games <-> Categories"""
    __tablename__ = 'game_categories'
    
    game_appid: Mapped[int] = mapped_column(Integer, primary_key=True)
    category_id: Mapped[int] = mapped_column(Integer, primary_key=True)


class GameTag(Base):
    """Many-to-many: Games <-> Tags with vote counts"""
    __tablename__ = 'game_tags'
    
    game_appid: Mapped[int] = mapped_column(Integer, primary_key=True)
    tag_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    vote_count: Mapped[Optional[int]] = mapped_column(Integer, default=0)


# USER DIMENSION - Extracted from reviews

class UserProfile(Base):
    """User dimension extracted from review author data"""
    __tablename__ = 'user_profiles'
    
    author_steamid: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    num_games_owned: Mapped[Optional[int]] = mapped_column(Integer)
    num_reviews: Mapped[Optional[int]] = mapped_column(Integer)
    total_playtime_minutes: Mapped[Optional[float]] = mapped_column(Float)
    first_review_date: Mapped[Optional[Date]] = mapped_column(Date)
    last_review_date: Mapped[Optional[Date]] = mapped_column(Date)
    positive_review_count: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    negative_review_count: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    avg_review_length: Mapped[Optional[int]] = mapped_column(Integer)
    helpful_votes_received: Mapped[Optional[int]] = mapped_column(Integer, default=0)


# AGGREGATION TABLES - Pre-computed summaries for testing

class GameReviewSummary(Base):
    """Aggregated review statistics per game"""
    __tablename__ = 'game_review_summary'
    
    game_appid: Mapped[int] = mapped_column(Integer, primary_key=True)
    total_reviews: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    positive_reviews: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    negative_reviews: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    avg_playtime_at_review: Mapped[Optional[float]] = mapped_column(Float)
    median_playtime_at_review: Mapped[Optional[float]] = mapped_column(Float)
    avg_helpful_votes: Mapped[Optional[float]] = mapped_column(Float)
    most_common_language: Mapped[Optional[str]] = mapped_column(String(50))
    steam_purchase_ratio: Mapped[Optional[float]] = mapped_column(Float)
    early_access_review_count: Mapped[Optional[int]] = mapped_column(Integer, default=0)


class DeveloperStats(Base):
    """Aggregated statistics per developer"""
    __tablename__ = 'developer_stats'
    
    developer_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    total_games: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    avg_game_price: Mapped[Optional[float]] = mapped_column(Float)
    avg_metacritic_score: Mapped[Optional[float]] = mapped_column(Float)
    total_positive_reviews: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    total_negative_reviews: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    avg_playtime: Mapped[Optional[float]] = mapped_column(Float)
    most_common_genre: Mapped[Optional[str]] = mapped_column(String(100))


# TIME SERIES TABLE - For temporal queries

class GamePriceHistory(Base):
    """Simulated price history for time-series queries"""
    __tablename__ = 'game_price_history'
    
    history_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_appid: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    price: Mapped[Optional[float]] = mapped_column(Float)
    discount_percent: Mapped[Optional[int]] = mapped_column(Integer)
    recorded_date: Mapped[Date] = mapped_column(Date, nullable=False, index=True)
