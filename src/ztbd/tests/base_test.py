"""
Base test class for cross-database query testing
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger('ztbd.tests')


@dataclass
class QueryResult:
    """Standardized query result format"""
    rows: List[Dict[str, Any]]
    row_count: int
    execution_time: float
    database: str
    test_name: str
    timestamp: datetime = field(default_factory=datetime.now)
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.rows is None:
            self.rows = []
        self.row_count = len(self.rows)


class BaseTest(ABC):
    """
    Base class for database tests
    Each test should inherit from this and implement the run_* methods
    """
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.results: Dict[str, QueryResult] = {}
    
    @abstractmethod
    def run_postgresql(self, engine) -> QueryResult:
        """Execute test on PostgreSQL"""
        pass
    
    @abstractmethod
    def run_mysql(self, engine) -> QueryResult:
        """Execute test on MySQL"""
        pass
    
    @abstractmethod
    def run_mongodb(self, db) -> QueryResult:
        """Execute test on MongoDB"""
        pass
    
    @abstractmethod
    def run_neo4j(self, driver) -> QueryResult:
        """Execute test on Neo4j"""
        pass
    
    def execute_with_timing(self, db_name: str, func, *args, **kwargs) -> QueryResult:
        """Execute a query function with timing"""
        logger.info(f"Running {self.name} on {db_name}...")
        
        try:
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            execution_time = time.perf_counter() - start_time
            
            if isinstance(result, QueryResult):
                result.execution_time = execution_time
                return result
            
            # If function returned raw data, wrap it
            return QueryResult(
                rows=result if isinstance(result, list) else [],
                row_count=len(result) if isinstance(result, list) else 0,
                execution_time=execution_time,
                database=db_name,
                test_name=self.name
            )
            
        except Exception as e:
            logger.error(f"Error in {self.name} on {db_name}: {e}")
            return QueryResult(
                rows=[],
                row_count=0,
                execution_time=0.0,
                database=db_name,
                test_name=self.name,
                error=str(e)
            )
    
    def run_all(self, connections: Dict[str, Any]) -> Dict[str, QueryResult]:
        """Run test on all available databases"""
        self.results = {}
        
        if 'postgresql' in connections:
            self.results['postgresql'] = self.execute_with_timing(
                'postgresql', self.run_postgresql, connections['postgresql']
            )
        
        if 'mysql' in connections:
            self.results['mysql'] = self.execute_with_timing(
                'mysql', self.run_mysql, connections['mysql']
            )
        
        if 'mongodb' in connections:
            self.results['mongodb'] = self.execute_with_timing(
                'mongodb', self.run_mongodb, connections['mongodb']
            )
        
        if 'neo4j' in connections:
            self.results['neo4j'] = self.execute_with_timing(
                'neo4j', self.run_neo4j, connections['neo4j']
            )
        
        return self.results
    
    def normalize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a row for comparison across databases"""
        normalized = {}
        for key, value in row.items():
            # Convert datetime objects to ISO strings
            if isinstance(value, datetime):
                normalized[key] = value.isoformat()
            # Convert None/null to consistent representation
            elif value is None:
                normalized[key] = None
            # Convert numeric types
            elif isinstance(value, (int, float)):
                normalized[key] = float(value) if '.' in str(value) else int(value)
            else:
                normalized[key] = value
        return normalized
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of test results"""
        summary = {
            'test_name': self.name,
            'description': self.description,
            'results': {}
        }
        
        for db_name, result in self.results.items():
            summary['results'][db_name] = {
                'row_count': result.row_count,
                'execution_time_ms': round(result.execution_time * 1000, 2),
                'success': result.error is None,
                'error': result.error
            }
        
        return summary
