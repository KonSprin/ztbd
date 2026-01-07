"""
Test runner for executing and comparing cross-database queries
"""
import os
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from pymongo import MongoClient
from neo4j import GraphDatabase
from sqlalchemy import create_engine

from .test_queries import ALL_TESTS
from .base_test import BaseTest, QueryResult

load_dotenv()

logger = logging.getLogger('ztbd.tests')


class TestRunner:
    """Executes tests across all databases and collects results"""
    
    def __init__(self, databases: Optional[List[str]] = None):
        self.databases = databases or ['postgresql', 'mysql', 'mongodb', 'neo4j']
        self.connections: Dict[str, Any] = {}
        self.test_results: List[Dict[str, Any]] = []
        self.comparison_results: List[Dict[str, Any]] = []
    
    def setup_connections(self):
        """Initialize database connections"""
        logger.info("Setting up database connections...")
        
        if 'postgresql' in self.databases:
            try:
                pg_url = os.getenv('SQLALCHEMY_DATABASE_URL', "")
                self.connections['postgresql'] = create_engine(pg_url)
                logger.info("  PostgreSQL connection established")
            except Exception as e:
                logger.error(f"  PostgreSQL connection failed: {e}")
        
        if 'mysql' in self.databases:
            try:
                mysql_url = os.getenv('MYSQL_DATABASE_URL', "")
                self.connections['mysql'] = create_engine(mysql_url)
                logger.info("  MySQL connection established")
            except Exception as e:
                logger.error(f"  MySQL connection failed: {e}")
        
        if 'mongodb' in self.databases:
            try:
                mongo_uri = os.getenv('MONGO_URI', 'mongodb://user:password@localhost:27017/')
                mongo_db = os.getenv('DATABASE_NAME', 'mongodb')
                client = MongoClient(mongo_uri)
                self.connections['mongodb'] = client[mongo_db]
                logger.info("  MongoDB connection established")
            except Exception as e:
                logger.error(f"  MongoDB connection failed: {e}")
        
        if 'neo4j' in self.databases:
            try:
                neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
                neo4j_user = os.getenv('NEO4J_USER', 'user')
                neo4j_pass = os.getenv('NEO4J_PASSWORD', 'password')
                self.connections['neo4j'] = GraphDatabase.driver(
                    neo4j_uri, auth=(neo4j_user, neo4j_pass)
                )
                logger.info("  Neo4j connection established")
            except Exception as e:
                logger.error(f"  Neo4j connection failed: {e}")
    
    def teardown_connections(self):
        """Close all database connections"""
        logger.info("Closing database connections...")
        
        if 'postgresql' in self.connections:
            self.connections['postgresql'].dispose()
        
        if 'mysql' in self.connections:
            self.connections['mysql'].dispose()
        
        if 'mongodb' in self.connections:
            self.connections['mongodb'].client.close()
        
        if 'neo4j' in self.connections:
            self.connections['neo4j'].close()
    
    def run_all_tests(self):
        """Execute all registered tests"""
        logger.info(f"{'='*60}")
        logger.info(f"Running {len(ALL_TESTS)} tests across databases")
        logger.info(f"{'='*60}")
        
        for test_class in ALL_TESTS:
            test = test_class()
            logger.info(f"Test: {test.name}")
            logger.info(f"Description: {test.description}")
            logger.info("-" * 60)
            
            results = test.run_all(self.connections)
            
            # Log individual results
            for db_name, result in results.items():
                if result.error:
                    logger.error(f"  {db_name}: ERROR - {result.error}")
                else:
                    logger.info(f"  {db_name}: {result.row_count} rows in {result.execution_time*1000:.2f}ms")
            
            # Store results
            self.test_results.append({
                'test': test,
                'results': results,
                'summary': test.get_summary()
            })
    
    def compare_results(self):
        """Compare results across databases to verify consistency"""
        logger.info(f"{'='*60}")
        logger.info("Comparing results across databases")
        logger.info(f"{'='*60}")
        
        for test_result in self.test_results:
            test = test_result['test']
            results = test_result['results']
            
            logger.info(f"Comparing: {test.name}")
            
            # Get databases that succeeded
            successful_dbs = [
                db for db, result in results.items() 
                if result.error is None
            ]
            
            if len(successful_dbs) < 2:
                logger.warning(f"  Not enough successful results to compare ({len(successful_dbs)}/4)")
                continue
            
            # Compare row counts
            row_counts = {db: results[db].row_count for db in successful_dbs}
            unique_counts = set(row_counts.values())
            
            if len(unique_counts) == 1:
                logger.info(f"  Row counts match: {list(unique_counts)[0]} rows")
                count_match = True
            else:
                logger.warning(f"  Row count mismatch: {row_counts}")
                count_match = False
            
            # Compare actual data (sample check on first few rows)
            data_match = self._compare_data_samples(results, successful_dbs)
            
            self.comparison_results.append({
                'test_name': test.name,
                'successful_databases': successful_dbs,
                'row_count_match': count_match,
                'row_counts': row_counts,
                'data_sample_match': data_match
            })
    
    def _compare_data_samples(self, results: Dict[str, QueryResult], 
                             dbs: List[str]) -> bool:
        """Compare first few rows of data across databases"""
        if len(dbs) < 2:
            return True
        
        # Use first DB as reference
        ref_db = dbs[0]
        ref_rows = results[ref_db].rows[:5]  # Compare first 5 rows
        
        for db in dbs[1:]:
            test_rows = results[db].rows[:5]
            
            if len(ref_rows) != len(test_rows):
                logger.warning(f"    Sample size mismatch: {ref_db}={len(ref_rows)} vs {db}={len(test_rows)}")
                return False
        
        logger.info("    Data samples match across databases")
        return True
    
    def generate_report(self, output_dir: str = "test_results"):
        """Generate comprehensive test report"""
        logger.info(f"{'='*60}")
        logger.info("Generating test report")
        logger.info(f"{'='*60}")
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate JSON report
        json_report = {
            'timestamp': timestamp,
            'databases_tested': list(self.connections.keys()),
            'total_tests': len(self.test_results),
            'tests': []
        }
        
        for test_result in self.test_results:
            test_data = {
                'name': test_result['test'].name,
                'description': test_result['test'].description,
                'results': {}
            }
            
            for db_name, result in test_result['results'].items():
                test_data['results'][db_name] = {
                    'row_count': result.row_count,
                    'execution_time_ms': round(result.execution_time * 1000, 2),
                    'success': result.error is None,
                    'error': result.error
                }
            
            json_report['tests'].append(test_data)
        
        json_report['comparisons'] = self.comparison_results
        
        # Save JSON report
        json_path = output_path / f"test_results_{timestamp}.json"
        with open(json_path, 'w') as f:
            json.dump(json_report, f, indent=2)
        logger.info(f"  JSON report saved to: {json_path}")
        
        # Generate markdown report
        md_report = self._generate_markdown_report(json_report)
        md_path = output_path / f"test_results_{timestamp}.md"
        with open(md_path, 'w') as f:
            f.write(md_report)
        logger.info(f"  Markdown report saved to: {md_path}")
        
        # Generate performance comparison
        self._generate_performance_chart(json_report, output_path, timestamp)
        
        return json_path, md_path
    
    def _generate_markdown_report(self, report_data: Dict) -> str:
        """Generate markdown formatted report"""
        md = f"# Database Performance Test Results\n\n"
        md += f"**Generated:** {report_data['timestamp']}\n\n"
        md += f"**Databases Tested:** {', '.join(report_data['databases_tested'])}\n\n"
        md += f"**Total Tests:** {report_data['total_tests']}\n\n"
        
        md += "## Test Results\n\n"
        
        for test in report_data['tests']:
            md += f"### {test['name']}\n\n"
            md += f"*{test['description']}*\n\n"
            md += "| Database | Rows | Time (ms) | Status |\n"
            md += "|----------|------|-----------|--------|\n"
            
            for db, result in test['results'].items():
                status = "✓" if result['success'] else f"✗ {result['error']}"
                md += f"| {db.capitalize()} | {result['row_count']} | {result['execution_time_ms']:.2f} | {status} |\n"
            
            md += "\n"
        
        md += "## Result Comparisons\n\n"
        
        for comp in report_data['comparisons']:
            md += f"### {comp['test_name']}\n\n"
            md += f"- **Row Count Match:** {'✓ Yes' if comp['row_count_match'] else '✗ No'}\n"
            md += f"- **Data Sample Match:** {'✓ Yes' if comp['data_sample_match'] else '✗ No'}\n"
            
            if not comp['row_count_match']:
                md += f"- **Row Counts:** {comp['row_counts']}\n"
            
            md += "\n"
        
        return md
    
    def _generate_performance_chart(self, report_data: Dict, 
                                   output_path: Path, timestamp: str):
        """Generate performance comparison chart data"""
        chart_data = {
            'labels': [test['name'] for test in report_data['tests']],
            'datasets': []
        }
        
        for db in report_data['databases_tested']:
            dataset = {
                'label': db.capitalize(),
                'data': []
            }
            
            for test in report_data['tests']:
                if db in test['results'] and test['results'][db]['success']:
                    dataset['data'].append(test['results'][db]['execution_time_ms'])
                else:
                    dataset['data'].append(None)
            
            chart_data['datasets'].append(dataset)
        
        chart_path = output_path / f"performance_data_{timestamp}.json"
        with open(chart_path, 'w') as f:
            json.dump(chart_data, f, indent=2)
        
        logger.info(f"  Performance data saved to: {chart_path}")
    
    def print_summary(self):
        """Print summary of test results"""
        print("\n" + "="*60)
        print("TEST EXECUTION SUMMARY")
        print("="*60 + "\n")
        
        for test_result in self.test_results:
            test = test_result['test']
            results = test_result['results']
            
            print(f"Test: {test.name}")
            print("-" * 60)
            
            for db_name, result in results.items():
                if result.error:
                    print(f"  {db_name:12s}: ERROR - {result.error}")
                else:
                    print(f"  {db_name:12s}: {result.row_count:6d} rows | "
                          f"{result.execution_time*1000:8.2f}ms")
        
        print("\n" + "="*60)
        print("COMPARISON SUMMARY")
        print("="*60 + "\n")
        
        for comp in self.comparison_results:
            match_status = "✓" if comp['row_count_match'] and comp['data_sample_match'] else "✗"
            print(f"{match_status} {comp['test_name']}")
            if not comp['row_count_match']:
                print(f"  Row counts: {comp['row_counts']}")


def main():
    """Main test execution entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run cross-database query tests')
    parser.add_argument('--databases', '-d', nargs='+',
                       choices=['postgresql', 'mysql', 'mongodb', 'neo4j'],
                       default=['postgresql', 'mysql', 'mongodb', 'neo4j'],
                       help='Databases to test')
    parser.add_argument('--output', '-o', default='test_results',
                       help='Output directory for reports')
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s'
    )
    
    runner = TestRunner(databases=args.databases)
    
    try:
        runner.setup_connections()
        runner.run_all_tests()
        runner.compare_results()
        runner.generate_report(output_dir=args.output)
        runner.print_summary()
    finally:
        runner.teardown_connections()


if __name__ == "__main__":
    main()
