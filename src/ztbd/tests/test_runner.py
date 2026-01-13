"""
Test runner for executing and comparing cross-database queries
"""
import os
import json
import csv
import logging
import statistics

from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from neo4j import GraphDatabase
from sqlalchemy import create_engine

from .test_queries import ALL_TESTS
from .base_test import QueryResult

load_dotenv()

logger = logging.getLogger('ztbd.tests')


class TestRunner:
    """Executes tests across all databases and collects results"""
    
    def __init__(self, databases: Optional[List[str]] = None, 
                 limit: int = 100, repeats: int = 1):
        self.databases = databases or ['postgresql', 'mysql', 'mongodb', 'neo4j']
        self.limit = limit
        self.repeats = repeats
        self.connections: Dict[str, Any] = {}
        self.test_results: List[Dict[str, Any]] = []
        self.comparison_results: List[Dict[str, Any]] = []
        self.raw_executions: List[Dict[str, Any]] = []  # Store every execution
    
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
        """Execute all registered tests with repeats"""
        logger.info(f"{'='*60}")
        logger.info(f"Running {len(ALL_TESTS)} tests x {self.repeats} repeats")
        logger.info(f"Result limit: {self.limit}")
        logger.info(f"{'='*60}")
        
        for test_class in ALL_TESTS:
            test = test_class(limit=self.limit)
            logger.info(f"Test: {test.name}")
            logger.info(f"Description: {test.description}")
            logger.info("-" * 60)
            
            # Run test multiple times
            all_run_results = []
            for run_num in range(self.repeats):
                if self.repeats > 1:
                    logger.info(f"  Run {run_num + 1}/{self.repeats}")
                
                results = test.run_all(self.connections)
                
                # Store raw execution data
                for db_name, result in results.items():
                    execution_record = {
                        'test_name': test.name,
                        'database': db_name,
                        'run_number': run_num + 1,
                        'execution_time_ms': result.execution_time * 1000,
                        'row_count': result.row_count,
                        'success': result.error is None,
                        'error': result.error,
                        'timestamp': result.timestamp.isoformat()
                    }
                    self.raw_executions.append(execution_record)
                    
                    if result.error:
                        logger.error(f"    {db_name}: ERROR - {result.error}")
                    else:
                        logger.info(f"    {db_name}: {result.row_count} rows in {result.execution_time*1000:.2f}ms")
                
                all_run_results.append(results)
            
            # Calculate statistics if multiple runs
            if self.repeats > 1:
                stats = self._calculate_statistics(all_run_results)
                logger.info("  Statistics (execution time in ms):")
                for db_name, db_stats in stats.items():
                    if db_stats['success_rate'] == 1.0:
                        logger.info(f"    {db_name}: mean={db_stats['mean']:.2f}, "
                                  f"std={db_stats['std']:.2f}, "
                                  f"min={db_stats['min']:.2f}, "
                                  f"max={db_stats['max']:.2f}")
                    else:
                        logger.warning(f"    {db_name}: {db_stats['failures']} failures")
            
            # Store aggregated results (use last run for actual data)
            self.test_results.append({
                'test': test,
                'results': all_run_results[-1],  # Last run
                'summary': test.get_summary(),
                'statistics': self._calculate_statistics(all_run_results) if self.repeats > 1 else None
            })
    
    def _calculate_statistics(self, all_run_results: List[Dict[str, QueryResult]]) -> Dict[str, Dict]:
        """Calculate timing statistics across multiple runs"""
        stats = {}
        
        # Group by database
        db_names = all_run_results[0].keys() if all_run_results else []
        
        for db_name in db_names:
            timings = []
            row_counts = []
            successes = 0
            failures = 0
            
            for run_results in all_run_results:
                result = run_results[db_name]
                if result.error is None:
                    timings.append(result.execution_time * 1000)  # Convert to ms
                    row_counts.append(result.row_count)
                    successes += 1
                else:
                    failures += 1
            
            if timings:
                stats[db_name] = {
                    'mean': statistics.mean(timings),
                    'median': statistics.median(timings),
                    'std': statistics.stdev(timings) if len(timings) > 1 else 0,
                    'min': min(timings),
                    'max': max(timings),
                    'successes': successes,
                    'failures': failures,
                    'success_rate': successes / len(all_run_results),
                    'avg_row_count': statistics.mean(row_counts) if row_counts else 0
                }
            else:
                logger.info(f"No successful runs for {db_name}, setting stats to None")
                stats[db_name] = {
                    'mean': None,
                    'median': None,
                    'std': None,
                    'min': None,
                    'max': None,
                    'successes': 0,
                    'failures': failures,
                    'success_rate': 0,
                    'avg_row_count': 0
                }
        
        return stats
    
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
                self.comparison_results.append({
                    'test_name': test.name,
                    'successful_databases': successful_dbs,
                    'comparison_possible': False,
                    'reason': 'Insufficient successful results'
                })
                continue
            
            # Compare row counts
            row_counts = {db: results[db].row_count for db in successful_dbs}
            unique_counts = set(row_counts.values())
            
            if len(unique_counts) == 1:
                logger.info(f"  ✓ Row counts match: {list(unique_counts)[0]} rows")
                count_match = True
            else:
                logger.warning(f"  ✗ Row count mismatch: {row_counts}")
                count_match = False
            
            # Compare actual data
            data_comparison = self._compare_data_samples(results, successful_dbs)
            
            self.comparison_results.append({
                'test_name': test.name,
                'successful_databases': successful_dbs,
                'comparison_possible': True,
                'row_count_match': count_match,
                'row_counts': row_counts,
                'data_comparison': data_comparison
            })
    
    def _compare_data_samples(self, results: Dict[str, QueryResult], 
                             dbs: List[str]) -> Dict[str, Any]:
        """Compare data across databases"""
        if len(dbs) < 2:
            return {'match': True, 'details': 'Only one database to compare'}
        
        # Get reference database (first successful one)
        ref_db = dbs[0]
        ref_rows = results[ref_db].rows
        
        comparison_details = {
            'match': True,
            'row_count_matches': True,
            'data_matches': True,
            'field_name_matches': True,
            'issues': []
        }
        
        for db in dbs[1:]:
            test_rows = results[db].rows
            
            # Compare row counts
            if len(ref_rows) != len(test_rows):
                comparison_details['match'] = False
                comparison_details['row_count_matches'] = False
                comparison_details['issues'].append(
                    f"Row count mismatch: {ref_db}={len(ref_rows)} vs {db}={len(test_rows)}"
                )
                continue
            
            # Compare field names (first row)
            if ref_rows and test_rows:
                ref_fields = set(ref_rows[0].keys())
                test_fields = set(test_rows[0].keys())
                
                if ref_fields != test_fields:
                    comparison_details['match'] = False
                    comparison_details['field_name_matches'] = False
                    missing_in_ref = test_fields - ref_fields
                    missing_in_test = ref_fields - test_fields
                    
                    if missing_in_ref:
                        comparison_details['issues'].append(
                            f"Fields in {db} but not {ref_db}: {missing_in_ref}"
                        )
                    if missing_in_test:
                        comparison_details['issues'].append(
                            f"Fields in {ref_db} but not {db}: {missing_in_test}"
                        )
            
            # Compare actual data (row by row)
            data_mismatches = []
            sample_size = min(10, len(ref_rows))  # Compare first 10 rows
            
            for i in range(sample_size):
                ref_row = ref_rows[i]
                test_row = test_rows[i]
                
                # Compare common fields
                common_fields = set(ref_row.keys()) & set(test_row.keys())
                
                for field in common_fields:
                    ref_val = ref_row[field]
                    test_val = test_row[field]
                    
                    # Handle numeric comparisons with tolerance
                    if isinstance(ref_val, (int, float)) and isinstance(test_val, (int, float)):
                        if abs(ref_val - test_val) > 0.01:  # 0.01 tolerance
                            data_mismatches.append(
                                f"Row {i}, field '{field}': {ref_db}={ref_val} vs {db}={test_val}"
                            )
                    elif ref_val != test_val:
                        data_mismatches.append(
                            f"Row {i}, field '{field}': {ref_db}={ref_val} vs {db}={test_val}"
                        )
            
            if data_mismatches:
                comparison_details['match'] = False
                comparison_details['data_matches'] = False
                comparison_details['issues'].extend(data_mismatches[:5])  # Show first 5 mismatches
                
                if len(data_mismatches) > 5:
                    comparison_details['issues'].append(
                        f"... and {len(data_mismatches) - 5} more data mismatches"
                    )
        
        # Log results
        if comparison_details['match']:
            logger.info("    ✓ Data matches across all databases")
        else:
            logger.warning("    ✗ Data inconsistencies found:")
            for issue in comparison_details['issues']:
                logger.warning(f"      - {issue}")
        
        return comparison_details
    
    def generate_report(self, output_dir: str = "test_results", 
                       csv_only: bool = False) -> Dict[str, Path]:
        """Generate comprehensive test report in multiple formats"""
        logger.info(f"{'='*60}")
        logger.info("Generating test report")
        logger.info(f"{'='*60}")
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        paths = {}
        
        # Always generate CSV (for R/Python analysis)
        csv_path = self._generate_csv_report(output_path, timestamp)
        paths['csv'] = csv_path
        logger.info(f"  Raw data CSV saved to: {csv_path}")
        
        # Generate statistics CSV if multiple runs
        if self.repeats > 1:
            stats_csv_path = self._generate_statistics_csv(output_path, timestamp)
            paths['stats_csv'] = stats_csv_path
            logger.info(f"  Statistics CSV saved to: {stats_csv_path}")
        
        # Generate JSON and Markdown reports unless csv_only
        if not csv_only:
            json_path, md_path = self._generate_legacy_reports(output_path, timestamp)
            paths['json'] = json_path
            paths['md'] = md_path
            logger.info(f"  JSON report saved to: {json_path}")
            logger.info(f"  Markdown report saved to: {md_path}")
        
        return paths
    
    def _generate_csv_report(self, output_path: Path, timestamp: str) -> Path:
        """Generate CSV with raw execution data"""
        csv_path = output_path / f"raw_results_{self.limit}_{timestamp}.csv"
        
        fieldnames = [
            'test_name', 'database', 'run_number', 
            'execution_time_ms', 'row_count', 
            'success', 'error', 'timestamp'
        ]
        
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.raw_executions)
        
        return csv_path
    
    def _generate_statistics_csv(self, output_path: Path, timestamp: str) -> Path:
        """Generate CSV with aggregated statistics"""
        stats_path = output_path / f"statistics_{self.limit}_{timestamp}.csv"
        
        fieldnames = [
            'test_name', 'database',
            'mean_ms', 'median_ms', 'std_ms', 
            'min_ms', 'max_ms',
            'successes', 'failures', 'success_rate',
            'avg_row_count'
        ]
        
        rows = []
        for test_result in self.test_results:
            if test_result['statistics']:
                test_name = test_result['test'].name
                for db_name, stats in test_result['statistics'].items():
                    rows.append({
                        'test_name': test_name,
                        'database': db_name,
                        'mean_ms': stats['mean'],
                        'median_ms': stats['median'],
                        'std_ms': stats['std'],
                        'min_ms': stats['min'],
                        'max_ms': stats['max'],
                        'successes': stats['successes'],
                        'failures': stats['failures'],
                        'success_rate': stats['success_rate'],
                        'avg_row_count': stats['avg_row_count']
                    })
        
        with open(stats_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        return stats_path
    
    def _generate_legacy_reports(self, output_path: Path, timestamp: str):
        """Generate JSON and Markdown reports (original format)"""
        # JSON report
        json_report = {
            'timestamp': timestamp,
            'databases_tested': list(self.connections.keys()),
            'total_tests': len(self.test_results),
            'limit': self.limit,
            'repeats': self.repeats,
            'tests': []
        }
        
        for test_result in self.test_results:
            test_data = {
                'name': test_result['test'].name,
                'description': test_result['test'].description,
                'results': {}
            }
            
            # Add results from last run
            for db_name, result in test_result['results'].items():
                test_data['results'][db_name] = {
                    'row_count': result.row_count,
                    'execution_time_ms': round(result.execution_time * 1000, 2),
                    'success': result.error is None,
                    'error': result.error
                }
            
            # Add statistics if available
            if test_result['statistics']:
                test_data['statistics'] = {}
                for db_name, stats in test_result['statistics'].items():
                    test_data['statistics'][db_name] = {
                        'mean_ms': round(stats['mean'], 2) if stats['mean'] else None,
                        'std_ms': round(stats['std'], 2) if stats['std'] else None,
                        'min_ms': round(stats['min'], 2) if stats['min'] else None,
                        'max_ms': round(stats['max'], 2) if stats['max'] else None,
                        'success_rate': stats['success_rate']
                    }
            
            json_report['tests'].append(test_data)
        
        json_report['comparisons'] = self.comparison_results
        
        json_path = output_path / f"test_results_{self.limit}_{timestamp}.json"
        with open(json_path, 'w') as f:
            json.dump(json_report, f, indent=2)
        
        # Markdown report
        md_report = self._generate_markdown_report(json_report)
        md_path = output_path / f"test_results_{self.limit}_{timestamp}.md"
        with open(md_path, 'w') as f:
            f.write(md_report)
        
        return json_path, md_path
    
    def _generate_markdown_report(self, report_data: Dict) -> str:
        """Generate markdown formatted report"""
        md = f"# Database Performance Test Results\n\n"
        md += f"**Generated:** {report_data['timestamp']}\n\n"
        md += f"**Databases Tested:** {', '.join(report_data['databases_tested'])}\n\n"
        md += f"**Total Tests:** {report_data['total_tests']}\n\n"
        md += f"**Result Limit:** {report_data['limit']}\n\n"
        md += f"**Repeats per Test:** {report_data['repeats']}\n\n"
        
        md += "## Test Results\n\n"
        
        for test in report_data['tests']:
            md += f"### {test['name']}\n\n"
            md += f"*{test['description']}*\n\n"
            
            if report_data['repeats'] == 1:
                md += "| Database | Rows | Time (ms) | Status |\n"
                md += "|----------|------|-----------|--------|\n"
                
                for db, result in test['results'].items():
                    status = "✓" if result['success'] else f"✗ {result['error']}"
                    md += f"| {db.capitalize()} | {result['row_count']} | {result['execution_time_ms']:.2f} | {status} |\n"
            else:
                md += "| Database | Rows | Mean (ms) | Std (ms) | Min (ms) | Max (ms) | Success Rate |\n"
                md += "|----------|------|-----------|----------|----------|----------|-------------|\n"
                
                for db, stats in test.get('statistics', {}).items():
                    if stats.get('mean_ms') is not None:
                        md += f"| {db.capitalize()} | {test['results'][db]['row_count']} | "
                        md += f"{stats['mean_ms']:.2f} | {stats['std_ms']:.2f} | "
                        md += f"{stats['min_ms']:.2f} | {stats['max_ms']:.2f} | "
                        md += f"{stats['success_rate']:.1%} |\n"
                    else:
                        md += f"| {db.capitalize()} | - | Failed | - | - | - | 0% |\n"
            
            md += "\n"
        
        if report_data.get('comparisons'):
            md += "## Result Comparisons\n\n"
            
            for comp in report_data['comparisons']:
                md += f"### {comp['test_name']}\n\n"
                md += f"- **Row Count Match:** {'✓ Yes' if comp['row_count_match'] else '✗ No'}\n"
                md += f"- **Data Sample Match:** {'✓ Yes' if comp['data_sample_match'] else '✗ No'}\n"
                
                if not comp['row_count_match']:
                    md += f"- **Row Counts:** {comp['row_counts']}\n"
                
                md += "\n"
        
        return md
    
    def print_summary(self):
        """Print summary of test results"""
        print("\n" + "="*60)
        print("TEST EXECUTION SUMMARY")
        print("="*60 + "\n")
        
        for test_result in self.test_results:
            test = test_result['test']
            results = test_result['results']
            stats = test_result.get('statistics')
            
            print(f"Test: {test.name}")
            print("-" * 60)
            
            if stats:
                # Print statistics
                for db_name, db_stats in stats.items():
                    if db_stats['mean']:
                        print(f"  {db_name:12s}: mean={db_stats['mean']:8.2f}ms "
                              f"std={db_stats['std']:6.2f}ms "
                              f"min={db_stats['min']:8.2f}ms "
                              f"max={db_stats['max']:8.2f}ms")
                    else:
                        print(f"  {db_name:12s}: FAILED ({db_stats['failures']} failures)")
            else:
                # Print single run results
                for db_name, result in results.items():
                    if result.error:
                        print(f"  {db_name:12s}: ERROR - {result.error}")
                    else:
                        print(f"  {db_name:12s}: {result.row_count:6d} rows | "
                              f"{result.execution_time*1000:8.2f}ms")
            print()
        
        if self.comparison_results:
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
    parser.add_argument('--limit', '-l', type=int, default=100,
                       help='Result set size limit')
    parser.add_argument('--repeats', '-r', type=int, default=1,
                       help='Number of times to run each test')
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s'
    )
    
    runner = TestRunner(databases=args.databases, limit=args.limit, repeats=args.repeats)
    
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
