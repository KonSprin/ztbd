"""
Main script to run cross-database tests
"""
import sys
import os
import logging
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from ztbd.tests.test_runner import TestRunner


def setup_logging():
    """Configure logging for test execution"""
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(log_dir, f'{timestamp}-tests.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Run cross-database query tests',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tests on all databases
  python tests.py
  
  # Run tests only on PostgreSQL and MySQL
  python tests.py -d postgresql mysql
  
  # Test with larger result sets (LIMIT 500)
  python tests.py --limit 500
  
  # Run each test 10 times for statistical analysis
  python tests.py --repeats 10
  
  # Comprehensive performance test
  python tests.py --limit 1000 --repeats 5 -o perf_results
  
  # Run with verbose logging
  python tests.py --verbose
        """
    )
    
    parser.add_argument(
        '--databases', '-d', 
        nargs='+',
        choices=['postgresql', 'mysql', 'mongodb', 'neo4j', 'all'],
        default=['all'],
        help='Databases to test (default: all)'
    )
    
    parser.add_argument(
        '--output', '-o', 
        default='test_results',
        help='Output directory for reports (default: test_results)'
    )
    
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=100,
        help='Result set size limit for queries (default: 100)'
    )
    
    parser.add_argument(
        '--repeats', '-r',
        type=int,
        default=1,
        help='Number of times to run each test (default: 1)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--no-comparison',
        action='store_true',
        help='Skip result comparison step'
    )
    
    parser.add_argument(
        '--csv-only',
        action='store_true',
        help='Only generate CSV output (skip JSON/Markdown)'
    )
    
    args = parser.parse_args()
    
    # Handle 'all' databases
    if 'all' in args.databases:
        args.databases = ['postgresql', 'mysql', 'mongodb', 'neo4j']
    
    # Setup logging
    setup_logging()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger = logging.getLogger(__name__)
    
    logger.info("="*70)
    logger.info("CROSS-DATABASE QUERY TEST SUITE")
    logger.info("="*70)
    logger.info(f"Target databases: {', '.join(args.databases)}")
    logger.info(f"Result limit: {args.limit}")
    logger.info(f"Repeats per test: {args.repeats}")
    logger.info(f"Output directory: {args.output}")
    logger.info("="*70)
    
    # Validate inputs
    if args.limit < 1:
        logger.error("Limit must be at least 1")
        return 1
    
    if args.repeats < 1:
        logger.error("Repeats must be at least 1")
        return 1
    
    # Create test runner
    runner = TestRunner(
        databases=args.databases,
        limit=args.limit,
        repeats=args.repeats
    )
    
    try:
        # Setup connections
        logger.info("[1/4] Setting up database connections...")
        runner.setup_connections()
        
        if not runner.connections:
            logger.error("No database connections established. Exiting.")
            return 1
        
        # Run tests
        logger.info("[2/4] Running tests...")
        runner.run_all_tests()
        
        # Compare results
        if not args.no_comparison and args.repeats == 1:
            logger.info("[3/4] Comparing results...")
            runner.compare_results()
        elif args.repeats > 1:
            logger.info("[3/4] Skipping comparison (multiple runs)...")
        else:
            logger.info("[3/4] Skipping result comparison...")
        
        # Generate reports
        logger.info("[4/4] Generating reports...")
        paths = runner.generate_report(
            output_dir=args.output,
            csv_only=args.csv_only
        )
        
        # Print summary
        runner.print_summary()
        
        logger.info("="*70)
        logger.info("TEST EXECUTION COMPLETED")
        logger.info("="*70)
        logger.info(f"Raw data CSV: {paths.get('csv')}")
        if args.repeats > 1:
            logger.info(f"Statistics CSV: {paths.get('stats_csv')}")
        if not args.csv_only:
            logger.info(f"JSON report: {paths.get('json')}")
            logger.info(f"Markdown report: {paths.get('md')}")
        logger.info("="*70)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("Test execution interrupted by user")
        return 130
        
    except Exception as e:
        logger.error(f"Critical error during test execution: {e}", exc_info=True)
        return 1
        
    finally:
        logger.info("Cleaning up connections...")
        runner.teardown_connections()


if __name__ == "__main__":
    sys.exit(main())
