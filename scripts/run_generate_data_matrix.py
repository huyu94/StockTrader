import sys
import os
import dotenv
from loguru import logger

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

dotenv.load_dotenv()
from config import setup_logger
setup_logger()

from src.common.data_matrix_manager import DataMatrixManager

def main():
    manager = DataMatrixManager()
    
    # Generate matrix for last 5 years as an example, or all
    # Start date 20100101 covers most reasonable backtest periods
    start_date = "20100101"
    
    logger.info(f"Start generating data matrix from {start_date}...")
    df = manager.generate_matrix(start_date=start_date)
    
    if not df.empty:
        logger.info("Matrix generation complete.")
        logger.info(f"Shape: {df.shape}")
        
        # Simple stats
        total_cells = df.size
        filled_cells = df.sum().sum()
        coverage = (filled_cells / total_cells) * 100 if total_cells > 0 else 0
        logger.info(f"Data Coverage: {coverage:.2f}%")
        
        # Check first few rows/cols
        print(df.iloc[:5, :5])
    else:
        logger.warning("Generated matrix is empty.")

if __name__ == "__main__":
    main()
