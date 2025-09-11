"""Common utility functions for MECSR scraper."""

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from decimal import Decimal


def get_iso_timestamp() -> str:
    """Get current timestamp in ISO format."""
    return datetime.now().isoformat()


def ensure_directory(path: str) -> Path:
    """Ensure directory exists, create if needed."""
    dir_path = Path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def format_percentage(value: float, decimals: int = 1) -> str:
    """Format float as percentage string."""
    return f"{value:.{decimals}f}%"


def create_success_result(data: Dict[str, Any], url: str) -> Dict[str, Any]:
    """Create standardized success result."""
    return {
        'url': url,
        'success': True,
        'data': data,
        'scraped_at': get_iso_timestamp()
    }


def create_error_result(error: str, url: str) -> Dict[str, Any]:
    """Create standardized error result."""
    return {
        'url': url,
        'success': False,
        'error': error,
        'scraped_at': get_iso_timestamp()
    }


def calculate_success_rate(successes: int, total: int) -> float:
    """Calculate success rate as percentage."""
    if total == 0:
        return 0.0
    return (successes / total) * 100


def format_throughput(throughput: float) -> str:
    """Format throughput value."""
    return f"{throughput:.2f} req/sec"


def safe_get(data: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Safely get value from dictionary."""
    return data.get(key, default)


def clean_text(text: Optional[str]) -> str:
    """Clean and normalize text."""
    if not text:
        return ""
    return text.strip()


def parse_number(text: Optional[str]) -> Optional[Decimal]:
    """Parse number from text, handling various formats."""
    if not text:
        return None

    # Remove commas and other common separators
    cleaned = text.replace(',', '').replace(' ', '').strip()

    try:
        return Decimal(cleaned)
    except:
        return None


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split list into chunks of specified size."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def calculate_eta(current: int, total: int, elapsed_seconds: float) -> str:
    """Calculate estimated time remaining."""
    if current == 0 or elapsed_seconds == 0:
        return "calculating..."

    rate = current / elapsed_seconds
    remaining = total - current
    eta_seconds = remaining / rate if rate > 0 else 0

    if eta_seconds < 60:
        return f"{int(eta_seconds)}s"
    elif eta_seconds < 3600:
        return f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
    else:
        hours = int(eta_seconds // 3600)
        minutes = int((eta_seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
