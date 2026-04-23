# tests/test_extract.py
import pandas as pd
import pytest
import sys
sys.path.insert(0, 'ingestion')
from extract import validate, TABLES
 
def make_orders(**overrides):
    """Create a minimal orders dataframe for testing."""
    data = {
        'order_id': ['abc123', 'def456'],
        'customer_id': ['c1', 'c2'],
        'order_status': ['delivered', 'shipped'],
        'order_purchase_timestamp': ['2021-01-01', '2021-01-02'],
    }
    data.update(overrides)
    return pd.DataFrame(data)
 
 
def test_validate_passes_good_data():
    """Clean data with no issues should pass validation."""
    df = make_orders()
    cfg = TABLES['raw_orders']
    result = validate(df.copy(), 'raw_orders', cfg)
    assert len(result) == 2
    assert '_ingested_at' in result.columns
    assert '_source_file' in result.columns
 
 
def test_validate_raises_on_missing_columns():
    """Missing required columns must raise ValueError."""
    df = pd.DataFrame({'order_id': ['a']})  # missing required cols
    cfg = TABLES['raw_orders']
    with pytest.raises(ValueError, match='Missing columns'):
        validate(df, 'raw_orders', cfg)
 
 
def test_validate_removes_duplicate_primary_keys():
    """Duplicate order_ids should be deduplicated."""
    df = make_orders(
        order_id=['same_id', 'same_id'],
        customer_id=['c1', 'c2'],
    )
    cfg = TABLES['raw_orders']
    result = validate(df.copy(), 'raw_orders', cfg)
    assert len(result) == 1
    assert result['order_id'].iloc[0] == 'same_id'
 
 
def test_validate_drops_empty_rows():
    """Fully empty rows should be dropped."""
    df = make_orders()
    empty_row = pd.DataFrame({c: [None] for c in df.columns})
    df = pd.concat([df, empty_row], ignore_index=True)
    cfg = TABLES['raw_orders']
    result = validate(df.copy(), 'raw_orders', cfg)
    assert len(result) == 2  # empty row removed
 
 
def test_all_tables_defined():
    """All 9 expected tables should be in the TABLES config."""
    expected = {
        'raw_orders', 'raw_order_items', 'raw_customers',
        'raw_products', 'raw_order_payments', 'raw_order_reviews',
        'raw_sellers', 'raw_geolocation', 'raw_category_translation',
    }
    assert set(TABLES.keys()) == expected
 
 
def test_all_tables_have_required_fields():
    """Every table config must define file, required, dates, pk."""
    for name, cfg in TABLES.items():
        assert 'file' in cfg,     f'{name} missing file'
        assert 'required' in cfg, f'{name} missing required'
        assert 'dates' in cfg,    f'{name} missing dates'
        assert 'pk' in cfg,       f'{name} missing pk'
