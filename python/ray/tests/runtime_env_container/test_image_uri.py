import asyncio
import logging
from ray._private.runtime_env.image_uri import _apply_output_filter


def test_apply_output_filter_with_grep_v_head():    
    logger = logging.getLogger(__name__)
    worker_path = """time=1234567890
                    /path/to/worker.py
                    time=1234567891
                    /other/path/file.py
                    time=1234567892
                    /final/path/worker.py"""
    
    result = asyncio.run(_apply_output_filter(logger, worker_path, "grep -v '^time=' | head -1")) 
    assert result.strip() == "/path/to/worker.py"
