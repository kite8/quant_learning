# Type:        module
# String form: <module 'WindAlpha' from '/opt/conda/lib/python3.5/site-packages/WindAlpha/__init__.py'>
# File:        /opt/conda/lib/python3.5/site-packages/WindAlpha/__init__.py
# Source:     
from .analysis import (prepare_raw_data, process_raw_data, add_group, get_ic_decay, get_ic_series, signal_decay_and_reversal, auto_correlation)
from .analysis import (return_analysis, turnover_analysis, ic_analysis, sector_analysis, code_analysis, score_indicators, regress_indicators)
from .util import (extreme_process, scale_process)
from .model import AlphaModel

__version__ = '1.0'