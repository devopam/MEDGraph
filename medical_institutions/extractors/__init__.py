# Registry for extractors (add new countries here)
from .base import BaseExtractor

extractor_registry = {}

def register_extractor(country_code, extractor_class):
    extractor_registry[country_code.upper()] = extractor_class

from .usa import USAExtractor
register_extractor('USA', USAExtractor)

from .can import CANExtractor
register_extractor('CAN', CANExtractor)

from .chn import CHNExtractor
register_extractor('CHN', CHNExtractor)

from .ind import INDExtractor
register_extractor('IND', INDExtractor)
