"""
CODE OWNERS: Umang Gupta, Pierre Cornell

OBJECTIVE:
    Maintain the logic for snf all cause readmission logic

DEVELOPER NOTES:
    Will share some tooling with analytics-pipeline library
    Logic found here:
    https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/sharedsavingsprogram/Downloads/2018-reporting-year-narrative-specifications.pdf
"""
# pylint: disable=no-member
import logging
from pyspark.sql import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as spark_funcs

from prm.decorators.base_classes import ClaimDecorator

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================

def calculate_snfrm_decorator(
        dfs_input: "typing.Mapping[str, DataFrame]",
        **kwargs
    ) -> DataFrame:
    """Calculate numerator and denominator for SNF readmission rate quality measure"""
    LOGGER.info('Calculating SNFRM decorator')

    claims_flagged = dfs_input['outclaims']
    
    return claims_flagged

class SNFRMDecorator(ClaimDecorator):
    """Calculate the preference-sensitive surgery decorators"""
    @staticmethod
    def validate_decor_column_name(name: str) -> bool:
        """Defines what naming convention the decorator columns should follow"""
        return name.startswith("snfrm_")

    def _calc_decorator(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
            **kwargs
        ) -> DataFrame:

        return calculate_snfrm_decorator(
            dfs_input,
            **kwargs,
        )
