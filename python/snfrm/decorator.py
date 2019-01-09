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
MIN_AGE = 65

LOGGER = logging.getLogger(__name__)

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================
def _relevant_claims(
        outclaims: DataFrame,
        member: DataFrame,
        reference: DataFrame
    ) -> DataFrame:
    
    ip_claims = outclaims.where(
        spark_funcs.col('prm_line').startswith('I')
    )
    
    relevant_claims = ip_claims.join(
        member,
        on='member_id',
        how='left_outer',
    ).join(
        reference,
        on=(ip_claims.icddiag1 == reference.code),
        how='left_outer',
    ).where(
        spark_funcs.floor(
            spark_funcs.datediff(
                spark_funcs.col('prm_todate_case'),
                spark_funcs.col('dob')
            )
        ) / 365.25 >= MIN_AGE
    ).select(
        'member_id',
        'caseadmitid',
        'dischargestatus',
        'prm_fromdate_case',
        'prm_todate_case',
        'prm_acute_transfer_to_snf_yn',
        'prm_plannedadmit_thrusnf_elig_yn',
        'prm_plannedadmit_thrusnf_yn',
        'prm_readmit_potential_yn',
        'prm_readmit_all_cause_yn',
        'prm_readmit_all_cause_caseid',
        'prm_admits',
        spark_funcs.when(
            spark_funcs.col('code').isNull(),
            spark_funcs.lit('N'),
        ).otherwise(
            spark_funcs.lit('Y')
        ).alias('ip_denom_exclusion_yn'),
        spark_funcs.when(
            spark_funcs.col('prm_line').isin(['I11b', 'I11c']),
            spark_funcs.lit('IP_Rehab'),
        ).when(
            spark_funcs.col('prm_line') == 'I31',
            spark_funcs.lit('SNF')
        ).otherwise(
            spark_funcs.lit('IP_Acute')
        ).alias('stay_type')
    )
    
    relevant_window = Window().partitionBy(
        'member_id',
        'prm_fromdate_case',
        'prm_todate_case',
    ).orderBy(
        'prm_todate_case',
    )
    
    relevant_claims_oneline = relevant_claims.withColumn(
        'row',
        spark_funcs.row_number().over(relevant_window)
    ).where(
        spark_funcs.col('row') == 1
    ).drop(
        'row'
    )
    
    return relevant_claims_oneline

def _readmit_merge(
        src: DataFrame,
        readmit: DataFrame
    ) -> DataFrame:
    
    readmits = readmit.withColumnRenamed(
        'prm_plannedadmit_thrusnf_elig_yn',
        'planned_readmit_thrusnf_elig_yn',
    ).withColumnRenamed(
        'prm_plannedadmit_thrusnf_yn',
        'plannedadmit_thrusnf_yn',
    )
    readmit_merge = src.join(
        readmits,
        on=(src.member_id == readmit.member_id)
        & (src.prm_readmit_all_cause_caseid == readmit.caseadmitid),
        how='left_outer',
    ).select(
        *[src.columns],
        src.prm_todate_case.alias('risk_window_start'),
        spark_funcs.date_add(
            src.prm_todate_case,
            30
        ).alias('risk_window_end'),
        'planned_readmit_thrusnf_elig_yn',
        'plannedadmit_thrusnf_yn'
    )
        
    return readmit_merge

def calculate_snfrm_decorator(
        dfs_input: "typing.Mapping[str, DataFrame]",
        **kwargs
    ) -> DataFrame:
    """Calculate numerator and denominator for SNF readmission rate quality measure"""
    LOGGER.info('Calculating SNFRM decorator')
   
    relevant_claims = _relevant_claims(
        dfs_input['outclaims'],
        dfs_input['member'],
        dfs_input['reference'],
    )
    
    readmit_merge = _readmit_merge(
        relevant_claims,
        relevant_claims
    )
    
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
