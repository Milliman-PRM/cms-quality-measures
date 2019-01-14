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
        spark_funcs.desc('prm_admits'),
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
        relevant_claims: DataFrame,
    ) -> DataFrame:
    
    readmits = relevant_claims.select(
        'member_id',
        spark_funcs.col('caseadmitid').alias('prm_readmit_all_cause_caseid'),
        spark_funcs.col('prm_plannedadmit_thrusnf_elig_yn').alias('planned_readmit_thrusnf_elig_yn'),
        spark_funcs.col('prm_plannedadmit_thrusnf_yn').alias('plannedadmit_thrusnf_yn'),
    )
    
    readmit_merge = relevant_claims.join(
        readmits,
        on=['member_id', 'prm_readmit_all_cause_caseid'],
        how='left_outer',
    ).select(
        '*',
        spark_funcs.col('prm_todate_case').alias('risk_window_start'),
        spark_funcs.date_add(
            spark_funcs.col('prm_todate_case'),
            30
        ).alias('risk_window_end'),
    ).where(
        (spark_funcs.col('stay_type') == 'IP_Acute')
        & (spark_funcs.col('prm_acute_transfer_to_snf_yn') == 'Y')
    )
        
    return readmit_merge

def _snf_irf_flags(
        relevant_claims: DataFrame,
        readmit_merge: DataFrame,
    ) -> DataFrame:
    
    snf_claims = relevant_claims.where(
        spark_funcs.col('stay_type') == 'SNF'
    ).withColumn(
        'snf_exclusion',
        spark_funcs.when(
            spark_funcs.col('dischargestatus').isin(['07', '20']),
            spark_funcs.lit(1),
        ).otherwise(
            spark_funcs.lit(0)
        )
    ).groupBy(
        'member_id',
        spark_funcs.col('prm_fromdate_case').alias('snf_fromdate'),
    ).agg(
        spark_funcs.max('snf_exclusion').alias('snf_exclusion')
    )
        
    rehab_claims = relevant_claims.where(
        spark_funcs.col('stay_type') == 'IP_Rehab'
    ).select(
        'member_id',
        spark_funcs.col('prm_fromdate_case').alias('rehab_fromdate'),
    ).distinct()
    
    snf_irf_flags = readmit_merge.join(
        snf_claims,
        on='member_id',
        how='left_outer',
    ).join(
        rehab_claims,
        on='member_id',
        how='left_outer',
    ).select(
        'member_id',
        'prm_readmit_all_cause_caseid',
        'caseadmitid',
        'dischargestatus',
        'prm_fromdate_case',
        'prm_todate_case',
        'prm_acute_transfer_to_snf_yn',
        'prm_plannedadmit_thrusnf_elig_yn',
        'prm_plannedadmit_thrusnf_yn',
        'prm_readmit_potential_yn',
        'prm_readmit_all_cause_yn',
        'prm_admits',
        'ip_denom_exclusion_yn',
        'stay_type',
        'planned_readmit_thrusnf_elig_yn',
        'plannedadmit_thrusnf_yn',
        'risk_window_start',
        'risk_window_end',
        spark_funcs.when(
            spark_funcs.col('snf_fromdate').between(
                spark_funcs.col('risk_window_start'),
                spark_funcs.col('risk_window_end'),
            ),
            spark_funcs.lit(1),
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('count_snf_in_risk_window'),
        spark_funcs.when(
            spark_funcs.col('rehab_fromdate').between(
                spark_funcs.col('risk_window_start'),
                spark_funcs.col('risk_window_end'),
            ),
            spark_funcs.lit(1),
        ).otherwise(
            spark_funcs.lit(0)
        ).alias('count_rehab_in_risk_window'),
        'snf_exclusion',
    ).groupBy(
        'member_id',
        'prm_readmit_all_cause_caseid',
        'caseadmitid',
        'dischargestatus',
        'prm_fromdate_case',
        'prm_todate_case',
        'prm_acute_transfer_to_snf_yn',
        'prm_plannedadmit_thrusnf_elig_yn',
        'prm_plannedadmit_thrusnf_yn',
        'prm_readmit_potential_yn',
        'prm_readmit_all_cause_yn',
        'prm_admits',
        'ip_denom_exclusion_yn',
        'stay_type',
        'planned_readmit_thrusnf_elig_yn',
        'plannedadmit_thrusnf_yn',
        'risk_window_start',
        'risk_window_end',
    ).agg(
        spark_funcs.sum('count_snf_in_risk_window').alias('count_snf_in_risk_window'),
        spark_funcs.sum('count_rehab_in_risk_window').alias('count_rehab_in_risk_window'),
        spark_funcs.sum('snf_exclusion').alias('snf_exclusion'),
    )
    
    return snf_irf_flags

def _calc_mem_elig(
        readmit_merge: DataFrame,
        member_months: DataFrame
    ) -> DataFrame:
    
    elig_months = member_months.where(
        spark_funcs.col('cover_medical') == 'Y'
    ).select(
        'member_id',
        'elig_month',
    ).distinct()
    
    discharges = readmit_merge.select(
        'member_id',
        'caseadmitid',
        'prm_fromdate_case',
        'prm_todate_case',
        spark_funcs.add_months(
            spark_funcs.col('prm_todate_case'),
            -12
        ).alias('target_elig_start'),
        spark_funcs.last_day(
            spark_funcs.date_add(
                spark_funcs.col('prm_todate_case'),
                30
            )
        ).alias('fromdate_window_end')
    ).withColumn(
        'fromdate_window_beg',
        spark_funcs.concat_ws(
            '-',
            spark_funcs.year('target_elig_start'),
            spark_funcs.lpad(spark_funcs.month('target_elig_start'), 2, '0'),
            spark_funcs.lit('01')
        ).cast('date')
    ).drop(
        'target_elig_start',
    )
        
    discharges_w_mem = discharges.join(
        elig_months,
        on=(discharges.member_id == elig_months.member_id)
        & (elig_months.elig_month.between(
            discharges.fromdate_window_beg,
            discharges.fromdate_window_end
        )),
        how='left_outer'
    ).groupBy(
        discharges.member_id,
        'caseadmitid',
        'prm_fromdate_case',
        'prm_todate_case',
    ).agg(
        spark_funcs.count('elig_month').alias('enrollment_count')
    ).where(
        spark_funcs.col('enrollment_count') >= 13
    )
        
    return discharges_w_mem

def _calc_measure_elig(
        claims_flagged: DataFrame,
        membership_elig: DataFrame,
    ) -> DataFrame:
    
    measure_eligible = claims_flagged.join(
        membership_elig,
        on=['member_id', 'caseadmitid', 'prm_fromdate_case', 'prm_todate_case'],
        how='left_outer',
    ).select(
        'member_id',
        'caseadmitid',
        'prm_fromdate_case',
        'prm_todate_case',
        spark_funcs.when(
            (spark_funcs.col('prm_readmit_potential_yn') == 'Y') &
            (spark_funcs.col('ip_denom_exclusion_yn') == 'N') &
            (spark_funcs.col('count_snf_in_risk_window') <= 1) &
            (spark_funcs.col('count_rehab_in_risk_window') == 0) &
            (spark_funcs.col('snf_exclusion') == 0) &
            (~spark_funcs.col('enrollment_count').isNull()),
            spark_funcs.lit('Y')
        ).otherwise(
            spark_funcs.lit('N')
        ).alias('snfrm_denom_yn'),
        spark_funcs.when(
            (spark_funcs.col('prm_readmit_potential_yn') == 'Y') &
            (spark_funcs.col('ip_denom_exclusion_yn') == 'N') &
            (spark_funcs.col('count_snf_in_risk_window') <= 1) &
            (spark_funcs.col('count_rehab_in_risk_window') == 0) &
            (spark_funcs.col('snf_exclusion') == 0) &
            (~spark_funcs.col('enrollment_count').isNull()) &
            (spark_funcs.col('prm_readmit_all_cause_yn') == 'Y') &
            (spark_funcs.col('planned_readmit_thrusnf_elig_yn') == 'Y') &
            (spark_funcs.col('plannedadmit_thrusnf_yn') == 'N'),
            spark_funcs.lit('Y')
        ).otherwise(
            spark_funcs.lit('N')
        ).alias('snfrm_numer_yn'),
    )
        
    return measure_eligible

def _flag_all_claims(
        outclaims: DataFrame,
        all_eligible: DataFrame
    ) -> DataFrame:
    
    snf_cases = outclaims.where(
        spark_funcs.col('prm_line') == 'I31'
    ).select(
        'member_id',
        spark_funcs.col('caseadmitid').alias('snf_caseadmitid'),
        spark_funcs.col('prm_fromdate_case').alias('snf_fromdate_case'),
        spark_funcs.col('prm_todate_case').alias('snf_todate_case'),
    ).distinct()
    
    snf_decorated = all_eligible.join(
        snf_cases,
        on='member_id',
        how='left_outer',
    ).where(
        spark_funcs.col('snf_fromdate_case').between(
            spark_funcs.col('prm_todate_case'),
            spark_funcs.date_add(
                'prm_todate_case',
                1
            )
        )
    ).select(
        spark_funcs.col('member_id'),
        spark_funcs.col('snf_caseadmitid').alias('caseadmitid'),
        spark_funcs.col('snfrm_numer_yn'),
        spark_funcs.col('snfrm_denom_yn'),
    )
    
    claims_decorated = outclaims.join(
        snf_decorated,
        on=['member_id', 'caseadmitid'],
        how='left_outer',
    ).select(
        'sequencenumber',
        spark_funcs.when(
            spark_funcs.col('snfrm_numer_yn').isNull(),
            spark_funcs.lit('N')
        ).otherwise(
            spark_funcs.col('snfrm_numer_yn')
        ).alias('snfrm_numer_yn'),
        spark_funcs.when(
            spark_funcs.col('snfrm_denom_yn').isNull(),
            spark_funcs.lit('N')
        ).otherwise(
            spark_funcs.col('snfrm_denom_yn')
        ).alias('snfrm_denom_yn'),
        'prm_admits',
    )    
        
    return claims_decorated

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
    )
    
    snf_irf_flags = _snf_irf_flags(
        relevant_claims,
        readmit_merge,
    )
    
    membership_elig = _calc_mem_elig(
        readmit_merge,
        dfs_input['member_time']
    )

    all_eligible = _calc_measure_elig(
        snf_irf_flags,
        membership_elig
    )
    
    claims_decorated = _flag_all_claims(
        dfs_input['outclaims'],
        all_eligible
    )
    
    return claims_decorated

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
